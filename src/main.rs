//! ollamux — key-rotating reverse proxy for the Ollama Cloud API.
//!
//! tiny_http listener, ureq upstream client, per-key concurrency slots,
//! invisible pre-first-byte failover. No signal-handling dependencies:
//! SIGINT/SIGTERM are awaited with `sigwait` on a dedicated thread (unix).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DEFAULT_ADDR: &str = "127.0.0.1:11435";
/// Per-key bound on requests waiting for a free slot (hardcoded: config
/// bloat per review; effectively a 32×slots global admission bound).
const WAITER_CAP: u32 = 32;
/// Grace-shutdown window before force-exit.
const DRAIN: Duration = Duration::from_secs(5);

struct Config {
    addr: String,
    keys_path: Option<std::path::PathBuf>,
    verbose: bool,
    /// Quota-aware routing threshold in percent (None = disabled).
    usage_aware: Option<u32>,
    /// Prompt-cache affinity (default on; --no-affinity disables).
    affinity: bool,
    /// Local token statistics persistence (default on).
    stats_enabled: bool,
    /// Explicit --stats-file target (None = default state path).
    stats_path: Option<std::path::PathBuf>,
}

/// Slots are per key today, while Ollama Cloud's concurrency limit is
/// per account. Grouping several keys under one `@account` therefore
/// multiplies the in-flight bound; say so once at startup rather than
/// letting the plan limit surprise the user as upstream 429s.
fn warn_multi_key_accounts(keys: &ollamux::Keys) {
    let mut accounts: Vec<(&str, usize)> = Vec::new();
    for e in &keys.entries {
        match accounts.iter_mut().find(|(a, _)| *a == e.account) {
            Some(entry) => entry.1 += 1,
            None => accounts.push((&e.account, 1)),
        }
    }
    for (account, count) in accounts {
        if count > 1 {
            let slots: u32 = keys
                .entries
                .iter()
                .filter(|e| e.account == account)
                .map(|e| e.concurrency)
                .sum();
            eprintln!(
                "ollamux: warning: account {account:?} has {count} keys ({slots} slots); \
                 Ollama Cloud's concurrency limit is per account — use one key per account \
                 for a strict cap"
            );
        }
    }
}

fn main() {
    // FIRST THING, before any thread can exist (theirs or ours): block
    // SIGINT/SIGTERM. POSIX threads inherit the caller's mask, so every
    // thread spawned later (tiny_http's accept thread, ollamux workers, the
    // sigwait thread) starts with them blocked and the signals stay
    // process-pending for `sigwait`. Doing this after spawning left those
    // threads eligible for default delivery: instant death, no drain.
    #[cfg(unix)]
    block_termination_signals_before_threading();

    let cfg = match parse_args(std::env::args().skip(1)) {
        Ok(Some(cfg)) => cfg,
        Ok(None) => return, // --help / --version already printed
        Err(e) => {
            eprintln!("ollamux: {e}\nrun `ollamux --help` for usage");
            std::process::exit(2);
        }
    };

    let keys = match ollamux::Keys::load(cfg.keys_path.as_deref(), "") {
        Ok(k) => k,
        Err(e) => {
            eprintln!("ollamux: {e}");
            std::process::exit(1);
        }
    };
    if keys.is_empty() {
        eprintln!("ollamux: no keys configured; refusing to start");
        std::process::exit(1);
    }
    warn_multi_key_accounts(&keys);

    let pool = Arc::new(ollamux::Pool::from_entries(
        keys.entries.clone(),
        WAITER_CAP,
        cfg.verbose,
    ));
    // Quota-aware routing is opt-in: set the threshold before anything
    // serves, per the pool's write-once usage configuration.
    if let Some(pct) = cfg.usage_aware {
        pool.set_usage_threshold(pct);
    }
    // Prompt-cache affinity is on by default; --no-affinity / env disables.
    pool.set_affinity_enabled(cfg.affinity);
    // Local token/request statistics: persisted by default, resolved before
    // the server starts so the file is loaded before any request lands.
    let meter = if cfg.stats_enabled {
        let path = cfg
            .stats_path
            .clone()
            .unwrap_or_else(ollamux::Meter::default_path);
        if cfg.verbose {
            eprintln!("ollamux: local stats: {}", path.display());
        }
        let present: Vec<(String, String, String)> = (0..pool.len())
            .map(|i| {
                (
                    pool.account_of(i).to_string(),
                    ollamux::key_fingerprint(pool.secret_of(i)),
                    pool.suffix_of(i),
                )
            })
            .collect();
        Arc::new(ollamux::Meter::open_with_present(path, &present))
    } else {
        Arc::new(ollamux::Meter::disabled())
    };
    let proxy = Arc::new(ollamux::proxy::Server::with_meter(pool.clone(), meter));

    let addr = cfg.addr.clone();
    let tiny = match tiny_http::Server::http(addr.as_str()) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("ollamux: cannot bind {addr}: {e}");
            std::process::exit(1);
        }
    };

    if cfg.verbose {
        eprintln!(
            "ollamux v{}: {} key(s) loaded [{}], {} slot(s), listening on http://{addr}",
            ollamux::VERSION,
            keys.len(),
            keys.suffixes().join(", "),
            pool.total_slots(),
        );
        eprintln!("ollamux: point clients at http://{addr} (OLLAMA_HOST or base_url …/v1)");
    }

    let stop = Arc::new(AtomicBool::new(false));
    // Workers keep accepting during the drain; this second flag is set after
    // the drain loop (quiet window or DRAIN deadline), so the main thread
    // never blocks on a request.
    let worker_stop = Arc::new(AtomicBool::new(false));
    let workers = spawn_workers(tiny.clone(), proxy.clone(), worker_stop.clone());
    // Persist local counters periodically and once more on shutdown.
    proxy.meter.spawn_flusher(stop.clone());

    // Quota-aware poller: only exists when the feature is enabled.
    if cfg.usage_aware.is_some() {
        proxy.spawn_usage_poller(stop.clone());
        if cfg.verbose {
            eprintln!(
                "ollamux: quota-aware routing enabled (threshold {}%; usage refresh every {}s)",
                cfg.usage_aware.unwrap_or_default(),
                ollamux::USAGE_TTL.as_secs()
            );
        }
    }

    let verbose = cfg.verbose;
    let stop_ctrl = stop.clone();
    install_termination_handler(move || {
        stop_ctrl.store(true, Ordering::SeqCst);
        if verbose {
            eprintln!(
                "ollamux: shutting down (draining up to {}s; signal again to force quit)",
                DRAIN.as_secs()
            )
        }
    });

    // --- main loop: workers own request handling; main only watches stop ---
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(250));
    }

    // Drain: workers continue serving queued/in-flight requests until the
    // deadline. A quiet window (longer than workers' 250 ms recv timeout)
    // lets an idle shutdown finish early.
    let deadline = std::time::Instant::now() + DRAIN;
    let mut quiet_since: Option<std::time::Instant> = None;
    while std::time::Instant::now() < deadline {
        if proxy.active_requests() == 0 {
            let since = *quiet_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= Duration::from_millis(300) {
                break;
            }
        } else {
            quiet_since = None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    if verbose {
        eprintln!("ollamux: bye");
    }
    // Stop workers, then give them the rest of the DRAIN window to finish
    // (their recv timeout is 250 ms), and persist whatever completed. A
    // request still running when DRAIN expires is force-closed.
    worker_stop.store(true, Ordering::SeqCst);
    while !workers.iter().all(|w| w.is_finished()) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = proxy.meter.flush();
}

/// One blocking dispatcher thread per worker; tiny_http's recv() is shared
/// (it's internally synchronized), so N workers = N concurrent requests.
fn spawn_workers(
    tiny: Arc<tiny_http::Server>,
    proxy: Arc<ollamux::proxy::Server>,
    worker_stop: Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<()>> {
    let workers = proxy.pool.total_slots().clamp(4, 64) as usize;
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let tiny = Arc::clone(&tiny);
        let proxy = Arc::clone(&proxy);
        let worker_stop = Arc::clone(&worker_stop);
        handles.push(std::thread::spawn(move || {
            while !worker_stop.load(Ordering::SeqCst) {
                match tiny.recv_timeout(Duration::from_millis(250)) {
                    Ok(Some(request)) => proxy.handle(request),
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
        }));
    }
    handles
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Option<Config>, String> {
    let stats_env_off =
        std::env::var("OLLAMUX_NO_STATS").is_ok_and(|v| !v.is_empty() && v != "0" && v != "false");
    let mut cfg = Config {
        addr: DEFAULT_ADDR.to_string(),
        keys_path: None,
        verbose: false,
        usage_aware: None,
        affinity: true,
        stats_enabled: !stats_env_off,
        stats_path: None,
    };
    let mut it = args.peekable();
    // Splits `--opt=value` into (`--opt`, Some(`value`)); plain `--opt`
    // yields (arg, None) and its value comes from the next argv entry.
    fn split_kv(arg: &str) -> (&str, Option<&str>) {
        match arg.split_once('=') {
            Some((k, v)) if k.starts_with("--") => (k, Some(v)),
            _ => (arg, None),
        }
    }
    // A threshold string ("80" etc.) into its percent value; anything not
    // a sane percent is a usage error, never a silent default.
    fn parse_pct(s: &str) -> Result<u32, String> {
        let pct: u32 = s
            .trim()
            .trim_end_matches('%')
            .parse()
            .map_err(|_| format!("invalid --usage-aware threshold {s:?} (want 1–99)"))?;
        if pct == 0 || pct > 99 {
            return Err(format!(
                "invalid --usage-aware threshold {pct} (want 1–99: 0 would disable the feature)"
            ));
        }
        Ok(pct)
    }
    while let Some(arg) = it.next() {
        let (flag, inline) = split_kv(&arg);
        let mut value = || match inline {
            Some(v) => Ok(v.to_string()),
            None => it.next().ok_or(format!("missing value for {flag}")),
        };
        match flag {
            "-h" | "--help" => {
                print_help();
                return Ok(None);
            }
            "-V" | "--version" => {
                println!("ollamux {}", ollamux::VERSION);
                return Ok(None);
            }
            "-a" | "--addr" => cfg.addr = value()?,
            "-k" | "--keys" => {
                cfg.keys_path = Some(std::path::PathBuf::from(value()?));
            }
            "-v" | "--verbose" => cfg.verbose = true,
            "--no-affinity" => cfg.affinity = false,
            "--no-stats" => cfg.stats_enabled = false,
            "--stats-file" => {
                let path = value()?;
                if path.is_empty() {
                    return Err("--stats-file requires a non-empty path".to_string());
                }
                cfg.stats_path = Some(std::path::PathBuf::from(path));
                cfg.stats_enabled = true;
            }
            "--usage-aware" => {
                // `--usage-aware=PCT`, `--usage-aware PCT`, or bare
                // `--usage-aware` (default threshold). A following token
                // is only eaten when it looks numeric — flags stay flags.
                cfg.usage_aware = Some(match inline {
                    Some(v) => parse_pct(v)?,
                    None => match it.peek() {
                        Some(next) if next.parse::<u32>().is_ok() => {
                            let v = it.next().expect("peeked value exists");
                            parse_pct(&v)?
                        }
                        _ => DEFAULT_USAGE_AWARE,
                    },
                });
            }
            _other => {
                return Err(format!("unknown argument: {arg:?}"));
            }
        }
    }
    if cfg.usage_aware.is_none() {
        // Flag wins over env (keys precedence is the same shape).
        if let Ok(raw) = std::env::var("OLLAMUX_USAGE_AWARE") {
            if !raw.is_empty() {
                cfg.usage_aware = Some(parse_pct(&raw)?);
            }
        }
    }
    // Both --no-affinity and the env var only ever disable, so there is
    // no precedence conflict — the env check runs unconditionally.
    if std::env::var("OLLAMUX_NO_AFFINITY").is_ok_and(|v| !v.is_empty() && v != "0" && v != "false")
    {
        cfg.affinity = false;
    }
    Ok(Some(cfg))
}

/// Default demotion threshold for --usage-aware (matches ollama-bar's
/// warning level).
const DEFAULT_USAGE_AWARE: u32 = 80;

fn print_help() {
    println!(
        "ollamux {} — key-rotating proxy for the Ollama Cloud API

USAGE:
    ollamux [--addr HOST:PORT] [--keys PATH] [--usage-aware[=PCT]] [--no-affinity]
            [--stats-file PATH] [--no-stats] [-v]

OPTIONS:
    -a, --addr <HOST:PORT>   Listen address [default: {DEFAULT_ADDR}]
    -k, --keys <PATH>        Keys file (default: $OLLAMUX_KEYS, else
                             $XDG_CONFIG_HOME/ollamux/keys, else ~/.config/ollamux/keys)
    --usage-aware[=PCT]      Quota-aware key selection: keys whose session
                             usage is at/over PCT percent are served last
                             (never excluded). Default PCT: {DEFAULT_USAGE_AWARE}.
                             Also via OLLAMUX_USAGE_AWARE (flag wins).
    --no-affinity            Disable prompt-cache affinity: by default
                             requests with the same conversation prefix are
                             pinned to the API key that warmed ollama.com's
                             server-side prompt cache (faster first tokens).
                             Also via OLLAMUX_NO_AFFINITY=1 (flag wins).
    --stats-file <PATH>      Where to persist local token/request counters
                             (default: $OLLAMUX_STATS_FILE, else
                             $XDG_STATE_HOME/ollamux/stats.json, else
                             ~/.local/state/ollamux/stats.json, else
                             ./ollamux-stats.json). Written atomically
                             every 5 s and after the shutdown drain.
    --no-stats               Disable local counters and persistence
                             (also OLLAMUX_NO_STATS=1; explicit flags win).
    -v, --verbose            Verbose stderr (startup banner, per-request log,
                             key cooldowns/deaths, upstream snippets)
    -h, --help               This help
    -V, --version            Print version

POINT CLIENTS AT IT:
    export OLLAMA_HOST=http://localhost:11435    # then `ollama run gpt-oss:120b`
    base_url = \"http://localhost:11435/v1\"       # OpenAI-compatible SDKs

KEYS:
    one key per line, `#` comments allowed; format `KEY[:N]@ACCOUNT`.
    ACCOUNT is required (1-64 chars of A-Z a-z 0-9 . _ -): keys sharing
    a label belong to one ollama.com account. `:N` sets per-key
    concurrency (default 3; free=1 pro=3 max=10).
    Get keys: https://ollama.com/settings/keys

ENDPOINTS:
    /api/*, /v1/*   proxied to https://ollama.com with key rotation
                    (prompt-cache affinity by default; X-Ollamux-Affinity
                    response header: hit/miss/off)
    /_keys          per-key health JSON (account, tier, embedded usage)
    /_usage         per-account + per-key Ollama Cloud usage JSON plus a
                    pool-capacity aggregate. ?range=24h|7d|30d adds
                    per-account model/time statistics (group_by=model
                    optional); ?refresh=1 forces, at most one attempt
                    per 5 s
    /_stats         local, per-request counters observed by ollamux:
                    requests, upstream-reported tokens, duration, broken
                    down by key/account/model/endpoint and UTC hour/day.
                    ?range=24h|7d|30d (default 24h). Read-only, instant,
                    in-memory (plus the persisted file)
    /_health        liveness JSON

USAGE SOURCE:
    /_usage reads the documented balance endpoint
    (docs.ollama.com/api/balance), once per @account: legacy plans carry
    session/weekly remaining percents + reset instants; credit plans
    carry USD amounts. ?range=... also reads the documented cloud usage
    endpoint (docs.ollama.com/api/cloud-usage) for request counts,
    tokens, cost and per-model buckets. Both are polled at most once per
    60 s (upstream allows 10 req/min per user, shared across that
    account's keys; pools larger than ~10 accounts can burst over that
    limit — affected accounts report 429 and back off; forced refreshes
    bypass the TTL and are capped per account at one attempt per 5 s).",
        ollamux::VERSION
    );
}

// --- SIGINT/SIGTERM via sigwait (no crates, no async-signal-unsafe work) ---
//
// Correct delivery requires both signals blocked in every thread. `main`
// calls `block_termination_signals_before_threading()` before spawning;
// POSIX guarantees each `spawn`ed thread inherits that mask, so the
// signals can only become process-pending and are consumed exclusively by
// the `sigwait` below.

#[cfg(unix)]
fn block_termination_signals_before_threading() {
    #[repr(C)]
    struct SigSet([u64; 16]);

    unsafe extern "C" {
        fn sigemptyset(set: *mut SigSet) -> i32;
        fn sigaddset(set: *mut SigSet, signum: i32) -> i32;
        fn pthread_sigmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
    }

    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SIG_BLOCK: i32 = 0;

    unsafe {
        let mut set = SigSet([0; 16]);
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        sigaddset(&mut set, SIGTERM);
        let rc = pthread_sigmask(SIG_BLOCK, &set, std::ptr::null_mut());
        if rc != 0 {
            eprintln!(
                "ollamux: warning: cannot block SIGINT/SIGTERM ({rc}); shutdown signals will hard-exit"
            );
        }
    }
}

fn install_termination_handler<F>(notify: F)
where
    F: Fn() + Send + 'static,
{
    std::thread::spawn(move || signal_thread(notify));
}

#[cfg(unix)]
fn signal_thread<F: Fn()>(notify: F) {
    #[repr(C)]
    struct SigSet([u64; 16]);
    unsafe extern "C" {
        fn sigemptyset(set: *mut SigSet) -> i32;
        fn sigaddset(set: *mut SigSet, signum: i32) -> i32;
        fn sigprocmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
        fn sigwait(set: *const SigSet, sig: *mut i32) -> i32;
    }

    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SIG_BLOCK: i32 = 0;

    unsafe {
        // Idempotent belt-and-braces: this thread also blocks the signals
        // in case it was EVER started from a non-blocked context (tests).
        let mut set = SigSet([0; 16]);
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        sigaddset(&mut set, SIGTERM);
        let mut old = SigSet([0; 16]);
        sigprocmask(SIG_BLOCK, &set, &mut old);

        let mut sig: i32 = 0;
        sigwait(&set, &mut sig);
        notify();
        // Second termination signal: immediate exit.
        sigwait(&set, &mut sig);
        std::process::exit(130);
    }
}

#[cfg(not(unix))]
fn signal_thread<F: Fn()>(_notify: F) {
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Environment variables are process-global; serialize the two tests
    /// that mutate them even though the harness runs tests in parallel.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The env var must disable affinity on its own — a regression guard:
    /// this check once lived inside `if !cfg.affinity`, making the env var
    /// silently inert unless the flag was also passed.
    #[test]
    fn env_var_disables_affinity_without_flag() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: ENV_LOCK serializes env mutation in this test binary.
        // (Rust 2024: env access in tests is unsafe.)
        unsafe { std::env::set_var("OLLAMUX_NO_AFFINITY", "1") };
        let cfg = parse_args(std::iter::empty()).expect("args parse");
        assert!(cfg.is_some_and(|c| !c.affinity), "env alone must disable");

        // Falsy values do not disable.
        unsafe { std::env::set_var("OLLAMUX_NO_AFFINITY", "0") };
        let cfg = parse_args(std::iter::empty()).expect("args parse");
        assert!(cfg.is_some_and(|c| c.affinity), "'0' must not disable");

        unsafe { std::env::remove_var("OLLAMUX_NO_AFFINITY") };
    }

    #[test]
    fn explicit_stats_flags_win_over_the_no_stats_env() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: ENV_LOCK serializes env mutation in this test binary.
        unsafe { std::env::set_var("OLLAMUX_NO_STATS", "1") };
        let cfg = parse_args(std::iter::empty())
            .expect("args parse")
            .expect("not help/version");
        assert!(!cfg.stats_enabled, "env alone disables stats");

        let args = [
            "--stats-file".to_string(),
            "/tmp/ollamux-test.json".to_string(),
        ];
        let cfg = parse_args(args.into_iter())
            .expect("args parse")
            .expect("not help/version");
        assert!(
            cfg.stats_enabled,
            "an explicit --stats-file must win over the env"
        );

        let args = ["--stats-file".to_string(), String::new()];
        assert!(parse_args(args.into_iter()).is_err(), "empty path rejected");

        unsafe { std::env::remove_var("OLLAMUX_NO_STATS") };
    }
}
