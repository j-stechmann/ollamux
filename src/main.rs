//! ollamux — key-rotating reverse proxy for the Ollama Cloud API.
//!
//! tiny_http listener, ureq upstream client, per-key concurrency slots,
//! invisible pre-first-byte failover. No signal-handling dependencies:
//! SIGINT is awaited with `sigwait` on a dedicated thread (unix).

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
}

fn main() {
    // FIRST THING, before any thread can exist (theirs or ours): block
    // SIGINT. POSIX threads inherit the caller's mask, so every thread
    // spawned later (tiny_http's accept thread, ollamux workers, the sigwait
    // thread) starts with SIGINT blocked and the signal stays
    // process-pending for `sigwait`. Doing this after spawning left those
    // threads eligible for default delivery: instant death, no drain.
    #[cfg(unix)]
    block_sigint_before_threading();

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

    let pool = Arc::new(ollamux::Pool::new(
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
    let proxy = Arc::new(ollamux::proxy::Server::new(pool.clone()));

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
    spawn_workers(tiny.clone(), proxy.clone(), stop.clone());

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
    install_sigint(move || {
        stop_ctrl.store(true, Ordering::SeqCst);
        if verbose {
            eprintln!(
                "ollamux: shutting down (draining up to {}s; ctrl-c again to force quit)",
                DRAIN.as_secs()
            )
        }
    });

    // --- main loop: accept until stopped, then drain ---
    while !stop.load(Ordering::Relaxed) {
        match tiny.recv_timeout(Duration::from_millis(250)) {
            Ok(Some(request)) => dispatch(request, &proxy),
            Ok(None) => {}
            Err(e) => {
                if verbose {
                    eprintln!("ollamux: recv error: {e}")
                }
            }
        }
    }

    let deadline = std::time::Instant::now() + DRAIN;
    while std::time::Instant::now() < deadline {
        match tiny.try_recv() {
            Ok(Some(request)) => dispatch(request, &proxy),
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(_) => break,
        }
    }
    if verbose {
        eprintln!("ollamux: bye");
    }
}

fn dispatch(request: tiny_http::Request, proxy: &Arc<ollamux::proxy::Server>) {
    proxy.handle(request);
}

/// One blocking dispatcher thread per worker; tiny_http's recv() is shared
/// (it's internally synchronized), so N workers = N concurrent requests.
fn spawn_workers(
    tiny: Arc<tiny_http::Server>,
    proxy: Arc<ollamux::proxy::Server>,
    stop: Arc<AtomicBool>,
) {
    let workers = proxy.pool.total_slots().clamp(4, 64) as usize;
    for _ in 0..workers {
        let tiny = Arc::clone(&tiny);
        let proxy = Arc::clone(&proxy);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match tiny.recv_timeout(Duration::from_millis(250)) {
                    Ok(Some(request)) => proxy.handle(request),
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
        });
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Option<Config>, String> {
    let mut cfg = Config {
        addr: DEFAULT_ADDR.to_string(),
        keys_path: None,
        verbose: false,
        usage_aware: None,
        affinity: true,
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
    ollamux [--addr HOST:PORT] [--keys PATH] [--usage-aware[=PCT]] [--no-affinity] [-v]

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
    -v, --verbose            Verbose stderr (startup banner, per-request log,
                             key cooldowns/deaths, upstream snippets)
    -h, --help               This help
    -V, --version            Print version

POINT CLIENTS AT IT:
    export OLLAMA_HOST=http://localhost:11435    # then `ollama run gpt-oss:120b`
    base_url = \"http://localhost:11435/v1\"       # OpenAI-compatible SDKs

KEYS:
    one key per line, `#` comments allowed; `KEY:N` sets per-key
    concurrency (default 3; free=1 pro=3 max=10).
    Get keys: https://ollama.com/settings/keys

ENDPOINTS:
    /api/*, /v1/*   proxied to https://ollama.com with key rotation
                    (prompt-cache affinity by default; X-Ollamux-Affinity
                    response header: hit/miss/off)
    /_keys          per-key health JSON (embeds usage when known)
    /_usage         per-key Ollama Cloud usage JSON (?refresh=1 forces,
                    at most one fetch attempt per 5 s)
    /_health        liveness JSON",
        ollamux::VERSION
    );
}

// --- SIGINT via sigwait (no external crates, no async-signal-unsafe work) ---
//
// Correct delivery requires SIGINT blocked in every thread. `main` calls
// `block_sigint_before_threading()` before spawning; POSIX guarantees each
// `spawn`ed thread inherits that mask, so the signal can only ever become
// process-pending and is consumed exclusively by the `sigwait` below.

#[cfg(unix)]
fn block_sigint_before_threading() {
    #[repr(C)]
    struct SigSet([u64; 16]);

    unsafe extern "C" {
        fn sigemptyset(set: *mut SigSet) -> i32;
        fn sigaddset(set: *mut SigSet, signum: i32) -> i32;
        fn pthread_sigmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
    }

    const SIGINT: i32 = 2;
    const SIG_BLOCK: i32 = 0;

    unsafe {
        let mut set = SigSet([0; 16]);
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        let rc = pthread_sigmask(SIG_BLOCK, &set, std::ptr::null_mut());
        if rc != 0 {
            eprintln!("ollamux: warning: cannot block SIGINT ({rc}); ctrl-c will hard-exit");
        }
    }
}

fn install_sigint<F>(notify: F)
where
    F: Fn() + Send + 'static,
{
    std::thread::spawn(move || sigint_thread(notify));
}

#[cfg(unix)]
fn sigint_thread<F: Fn()>(notify: F) {
    #[repr(C)]
    struct SigSet([u64; 16]);
    unsafe extern "C" {
        fn sigemptyset(set: *mut SigSet) -> i32;
        fn sigaddset(set: *mut SigSet, signum: i32) -> i32;
        fn sigprocmask(how: i32, set: *const SigSet, old: *mut SigSet) -> i32;
        fn sigwait(set: *const SigSet, sig: *mut i32) -> i32;
    }

    const SIGINT: i32 = 2;
    const SIG_BLOCK: i32 = 0;

    unsafe {
        // Idempotent belt-and-braces: this thread also blocks SIGINT in
        // case it was EVER started from a non-blocked context (e.g. tests).
        let mut set = SigSet([0; 16]);
        sigemptyset(&mut set);
        sigaddset(&mut set, SIGINT);
        let mut old = SigSet([0; 16]);
        sigprocmask(SIG_BLOCK, &set, &mut old);

        let mut sig: i32 = 0;
        sigwait(&set, &mut sig);
        notify();
        // Second SIGINT: immediate exit.
        sigwait(&set, &mut sig);
        std::process::exit(130);
    }
}

#[cfg(not(unix))]
fn sigint_thread<F: Fn()>(_notify: F) {
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The env var must disable affinity on its own — a regression guard:
    /// this check once lived inside `if !cfg.affinity`, making the env var
    /// silently inert unless the flag was also passed.
    #[test]
    fn env_var_disables_affinity_without_flag() {
        // SAFETY: tests run single-threaded with respect to env mutation
        // within this test binary's `parse_args` calls; other tests in
        // this binary do not read OLLAMUX_NO_AFFINITY.
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
}
