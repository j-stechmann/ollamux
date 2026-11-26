//! Local per-request accounting: request counts, upstream-reported token
//! counts, wall time and status, bucketed by UTC hour/day and broken down
//! by key, account, model and endpoint.
//!
//! This is the *local* counterpart to `stats.rs`: it observes the traffic
//! that passes through ollamux itself, so it can attribute numbers to a
//! single key (upstream's `/api/usage` is account-scoped) and to a single
//! request. Token counts are whatever the upstream put in the response
//! body (`prompt_eval_count`/`eval_count` for Ollama's native API,
//! `usage.*` for the OpenAI-compatible surface); no input/output split is
//! synthesized (a total-only object is treated as missing).
//! A successful request whose response carried no counts is recorded as
//! `requests_without_usage` so the gap stays visible.
//!
//! Everything is aggregated: there is no per-request log and no
//! prompt/completion content. Buckets are fixed-size (25 hourly + 31
//! daily) and pruned on load and on record, so memory and the on-disk
//! file stay bounded regardless of traffic.
//!
//! Counters are persisted as JSON (`--stats-file`, default under
//! `$XDG_STATE_HOME`) with an atomic temp-file rename. Persistence is
//! best-effort: a missing file starts fresh, while unreadable, foreign,
//! oversized or newer-versioned files are reported and never prevent
//! startup (they are left untouched and persistence is disabled).

use crate::stats::Range;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// On-disk schema version. Bump only on incompatible layouts; decoding is
/// forward-tolerant for missing fields.
pub const STATS_VERSION: u32 = 2;
/// Marker that distinguishes an ollamux stats file from arbitrary JSON
/// (including a config or keys file accidentally passed via --stats-file).
pub const STATS_MARKER: &str = "ollamux-stats";
/// Cadence of the background flusher.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Hourly buckets kept (current + 24 previous).
const HOURLY_KEEP: u64 = 25;
/// Daily buckets kept (current + 30 previous).
const DAILY_KEEP: u64 = 31;
/// Distinct model names retained before folding into `<other>`.
const MODEL_CAP: usize = 256;
/// Distinct endpoint paths retained before folding into `<other>`.
/// Endpoint names are client-controlled (any `/api/*` or `/v1/*` path is
/// proxied), so this dimension needs its own hard cap.
const ENDPOINT_CAP: usize = 128;
/// Historical key rows retained (current keys plus rows from previous
/// keys-file revisions) before folding into a synthetic `<other>` row.
const KEY_CAP: usize = 512;
/// Distinct account labels retained before folding into `<other>`.
const ACCOUNT_CAP: usize = 256;
/// Refuse to load an implausibly large stats file (a bounded writer stays
/// far below this; a hand-edited/corrupt file should not OOM startup).
const MAX_STATS_FILE: u64 = 16 * 1024 * 1024;
/// Longest model/endpoint label kept verbatim.
const MAX_NAME_LEN: usize = 200;
/// Bytes of a non-streaming response retained for usage extraction. The
/// usage counters sit at the tail of the JSON, so keeping the tail is
/// enough even for large bodies (1 MiB also bounds per-request memory).
const MAX_CAPTURE: usize = 1024 * 1024;
/// Longest streaming line retained while waiting for a newline.
const MAX_LINE: usize = 64 * 1024;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Stable, non-secret identity for a pool key: the first 128 bits of
/// SHA-256 over the full secret. This is a cryptographic commitment rather
/// than a fast hash, makes accidental collisions negligible, and does not
/// expose the four-character display suffix's collision surface. The
/// secret itself is never persisted or exposed.
pub fn key_fingerprint(secret: &str) -> String {
    use std::fmt::Write as _;

    let digest = ring::digest::digest(&ring::digest::SHA256, secret.as_bytes());
    let mut out = String::with_capacity(32);
    for byte in &digest.as_ref()[..16] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

/// Aggregated request/token counters shared by totals, dimensions and
/// buckets. `total_tokens` is derived at render time (input + output).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metrics {
    #[serde(default)]
    pub requests: u64,
    #[serde(default)]
    pub ok: u64,
    #[serde(default)]
    pub errors: u64,
    /// Requests the client aborted after the response had started (status
    /// 499). Tracked separately from `errors`: the upstream key served the
    /// request, so an abort is neither a success nor a server-side failure.
    #[serde(default)]
    pub client_aborts: u64,
    /// Successful requests whose response carried no token counts.
    #[serde(default)]
    pub requests_without_usage: u64,
    #[serde(default)]
    pub input_tokens: u64,
    /// Input tokens read from cache, when the upstream reports them
    /// (included in `input_tokens`).
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub duration_ms: u64,
    /// Failover attempts made by these requests (0 for first-try wins).
    #[serde(default)]
    pub retries: u64,
}

impl Metrics {
    fn apply(&mut self, s: &Sample<'_>) {
        self.requests = self.requests.saturating_add(1);
        if s.status == 499 {
            self.client_aborts = self.client_aborts.saturating_add(1);
        } else if s.status < 400 {
            self.ok = self.ok.saturating_add(1);
        } else {
            self.errors = self.errors.saturating_add(1);
        }
        self.retries = self.retries.saturating_add(s.retries as u64);
        self.duration_ms = self
            .duration_ms
            .saturating_add(u64::try_from(s.duration.as_millis()).unwrap_or(u64::MAX));
        if s.usage.reported {
            self.input_tokens = self.input_tokens.saturating_add(s.usage.input);
            self.cached_input_tokens = self.cached_input_tokens.saturating_add(s.usage.cached);
            self.output_tokens = self.output_tokens.saturating_add(s.usage.output);
        } else if (200..300).contains(&s.status) && s.usage_expected {
            self.requests_without_usage = self.requests_without_usage.saturating_add(1);
        }
    }

    fn merge(&mut self, other: &Metrics) {
        self.requests = self.requests.saturating_add(other.requests);
        self.ok = self.ok.saturating_add(other.ok);
        self.errors = self.errors.saturating_add(other.errors);
        self.client_aborts = self.client_aborts.saturating_add(other.client_aborts);
        self.requests_without_usage = self
            .requests_without_usage
            .saturating_add(other.requests_without_usage);
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(other.cached_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
        self.duration_ms = self.duration_ms.saturating_add(other.duration_ms);
        self.retries = self.retries.saturating_add(other.retries);
    }
}

/// Token counts observed in one upstream response. `reported` is true iff
/// at least one counter key was present; `input`/`output` of 0 alone is a
/// legitimate report (e.g. an empty completion).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageCounts {
    pub input: u64,
    pub cached: u64,
    pub output: u64,
    pub reported: bool,
}

impl UsageCounts {
    /// Streaming lines may repeat cumulative counts; keep the maximum and
    /// OR the reported flag so a later zero-filled chunk cannot erase a
    /// real measurement.
    fn merge(&mut self, other: UsageCounts) {
        if !other.reported {
            return;
        }
        self.input = self.input.max(other.input);
        self.cached = self.cached.max(other.cached);
        self.output = self.output.max(other.output);
        self.reported = true;
    }
}

/// One completed client-visible request.
pub struct Sample<'a> {
    /// Account label, display suffix and stable key fingerprint, when a
    /// pool key served the request.
    pub key: Option<(&'a str, &'a str, &'a str)>,
    pub model: Option<&'a str>,
    pub endpoint: &'a str,
    pub status: u16,
    pub retries: usize,
    pub duration: Duration,
    pub usage: UsageCounts,
    /// Whether this endpoint is expected to report tokens at all. Only the
    /// token-bearing generation/embedding endpoints are true; metadata and
    /// management paths (`/api/tags`, `/api/show`, `/api/ps`, …) are not.
    pub usage_expected: bool,
}

// ---------------------------------------------------------------------------
// Stored state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct NamedRow {
    name: String,
    /// Last time this name was recorded; used for eviction and persisted so
    /// LRU ordering survives a restart.
    #[serde(default)]
    last_seen: u64,
    #[serde(flatten)]
    metrics: Metrics,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct KeyRow {
    account: String,
    /// Stable, non-secret fingerprint of the full key (account + last-4 is
    /// not unique enough); used for identity across restarts.
    #[serde(default)]
    id: String,
    suffix: String,
    #[serde(default)]
    last_seen: u64,
    #[serde(flatten)]
    metrics: Metrics,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct BucketRow {
    from: u64,
    #[serde(flatten)]
    metrics: Metrics,
}

struct State {
    /// First observation ever (persisted).
    since: u64,
    totals: Metrics,
    keys: Vec<KeyRow>,
    accounts: Vec<NamedRow>,
    models: Vec<NamedRow>,
    endpoints: Vec<NamedRow>,
    by_status: BTreeMap<u16, u64>,
    hourly: BTreeMap<u64, Metrics>,
    daily: BTreeMap<u64, Metrics>,
    /// Currently configured `(account, fingerprint)` keys (runtime only).
    present: HashSet<(String, String)>,
    dirty: bool,
}

impl State {
    fn fresh() -> State {
        State {
            since: unix_now(),
            totals: Metrics::default(),
            keys: Vec::new(),
            accounts: Vec::new(),
            models: Vec::new(),
            endpoints: Vec::new(),
            by_status: BTreeMap::new(),
            hourly: BTreeMap::new(),
            daily: BTreeMap::new(),
            present: HashSet::new(),
            // A fresh state has an unsaved `since` baseline; the first
            // flush (within 5 s) creates the file even before traffic.
            dirty: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Persisted {
    #[serde(default)]
    marker: String,
    #[serde(default)]
    version: u32,
    #[serde(default)]
    since: u64,
    #[serde(default)]
    saved_at: u64,
    #[serde(default)]
    totals: Metrics,
    #[serde(default)]
    keys: Vec<KeyRow>,
    #[serde(default)]
    accounts: Vec<NamedRow>,
    #[serde(default)]
    models: Vec<NamedRow>,
    #[serde(default)]
    endpoints: Vec<NamedRow>,
    #[serde(default)]
    by_status: BTreeMap<u16, u64>,
    #[serde(default)]
    hourly: Vec<BucketRow>,
    #[serde(default)]
    daily: Vec<BucketRow>,
}

impl Persisted {
    fn from_state(st: &State, saved_at: u64) -> Persisted {
        Persisted {
            marker: STATS_MARKER.to_string(),
            version: STATS_VERSION,
            since: st.since,
            saved_at,
            totals: st.totals.clone(),
            keys: st.keys.clone(),
            accounts: st.accounts.clone(),
            models: st.models.clone(),
            endpoints: st.endpoints.clone(),
            by_status: st.by_status.clone(),
            hourly: st
                .hourly
                .iter()
                .map(|(&from, m)| BucketRow {
                    from,
                    metrics: m.clone(),
                })
                .collect(),
            daily: st
                .daily
                .iter()
                .map(|(&from, m)| BucketRow {
                    from,
                    metrics: m.clone(),
                })
                .collect(),
        }
    }

    fn into_state(self, now: u64, present: &[(String, String, String)]) -> State {
        let mut st = State {
            since: if self.since == 0 { now } else { self.since },
            totals: self.totals,
            keys: cap_keys(self.keys, KEY_CAP, present),
            accounts: cap_named(self.accounts, ACCOUNT_CAP, &|_| false),
            models: cap_named(self.models, MODEL_CAP, &|_| false),
            endpoints: cap_named(self.endpoints, ENDPOINT_CAP, &is_token_endpoint),
            by_status: self.by_status,
            hourly: self
                .hourly
                .into_iter()
                .map(|b| (b.from, b.metrics))
                .collect(),
            daily: self
                .daily
                .into_iter()
                .map(|b| (b.from, b.metrics))
                .collect(),
            present: present
                .iter()
                .map(|(a, id, _)| (a.clone(), id.clone()))
                .collect(),
            dirty: false,
        };
        let before = st.keys.len();
        ensure_present_keys(&mut st.keys, present);
        if st.keys.len() != before {
            st.dirty = true;
        }
        prune(&mut st, now);
        st
    }
}

fn prune(st: &mut State, now: u64) {
    let hour = now - now % 3600;
    let oldest_hour = hour.saturating_sub(3600 * (HOURLY_KEEP - 1));
    st.hourly.retain(|&from, _| from >= oldest_hour);
    let day = now - now % 86400;
    let oldest_day = day.saturating_sub(86400 * (DAILY_KEEP - 1));
    st.daily.retain(|&from, _| from >= oldest_day);
}

// ---------------------------------------------------------------------------
// Meter
// ---------------------------------------------------------------------------

/// Aggregating, optionally persisting local statistics store.
pub struct Meter {
    /// False for `--no-stats` / OLLAMUX_NO_STATS: recording is skipped
    /// entirely and `/_stats` exposes no counters.
    enabled: bool,
    state: Mutex<State>,
    /// Serializes concurrent flushes (background cadence vs. shutdown) so
    /// two writers never interleave on the temp file.
    flush_lock: Mutex<()>,
    path: Option<PathBuf>,
    /// Set when the target must not be written: foreign/corrupt/newer/
    /// oversized file, unwritable directory or another live instance.
    blocked: AtomicBool,
    /// Human-readable reason for `blocked` (mutated after a live-flush
    /// recheck, so behind a lock).
    persistence_note: Mutex<Option<String>>,
    /// Last write failure, cleared by a successful flush. Surfaced in
    /// `/_stats` so a broken path is not silently ignored forever.
    last_flush_error: Mutex<Option<String>>,
    /// Metadata of the file this process last wrote. When the target still
    /// matches it, the pre-rename marker/version re-read is skipped: the
    /// file can only be the one we just wrote. Any change (including a
    /// non-cooperating writer) invalidates the stamp and restores the full
    /// check.
    last_written: Mutex<Option<TargetStamp>>,
    /// Advisory cross-process owner lock (unix), held while persistence is
    /// active. Cleared when persistence becomes blocked.
    #[cfg(unix)]
    lock_file: Mutex<Option<std::fs::File>>,
    started: Instant,
    started_at: u64,
    saved_at: AtomicU64,
}

impl Meter {
    /// Non-persisting meter (tests).
    pub fn in_memory() -> Meter {
        Meter::from_state(None, State::fresh())
    }

    /// Genuinely disabled meter (`--no-stats`): no counting, no persistence
    /// and no key/model/endpoint names retained in memory.
    pub fn disabled() -> Meter {
        let mut meter = Meter::from_state(None, State::fresh());
        meter.enabled = false;
        lock(&meter.state).dirty = false;
        meter
    }

    /// Load a persisted file if present, then continue from it. A missing
    /// file starts fresh. An existing file is adopted only when it carries
    /// the ollamux marker and the exact supported schema version; foreign,
    /// corrupt, oversized, non-regular or newer-versioned files are left
    /// untouched and persistence is disabled (`blocked`) rather than
    /// risking another program's data. It never fails startup.
    ///
    /// This constructor passes no current-key hints; prefer
    /// [`Meter::open_with_present`] or let `Server::with_meter` apply the
    /// pool's present set.
    pub fn open(path: PathBuf) -> Meter {
        Self::open_with_present(path, &[])
    }

    /// Like [`Meter::open`], but keeps currently configured keys when the
    /// historical key table is capped. `present` is
    /// `(account, fingerprint, display suffix)`.
    pub fn open_with_present(path: PathBuf, present: &[(String, String, String)]) -> Meter {
        let now = unix_now();
        let mut state = State::fresh();
        let mut note: Option<String> = None;
        let mut blocked = false;
        let mut saved_at = 0u64;
        let mut exists_regular = false;
        #[cfg(unix)]
        let mut lock_file = None;

        // Cheap pre-check for obviously invalid targets: avoid creating a
        // sibling lock file for a directory/device/oversized file. The
        // missing→created race is still closed by re-checking after the
        // lock below.
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if !meta.file_type().is_file() => {
                blocked = true;
                note = Some(format!(
                    "{} is not a regular file; statistics persistence disabled",
                    path.display()
                ));
            }
            Ok(meta) if meta.len() > MAX_STATS_FILE => {
                blocked = true;
                note = Some(format!(
                    "stats file {} is {} bytes (limit {MAX_STATS_FILE}); left untouched, persistence disabled",
                    path.display(),
                    meta.len()
                ));
            }
            _ => {}
        }

        // Acquire ownership before reading: otherwise a process could load
        // stale counters before another instance flushes and exits, then
        // overwrite the newer file with the stale snapshot.
        if !blocked {
            match acquire_owner_lock(&path) {
                Ok(OwnerLock::Acquired(lock)) => {
                    #[cfg(unix)]
                    {
                        lock_file = lock;
                    }
                    #[cfg(not(unix))]
                    let _ = lock;
                }
                Ok(OwnerLock::Held) => {
                    blocked = true;
                    note = Some(format!(
                        "another ollamux instance owns {}; persistence disabled",
                        path.display()
                    ));
                }
                Err(e) => {
                    blocked = true;
                    note = Some(format!(
                        "cannot lock stats file {}: {e}; persistence disabled",
                        path.display()
                    ));
                }
            }
        }

        // Re-stat after acquiring the lock: the file may have been created
        // or replaced between the caller's intent and lock acquisition.
        if !blocked {
            match std::fs::symlink_metadata(&path) {
                Ok(meta) if !meta.file_type().is_file() => {
                    blocked = true;
                    note = Some(format!(
                        "{} is not a regular file; statistics persistence disabled",
                        path.display()
                    ));
                }
                Ok(meta) if meta.len() > MAX_STATS_FILE => {
                    blocked = true;
                    note = Some(format!(
                        "stats file {} is {} bytes (limit {MAX_STATS_FILE}); left untouched, persistence disabled",
                        path.display(),
                        meta.len()
                    ));
                }
                Ok(_) => exists_regular = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    blocked = true;
                    note = Some(format!(
                        "cannot inspect stats path {}: {e}; persistence disabled",
                        path.display()
                    ));
                }
            }
        }

        if !blocked && exists_regular {
            match std::fs::read(&path) {
                Ok(bytes) => match serde_json::from_slice::<Persisted>(&bytes) {
                    Ok(p) if p.marker == STATS_MARKER && p.version == STATS_VERSION => {
                        saved_at = p.saved_at;
                        state = p.into_state(now, present);
                    }
                    Ok(p) if p.marker == STATS_MARKER && p.version > STATS_VERSION => {
                        blocked = true;
                        note = Some(format!(
                            "stats file {} has newer version {} (supported {STATS_VERSION}); left untouched, persistence disabled",
                            path.display(),
                            p.version
                        ));
                    }
                    Ok(p) if p.marker == STATS_MARKER => {
                        blocked = true;
                        note = Some(format!(
                            "stats file {} has unsupported version {} (expected {STATS_VERSION}); left untouched, persistence disabled",
                            path.display(),
                            p.version
                        ));
                    }
                    Ok(_) => {
                        blocked = true;
                        note = Some(format!(
                            "{} is not an ollamux stats file (missing marker); left untouched, persistence disabled",
                            path.display()
                        ));
                    }
                    Err(e) => {
                        blocked = true;
                        note = Some(format!(
                            "{} is not readable as an ollamux stats file ({e}); left untouched, persistence disabled",
                            path.display()
                        ));
                    }
                },
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    blocked = true;
                    note = Some(format!(
                        "cannot read stats file {}: {e}; left untouched, persistence disabled",
                        path.display()
                    ));
                }
            }
        }

        state.present = present
            .iter()
            .map(|(a, id, _)| (a.clone(), id.clone()))
            .collect();
        let before = state.keys.len();
        ensure_present_keys(&mut state.keys, present);
        if state.keys.len() != before {
            state.dirty = true;
        }

        if let Some(n) = &note {
            eprintln!("ollamux: {n}");
        }
        if blocked {
            // A blocked instance will never write; do not hold the lock and
            // prevent a corrected/next instance from starting.
            #[cfg(unix)]
            {
                lock_file = None;
            }
        }
        let meter = Meter::from_state(Some(path), state);
        #[cfg(unix)]
        {
            *lock(&meter.lock_file) = lock_file;
        }
        meter.blocked.store(blocked, Ordering::SeqCst);
        *lock(&meter.persistence_note) = note;
        meter.saved_at.store(saved_at, Ordering::Relaxed);
        meter
    }

    fn from_state(path: Option<PathBuf>, state: State) -> Meter {
        Meter {
            enabled: true,
            state: Mutex::new(state),
            flush_lock: Mutex::new(()),
            path,
            blocked: AtomicBool::new(false),
            persistence_note: Mutex::new(None),
            last_flush_error: Mutex::new(None),
            last_written: Mutex::new(None),
            #[cfg(unix)]
            lock_file: Mutex::new(None),
            started: Instant::now(),
            started_at: unix_now(),
            saved_at: AtomicU64::new(0),
        }
    }

    /// Whether counting is enabled (`--no-stats` / env makes this false).
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Apply the current pool's keys to an already-constructed meter.
    /// Used by `Server` so embedders get the same present-key priority as
    /// the CLI even when they did not call `open_with_present`.
    pub(crate) fn set_present(&self, present: &[(String, String, String)]) {
        if !self.enabled {
            return;
        }
        let mut st = lock(&self.state);
        let new_present: HashSet<(String, String)> = present
            .iter()
            .map(|(a, id, _)| (a.clone(), id.clone()))
            .collect();
        let changed = st.present != new_present;
        st.present = new_present;
        let before = st.keys.len();
        ensure_present_keys(&mut st.keys, present);
        if changed || st.keys.len() != before {
            st.dirty = true;
        }
    }

    /// Persistence target, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Default persistence location: `$OLLAMUX_STATS_FILE`, else
    /// `$XDG_STATE_HOME/ollamux/stats.json`, else
    /// `~/.local/state/ollamux/stats.json`, else a file in the CWD for
    /// HOME-less environments.
    pub fn default_path() -> PathBuf {
        if let Some(p) = std::env::var_os("OLLAMUX_STATS_FILE") {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
        if let Some(x) = std::env::var_os("XDG_STATE_HOME") {
            if !x.is_empty() {
                return PathBuf::from(x).join("ollamux").join("stats.json");
            }
        }
        if let Some(h) = std::env::var_os("HOME") {
            if !h.is_empty() {
                return PathBuf::from(h)
                    .join(".local")
                    .join("state")
                    .join("ollamux")
                    .join("stats.json");
            }
        }
        PathBuf::from("ollamux-stats.json")
    }

    /// Record one completed request (crate-internal: `Sample` is not
    /// re-exported).
    pub(crate) fn record(&self, sample: Sample<'_>) {
        self.record_at(sample, unix_now());
    }

    /// Time-injectable recording (unit tests use a fixed instant).
    fn record_at(&self, s: Sample<'_>, now: u64) {
        if !self.enabled {
            return;
        }
        let mut st = lock(&self.state);
        prune(&mut st, now);
        st.totals.apply(&s);

        if let Some((account, suffix, id)) = s.key {
            {
                let State { keys, present, .. } = &mut *st;
                bump_key(keys, account, suffix, id, &s, now, present);
            }
            bump_capped(&mut st.accounts, account, &s, ACCOUNT_CAP, &|_| false, now);
        } else {
            {
                let State { keys, present, .. } = &mut *st;
                bump_key(
                    keys,
                    "(unattributed)",
                    "(unattributed)",
                    "(unattributed)",
                    &s,
                    now,
                    present,
                );
            }
            bump_capped(
                &mut st.accounts,
                "(unattributed)",
                &s,
                ACCOUNT_CAP,
                &|_| false,
                now,
            );
        }

        let model = normalize_name(s.model.unwrap_or("(none)"));
        bump_model(&mut st.models, &model, &s, now);
        let endpoint = normalize_name(s.endpoint);
        bump_endpoint(&mut st.endpoints, &endpoint, &s, now);

        let status_entry = st.by_status.entry(s.status).or_insert(0);
        *status_entry = status_entry.saturating_add(1);
        let hour = now - now % 3600;
        st.hourly.entry(hour).or_default().apply(&s);
        let day = now - now % 86400;
        st.daily.entry(day).or_default().apply(&s);
        st.dirty = true;
    }

    /// Render the `/_stats` body for one range. `present` is the current
    /// pool's `(account, fingerprint, display suffix)` triples, used to
    /// flag historical rows and to show configured keys before traffic.
    pub fn render(&self, range: Range, present: &[(String, String, String)]) -> serde_json::Value {
        if !self.enabled {
            return self.disabled_json(range);
        }
        self.render_at(range, present, unix_now())
    }

    /// Envelope for `--no-stats`: explicitly reports that nothing is
    /// collected or retained (no key/model/endpoint names either).
    fn disabled_json(&self, range: Range) -> serde_json::Value {
        let granularity = match range {
            Range::H24 => "hour",
            Range::D7 | Range::D30 => "day",
        };
        serde_json::json!({
            "source": "local",
            "enabled": false,
            "version": STATS_VERSION,
            "since": self.started_at,
            "since_iso": iso8601(self.started_at),
            "started_at": self.started_at,
            "uptime_s": self.started.elapsed().as_secs(),
            "saved_at": serde_json::Value::Null,
            "pending_flush": false,
            "persistence": "disabled",
            "persistence_note": serde_json::Value::Null,
            "last_flush_error": serde_json::Value::Null,
            "range": range.as_str(),
            "granularity": granularity,
            "totals_scope": "lifetime",
            "totals": metrics_json(&Metrics::default()),
            "window": serde_json::Value::Null,
            "window_totals": metrics_json(&Metrics::default()),
            "keys": [],
            "accounts": [],
            "models": [],
            "endpoints": [],
            "by_status": {},
            "buckets": [],
        })
    }

    fn render_at(
        &self,
        range: Range,
        present: &[(String, String, String)],
        now: u64,
    ) -> serde_json::Value {
        let persistence_note = lock(&self.persistence_note).clone();
        let last_flush_error = lock(&self.last_flush_error).clone();
        let blocked = self.blocked.load(Ordering::SeqCst);
        let st = lock(&self.state);
        let present_set: HashSet<(&str, &str)> = present
            .iter()
            .map(|(a, id, _)| (a.as_str(), id.as_str()))
            .collect();

        let mut keys: Vec<serde_json::Value> = st
            .keys
            .iter()
            .map(|row| {
                let mut v = metrics_json(&row.metrics);
                if let Some(o) = v.as_object_mut() {
                    o.insert("account".into(), serde_json::json!(row.account));
                    o.insert("id".into(), serde_json::json!(row.id));
                    o.insert("suffix".into(), serde_json::json!(row.suffix));
                    o.insert(
                        "present".into(),
                        serde_json::json!(
                            present_set.contains(&(row.account.as_str(), row.id.as_str()))
                        ),
                    );
                }
                v
            })
            .collect();
        // Configured keys that have not served a request yet still appear,
        // with zero counters, so a fresh process does not look like a
        // missing key.
        for (account, id, suffix) in present {
            if !st.keys.iter().any(|r| r.account == *account && r.id == *id) {
                let mut v = metrics_json(&Metrics::default());
                v["account"] = serde_json::json!(account);
                v["id"] = serde_json::json!(id);
                v["suffix"] = serde_json::json!(suffix);
                v["present"] = serde_json::json!(true);
                keys.push(v);
            }
        }

        let accounts: Vec<serde_json::Value> = st
            .accounts
            .iter()
            .map(|row| named_json("account", row))
            .collect();
        let models: Vec<serde_json::Value> = st
            .models
            .iter()
            .map(|row| named_json("model", row))
            .collect();
        let endpoints: Vec<serde_json::Value> = st
            .endpoints
            .iter()
            .map(|row| named_json("endpoint", row))
            .collect();

        let (granularity, buckets, window_totals, window_from, window_until) =
            bucket_series(&st, range, now);
        let since = st.since;
        let started_at = self.started_at;
        let saved_at = self.saved_at.load(Ordering::Relaxed);
        let persistence = if blocked {
            "blocked"
        } else if last_flush_error.is_some() {
            "error"
        } else if self.path.is_some() {
            "file"
        } else {
            "memory"
        };
        serde_json::json!({
            "source": "local",
            "enabled": true,
            "version": STATS_VERSION,
            "since": since,
            "since_iso": iso8601(since),
            "started_at": started_at,
            "uptime_s": self.started.elapsed().as_secs(),
            "saved_at": if saved_at == 0 { serde_json::Value::Null } else { serde_json::json!(saved_at) },
            "pending_flush": st.dirty && self.path.is_some() && !blocked,
            "persistence": persistence,
            "persistence_note": persistence_note,
            "last_flush_error": last_flush_error,
            "range": range.as_str(),
            "granularity": granularity,
            "totals_scope": "lifetime",
            "totals": metrics_json(&st.totals),
            "window": {
                "from": iso8601(window_from),
                "until": iso8601(window_until),
            },
            "window_totals": metrics_json(&window_totals),
            "keys": keys,
            "accounts": accounts,
            "models": models,
            "endpoints": endpoints,
            "by_status": st.by_status,
            "buckets": buckets,
        })
    }

    /// Persist the current state atomically. No-op for in-memory meters and
    /// for blocked persistence (foreign/newer/oversized file, another live
    /// instance, or an unwritable target).
    pub fn flush(&self) -> io::Result<()> {
        if !self.enabled || self.blocked.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _flush = lock(&self.flush_lock);
        let Some(path) = &self.path else {
            lock(&self.state).dirty = false;
            return Ok(());
        };
        let now = unix_now();
        let persisted = {
            let mut st = lock(&self.state);
            if !st.dirty {
                // Idle flushes must not read/parse the stats file.
                return Ok(());
            }
            let p = Persisted::from_state(&st, now);
            st.dirty = false;
            p
        };
        let bytes = match serde_json::to_vec_pretty(&persisted) {
            Ok(b) => b,
            Err(e) => {
                lock(&self.state).dirty = true;
                let err = io::Error::new(io::ErrorKind::InvalidData, e);
                self.note_flush_error(path, &err);
                return Err(err);
            }
        };
        // The last file we wrote is expected to still be at the target.
        // `write_atomic` re-stats immediately before the rename: a match
        // skips the full marker/version parse, anything else takes the full
        // check.
        let expected = lock(&self.last_written).clone();
        match write_atomic(path, &bytes, expected.as_ref()) {
            Ok(WriteOutcome::Written(stamp)) => {
                *lock(&self.last_written) = stamp;
                self.saved_at.store(now, Ordering::Relaxed);
                *lock(&self.last_flush_error) = None;
                *lock(&self.persistence_note) = None;
                Ok(())
            }
            Ok(WriteOutcome::TargetChanged) => {
                self.block_persistence(
                    path,
                    "stats target changed during flush; persistence disabled",
                );
                Ok(())
            }
            Err(e) => {
                lock(&self.state).dirty = true;
                self.note_flush_error(path, &e);
                Err(e)
            }
        }
    }

    /// Permanently stop writing to this target (used when the file we
    /// loaded is replaced at runtime by a foreign/newer file).
    fn block_persistence(&self, path: &Path, reason: &str) {
        #[cfg(unix)]
        {
            *lock(&self.lock_file) = None;
        }
        let msg = format!("{}: {reason}", path.display());
        let first = !self.blocked.swap(true, Ordering::SeqCst);
        *lock(&self.persistence_note) = Some(msg.clone());
        if first {
            eprintln!("ollamux: {msg}");
        }
    }

    /// Remember and log a persistence failure once per distinct message,
    /// so an unwritable `--stats-file` is visible instead of silently
    /// dropping every counter at exit.
    fn note_flush_error(&self, path: &Path, err: &io::Error) {
        let msg = format!("{}: {err}", path.display());
        let mut slot = lock(&self.last_flush_error);
        if slot.as_deref() != Some(msg.as_str()) {
            eprintln!("ollamux: cannot persist stats: {msg}");
        }
        *slot = Some(msg);
    }

    /// Persist every [`FLUSH_INTERVAL`] while running, and once more when
    /// `stop` is set. No-op for in-memory meters.
    pub fn spawn_flusher(self: &Arc<Self>, stop: Arc<AtomicBool>) {
        if !self.enabled || self.path.is_none() || self.blocked.load(Ordering::SeqCst) {
            return;
        }
        let meter = Arc::clone(self);
        std::thread::spawn(move || {
            loop {
                let deadline = Instant::now() + FLUSH_INTERVAL;
                while Instant::now() < deadline {
                    if stop.load(Ordering::Relaxed) {
                        let _ = meter.flush();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
                let _ = meter.flush();
                if meter.blocked.load(Ordering::SeqCst) {
                    return;
                }
            }
        });
    }
}

fn bump_key(
    rows: &mut Vec<KeyRow>,
    account: &str,
    suffix: &str,
    id: &str,
    s: &Sample<'_>,
    now: u64,
    present: &HashSet<(String, String)>,
) {
    if let Some(row) = rows.iter_mut().find(|r| r.account == account && r.id == id) {
        row.metrics.apply(s);
        row.last_seen = now;
        return;
    }
    let new_row = |metrics: Metrics| KeyRow {
        account: account.to_string(),
        id: id.to_string(),
        suffix: suffix.to_string(),
        last_seen: now,
        metrics,
    };
    let has_other = rows
        .iter()
        .any(|r| r.account == "<other>" && r.suffix == "<other>");
    if rows.len() < KEY_CAP - 1 || (rows.len() == KEY_CAP - 1 && has_other) {
        let mut row = new_row(Metrics::default());
        row.metrics.apply(s);
        rows.push(row);
        return;
    }
    // A currently configured key must always own a row: evict the
    // least-recently-seen non-present historical row into `<other>`.
    if present.contains(&(account.to_string(), id.to_string())) {
        if let Some(pos) = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.account != "<other>" && !present.contains(&(r.account.clone(), r.id.clone()))
            })
            .min_by_key(|(_, r)| r.last_seen)
            .map(|(i, _)| i)
        {
            let victim = rows.remove(pos);
            merge_key_into_other(rows, &victim.metrics, now);
            let mut row = new_row(Metrics::default());
            row.metrics.apply(s);
            rows.push(row);
            return;
        }
        // All rows are present (or `<other>`); allow a bounded overflow.
        let mut row = new_row(Metrics::default());
        row.metrics.apply(s);
        rows.push(row);
        return;
    }
    let mut metrics = Metrics::default();
    metrics.apply(s);
    merge_key_into_other(rows, &metrics, now);
}

fn merge_key_into_other(rows: &mut Vec<KeyRow>, metrics: &Metrics, now: u64) {
    if let Some(other) = rows
        .iter_mut()
        .find(|r| r.account == "<other>" && r.suffix == "<other>")
    {
        other.metrics.merge(metrics);
        other.last_seen = now;
    } else {
        rows.push(KeyRow {
            account: "<other>".to_string(),
            id: "<other>".to_string(),
            suffix: "<other>".to_string(),
            last_seen: now,
            metrics: metrics.clone(),
        });
    }
}

/// Ensure every currently configured key has a row, even when the loaded
/// history is already at the cap: evict the least-recently-seen historical
/// row into `<other>` (or allow a bounded overflow if all are present).
fn ensure_present_keys(rows: &mut Vec<KeyRow>, present: &[(String, String, String)]) {
    for (account, id, suffix) in present {
        if rows.iter().any(|r| r.account == *account && r.id == *id) {
            continue;
        }
        let has_other = rows
            .iter()
            .any(|r| r.account == "<other>" && r.suffix == "<other>");
        if rows.len() > KEY_CAP - 1 || (rows.len() == KEY_CAP - 1 && !has_other) {
            if let Some(pos) = rows
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    r.account != "<other>"
                        && !present
                            .iter()
                            .any(|(a, i, _)| a == &r.account && i == &r.id)
                })
                .min_by_key(|(_, r)| r.last_seen)
                .map(|(i, _)| i)
            {
                let victim = rows.remove(pos);
                merge_key_into_other(rows, &victim.metrics, unix_now());
            }
        }
        rows.push(KeyRow {
            account: account.clone(),
            id: id.clone(),
            suffix: suffix.clone(),
            last_seen: 0,
            metrics: Metrics::default(),
        });
    }
}

/// Cap historical key rows on load, prioritising currently configured
/// keys (`present`) and then the most recent history. Overflow is merged
/// into a synthetic `<other>` row.
fn cap_keys(rows: Vec<KeyRow>, cap: usize, present: &[(String, String, String)]) -> Vec<KeyRow> {
    let has_other = rows
        .iter()
        .any(|r| r.account == "<other>" && r.suffix == "<other>");
    let present_set: HashSet<(&str, &str)> = present
        .iter()
        .map(|(a, id, _)| (a.as_str(), id.as_str()))
        .collect();
    let all_present_kept = present_set
        .iter()
        .all(|(a, id)| rows.iter().any(|r| r.account == *a && r.id == *id));
    if rows.len() < cap || (rows.len() == cap && has_other && all_present_kept) {
        return rows;
    }
    let keep = cap - 1;
    let mut keep_flags = vec![false; rows.len()];
    let mut marked = 0usize;
    for (i, row) in rows.iter().enumerate() {
        if marked < keep
            && row.account != "<other>"
            && present_set.contains(&(row.account.as_str(), row.id.as_str()))
        {
            keep_flags[i] = true;
            marked += 1;
        }
    }
    let mut candidates: Vec<usize> = (0..rows.len())
        .filter(|&i| !keep_flags[i] && rows[i].account != "<other>")
        .collect();
    candidates.sort_by_key(|&i| std::cmp::Reverse((rows[i].last_seen, i)));
    for i in candidates.into_iter().take(keep - marked) {
        keep_flags[i] = true;
    }
    let mut kept: Vec<KeyRow> = Vec::with_capacity(cap);
    let mut other = KeyRow {
        account: "<other>".to_string(),
        id: "<other>".to_string(),
        suffix: "<other>".to_string(),
        last_seen: 0,
        metrics: Metrics::default(),
    };
    for (i, row) in rows.into_iter().enumerate() {
        if keep_flags[i] {
            kept.push(row);
        } else {
            other.metrics.merge(&row.metrics);
        }
    }
    if let Some(existing) = kept
        .iter_mut()
        .find(|r| r.account == "<other>" && r.suffix == "<other>")
    {
        existing.metrics.merge(&other.metrics);
    } else {
        kept.push(other);
    }
    kept
}

fn bump_named(rows: &mut Vec<NamedRow>, name: &str, s: &Sample<'_>, now: u64) {
    if let Some(row) = rows.iter_mut().find(|r| r.name == name) {
        row.metrics.apply(s);
        row.last_seen = now;
        return;
    }
    let mut metrics = Metrics::default();
    metrics.apply(s);
    rows.push(NamedRow {
        name: name.to_string(),
        last_seen: now,
        metrics,
    });
}

/// Endpoints whose responses may carry token counters. This list is
/// deliberately closed: metadata/management paths and arbitrary client
/// paths must not inflate `requests_without_usage`, and these names are
/// protected from endpoint-cardinality eviction.
pub fn is_token_endpoint(path: &str) -> bool {
    matches!(
        path,
        "api/chat"
            | "api/generate"
            | "api/embed"
            | "v1/chat/completions"
            | "v1/completions"
            | "v1/embeddings"
            | "v1/responses"
    )
}

/// Retain at most `MODEL_CAP` distinct model names. The oldest unpinned
/// name is evicted into `<other>` when the table fills, so first-come junk
/// cannot permanently crowd out a real model.
fn bump_model(rows: &mut Vec<NamedRow>, name: &str, s: &Sample<'_>, now: u64) {
    bump_capped(rows, name, s, MODEL_CAP, &|_| false, now);
}

/// Endpoint paths are client-controlled, so they get the same bounded-row
/// treatment as models, with token-bearing endpoints pinned.
fn bump_endpoint(rows: &mut Vec<NamedRow>, name: &str, s: &Sample<'_>, now: u64) {
    bump_capped(rows, name, s, ENDPOINT_CAP, &is_token_endpoint, now);
}

fn bump_capped(
    rows: &mut Vec<NamedRow>,
    name: &str,
    s: &Sample<'_>,
    cap: usize,
    protected: &dyn Fn(&str) -> bool,
    now: u64,
) {
    if rows.iter().any(|r| r.name == name) {
        bump_named(rows, name, s, now);
        return;
    }
    let has_other = rows.iter().any(|r| r.name == "<other>");
    if name == "<other>" || rows.len() < cap - 1 || (rows.len() == cap - 1 && has_other) {
        bump_named(rows, name, s, now);
        return;
    }
    // Evict the least-recently-seen unpinned row, folding its metrics into
    // `<other>`, so a new real model/endpoint can displace junk.
    if let Some(pos) = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.name != "<other>" && !protected(&r.name))
        .min_by_key(|(_, r)| r.last_seen)
        .map(|(i, _)| i)
    {
        let victim = rows.remove(pos);
        merge_into_other(rows, &victim.metrics, now);
        bump_named(rows, name, s, now);
    } else if protected(name) {
        // A fixed token endpoint is never folded into `<other>`; there are
        // only a handful of these, so the bound grows by at most that many.
        bump_named(rows, name, s, now);
    } else {
        bump_named(rows, "<other>", s, now);
    }
}

fn merge_into_other(rows: &mut Vec<NamedRow>, metrics: &Metrics, now: u64) {
    if let Some(other) = rows.iter_mut().find(|r| r.name == "<other>") {
        other.metrics.merge(metrics);
        other.last_seen = now;
    } else {
        rows.push(NamedRow {
            name: "<other>".to_string(),
            last_seen: now,
            metrics: metrics.clone(),
        });
    }
}

/// Enforce a cardinality cap when loading a persisted file (which may have
/// been written by an older build or edited by hand). Protected names are
/// kept first, then the most recent non-protected rows; everything else is
/// merged into `<other>`.
fn cap_named(rows: Vec<NamedRow>, cap: usize, protected: &dyn Fn(&str) -> bool) -> Vec<NamedRow> {
    let has_other = rows.iter().any(|r| r.name == "<other>");
    if rows.len() < cap || (rows.len() == cap && has_other) {
        return rows;
    }
    let keep = cap - 1;
    let mut keep_flags = vec![false; rows.len()];
    let mut marked = 0usize;
    for (i, row) in rows.iter().enumerate() {
        if marked < keep && row.name != "<other>" && protected(&row.name) {
            keep_flags[i] = true;
            marked += 1;
        }
    }
    let mut candidates: Vec<usize> = (0..rows.len())
        .filter(|&i| !keep_flags[i] && rows[i].name != "<other>")
        .collect();
    candidates.sort_by_key(|&i| std::cmp::Reverse((rows[i].last_seen, i)));
    for i in candidates.into_iter().take(keep - marked) {
        keep_flags[i] = true;
    }
    let mut kept: Vec<NamedRow> = Vec::with_capacity(cap);
    let mut other = NamedRow {
        name: "<other>".to_string(),
        last_seen: 0,
        metrics: Metrics::default(),
    };
    for (i, row) in rows.into_iter().enumerate() {
        if keep_flags[i] {
            kept.push(row);
        } else {
            other.metrics.merge(&row.metrics);
        }
    }
    if let Some(existing) = kept.iter_mut().find(|r| r.name == "<other>") {
        existing.metrics.merge(&other.metrics);
    } else {
        kept.push(other);
    }
    kept
}

fn normalize_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "(none)".to_string();
    }
    if trimmed.chars().count() <= MAX_NAME_LEN {
        return trimmed.to_string();
    }
    trimmed.chars().take(MAX_NAME_LEN).collect()
}

fn named_json(field: &str, row: &NamedRow) -> serde_json::Value {
    let mut v = metrics_json(&row.metrics);
    v[field] = serde_json::json!(row.name);
    v
}

fn metrics_json(m: &Metrics) -> serde_json::Value {
    let mut v = serde_json::to_value(m).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(o) = v.as_object_mut() {
        o.insert(
            "total_tokens".into(),
            serde_json::json!(m.input_tokens.saturating_add(m.output_tokens)),
        );
        o.insert(
            "avg_ms".into(),
            if m.requests == 0 {
                serde_json::Value::Null
            } else {
                serde_json::json!(round1(m.duration_ms as f64 / m.requests as f64))
            },
        );
    }
    v
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

fn bucket_series(
    st: &State,
    range: Range,
    now: u64,
) -> (&'static str, Vec<serde_json::Value>, Metrics, u64, u64) {
    let (step, count, map, granularity) = match range {
        Range::H24 => (3600u64, HOURLY_KEEP, &st.hourly, "hour"),
        Range::D7 => (86400u64, 7, &st.daily, "day"),
        Range::D30 => (86400u64, 30, &st.daily, "day"),
    };
    let current = now - now % step;
    let first = current.saturating_sub(step * (count - 1));
    let mut out = Vec::with_capacity(count as usize);
    let mut totals = Metrics::default();
    let mut from = first;
    while from <= current {
        let metrics = map.get(&from).cloned().unwrap_or_default();
        totals.merge(&metrics);
        let mut v = metrics_json(&metrics);
        v["from"] = serde_json::json!(iso8601(from));
        v["until"] = serde_json::json!(iso8601(from + step));
        v["partial"] = serde_json::json!(from == current);
        out.push(v);
        from += step;
    }
    (granularity, out, totals, first, current + step)
}

enum OwnerLock {
    /// Lock acquired (the file handle must stay alive for the lock's life).
    Acquired(Option<std::fs::File>),
    /// Another process currently holds the lock.
    Held,
}

fn lock_path(path: &Path) -> PathBuf {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("stats");
    path.with_file_name(format!("{name}.lock"))
}

#[cfg(unix)]
fn acquire_owner_lock(path: &Path) -> io::Result<OwnerLock> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    let lock = lock_path(path);
    if let Some(parent) = lock.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock)?;
    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc == 0 {
        Ok(OwnerLock::Acquired(Some(file)))
    } else {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            Ok(OwnerLock::Held)
        } else {
            Err(err)
        }
    }
}

#[cfg(not(unix))]
fn acquire_owner_lock(_path: &Path) -> io::Result<OwnerLock> {
    // No portable advisory lock in std; persistence remains last-writer-
    // wins on non-unix platforms.
    Ok(OwnerLock::Acquired(None))
}

/// Lightweight identity of the target file, used to skip the full
/// marker/version re-read when the file is still the one this process
/// wrote. `modified` is `Option` so a platform without mtime support
/// simply never produces a trusted stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TargetStamp {
    len: u64,
    modified: Option<SystemTime>,
}

fn target_stamp(path: &Path) -> Option<TargetStamp> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    Some(TargetStamp {
        len: meta.len(),
        modified: meta.modified().ok(),
    })
}

/// True when the target is absent or is a valid current stats file. A
/// foreign/corrupt/newer/oversized/non-regular file must never be
/// overwritten.
fn target_is_ours(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => Ok(false),
        Ok(meta) if meta.len() > MAX_STATS_FILE => Ok(false),
        Ok(_) => match std::fs::read(path) {
            Ok(bytes) => Ok(serde_json::from_slice::<Persisted>(&bytes)
                .map(|p| p.marker == STATS_MARKER && p.version == STATS_VERSION)
                .unwrap_or(false)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

enum WriteOutcome {
    /// The rename succeeded; the stamp describes the bytes just written
    /// (taken from the temp file before the rename, so it is never the
    /// metadata of an unrelated file that appeared at the target).
    Written(Option<TargetStamp>),
    TargetChanged,
}

/// `expected` is the stamp of the file this process last wrote (if any).
/// Immediately before the rename, the target is re-statted: a match means
/// it is still that validated file, so the expensive full marker/version
/// re-read can be skipped. Anything else (including no prior write, a
/// deleted/replaced file, or mismatched metadata) takes the full check, so
/// the protection does not weaken for a changed or foreign target.
fn write_atomic(
    path: &Path,
    bytes: &[u8],
    expected: Option<&TargetStamp>,
) -> io::Result<WriteOutcome> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let (tmp, mut f) = create_tmp(path)?;
    if let Err(e) = f.write_all(bytes).and_then(|()| f.sync_all()) {
        drop(f);
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    drop(f);
    // Check as late as possible, immediately before the rename. The owner
    // lock protects cooperating instances; this narrows the window for a
    // non-cooperating writer after the temp file was written. A matching
    // stamp means this exact file was already validated as ours.
    let current = target_stamp(path);
    let unchanged = match (expected, current.as_ref()) {
        (Some(known), Some(now)) => known == now,
        _ => false,
    };
    if !unchanged {
        match target_is_ours(path) {
            Ok(true) => {}
            Ok(false) => {
                let _ = std::fs::remove_file(&tmp);
                return Ok(WriteOutcome::TargetChanged);
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        }
    }
    // Capture the identity of the bytes we are about to publish from the
    // temp file, before it is renamed: the target then has the same inode
    // and metadata, and the stamp can never describe an unrelated file that
    // raced into the target after the rename.
    let written = target_stamp(&tmp);
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    // Best-effort directory fsync so the rename itself is durable.
    #[cfg(unix)]
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let _ = std::fs::File::open(parent).and_then(|d| d.sync_all());
    }
    Ok(WriteOutcome::Written(written))
}

/// Create a unique sibling temp file. The name includes the PID and a
/// process-local sequence so two ollamux instances sharing one stats file
/// never truncate each other's in-flight temp file (the final rename stays
/// atomic and last-writer-wins).
fn create_tmp(path: &Path) -> io::Result<(PathBuf, std::fs::File)> {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("stats");
    for _ in 0..4 {
        let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = path.with_file_name(format!(".{name}.{}.{seq}.{nanos}.tmp", std::process::id()));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&tmp) {
            Ok(f) => return Ok((tmp, f)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique stats temp file",
    ))
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

/// RFC 3339 UTC timestamp for an epoch-second value (buckets only; upstream
/// timestamps are relayed verbatim elsewhere).
pub fn iso8601(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let sod = secs % 86400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 → (y, m, d).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (y + i64::from(m <= 2), m, d)
}

// ---------------------------------------------------------------------------
// Response-body usage extraction
// ---------------------------------------------------------------------------

/// Extract token counters from a complete (or tail-truncated) non-streaming
/// JSON response body. Native Ollama counters win when present; otherwise
/// the OpenAI-compatible `usage` object is read. Bodies without counters
/// yield `reported: false`. A body that fails to parse (e.g. a truncated
/// tail) falls back to a lenient key scan.
pub fn parse_body_usage(bytes: &[u8]) -> UsageCounts {
    if bytes.is_empty() {
        return UsageCounts::default();
    }
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(bytes) {
        // Do not scan valid JSON: a model description or template string
        // containing a counter-looking key must not be mistaken for usage.
        return from_value(&v);
    }
    scan_usage(bytes)
}

fn from_value(v: &serde_json::Value) -> UsageCounts {
    let mut u = UsageCounts::default();
    if let Some(n) = v.get("prompt_eval_count").and_then(|x| x.as_u64()) {
        u.input = n;
        u.reported = true;
    }
    if let Some(n) = v.get("eval_count").and_then(|x| x.as_u64()) {
        u.output = n;
        u.reported = true;
    }
    if let Some(n) = v.get("prompt_eval_cached_count").and_then(|x| x.as_u64()) {
        u.cached = n;
    }
    // Native input/output counters win over any OpenAI-compatible `usage`
    // object. A cached-only native count is not a usable split, so it does
    // not by itself mark usage as reported.
    if u.reported {
        return u;
    }
    if let Some(usage) = v.get("usage") {
        absorb_usage(usage, &mut u);
    }
    // OpenAI Responses events nest usage under `response`:
    // {"type":"response.completed","response":{"usage":{...}}}
    if !u.reported {
        if let Some(usage) = v.get("response").and_then(|r| r.get("usage")) {
            absorb_usage(usage, &mut u);
        }
    }
    u
}

/// Merge one `usage` object (Chat Completions or Responses shape) into `u`.
/// Explicit input/output values are never overwritten by `total_tokens`;
/// the total only fills a missing side.
fn absorb_usage(usage: &serde_json::Value, u: &mut UsageCounts) {
    // Resolve each preferred field to a number before falling back: a
    // present-but-null `prompt_tokens` must not suppress `input_tokens`.
    let input = usage
        .get("prompt_tokens")
        .and_then(|x| x.as_u64())
        .or_else(|| usage.get("input_tokens").and_then(|x| x.as_u64()));
    let output = usage
        .get("completion_tokens")
        .and_then(|x| x.as_u64())
        .or_else(|| usage.get("output_tokens").and_then(|x| x.as_u64()));
    let cached = usage
        .get("prompt_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|x| x.as_u64())
        .or_else(|| {
            usage
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64())
        })
        .or_else(|| usage.get("cached_tokens").and_then(|x| x.as_u64()));
    let total = usage.get("total_tokens").and_then(|x| x.as_u64());

    let mut have_input = false;
    let mut have_output = false;
    if let Some(n) = input {
        u.input = n;
        have_input = true;
        u.reported = true;
    }
    if let Some(n) = output {
        u.output = n;
        have_output = true;
        u.reported = true;
    }
    if let Some(n) = cached {
        u.cached = n;
    }
    if let Some(total) = total {
        if have_input && !have_output {
            u.output = total.saturating_sub(u.input);
            u.reported = true;
        } else if have_output && !have_input {
            u.input = total.saturating_sub(u.output);
            u.reported = true;
        } else if !have_input && !have_output {
            // No split is knowable. Do not relabel the total as output and
            // do not mark usage as reported: the caller records this as
            // missing usage instead.
        }
    }
}

/// Lenient fallback for a body whose beginning was truncated: find the
/// last/first occurrence of each counter key and parse its integer value.
fn scan_usage(bytes: &[u8]) -> UsageCounts {
    let mut u = UsageCounts::default();
    if let Some(n) = scan_key(bytes, "prompt_eval_count") {
        u.input = n;
        u.reported = true;
    }
    if let Some(n) = scan_key(bytes, "eval_count") {
        u.output = n;
        u.reported = true;
    }
    if let Some(n) = scan_key(bytes, "prompt_eval_cached_count") {
        u.cached = n;
    }
    if let Some(n) = scan_key(bytes, "prompt_tokens").or_else(|| scan_key(bytes, "input_tokens")) {
        u.input = n;
        u.reported = true;
    }
    if let Some(n) =
        scan_key(bytes, "completion_tokens").or_else(|| scan_key(bytes, "output_tokens"))
    {
        u.output = n;
        u.reported = true;
    }
    if let Some(n) = scan_key(bytes, "cached_tokens") {
        u.cached = n;
    }
    if let Some(total) = scan_key(bytes, "total_tokens") {
        let have_input = scan_key(bytes, "prompt_tokens").is_some()
            || scan_key(bytes, "input_tokens").is_some()
            || scan_key(bytes, "prompt_eval_count").is_some();
        let have_output = scan_key(bytes, "completion_tokens").is_some()
            || scan_key(bytes, "output_tokens").is_some()
            || scan_key(bytes, "eval_count").is_some();
        if have_input && !have_output {
            u.output = total.saturating_sub(u.input);
            u.reported = true;
        } else if have_output && !have_input {
            u.input = total.saturating_sub(u.output);
            u.reported = true;
        } else if !have_input && !have_output {
            // Total-only: no split is knowable; leave it unreported.
        }
    }
    u
}

fn scan_key(hay: &[u8], key: &str) -> Option<u64> {
    let needle = format!("\"{key}\"");
    let nb = needle.as_bytes();
    let mut best: Option<u64> = None;
    let mut i = 0usize;
    while i + nb.len() <= hay.len() {
        if &hay[i..i + nb.len()] == nb {
            // An escaped quote inside a JSON string is not an object key;
            // ignore `\"prompt_tokens\": ...` content.
            if i > 0 && hay[i - 1] == b'\\' {
                i += 1;
                continue;
            }
            let mut j = i + nb.len();
            while j < hay.len() && hay[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < hay.len() && hay[j] == b':' {
                j += 1;
                while j < hay.len() && hay[j].is_ascii_whitespace() {
                    j += 1;
                }
                let start = j;
                while j < hay.len() && hay[j].is_ascii_digit() {
                    j += 1;
                }
                if let Ok(txt) = std::str::from_utf8(&hay[start..j]) {
                    if let Ok(n) = txt.parse::<u64>() {
                        best = Some(n);
                    }
                }
                i = j.max(i + nb.len());
                continue;
            }
            i += nb.len();
        } else {
            i += 1;
        }
    }
    best
}

/// Incremental NDJSON/SSE usage sniffer for streaming relays. Feed every
/// upstream chunk; lines are reassembled across chunk boundaries and only
/// candidate lines are parsed, so forwarding latency is unaffected.
pub struct UsageSniffer {
    pending: Vec<u8>,
    usage: UsageCounts,
}

impl Default for UsageSniffer {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageSniffer {
    pub fn new() -> UsageSniffer {
        UsageSniffer {
            pending: Vec::new(),
            usage: UsageCounts::default(),
        }
    }

    pub fn feed(&mut self, mut bytes: &[u8]) {
        while let Some(pos) = bytes.iter().position(|&b| b == b'\n') {
            self.pending.extend_from_slice(&bytes[..pos]);
            let line = std::mem::take(&mut self.pending);
            self.consume(&line);
            bytes = &bytes[pos + 1..];
        }
        self.pending.extend_from_slice(bytes);
        if self.pending.len() > MAX_LINE {
            let drop = self.pending.len() - MAX_LINE;
            self.pending.drain(..drop);
        }
    }

    /// Process a final partial line (no trailing newline).
    pub fn finish(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.pending);
        self.consume(&line);
    }

    pub fn usage(&self) -> UsageCounts {
        self.usage
    }

    fn consume(&mut self, line: &[u8]) {
        let mut s = trim_ascii(line);
        if let Some(rest) = s.strip_prefix(b"data:".as_slice()) {
            s = trim_ascii(rest);
        }
        if s.is_empty() || s == b"[DONE]" {
            return;
        }
        // Cheap prefilter: most streaming chunks are content deltas.
        if !contains_any(
            s,
            &[
                b"prompt_eval_count",
                b"prompt_eval_cached_count",
                b"eval_count",
                b"prompt_tokens",
                b"completion_tokens",
                b"input_tokens",
                b"output_tokens",
                b"total_tokens",
                b"cached_tokens",
            ],
        ) {
            return;
        }
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(s) {
            self.usage.merge(from_value(&v));
        } else {
            // A line longer than MAX_LINE was truncated (notably a large
            // non-streaming /api/embed body); counters live at the tail.
            self.usage.merge(scan_usage(s));
        }
    }
}

fn trim_ascii(mut s: &[u8]) -> &[u8] {
    while s.first().is_some_and(u8::is_ascii_whitespace) {
        s = &s[1..];
    }
    while s.last().is_some_and(u8::is_ascii_whitespace) {
        s = &s[..s.len() - 1];
    }
    s
}

fn contains_any(hay: &[u8], needles: &[&[u8]]) -> bool {
    needles
        .iter()
        .any(|needle| hay.windows(needle.len()).any(|w| w == *needle))
}

/// Reader wrapper that retains a bounded tail of everything read, for
/// non-streaming usage extraction. It never changes the bytes the client
/// receives.
pub struct TeeReader<R> {
    inner: R,
    shared: Arc<Mutex<VecDeque<u8>>>,
}

impl<R: Read> TeeReader<R> {
    /// Returns the wrapper plus the buffer to hand to [`captured_usage`]
    /// after the response has been relayed.
    pub fn new(inner: R) -> (TeeReader<R>, Arc<Mutex<VecDeque<u8>>>) {
        let shared = Arc::new(Mutex::new(VecDeque::new()));
        (
            TeeReader {
                inner,
                shared: Arc::clone(&shared),
            },
            shared,
        )
    }
}

impl<R: Read> Read for TeeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            let mut tail = lock(&self.shared);
            tail.extend(&buf[..n]);
            while tail.len() > MAX_CAPTURE {
                tail.pop_front();
            }
        }
        Ok(n)
    }
}

/// Parse (and consume) the tail captured by a [`TeeReader`].
pub fn captured_usage(shared: &Mutex<VecDeque<u8>>) -> UsageCounts {
    let tail: Vec<u8> = lock(shared).drain(..).collect();
    parse_body_usage(&tail)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> u64 {
        1_800_000_123 // fixed, deliberately not hour/day aligned
    }

    fn sample<'a>(key: Option<(&'a str, &'a str, &'a str)>, usage: UsageCounts) -> Sample<'a> {
        Sample {
            key,
            model: Some("gpt-oss:120b"),
            endpoint: "api/chat",
            status: 200,
            retries: 0,
            duration: Duration::from_millis(250),
            usage,
            usage_expected: true,
        }
    }

    #[test]
    fn parses_native_non_streaming_usage() {
        let body = br#"{"model":"m","message":{"content":"hi"},"done":true,"prompt_eval_count":120,"eval_count":34}"#;
        let u = parse_body_usage(body);
        assert!(u.reported);
        assert_eq!((u.input, u.output), (120, 34));
    }

    #[test]
    fn parses_openai_usage_including_cached() {
        let body = br#"{"choices":[],"usage":{"prompt_tokens":100,"completion_tokens":20,"total_tokens":120,"prompt_tokens_details":{"cached_tokens":40}}}"#;
        let u = parse_body_usage(body);
        assert!(u.reported);
        assert_eq!((u.input, u.output, u.cached), (100, 20, 40));
    }

    #[test]
    fn truncated_body_falls_back_to_key_scan() {
        let body = br#"..."prompt_eval_count":120,"eval_count":34}}"#;
        let u = parse_body_usage(body);
        assert!(u.reported);
        assert_eq!((u.input, u.output), (120, 34));
    }

    #[test]
    fn streaming_sniffer_handles_ndjson_and_split_lines() {
        let mut s = UsageSniffer::new();
        s.feed(br#"{"message":{"content":"hel"}}"#);
        s.feed(b"\n{\"message\":{\"content\":\"lo\"}}\n{\"done\":true,");
        s.feed(br#""prompt_eval_count":50,"eval_count":7}"#);
        s.feed(b"\n");
        s.finish();
        let u = s.usage();
        assert!(u.reported);
        assert_eq!((u.input, u.output), (50, 7));
    }

    #[test]
    fn streaming_sniffer_handles_sse_usage() {
        let mut s = UsageSniffer::new();
        s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":3,\"total_tokens\":12}}\n\n");
        s.feed(b"data: [DONE]\n\n");
        s.finish();
        let u = s.usage();
        assert!(u.reported);
        assert_eq!((u.input, u.output), (9, 3));
    }

    #[test]
    fn oversized_line_is_bounded_and_ignored() {
        let mut s = UsageSniffer::new();
        let big = vec![b'x'; MAX_LINE * 2];
        s.feed(&big);
        s.feed(b"\n");
        assert!(!s.usage().reported);
    }

    #[test]
    fn records_totals_dimensions_and_buckets() {
        let m = Meter::in_memory();
        m.record_at(
            sample(
                Some(("alice", "1234", "id1234")),
                UsageCounts {
                    input: 100,
                    cached: 40,
                    output: 20,
                    reported: true,
                },
            ),
            now(),
        );
        m.record_at(
            sample(
                Some(("alice", "5678", "id5678")),
                UsageCounts {
                    input: 10,
                    cached: 0,
                    output: 5,
                    reported: true,
                },
            ),
            now(),
        );
        // Missing usage on a successful request is counted, not estimated.
        m.record_at(
            sample(Some(("alice", "5678", "id5678")), UsageCounts::default()),
            now(),
        );
        let present = vec![(
            "alice".to_string(),
            "id1234".to_string(),
            "1234".to_string(),
        )];
        let v = m.render_at(Range::H24, &present, now());
        assert_eq!(v["source"], "local");
        assert_eq!(v["totals"]["requests"], 3);
        assert_eq!(v["totals"]["input_tokens"], 110);
        assert_eq!(v["totals"]["output_tokens"], 25);
        assert_eq!(v["totals"]["requests_without_usage"], 1);
        assert_eq!(v["models"][0]["model"], "gpt-oss:120b");
        assert_eq!(v["accounts"][0]["account"], "alice");
        assert_eq!(v["keys"].as_array().unwrap().len(), 2);
        // Only the configured key is present; the other is historical.
        let rows = v["keys"].as_array().unwrap();
        let k1234 = rows.iter().find(|r| r["suffix"] == "1234").unwrap();
        assert_eq!(k1234["present"], true);
        let k5678 = rows.iter().find(|r| r["suffix"] == "5678").unwrap();
        assert_eq!(k5678["present"], false);
        assert_eq!(v["buckets"].as_array().unwrap().len(), 25);
        let partial = v["buckets"].as_array().unwrap().last().unwrap();
        assert_eq!(partial["partial"], true);
    }

    #[test]
    fn errors_and_redirects_do_not_count_as_missing_usage() {
        let m = Meter::in_memory();
        let mut s = sample(None, UsageCounts::default());
        s.status = 400;
        m.record_at(s, now());
        let mut s = sample(None, UsageCounts::default());
        s.status = 302;
        m.record_at(s, now());
        let v = m.render_at(Range::D7, &[], now());
        assert_eq!(v["totals"]["requests"], 2);
        assert_eq!(v["totals"]["errors"], 1);
        assert_eq!(v["totals"]["requests_without_usage"], 0);
        let keys = v["keys"].as_array().unwrap();
        assert!(
            keys.iter().any(|r| {
                r["account"] == "(unattributed)"
                    && r["suffix"] == "(unattributed)"
                    && r["id"] == "(unattributed)"
                    && r["requests"] == serde_json::json!(2)
            }),
            "all three sentinels must match the documented shape: {v}"
        );
    }

    #[test]
    fn client_aborts_are_counted_separately_from_errors() {
        let m = Meter::in_memory();
        let mut s = sample(None, UsageCounts::default());
        s.status = 499;
        m.record_at(s, now());
        let v = m.render_at(Range::H24, &[], now());
        assert_eq!(v["totals"]["requests"], 1);
        assert_eq!(v["totals"]["ok"], 0);
        assert_eq!(v["totals"]["errors"], 0);
        assert_eq!(v["totals"]["client_aborts"], 1);
        assert_eq!(v["totals"]["requests_without_usage"], 0);
        assert_eq!(v["by_status"]["499"], 1);
    }

    #[test]
    fn model_cardinality_is_capped() {
        let m = Meter::in_memory();
        for i in 0..(MODEL_CAP + 20) {
            let name = format!("model-{i}");
            let mut s = sample(None, UsageCounts::default());
            s.model = Some(&name);
            s.usage_expected = false;
            m.record_at(s, now());
        }
        let v = m.render_at(Range::H24, &[], now());
        let models = v["models"].as_array().unwrap();
        assert!(models.len() <= MODEL_CAP, "{} models", models.len());
        assert!(
            models.iter().any(|r| r["model"] == "<other>"),
            "overflow row present"
        );
    }

    #[test]
    fn endpoint_cardinality_is_capped() {
        let m = Meter::in_memory();
        for i in 0..(ENDPOINT_CAP + 20) {
            let endpoint = format!("api/path-{i}");
            let mut s = sample(None, UsageCounts::default());
            s.endpoint = &endpoint;
            s.usage_expected = false;
            m.record_at(s, now());
        }
        let v = m.render_at(Range::H24, &[], now());
        let endpoints = v["endpoints"].as_array().unwrap();
        assert!(
            endpoints.len() <= ENDPOINT_CAP,
            "{} endpoints",
            endpoints.len()
        );
        assert!(
            endpoints.iter().any(|r| r["endpoint"] == "<other>"),
            "overflow row present"
        );
    }

    #[test]
    fn cap_named_merges_overflow_on_load() {
        let rows: Vec<NamedRow> = (0..(ENDPOINT_CAP + 5))
            .map(|i| NamedRow {
                name: format!("api/{i}"),
                last_seen: 0,
                metrics: Metrics {
                    requests: 1,
                    ..Metrics::default()
                },
            })
            .collect();
        let capped = cap_named(rows, ENDPOINT_CAP, &is_token_endpoint);
        assert_eq!(capped.len(), ENDPOINT_CAP);
        let other = capped.iter().find(|r| r.name == "<other>").unwrap();
        assert_eq!(other.metrics.requests, 6);
    }

    #[test]
    fn newer_versioned_file_is_left_untouched() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-newer-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let original =
            br#"{"marker":"ollamux-stats","version":99,"since":1,"totals":{"requests":123}}"#;
        std::fs::write(&path, original).unwrap();

        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original,
            "a newer-versioned file must not be clobbered"
        );
        let v = m.render_at(Range::H24, &[], now());
        assert_eq!(v["persistence"], "blocked");
        assert!(v["persistence_note"].as_str().unwrap().contains("newer"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn foreign_json_and_corrupt_files_are_left_untouched() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-foreign-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        for (name, original) in [
            ("empty.json", &b"{}"[..]),
            ("foreign.json", &br#"{"name":"not stats","version":1}"#[..]),
            ("binary.bin", &[0xff, 0x00, 0x01][..]),
        ] {
            let path = dir.join(name);
            std::fs::write(&path, original).unwrap();
            let m = Meter::open(path.clone());
            m.record_at(sample(None, UsageCounts::default()), now());
            m.flush().unwrap();
            assert_eq!(
                std::fs::read(&path).unwrap(),
                original,
                "{name} must not be overwritten"
            );
            let v = m.render_at(Range::H24, &[], now());
            assert_eq!(v["persistence"], "blocked", "{name}: {v}");
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn incompatible_newer_file_is_left_untouched() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-incompat-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let original = br#"{"marker":"ollamux-stats","version":99,"totals":"not-an-object"}"#;
        std::fs::write(&path, original).unwrap();
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(
            m.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn older_versioned_file_is_blocked_without_migration() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-old-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let original = br#"{"marker":"ollamux-stats","version":1,"totals":{"requests":7}}"#;
        std::fs::write(&path, original).unwrap();
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(
            m.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn non_regular_stats_path_is_blocked() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-dir-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        std::fs::create_dir(&path).unwrap();
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert!(path.is_dir());
        assert_eq!(
            m.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn second_instance_is_blocked_by_owner_lock() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-lock-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let first = Meter::open(path.clone());
        let second = Meter::open(path.clone());
        assert_eq!(
            second.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        drop(first);
        let third = Meter::open(path.clone());
        assert_ne!(
            third.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        drop(second);
        drop(third);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn oversized_stats_file_is_blocked_without_reading() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-big-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(MAX_STATS_FILE + 1).unwrap();
        drop(f);
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_STATS_FILE + 1);
        assert_eq!(
            m.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn runtime_newer_file_blocks_flush() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-runtime-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        // A future v2 process writes the path after this meter started.
        let newer = br#"{"marker":"ollamux-stats","version":99,"totals":{"requests":999}}"#;
        std::fs::write(&path, newer).unwrap();
        m.flush().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), newer);
        let v = m.render_at(Range::H24, &[], now());
        assert_eq!(v["persistence"], "blocked");
        assert!(v["persistence_note"].as_str().unwrap().contains("changed"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cached_target_stamp_does_not_hide_a_replacement() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-stamp-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let m = Meter::open(path.clone());
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert!(path.exists(), "first write creates the file");

        // Replace the file we just wrote. The cached stamp must not let the
        // next flush skip the marker/version check and clobber it.
        let foreign = br#"{"name":"not stats"}"#;
        std::fs::write(&path, foreign).unwrap();
        m.record_at(sample(None, UsageCounts::default()), now());
        m.flush().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), foreign);
        assert_eq!(
            m.render_at(Range::H24, &[], now())["persistence"],
            "blocked"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn flush_failures_are_reported() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-err-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let m = Meter::open(path);
        m.record_at(sample(None, UsageCounts::default()), now());

        // Make the directory read-only after the meter acquired its lock,
        // so creating the temp file fails.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let err = m.flush();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(err.is_err(), "{err:?}");
        let v = m.render_at(Range::H24, &[], now());
        assert!(v["last_flush_error"].as_str().is_some(), "{v}");
        assert_eq!(v["persistence"], "error");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn temp_names_are_unique() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-tmp-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("stats.json");
        let (a, fa) = create_tmp(&target).unwrap();
        let (b, fb) = create_tmp(&target).unwrap();
        assert_ne!(a, b);
        drop((fa, fb));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn parses_responses_usage_non_streaming() {
        let body = br#"{"output":[],"usage":{"input_tokens":20,"output_tokens":12,"total_tokens":32,"input_tokens_details":{"cached_tokens":5}}}"#;
        let u = parse_body_usage(body);
        assert!(u.reported);
        assert_eq!((u.input, u.output, u.cached), (20, 12, 5));
    }

    #[test]
    fn parses_responses_streaming_event() {
        let mut s = UsageSniffer::new();
        s.feed(b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":20,\"output_tokens\":12,\"total_tokens\":32}}}\n\n");
        s.finish();
        let u = s.usage();
        assert!(u.reported);
        assert_eq!((u.input, u.output), (20, 12));
    }

    #[test]
    fn total_tokens_fills_only_a_missing_side() {
        // Explicit completion_tokens must survive even when total_tokens is
        // present and prompt_tokens is missing.
        let u = parse_body_usage(br#"{"usage":{"completion_tokens":20,"total_tokens":120}}"#);
        assert_eq!((u.input, u.output, u.reported), (100, 20, true));

        let u = parse_body_usage(
            br#"{"usage":{"prompt_tokens":100,"completion_tokens":7,"total_tokens":120}}"#,
        );
        assert_eq!((u.input, u.output), (100, 7));
    }

    #[test]
    fn total_only_usage_is_not_split_or_reported() {
        let u = parse_body_usage(br#"{"usage":{"total_tokens":120}}"#);
        assert!(!u.reported);
        assert_eq!((u.input, u.output), (0, 0));
    }

    #[test]
    fn null_preferred_field_falls_back_to_the_other_name() {
        let u = parse_body_usage(
            br#"{"usage":{"prompt_tokens":null,"input_tokens":20,"completion_tokens":null,"output_tokens":12}}"#,
        );
        assert!(u.reported);
        assert_eq!((u.input, u.output), (20, 12));
    }

    #[test]
    fn null_cached_details_fall_back_to_the_next_cached_field() {
        // A present-but-null cached field must not suppress a populated
        // sibling: prompt_tokens_details wins only when it resolves to a
        // number, then input_tokens_details, then the top-level field.
        let u = parse_body_usage(
            br#"{"usage":{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":null},"input_tokens_details":{"cached_tokens":40}}}"#,
        );
        assert_eq!(u.cached, 40);
        let u = parse_body_usage(
            br#"{"usage":{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":null},"input_tokens_details":{"cached_tokens":null},"cached_tokens":30}}"#,
        );
        assert_eq!(u.cached, 30);
    }

    #[test]
    fn second_usage_object_does_not_overwrite_the_first() {
        let u = parse_body_usage(
            br#"{"usage":{"prompt_tokens":20,"completion_tokens":12},"response":{"usage":{"total_tokens":999}}}"#,
        );
        assert_eq!((u.input, u.output), (20, 12));
    }

    #[test]
    fn native_counters_take_precedence_over_usage() {
        let body = br#"{"prompt_eval_count":10,"eval_count":2,"usage":{"prompt_tokens":999,"completion_tokens":999}}"#;
        let u = parse_body_usage(body);
        assert_eq!((u.input, u.output), (10, 2));
    }

    #[test]
    fn escaped_counter_text_is_not_usage() {
        // The body is deliberately truncated (parse failure) and the
        // counter-looking text is inside a JSON string.
        let body = br#"...content: \"prompt_tokens\": 99..."#;
        let u = parse_body_usage(body);
        assert!(!u.reported);
    }

    #[test]
    fn key_fingerprints_distinguish_equal_suffixes() {
        assert_eq!(key_fingerprint("abc"), key_fingerprint("abc"));
        assert_ne!(
            key_fingerprint("secret-abcd1"),
            key_fingerprint("secret-abcd2")
        );
    }

    #[test]
    fn endpoint_cap_evicts_junk_and_keeps_token_endpoints() {
        let m = Meter::in_memory();
        for i in 0..(ENDPOINT_CAP + 20) {
            let endpoint = format!("api/junk-{i}");
            let mut s = sample(None, UsageCounts::default());
            s.endpoint = &endpoint;
            s.usage_expected = false;
            m.record_at(s, now());
        }
        let mut s = sample(None, UsageCounts::default());
        s.endpoint = "api/chat";
        m.record_at(s, now());
        let v = m.render_at(Range::H24, &[], now());
        let endpoints = v["endpoints"].as_array().unwrap();
        assert!(
            endpoints.iter().any(|r| r["endpoint"] == "api/chat"),
            "pinned token endpoint must survive junk flood: {v}"
        );
        assert!(endpoints.len() <= ENDPOINT_CAP);
    }

    #[test]
    fn window_totals_sum_the_selected_buckets() {
        let m = Meter::in_memory();
        m.record_at(
            sample(
                Some(("alice", "1234", "id1234")),
                UsageCounts {
                    input: 10,
                    cached: 0,
                    output: 2,
                    reported: true,
                },
            ),
            now(),
        );
        let v = m.render_at(Range::H24, &[], now());
        assert_eq!(v["window_totals"]["requests"], 1);
        assert_eq!(v["window_totals"]["input_tokens"], 10);
        assert_eq!(v["totals_scope"], "lifetime");
    }

    #[test]
    fn present_keys_are_applied_even_with_no_stats_file() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-present-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");
        let present = vec![(
            "alice".to_string(),
            "id-alice".to_string(),
            "1234".to_string(),
        )];
        let m = Meter::open_with_present(path, &present);
        let v = m.render_at(Range::H24, &present, now());
        let key = &v["keys"][0];
        assert_eq!(key["account"], "alice");
        assert_eq!(key["id"], "id-alice");
        assert_eq!(key["requests"], 0);
        assert_eq!(key["present"], true);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn present_key_absent_from_full_history_gets_a_row() {
        let rows: Vec<KeyRow> = (0..(KEY_CAP - 1))
            .map(|i| KeyRow {
                account: "alice".to_string(),
                id: format!("old-{i}"),
                suffix: format!("{i:04}"),
                last_seen: i as u64,
                metrics: Metrics {
                    requests: 1,
                    ..Metrics::default()
                },
            })
            .collect();
        let present = vec![(
            "alice".to_string(),
            "current".to_string(),
            "9999".to_string(),
        )];
        let mut capped = cap_keys(rows, KEY_CAP, &present);
        ensure_present_keys(&mut capped, &present);
        assert!(
            capped
                .iter()
                .any(|r| r.id == "current" && r.metrics.requests == 0),
            "current key must have its own zero row"
        );
        assert!(capped.iter().any(|r| r.account == "<other>"));
        assert!(capped.len() <= KEY_CAP);
    }

    #[test]
    fn cap_keys_prioritises_present_keys() {
        let mut rows: Vec<KeyRow> = (0..(KEY_CAP + 5))
            .map(|i| KeyRow {
                account: "alice".to_string(),
                id: format!("old-{i}"),
                suffix: format!("{i:04}"),
                last_seen: 0,
                metrics: Metrics {
                    requests: 1,
                    ..Metrics::default()
                },
            })
            .collect();
        // The present key is the last (oldest insertion order) row.
        rows.push(KeyRow {
            account: "alice".to_string(),
            id: "current".to_string(),
            suffix: "9999".to_string(),
            last_seen: 0,
            metrics: Metrics {
                requests: 1,
                ..Metrics::default()
            },
        });
        let present = vec![(
            "alice".to_string(),
            "current".to_string(),
            "9999".to_string(),
        )];
        let capped = cap_keys(rows, KEY_CAP, &present);
        assert!(capped.iter().any(|r| r.id == "current"));
        assert!(capped.len() <= KEY_CAP);
    }

    #[test]
    fn parses_native_prompt_cache_count() {
        let body = br#"{"prompt_eval_count":100,"prompt_eval_cached_count":30,"eval_count":5}"#;
        let u = parse_body_usage(body);
        assert!(u.reported);
        assert_eq!((u.input, u.cached, u.output), (100, 30, 5));
        let u = parse_body_usage(br#"{"prompt_eval_cached_count":30}"#);
        assert!(!u.reported, "cached-only is not a usable split");
    }

    #[test]
    fn oversized_single_line_still_recovers_tail_counters() {
        let mut s = UsageSniffer::new();
        let mut line = String::from("{\"embeddings\":[\"");
        line.push_str(&"x".repeat(MAX_LINE * 2));
        line.push_str("\"],\"prompt_eval_count\":50,\"eval_count\":0}");
        s.feed(line.as_bytes());
        s.finish();
        let u = s.usage();
        assert!(u.reported);
        assert_eq!((u.input, u.output), (50, 0));
    }

    #[test]
    fn valid_json_without_usage_is_not_scanned() {
        // A model description containing a counter-looking key must not be
        // mistaken for a measurement.
        let body = br#"{"description":"\"prompt_eval_count\": 99"}"#;
        let u = parse_body_usage(body);
        assert!(!u.reported);
    }

    #[test]
    fn disabled_meter_retains_nothing() {
        let m = Meter::disabled();
        m.record_at(
            sample(
                Some(("alice", "1234", "id1234")),
                UsageCounts {
                    input: 10,
                    cached: 0,
                    output: 2,
                    reported: true,
                },
            ),
            now(),
        );
        let present = vec![(
            "alice".to_string(),
            "id1234".to_string(),
            "1234".to_string(),
        )];
        let v = m.render(Range::H24, &present);
        assert_eq!(v["enabled"], false);
        assert_eq!(v["persistence"], "disabled");
        assert_eq!(v["totals"]["requests"], 0);
        assert_eq!(v["keys"].as_array().unwrap().len(), 0);
        assert_eq!(v["models"].as_array().unwrap().len(), 0);
        assert_eq!(v["endpoints"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn key_rows_and_accounts_are_capped() {
        let m = Meter::in_memory();
        for i in 0..(KEY_CAP + 10) {
            let account = format!("acct-{i}");
            let suffix = format!("s{i}");
            let mut s = sample(
                Some((&account[..], &suffix[..], &suffix[..])),
                UsageCounts::default(),
            );
            s.usage_expected = false;
            m.record_at(s, now());
        }
        let v = m.render_at(Range::H24, &[], now());
        let keys = v["keys"].as_array().unwrap();
        assert!(keys.len() <= KEY_CAP, "{} keys", keys.len());
        assert!(
            keys.iter()
                .any(|r| r["account"] == "<other>" && r["suffix"] == "<other>"),
            "key overflow row present"
        );
        let accounts = v["accounts"].as_array().unwrap();
        assert!(accounts.len() <= ACCOUNT_CAP, "{} accounts", accounts.len());
        assert!(
            accounts.iter().any(|r| r["account"] == "<other>"),
            "account overflow row present"
        );
    }

    #[test]
    fn cap_named_keeps_the_most_recent_last_seen_rows() {
        let rows: Vec<NamedRow> = (0..5)
            .map(|i| NamedRow {
                name: format!("m{i}"),
                last_seen: i as u64,
                metrics: Metrics {
                    requests: 1,
                    ..Metrics::default()
                },
            })
            .collect();
        let capped = cap_named(rows, 3, &|_| false);
        assert_eq!(capped.len(), 3);
        let names: Vec<&str> = capped.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"m4") && names.contains(&"m3"), "{names:?}");
    }

    #[test]
    fn cap_named_folds_an_exact_cap_without_other() {
        let rows: Vec<NamedRow> = (0..ENDPOINT_CAP)
            .map(|i| NamedRow {
                name: format!("api/{i}"),
                last_seen: 0,
                metrics: Metrics {
                    requests: 1,
                    ..Metrics::default()
                },
            })
            .collect();
        let capped = cap_named(rows, ENDPOINT_CAP, &is_token_endpoint);
        assert_eq!(capped.len(), ENDPOINT_CAP);
        assert!(capped.iter().any(|r| r.name == "<other>"));
    }

    #[test]
    fn prune_drops_old_buckets() {
        let mut st = State::fresh();
        st.hourly.insert(now() - 100 * 3600, Metrics::default());
        st.daily.insert(now() - 100 * 86400, Metrics::default());
        prune(&mut st, now());
        assert!(st.hourly.is_empty());
        assert!(st.daily.is_empty());
    }

    #[test]
    fn persistence_round_trips() {
        let dir = std::env::temp_dir().join(format!(
            "ollamux-meter-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stats.json");

        let m = Meter::open(path.clone());
        m.record_at(
            sample(
                Some(("alice", "1234", "id1234")),
                UsageCounts {
                    input: 7,
                    cached: 0,
                    output: 2,
                    reported: true,
                },
            ),
            now(),
        );
        m.flush().unwrap();
        assert!(path.exists());
        drop(m);

        let reloaded = Meter::open(path.clone());
        let v = reloaded.render_at(Range::H24, &[], now());
        assert_eq!(v["totals"]["input_tokens"], 7);
        assert_eq!(v["totals"]["output_tokens"], 2);
        assert_eq!(v["keys"][0]["suffix"], "1234");
        drop(reloaded);

        // A corrupt file is preserved (blocked), not overwritten.
        std::fs::write(&path, b"not json").unwrap();
        let fresh = Meter::open(path.clone());
        let v = fresh.render_at(Range::H24, &[], now());
        assert_eq!(v["totals"]["requests"], 0);
        assert_eq!(v["persistence"], "blocked");
        fresh.flush().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"not json");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn iso8601_formats_known_instants() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso8601(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
