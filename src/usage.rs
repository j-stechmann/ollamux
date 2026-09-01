//! Usage introspection for the undocumented Ollama Cloud usage endpoint.
//!
//! `GET https://ollama.com/api/usage` (undocumented; the same one the
//! ollama.com settings page uses) answers, per API key, with a JSON body
//! roughly shaped like:
//!
//! ```json
//! {"limits":{"session":{"usage":0.037,"models":[…]},
//!            "weekly": {"usage":0.007,"models":[…]}},
//!  "activity":{"cost":"$1.23"}}
//! ```
//!
//! `usage` is a *fraction of the plan cap* (0.0–1.0), not a token count,
//! `models[].request_count` is a request count, and there are no reset
//! timestamps (the session window rolls over roughly every 5 hours).
//!
//! The endpoint is undocumented and may change or disappear at any time:
//! decoding is therefore maximally tolerant — any shape drift becomes a
//! per-key error string in `/_usage`, never a panic, never a 5xx.
//!
//! Introspection is strictly read-only: usage fetches consume no pool
//! slots, and an auth failure here is *reported*, never `mark_dead` (only
//! client traffic drives key health; a truly dead key dies on its next
//! real request). /_keys embedding never fetches upstream either: it is
//! a pure in-memory read that merely renders the latest snapshot.

use crate::pool::Pool;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Serve-at-most-this-age snapshot before a refresh is considered. The
/// session window resets ~5h, so polling harder buys nothing.
pub const USAGE_TTL: Duration = Duration::from_secs(60);
/// Minimum interval between forced refreshes (`?refresh=1` spam guard).
const MIN_REFRESH: Duration = Duration::from_secs(5);
/// Upstream timeouts for usage fetches (bounded; never a whole-request
/// timeout beyond these).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(10);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Same poison-tolerance as the pool (pool.rs lock()).
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Re-wrap a guard from `try_lock` with the same poison tolerance (the
/// `Err` arm of try_lock only carries a PoisonError, not a WouldBlock).
fn lock_unpoisoned<'a, T>(
    g: Result<MutexGuard<'a, T>, std::sync::PoisonError<MutexGuard<'a, T>>>,
) -> MutexGuard<'a, T> {
    g.unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Wire model (tolerant decode of the undocumented payload)
// ---------------------------------------------------------------------------

/// One model's request tally inside a usage window.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ModelUsage {
    pub name: String,
    pub request_count: u64,
}

impl<'de> serde::Deserialize<'de> for ModelUsage {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Parts {
            #[serde(default)]
            name: Option<String>,
            #[serde(default, alias = "request_count", alias = "count")]
            request_count: Option<u64>,
        }
        let p = Parts::deserialize(d)?;
        Ok(ModelUsage {
            name: p.name.unwrap_or_default(),
            request_count: p.request_count.unwrap_or(0),
        })
    }
}

/// One metered window (session or weekly). `usage` is a fraction 0.0–1.0
/// of the plan's cap; absent → `None` (rendered as `null`, not 0.0).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct UsageWindow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<f64>,
    /// Top models of the window (upstream may omit them entirely).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelUsage>,
}

impl<'de> serde::Deserialize<'de> for UsageWindow {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Parts {
            #[serde(default)]
            usage: Option<f64>,
            #[serde(default)]
            models: Option<Vec<ModelUsage>>,
        }
        let p = Parts::deserialize(d)?;
        Ok(UsageWindow {
            usage: p.usage.filter(|u| u.is_finite()),
            models: p.models.unwrap_or_default(),
        })
    }
}

/// Decoded `/api/usage` payload. Every field is optional: shape drift on
/// the undocumented endpoint degrades to a per-key error, never a panic.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct UsagePayload {
    pub session: UsageWindow,
    pub weekly: UsageWindow,
    /// Reported 4-week rolling cost, passed through verbatim when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<String>,
}

impl<'de> serde::Deserialize<'de> for UsagePayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize, Default)]
        struct Limits {
            #[serde(default)]
            session: Option<UsageWindow>,
            #[serde(default)]
            weekly: Option<UsageWindow>,
        }
        #[derive(serde::Deserialize)]
        struct Activity {
            #[serde(default)]
            cost: Option<String>,
        }
        #[derive(serde::Deserialize)]
        struct Wire {
            #[serde(default)]
            limits: Option<Limits>,
            #[serde(default)]
            activity: Option<Activity>,
            // Absorb unknown top-level fields so additions upstream don't
            // turn into decode errors here.
            #[serde(flatten)]
            _rest: serde_json::Map<String, serde_json::Value>,
        }
        let w = Wire::deserialize(d)?;
        let limits = w.limits.unwrap_or_default();
        Ok(UsagePayload {
            session: limits.session.unwrap_or_default(),
            weekly: limits.weekly.unwrap_or_default(),
            cost: w.activity.and_then(|a| a.cost),
        })
    }
}

impl UsagePayload {
    /// True when the body carried at least one number we understand. A
    /// payload without any session/weekly usage is shape drift (the real
    /// endpoint always has both) and must surface as an error, not zeros.
    fn plausible(&self) -> bool {
        self.session.usage.is_some() || self.weekly.usage.is_some()
    }
}

// ---------------------------------------------------------------------------
// Per-key results and snapshots
// ---------------------------------------------------------------------------

/// One per-key fetch outcome; the "ok/error" vocabulary is generated by
/// ollamux and suffix-only — upstream error bodies are never relayed here
/// (an upstream echoing the Bearer secret must not leak into /_usage).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct KeyUsage {
    pub index: usize,
    pub suffix: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl KeyUsage {
    fn failed(index: usize, suffix: String, error: String) -> KeyUsage {
        KeyUsage {
            index,
            suffix,
            ok: false,
            status: None,
            session: None,
            weekly: None,
            session_pct: None,
            weekly_pct: None,
            models: Vec::new(),
            cost: None,
            error: Some(error),
        }
    }

    /// Test/fixture constructor for out-of-crate routing tests.
    #[doc(hidden)]
    pub fn for_test(index: usize, session: Option<f64>) -> KeyUsage {
        KeyUsage {
            index,
            suffix: format!("sfx{index}"),
            ok: session.is_some(),
            status: session.map(|_| 200),
            session,
            weekly: session,
            session_pct: session.map(pct),
            weekly_pct: session.map(pct),
            models: Vec::new(),
            cost: None,
            error: None,
        }
    }
}

/// Index-aligned snapshot of one fetch_all round.
#[derive(Debug)]
pub struct UsageSnapshot {
    /// When the fetch completed (age/freshness reference).
    pub fetched_at: Instant,
    /// Element i describes pool key i (indices are stable: the key list is
    /// fixed at startup and never mutated).
    pub keys: Vec<KeyUsage>,
}

impl UsageSnapshot {
    /// Test/fixture constructor for out-of-crate tests (routing tests).
    #[doc(hidden)]
    pub fn for_test(keys: Vec<KeyUsage>, fetched_at: Instant) -> UsageSnapshot {
        UsageSnapshot { fetched_at, keys }
    }

    /// Unix seconds for the client (fetched_at is a monotonic clock).
    pub fn updated_unix(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|now| {
                now.as_secs()
                    .saturating_sub(self.fetched_at.elapsed().as_secs())
            })
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Tracker
// ---------------------------------------------------------------------------

/// Fetch function seam: real impl hits the network; tests inject.
type FetchFn = dyn Fn(usize, &str, &str) -> Result<UsagePayload, FetchError> + Send + Sync;
type SnapshotCell = Mutex<Option<Arc<UsageSnapshot>>>;

pub struct UsageTracker {
    pool: Arc<Pool>,
    ttl: Duration,
    /// HTTP layer (seam: injected closures in unit tests skip ureq).
    fetch: Box<FetchFn>,
    /// Latest completed snapshot; `None` until the first fetch lands.
    snapshot: SnapshotCell,
    /// Single-flight guard for refreshes.
    fetch_mu: Mutex<()>,
}

/// Per-key fetch failure. upstream HTTP error bodies are withheld: they
/// may reflect the key and must never be relayed into /_usage.
#[derive(Debug)]
pub enum FetchError {
    /// Upstream answered with an HTTP error status.
    Status(u16),
    /// Transport-level failure (DNS, TLS, timeout…).
    Network(String),
}

fn parse_payload(body: &str) -> Result<UsagePayload, String> {
    // The serde error text embeds offending values from the body verbatim
    // (e.g. a hostile upstream echoing the Authorization secret into a
    // wrong-typed field). Only the location is relayed, never content.
    serde_json::from_str::<UsagePayload>(body)
        .map(|p| {
            if !p.plausible() {
                return Err("endpoint changed (no usage data in payload)".to_string());
            }
            Ok(p)
        })
        .unwrap_or_else(|_| Err("endpoint changed (unexpected payload)".to_string()))
}

impl UsageTracker {
    pub fn new(pool: Arc<Pool>, upstream: &str) -> UsageTracker {
        // Mirrors proxy.rs agent settings (redirects(0), timeouts) so the
        // Authorization header can never ride a redirect off-site and a
        // slow upstream can't stall a worker forever.
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .redirects(0)
            .build();
        let upstream = upstream.trim_end_matches('/').to_string();
        let base = upstream.clone();
        let agent = Arc::new(agent);
        UsageTracker {
            pool,
            ttl: USAGE_TTL,
            fetch: Box::new(move |index, suffix_unused, secret| {
                let _ = (index, suffix_unused);
                fetch_usage_http(&agent, &base, secret)
            }),
            snapshot: Mutex::new(None),
            fetch_mu: Mutex::new(()),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> UsageTracker {
        self.ttl = ttl;
        self
    }

    /// Swap the fetch implementation (tests: hermetic payload injection).
    #[doc(hidden)]
    pub fn with_fetch<F>(mut self, f: F) -> UsageTracker
    where
        F: Fn(usize, &str, &str) -> Result<UsagePayload, FetchError> + Send + Sync + 'static,
    {
        self.fetch = Box::new(f);
        self
    }

    /// Current snapshot if one exists (pure read; never fetches; used for
    /// the /_keys embed — incident readers must not trigger upstream calls).
    pub fn peek(&self) -> Option<Arc<UsageSnapshot>> {
        lock(&self.snapshot).clone()
    }

    /// Best-effort snapshot: serves fresh data, or stale data while another
    /// caller refreshes, or (first request only) blocks briefly for the
    /// first fetch. Used by GET /_usage.
    pub fn get(&self) -> Arc<UsageSnapshot> {
        if let Some(fresh) = self.fresh_snapshot() {
            return fresh;
        }
        match self.try_lock_fetch() {
            // Someone else is fetching: serve whatever exists (any age).
            None => match self.peek() {
                Some(s) => s,
                // First request ever and a fetch is in flight: wait for it
                // rather than answering with an empty snapshot.
                None => self.wait_for_snapshot(),
            },
            // We own the refresh: double-check freshness, then fetch.
            Some(guard) => {
                if let Some(s) = self.fresh_snapshot() {
                    return s;
                }
                self.do_fetch_inner(&guard)
            }
        }
    }

    /// Forced refresh (?refresh=1): block until a fetch attempt completes
    /// and return the newest snapshot. Failed fetches keep the previous
    /// snapshot (never overwrite good data with nothing).
    pub fn refresh(&self) -> Arc<UsageSnapshot> {
        let guard = lock(&self.fetch_mu);
        // Min-interval guard: a ?refresh=1 loop must not hammer upstream.
        if let Some(s) = self.peek() {
            if s.fetched_at.elapsed() < MIN_REFRESH {
                return s;
            }
        }
        self.do_fetch_inner(&guard)
    }

    /// Background poller step (--usage-aware): refresh when the TTL has
    /// elapsed. Returns true if a fetch actually ran (and was published).
    /// Uses take-the-lock semantics so a poller tick overlapping an
    /// on-demand refresh is a cheap no-op.
    pub fn tick(&self) -> bool {
        if self.fresh_snapshot().is_some() {
            return false;
        }
        let guard = match self.try_lock_fetch() {
            Some(g) => g,
            None => return false,
        };
        // Re-check: an on-demand refresh may have completed while we
        // contended for the lock.
        if self.fresh_snapshot().is_some() {
            return false;
        }
        self.do_fetch_inner(&guard);
        true
    }

    /// Try to grab the fetch guard without blocking.
    fn try_lock_fetch(&self) -> Option<MutexGuard<'_, ()>> {
        match self.fetch_mu.try_lock() {
            Ok(g) => Some(lock_unpoisoned(Ok(g))),
            Err(TryLockError::Poisoned(p)) => Some(lock_unpoisoned(Err(p))),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    fn fresh_snapshot(&self) -> Option<Arc<UsageSnapshot>> {
        let snap = self.peek()?;
        (snap.fetched_at.elapsed() < self.ttl).then_some(snap)
    }

    /// The one fetch routine (callers must hold the fetch guard). On
    /// failure the existing snapshot is kept untouched; on success a new
    /// snapshot replaces it and the pool is notified (quota-aware routing).
    fn do_fetch_inner(&self, _guard: &MutexGuard<'_, ()>) -> Arc<UsageSnapshot> {
        let results = self.fetch_all();
        let mut keys = Vec::with_capacity(self.pool.len());
        for (i, res) in results.into_iter().enumerate() {
            let suffix = self.pool.suffix_of(i);
            keys.push(match res {
                Ok(p) => {
                    let (session, weekly, models, cost) = p.into_parts();
                    KeyUsage {
                        index: i,
                        suffix,
                        ok: true,
                        status: Some(200),
                        session_pct: session.map(pct),
                        weekly_pct: weekly.map(pct),
                        session,
                        weekly,
                        models,
                        cost,
                        error: None,
                    }
                }
                Err(FetchError::Status(code)) => KeyUsage::failed(
                    i,
                    suffix,
                    match code {
                        401 | 403 => "unauthorized".to_string(),
                        404 => "endpoint gone (upstream 404)".to_string(),
                        429 => "rate limited (upstream 429)".to_string(),
                        other => format!("upstream HTTP {other}"),
                    },
                ),
                Err(FetchError::Network(e)) => KeyUsage::failed(i, suffix, format!("network: {e}")),
            });
        }
        // If every key failed, don't overwrite good data with nothing: a
        // transient failure (network blip, one 429) would otherwise wipe the
        // usage mask and re-admit over-quota keys for a full TTL. Keep the
        // previous snapshot (its usage is already published to the pool);
        // its age drives the next retry via TTL/MIN_REFRESH. First-ever
        // failures must still land so /_usage can surface them.
        if keys.iter().all(|k| !k.ok) {
            if let Some(prev) = self.peek() {
                return prev;
            }
        }
        let snap = Arc::new(UsageSnapshot {
            fetched_at: Instant::now(),
            keys,
        });
        *lock(&self.snapshot) = Some(Arc::clone(&snap));
        self.pool.publish_usage(&snap);
        snap
    }

    /// Parallel fan-out: one thread per key, index-aligned results. Never
    /// touches pool state (no admits, no health marks).
    fn fetch_all(&self) -> Vec<Result<UsagePayload, FetchError>> {
        let n = self.pool.len();
        let mut results: Vec<Option<Result<UsagePayload, FetchError>>> =
            (0..n).map(|_| None).collect();
        std::thread::scope(|s| {
            for (i, slot) in results.iter_mut().enumerate() {
                let fetch = &self.fetch;
                let secret = self.pool.secret_of(i).to_string();
                s.spawn(move || {
                    *slot = Some(fetch(i, &self.pool.suffix_of(i), &secret));
                });
            }
        });
        results
            .into_iter()
            .map(|slot| slot.unwrap_or_else(|| Err(FetchError::Network("no result".into()))))
            .collect()
    }

    /// Block until any snapshot exists (first-request-with-contended-fetch
    /// path only; bounded by the in-flight fetch's own timeouts).
    fn wait_for_snapshot(&self) -> Arc<UsageSnapshot> {
        let guard = lock(&self.fetch_mu);
        if let Some(s) = self.peek() {
            return s;
        }
        // We hold the fetch lock and there is no snapshot: we are the
        // refresher now.
        self.do_fetch_inner(&guard)
    }
}

fn pct(f: f64) -> f64 {
    // One decimal place (3.7) matches the percentages the upstream's own
    // settings UI shows; serde_json prints the shortest round-trip form.
    (f.clamp(0.0, 1.0) * 1000.0).round() / 10.0
}

impl UsagePayload {
    fn into_parts(self) -> (Option<f64>, Option<f64>, Vec<ModelUsage>, Option<String>) {
        (
            self.session.usage,
            self.weekly.usage,
            self.session.models,
            self.cost,
        )
    }
}

fn fetch_usage_http(
    agent: &ureq::Agent,
    upstream: &str,
    secret: &str,
) -> Result<UsagePayload, FetchError> {
    let url = format!("{upstream}/api/usage");
    let resp = agent
        .get(&url)
        .set("Authorization", &format!("Bearer {secret}"))
        .set("Accept", "application/json")
        .call()
        .map_err(|e| match e {
            ureq::Error::Status(code, _) => FetchError::Status(code),
            other => FetchError::Network(other.to_string()),
        })?;
    if resp.status() != 200 {
        // 3xx surfaces as Ok with redirects(0); treat non-200 as failure.
        return Err(FetchError::Status(resp.status()));
    }
    let body = resp
        .into_string()
        .map_err(|e| FetchError::Network(e.to_string()))?;
    parse_payload(&body).map_err(FetchError::Network)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failing_tracker() -> UsageTracker {
        let pool = Arc::new(Pool::new(vec![("omk-usage-dead1".into(), 1)], 4, false));
        UsageTracker::new(pool, "https://ollama.com")
            .with_fetch(|_, _, _| Err(FetchError::Status(401)))
    }

    #[test]
    fn decodes_full_payload() {
        let body = r#"{
            "limits": {
                "session": {"usage": 0.037, "models": [
                    {"name": "gpt-oss:120b", "request_count": 42},
                    {"name": "qwen3-coder:480b", "request_count": 7}
                ]},
                "weekly": {"usage": 0.007}
            },
            "activity": {"cost": "$1.23"}
        }"#;
        let p: UsagePayload = serde_json::from_str(body).unwrap();
        assert_eq!(p.session.usage, Some(0.037));
        assert_eq!(p.weekly.usage, Some(0.007));
        assert_eq!(p.session.models.len(), 2);
        assert_eq!(p.cost.as_deref(), Some("$1.23"));
        assert!(p.plausible());
    }

    #[test]
    fn tolerates_missing_fields() {
        let p: UsagePayload = serde_json::from_str("{}").unwrap();
        assert_eq!(p.session.usage, None);
        assert_eq!(p.weekly.usage, None);
        assert!(p.session.models.is_empty());
        assert_eq!(p.cost, None);
        // Absence of all numbers is drift, not zeros.
        assert!(!p.plausible());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let body = r#"{"limits":{"session":{"usage":0.5},"brand_new":{"x":1}},"future":42}"#;
        let p: UsagePayload = serde_json::from_str(body).unwrap();
        assert_eq!(p.session.usage, Some(0.5));
        assert!(p.plausible());
    }

    #[test]
    fn drift_is_reported_not_panic() {
        // Garbage body → decode error from the seam, surfaced as failure.
        let pool = Arc::new(Pool::new(vec![("omk-usage-drift".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com").with_fetch(|_, _, _| {
            Err(FetchError::Network(
                "endpoint changed (unexpected payload): eof".into(),
            ))
        });
        let snap = t.get();
        assert!(!snap.keys[0].ok);
        assert!(
            snap.keys[0]
                .error
                .as_deref()
                .unwrap()
                .contains("endpoint changed")
        );
    }

    #[test]
    fn parse_errors_never_echo_body_content() {
        // A hostile upstream answering 200 with the Authorization secret
        // echoed into a wrong-typed field must not leak it into the error
        // string (serde's own message embeds offending values verbatim).
        let body = r#"{"limits":{"session":{"usage":"Bearer omk-secret1234"}}}"#;
        let err = parse_payload(body).unwrap_err();
        assert!(
            !err.contains("omk-secret1234"),
            "parse error must not relay body content: {err}"
        );
        assert!(err.contains("endpoint changed"));
    }

    #[test]
    fn fetch_maps_status_and_network_errors() {
        let pool = Arc::new(Pool::new(vec![("omk-usage-er1".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_fetch(|_, _, _| Err(FetchError::Status(429)));
        let snap = t.get();
        assert!(!snap.keys[0].ok);
        assert_eq!(
            snap.keys[0].error.as_deref(),
            Some("rate limited (upstream 429)")
        );
    }

    #[test]
    fn get_serves_fresh_then_refetches_after_ttl() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let pool = Arc::new(Pool::new(vec![("omk-usage-ttl1".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_ttl(Duration::from_millis(50))
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::from_str(r#"{"limits":{"session":{"usage":0.25}}}"#).unwrap())
            });
        let _ = t.get();
        let _ = t.get(); // fresh: no second fetch
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
        std::thread::sleep(Duration::from_millis(80));
        let _ = t.get(); // stale → refetch
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn single_flight_burst_is_one_fetch() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        use std::sync::Barrier;
        let pool = Arc::new(Pool::new(vec![("omk-usage-sf01".into(), 1)], 4, false));
        let t = Arc::new(
            UsageTracker::new(pool, "https://ollama.com").with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Give the burst time to pile up behind the fetch mutex.
                std::thread::sleep(Duration::from_millis(50));
                Ok(serde_json::from_str(r#"{"limits":{"weekly":{"usage":0.1}}}"#).unwrap())
            }),
        );
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let t = Arc::clone(&t);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                t.get()
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            CALLS.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "concurrent callers must share one fetch"
        );
    }

    #[test]
    fn failed_refresh_keeps_previous_snapshot() {
        static FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        let pool = Arc::new(Pool::new(vec![("omk-usage-keep".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com").with_fetch(|_, _, _| {
            if FAIL.load(std::sync::atomic::Ordering::Relaxed) {
                Err(FetchError::Network("boom".into()))
            } else {
                Ok(serde_json::from_str(r#"{"limits":{"session":{"usage":0.5}}}"#).unwrap())
            }
        });
        let first = t.get();
        assert!(first.keys[0].ok);
        FAIL.store(true, std::sync::atomic::Ordering::Relaxed);
        // get() with a short TTL is not MIN_REFRESH-guarded (refresh() is),
        // so drive the failed round through get(): wait past the TTL, then
        // let the stale path fetch. The previous good snapshot must survive.
        std::thread::sleep(Duration::from_millis(40));
        let again = t.get();
        assert!(again.keys[0].ok, "failed refresh must keep good data");
        assert_eq!(again.keys[0].session, Some(0.5));
    }

    #[test]
    fn peek_never_fetches() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let pool = Arc::new(Pool::new(vec![("omk-usage-pk01".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com").with_fetch(|_, _, _| {
            CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(serde_json::from_str(r#"{"limits":{"session":{"usage":0.1}}}"#).unwrap())
        });
        assert!(t.peek().is_none());
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn failing_keys_surface_as_errors_with_state_untouched() {
        let t = failing_tracker();
        let snap = t.get();
        assert!(!snap.keys[0].ok);
        assert_eq!(snap.keys[0].error.as_deref(), Some("unauthorized"));
        // Usage introspection must not touch key health: still Up.
        assert_eq!(t.pool.info()[0].state, crate::pool::State::Up);
    }

    #[test]
    fn pct_conversion_rounds_to_one_decimal() {
        // pct() keeps one decimal place: 3.7% renders as 3.7.
        assert_eq!(pct(0.0), 0.0);
        assert_eq!(pct(1.0), 100.0);
        assert_eq!(pct(0.037), 3.7);
        assert_eq!(pct(0.955), 95.5);
        assert_eq!(pct(1.5), 100.0, "values above the cap clamp to 100");
        assert_eq!(pct(-0.5), 0.0, "negative values clamp to 0");
    }

    #[test]
    fn min_refresh_guards_forced_refresh() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let pool = Arc::new(Pool::new(vec![("omk-usage-min1".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_ttl(Duration::ZERO)
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::from_str(r#"{"limits":{"session":{"usage":0.1}}}"#).unwrap())
            });
        let _ = t.refresh();
        let _ = t.refresh(); // inside MIN_REFRESH window: no second fetch
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
        let _ = t.get(); // TTL=0 → stale → get() fetches (not guarded)
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
