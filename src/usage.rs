//! Usage introspection for Ollama Cloud's **documented** balance endpoint.
//!
//! `GET https://ollama.com/api/balance` (docs.ollama.com/api/balance)
//! answers, per API key, with the plan's remaining allowance. Two shapes
//! exist, chosen by `included`'s members:
//!
//! - Legacy plans (session/weekly limits) — the common case:
//!
//! ```json
//! {"included":{"session":{"remaining_percent":75,"resets_at":"2026-10-01T07:00:00Z"},
//!              "weekly": {"remaining_percent":40,"resets_at":"2026-10-05T00:00:00Z"}},
//!  "purchased":{"balance_usd":25}}
//! ```
//!
//! - Credit plans (usage in USD):
//!
//! ```json
//! {"included":{"balance_usd":72.5,"allowance_usd":100,
//!              "period":{"from":"…","until":"…"}},
//!  "purchased":{"balance_usd":25}}
//! ```
//!
//! For legacy plans the usage fraction is the *real* plan limit straight
//! from upstream: `1 - remaining_percent/100`, with the next reset time
//! included — no configured caps, no invented denominators. For credit
//! plans there is no percent-to-report (credits drain in USD); those rows
//! surface the amounts and the panel decides what to show. ollamux
//! supports both; the aggregate covers legacy rows only.
//!
//! The endpoint is rate-limited to 10 requests per minute per user
//! (shared across keys) and the docs recommend polling once per minute —
//! USAGE_TTL (60 s) matches exactly. Decoding is maximally tolerant: any
//! shape drift becomes a per-key error string in `/_usage`, never a
//! panic, never a 5xx.
//!
//! Introspection is strictly read-only: usage fetches consume no pool
//! slots, and an auth failure here is *reported*, never `mark_dead` (only
//! client traffic drives key health; a truly dead key dies on its next
//! real request). /_keys embedding never fetches upstream either: it is
//! a pure in-memory read that merely renders the latest snapshot.
//!
//! The tracker also derives a pool-wide aggregate: every key's usage
//! fraction (a fraction of *its own plan's allowance*) is weighted by the
//! plan tier implied by its concurrency (see `tier_for`) and averaged, so
//! the total reads as the fraction of the pool's combined capacity in use —
//! a number in [0, 1] like the per-key values. Fetching is health-blind,
//! so keys on cooldown contribute like any other; error rows (e.g. a
//! dead key's own 401, its expected outcome) carry no numbers and cannot
//! contribute.

use crate::pool::Pool;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Serve-at-most-this-age snapshot before a refresh is considered. The
/// balance docs recommend polling once per minute (10 req/min limit
/// shared across keys), so this doubles as the upstream rate-limit
/// courtesy.
pub const USAGE_TTL: Duration = Duration::from_secs(60);
/// Minimum interval between upstream fetch attempts: gates forced
/// refreshes (`?refresh=1` spam guard) and backs off revalidation after
/// failed rounds, which keep the previous snapshot and thus freeze its
/// age (see `do_fetch_inner`). Overridable via `with_min_refresh` for
/// tests.
const MIN_REFRESH: Duration = Duration::from_secs(5);
/// Upstream timeouts for usage fetches (bounded; never a whole-request
/// timeout beyond these).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(10);

// Plan-tier weighting. The usage endpoint reports each key's usage as a
// fraction of that key's own plan cap, so summing or averaging raw
// fractions across a mixed pool is meaningless: a pro key at 81% of its
// (much larger) cap has burned far more capacity than a free key at 81%
// of its tiny one. Weight every key's fraction by its plan's cap,
// expressed in free-plan-cap units, and take the weighted mean — the
// aggregate then reads as "what fraction of the pool's combined capacity
// has been used", in [0, 1] like the per-key numbers. We have no
// absolute cap numbers, only relative ones: pro has 50x free's cap and
// max 5x pro's (50 * 5 = 250). Tiers are inferred from the per-key
// concurrency in the keys file (KEY:N), which matches Ollama Cloud's
// plan limits (free=1, pro=3, max=10).
/// Free tier: weight 1.0 — the unit the aggregate is denominated in.
const FREE_WEIGHT: f64 = 1.0;
/// Pro tier: 50x free's usage cap.
const PRO_WEIGHT: f64 = 50.0;
/// Max tier: 5x pro's cap (250x free).
const MAX_WEIGHT: f64 = 250.0;

/// Tier name and weight for a key with concurrency `conc` (KEY:N; N is
/// always >= 1 — parsing clamps 0 to 1 and rejects junk). More concurrency
/// than a tier's normal limit means the next tier up: exactly 1 is free,
/// 2..=3 is pro, 4 and beyond is max.
pub fn tier_for(conc: u32) -> (&'static str, f64) {
    match conc {
        1 => ("free", FREE_WEIGHT),
        2..=3 => ("pro", PRO_WEIGHT),
        _ => ("max", MAX_WEIGHT),
    }
}

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
// Wire model (tolerant decode of the documented /api/balance payload)
// ---------------------------------------------------------------------------

/// A legacy-plan limit window: percent of the plan's allowance *remaining*
/// (0–100, upstream's own number) and the next reset instant (UTC, present
/// whenever upstream knows one). `remaining` is None when the field is
/// absent or non-finite — absence is "unknown", never 0%.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BalanceLimit {
    /// Percent of the window's allowance remaining (0.0–100.0).
    pub remaining_percent: Option<f64>,
    /// Next reset instant (ISO-8601 UTC), when upstream publishes one.
    pub resets_at: Option<String>,
}

impl<'de> serde::Deserialize<'de> for BalanceLimit {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Parts {
            #[serde(default)]
            remaining_percent: Option<f64>,
            #[serde(default)]
            resets_at: Option<String>,
        }
        let p = Parts::deserialize(d)?;
        Ok(BalanceLimit {
            remaining_percent: p.remaining_percent.filter(|v| v.is_finite()),
            resets_at: p.resets_at,
        })
    }
}

impl BalanceLimit {
    /// The used fraction in [0, 1] (1 − remaining/100), clamped into range
    /// against upstream rounding drift; None when the percent is absent.
    fn used_fraction(&self) -> Option<f64> {
        self.remaining_percent.map(|r| ((100.0 - r) / 100.0).clamp(0.0, 1.0))
    }
}

/// A credit-plan `included` object: USD amounts for the current period.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IncludedBalance {
    pub balance_usd: Option<f64>,
    pub allowance_usd: Option<f64>,
}

impl<'de> serde::Deserialize<'de> for IncludedBalance {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Wire {
            #[serde(default)]
            balance_usd: Option<f64>,
            #[serde(default)]
            allowance_usd: Option<f64>,
        }
        let w = Wire::deserialize(d)?;
        Ok(IncludedBalance {
            balance_usd: w.balance_usd.filter(|v| v.is_finite()),
            allowance_usd: w.allowance_usd.filter(|v| v.is_finite()),
        })
    }
}

/// Decoded `/api/balance` payload. Legacy and credit shapes share the
/// envelope (`included` + `purchased`); which one arrived is decided by
/// `included`'s members. Everything optional: shape drift degrades to a
/// per-key error, never a panic.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UsagePayload {
    /// Legacy-plan windows (session/weekly remaining percents + resets).
    pub session: Option<BalanceLimit>,
    pub weekly: Option<BalanceLimit>,
    /// Credit-plan included USD amounts (absent on legacy plans).
    pub included_usd: Option<IncludedBalance>,
    /// Remaining unexpired purchased credits, when the account has any.
    pub purchased_usd: Option<f64>,
}

impl<'de> serde::Deserialize<'de> for UsagePayload {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize, Default)]
        struct Included {
            #[serde(default)]
            session: Option<BalanceLimit>,
            #[serde(default)]
            weekly: Option<BalanceLimit>,
            #[serde(default)]
            balance_usd: Option<f64>,
            #[serde(default)]
            allowance_usd: Option<f64>,
        }
        #[derive(serde::Deserialize, Default)]
        struct Purchased {
            #[serde(default)]
            balance_usd: Option<f64>,
        }
        #[derive(serde::Deserialize)]
        struct Wire {
            #[serde(default)]
            included: Option<Included>,
            #[serde(default)]
            purchased: Option<Purchased>,
            // Absorb unknown top-level fields so additions upstream don't
            // turn into decode errors here.
            #[serde(flatten)]
            _rest: serde_json::Map<String, serde_json::Value>,
        }
        let w = Wire::deserialize(d)?;
        let included = w.included.unwrap_or_default();
        Ok(UsagePayload {
            session: included.session,
            weekly: included.weekly,
            included_usd: (included.balance_usd.is_some() || included.allowance_usd.is_some())
                .then_some(IncludedBalance {
                    balance_usd: included.balance_usd.filter(|v| v.is_finite()),
                    allowance_usd: included.allowance_usd.filter(|v| v.is_finite()),
                }),
            purchased_usd: w.purchased.and_then(|p| p.balance_usd.filter(|v| v.is_finite())),
        })
    }
}

impl UsagePayload {
    /// True when the body carried at least one number we understand. A
    /// payload with neither shape is drift (the documented endpoint always
    /// reports one of them) and must surface as an error, not zeros.
    fn plausible(&self) -> bool {
        self.session.as_ref().is_some_and(|s| s.remaining_percent.is_some())
            || self.weekly.as_ref().is_some_and(|w| w.remaining_percent.is_some())
            || self.included_usd.as_ref().is_some_and(|i| i.balance_usd.is_some() || i.allowance_usd.is_some())
            || self.purchased_usd.is_some()
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
    /// Used fraction of the window's allowance (0.0–1.0), straight from
    /// upstream's remaining_percent (legacy plans); None when unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly: Option<f64>,
    /// One-decimal percent mirrors of the fractions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly_pct: Option<f64>,
    /// Next window reset (ISO-8601 UTC) — countdown material for panels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_resets_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly_resets_at: Option<String>,
    /// Credit-plan amounts (USD), present only on credit-shape bodies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub included_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowance_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purchased_usd: Option<f64>,
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
            session_resets_at: None,
            weekly_resets_at: None,
            included_usd: None,
            allowance_usd: None,
            purchased_usd: None,
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
            session_resets_at: None,
            weekly_resets_at: None,
            included_usd: None,
            allowance_usd: None,
            purchased_usd: None,
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

/// Pool-wide usage aggregate: the capacity-weighted mean of every
/// contributing key's usage fraction, weighted by plan tier (see
/// `tier_for`). Like the per-key fractions it stays in [0, 1] and reads
/// as "the fraction of the pool's combined plan capacity that has been
/// used" (a single-key pool therefore reproduces that key's own
/// fraction, so consumers of per-key usage need no adjustment).
/// Windows with no contributing key are `None` (rendered `null`, never
/// fabricated as 0 or omitted — the envelope's schema has both fields
/// required-nullable, so serialization must always emit them). The reset
/// instant rides along only when every reporting key of that window
/// agrees on one (upstream resets per account-period, so same-tier pools
/// agree; mixed windows stay `None` rather than picking one account's
/// clock).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct UsageAggregate {
    pub session: Option<f64>,
    pub weekly: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_resets_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly_resets_at: Option<String>,
}

/// Unit label for the aggregate: a capacity-weighted mean of per-key
/// fractions, so it reads as the fraction of the pool's combined plan
/// capacity that has been used.
pub const AGGREGATE_UNIT: &str = "pool capacity fraction";

/// Round to 3 decimals. With the mean, the division genuinely produces
/// more decimals than the wire contract carries (e.g. 202.537/251 =
/// 0.806920…), so this is a precision cut, not just fp-noise trimming.
/// Unlike `pct` there is no [0,1] clamp — an upstream fraction above 1.0
/// would legitimately surface.
fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Aggregate a snapshot across all ok rows as the capacity-weighted mean
/// over the tier weights implied by each key's concurrency
/// (`concurrencies` is index-aligned with `snap.keys`): each window's
/// weighted sum is divided by the summed weights of exactly the keys
/// that reported it, so the result is a fraction in [0, 1] like the
/// per-key values (weights are all >= 1, so the divisor never vanishes).
/// Every key participates regardless of pool health —
/// cooldown/dead state never touches usage fetching, so cooling keys
/// contribute like any other; error rows (`ok:false`, e.g. a dead key's
/// own 401 on /api/balance, its expected outcome) carry no numbers and
/// cannot contribute. A window only some keys report is averaged over
/// exactly those keys (their weights form the denominator, so dead
/// keys never dilute the number); a window no ok row reports stays
/// `None` (never fabricated as 0). Reset instants are the unanimous
/// value across the keys that reported the window, else `None`.
pub fn aggregate(snap: &UsageSnapshot, concurrencies: &[u32]) -> UsageAggregate {
    let mut session = 0.0f64;
    let mut weekly = 0.0f64;
    let mut session_w = 0.0f64;
    let mut weekly_w = 0.0f64;
    let mut session_seen = false;
    let mut weekly_seen = false;
    let mut session_reset: Option<Option<String>> = None;
    let mut weekly_reset: Option<Option<String>> = None;
    for k in &snap.keys {
        let conc = concurrencies.get(k.index).copied().unwrap_or(1);
        let (_, weight) = tier_for(conc);
        if k.ok {
            if let Some(s) = k.session {
                session += s * weight;
                session_w += weight;
                session_seen = true;
                session_reset = unify_reset(session_reset, &k.session_resets_at);
            }
            if let Some(w) = k.weekly {
                weekly += w * weight;
                weekly_w += weight;
                weekly_seen = true;
                weekly_reset = unify_reset(weekly_reset, &k.weekly_resets_at);
            }
        }
    }
    UsageAggregate {
        session: session_seen.then(|| round3(session / session_w)),
        weekly: weekly_seen.then(|| round3(weekly / weekly_w)),
        session_resets_at: session_seen.then(|| session_reset.flatten()).flatten(),
        weekly_resets_at: weekly_seen.then(|| weekly_reset.flatten()).flatten(),
    }
}

/// Unanimous-string unification: None (no value yet) absorbs anything;
/// an existing value survives only when the new one is equal. The outer
/// Option distinguishes "seen nothing" from "seen a missing value".
fn unify_reset(seen: Option<Option<String>>, next: &Option<String>) -> Option<Option<String>> {
    match seen {
        None => Some(next.clone()),
        Some(prev) => Some(match (prev, next) {
            (Some(a), Some(b)) if a == b.as_str() => Some(a),
            _ => None,
        }),
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
    /// When the last fetch *attempt* (not publication) ended. Unlike
    /// snapshot age this advances on failed rounds, so it rate-limits
    /// retries even while a good snapshot is kept unchanged.
    last_attempt: Mutex<Option<Instant>>,
    /// Minimum interval between fetch attempts (tests may shorten it).
    min_refresh: Duration,
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
                fetch_balance_http(&agent, &base, secret)
            }),
            snapshot: Mutex::new(None),
            fetch_mu: Mutex::new(()),
            last_attempt: Mutex::new(None),
            min_refresh: MIN_REFRESH,
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> UsageTracker {
        self.ttl = ttl;
        self
    }

    /// Override the minimum interval between fetch attempts (tests).
    #[doc(hidden)]
    pub fn with_min_refresh(mut self, d: Duration) -> UsageTracker {
        self.min_refresh = d;
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
                // Failure backoff: a recent failed attempt must not turn
                // every /_usage request into a new full fan-out (the kept
                // previous snapshot never ages forward to gate this).
                if self.attempt_cooldown_left().is_some() {
                    return self.wait_or_peek(&guard);
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
        // Based on the last fetch attempt — not snapshot age — so sustained
        // upstream failures (which keep the old snapshot, freezing its age)
        // are backed off all the same.
        if self.attempt_cooldown_left().is_some() {
            return self.wait_or_peek(&guard);
        }
        self.do_fetch_inner(&guard)
    }

    /// Remaining min-interval cooldown since the last fetch attempt, if any.
    fn attempt_cooldown_left(&self) -> Option<Duration> {
        let last = (*lock(&self.last_attempt))?;
        let elapsed = last.elapsed();
        (elapsed < self.min_refresh).then(|| self.min_refresh - elapsed)
    }

    /// Inside the min-interval window: serve the freshest snapshot that
    /// exists (callers hold the fetch guard, so no fetch can be in flight;
    /// a first-ever fetch always runs because no snapshot implies no
    /// recorded attempt).
    fn wait_or_peek(&self, _guard: &MutexGuard<'_, ()>) -> Arc<UsageSnapshot> {
        self.peek().unwrap_or_else(|| self.do_fetch_inner(_guard))
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
        // Same failure backoff as get()/refresh(): a failed round must not
        // make every poller tick a new fan-out against a struggling upstream.
        if self.attempt_cooldown_left().is_some() {
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
    /// `last_attempt` is stamped on every round — success or failure — so
    /// the min-interval guard keeps rate-limiting retries while a failed
    /// round leaves the snapshot (and its age) frozen.
    fn do_fetch_inner(&self, _guard: &MutexGuard<'_, ()>) -> Arc<UsageSnapshot> {
        let results = self.fetch_all();
        *lock(&self.last_attempt) = Some(Instant::now());
        let mut keys = Vec::with_capacity(self.pool.len());
        for (i, res) in results.into_iter().enumerate() {
            let suffix = self.pool.suffix_of(i);
            keys.push(match res {
                Ok(p) => {
                    // Fractions are upstream's own numbers (1 − remaining),
                    // not derived from any configured cap. round3 cuts the
                    // fp noise of the ÷100 (0.037000000000000026 → 0.037).
                    let session = p.session.as_ref().and_then(|s| s.used_fraction()).map(round3);
                    let weekly = p.weekly.as_ref().and_then(|w| w.used_fraction()).map(round3);
                    KeyUsage {
                        index: i,
                        suffix,
                        ok: true,
                        status: Some(200),
                        session_pct: session.map(pct),
                        weekly_pct: weekly.map(pct),
                        session_resets_at: p.session.as_ref().and_then(|s| s.resets_at.clone()),
                        weekly_resets_at: p.weekly.as_ref().and_then(|w| w.resets_at.clone()),
                        session,
                        weekly,
                        included_usd: p.included_usd.as_ref().and_then(|i| i.balance_usd),
                        allowance_usd: p.included_usd.as_ref().and_then(|i| i.allowance_usd),
                        purchased_usd: p.purchased_usd,
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
        // retries are rate-limited by last_attempt via MIN_REFRESH, since
        // this path freezes the snapshot's own age. First-ever failures
        // must still land so /_usage can surface them.
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

/// One keyed HTTP GET of `/api/balance` decoded tolerantly. A single
/// request per key per round: the docs rate-limit 10 req/min per user
/// shared across all keys and devices, so the fan-out must stay at one
/// request per key and the TTL at >= 6 s (it is 60 s).
fn fetch_balance_http(
    agent: &ureq::Agent,
    upstream: &str,
    secret: &str,
) -> Result<UsagePayload, FetchError> {
    let url = format!("{upstream}/api/balance");
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

    /// Legacy-plan /api/balance body (docs.ollama.com/api/balance).
    fn legacy_body(session_rem: f64, weekly_rem: f64, resets: &str) -> String {
        format!(
            r#"{{"included":{{"session":{{"remaining_percent":{session_rem},"resets_at":"{resets}"}},
                               "weekly":{{"remaining_percent":{weekly_rem},"resets_at":"{resets}"}}}},
                "purchased":{{"balance_usd":0}}}}"#
        )
    }

    #[test]
    fn decodes_legacy_balance_payload() {
        let body = legacy_body(95.56, 49.67, "2026-10-09T00:00:00Z");
        let p: UsagePayload = serde_json::from_str(&body).unwrap();
        let s = p.session.clone().expect("session window decoded");
        let w = p.weekly.clone().expect("weekly window decoded");
        assert_eq!(s.remaining_percent, Some(95.56));
        assert_eq!(w.remaining_percent, Some(49.67));
        assert_eq!(s.resets_at.as_deref(), Some("2026-10-09T00:00:00Z"));
        assert_eq!(p.included_usd, None, "legacy body has no USD amounts");
        assert_eq!(p.purchased_usd, Some(0.0));
        assert!(p.plausible());
        // remaining → used fraction is the whole point.
        assert!((s.used_fraction().unwrap() - 0.0444).abs() < 1e-9);
        assert!((w.used_fraction().unwrap() - 0.5033).abs() < 1e-9);
    }

    #[test]
    fn decodes_credit_balance_payload() {
        // Credit-plan shape: USD amounts, no percents.
        let body = r#"{"included":{"balance_usd":72.5,"allowance_usd":100,
                                   "period":{"from":"2026-09-15T09:30:00Z","until":"2026-10-15T09:30:00Z"}},
                       "purchased":{"balance_usd":25}}"#;
        let p: UsagePayload = serde_json::from_str(body).unwrap();
        assert_eq!(p.session, None);
        assert_eq!(p.weekly, None);
        let inc = p.included_usd.clone().expect("included USD decoded");
        assert_eq!(inc.balance_usd, Some(72.5));
        assert_eq!(inc.allowance_usd, Some(100.0));
        assert_eq!(p.purchased_usd, Some(25.0));
        assert!(p.plausible());
    }

    #[test]
    fn zero_remaining_is_full_usage_not_drift() {
        // remaining_percent 0 = window exhausted: honest data, not drift.
        let p: UsagePayload =
            serde_json::from_str(r#"{"included":{"session":{"remaining_percent":0}}}"#).unwrap();
        assert_eq!(p.session.clone().unwrap().used_fraction(), Some(1.0));
        assert!(p.plausible());
    }

    #[test]
    fn used_fraction_clamps_rounding_drift() {
        // 100.2% remaining (rounding drift upstream) must not go negative.
        let p: UsagePayload = serde_json::from_str(
            r#"{"included":{"session":{"remaining_percent":100.2}}}"#,
        )
        .unwrap();
        assert_eq!(p.session.unwrap().used_fraction(), Some(0.0));
        // -0.5% remaining likewise clamps to 1.0, never beyond.
        let p: UsagePayload = serde_json::from_str(
            r#"{"included":{"weekly":{"remaining_percent":-0.5}}}"#,
        )
        .unwrap();
        assert_eq!(p.weekly.unwrap().used_fraction(), Some(1.0));
    }

    #[test]
    fn tolerates_missing_fields() {
        let p: UsagePayload = serde_json::from_str("{}").unwrap();
        assert_eq!(p.session, None);
        assert_eq!(p.weekly, None);
        assert_eq!(p.included_usd, None);
        assert_eq!(p.purchased_usd, None);
        // Absence of all numbers is drift, not zeros.
        assert!(!p.plausible());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let body = r#"{"included":{"session":{"remaining_percent":44.4},"brand_new":{"x":1}},"future":42}"#;
        let p: UsagePayload = serde_json::from_str(body).unwrap();
        assert_eq!(p.session.clone().unwrap().remaining_percent, Some(44.4));
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
        let body = r#"{"included":{"session":{"remaining_percent":"Bearer omk-secret1234"}}}"#;
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
            .with_min_refresh(Duration::ZERO) // isolate TTL behavior
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::from_str(&legacy_body(75.0, 60.0, "2026-10-09T00:00:00Z")).unwrap())
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
                Ok(serde_json::from_str(r#"{"included":{"weekly":{"remaining_percent":90}}}"#).unwrap())
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
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_ttl(Duration::from_millis(20))
            .with_min_refresh(Duration::from_millis(30))
            .with_fetch(|_, _, _| {
                if FAIL.load(std::sync::atomic::Ordering::Relaxed) {
                    Err(FetchError::Network("boom".into()))
                } else {
                    Ok(serde_json::from_str(&legacy_body(50.0, 50.0, "2026-10-09T00:00:00Z")).unwrap())
                }
            });
        let first = t.get();
        assert!(first.keys[0].ok);
        FAIL.store(true, std::sync::atomic::Ordering::Relaxed);
        // Wait past both TTL and min-interval so the stale path may fetch.
        std::thread::sleep(Duration::from_millis(60));
        let again = t.get();
        assert!(again.keys[0].ok, "failed refresh must keep good data");
        assert_eq!(again.keys[0].session, Some(0.5));
        assert_eq!(again.fetched_at, first.fetched_at);
    }

    #[test]
    fn failed_rounds_are_backed_off_like_successful_ones() {
        // The bug this pins: failed rounds keep the previous snapshot, so
        // snapshot age freezes and a snapshot-age-based guard would let a
        // ?refresh=1 (or stale get()) loop fan out on every call. The
        // attempt-based guard must rate-limit those rounds too.
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let pool = Arc::new(Pool::new(vec![("omk-usage-bo01".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_min_refresh(Duration::from_millis(100))
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(FetchError::Status(429))
            });
        let first = t.get(); // first-ever failure lands (no snapshot yet)
        assert!(!first.keys[0].ok);
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
        // Stale-snapshot + total failure: repeated refresh() must back off.
        let _ = t.refresh();
        let _ = t.refresh();
        let _ = t.get();
        assert_eq!(
            CALLS.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "failed rounds must be min-interval guarded like successes"
        );
        std::thread::sleep(Duration::from_millis(120));
        let _ = t.refresh(); // window elapsed → one more attempt
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn peek_never_fetches() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let pool = Arc::new(Pool::new(vec![("omk-usage-pk01".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com").with_fetch(|_, _, _| {
            CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(serde_json::from_str(&legacy_body(90.0, 90.0, "2026-10-09T00:00:00Z")).unwrap())
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
            .with_min_refresh(Duration::from_millis(120))
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::from_str(&legacy_body(90.0, 90.0, "2026-10-09T00:00:00Z")).unwrap())
            });
        let _ = t.refresh();
        let _ = t.refresh(); // inside min-interval: no second fetch
        let _ = t.get(); // also guarded now (stale path can't bypass it)
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
        std::thread::sleep(Duration::from_millis(140));
        let _ = t.get(); // window elapsed → refetch
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    // ----- tier weighting and aggregate -----

    #[test]
    fn tier_for_matches_concurrency_to_plan_tier() {
        // 1 → free; 2..=3 → pro; 4 and beyond → max ("more concurrency
        // than normal is the next tier up").
        assert_eq!(tier_for(1), ("free", FREE_WEIGHT));
        assert_eq!(tier_for(2), ("pro", PRO_WEIGHT));
        assert_eq!(tier_for(3), ("pro", PRO_WEIGHT));
        assert_eq!(tier_for(4), ("max", MAX_WEIGHT));
        assert_eq!(tier_for(10), ("max", MAX_WEIGHT));
        assert_eq!(tier_for(u32::MAX), ("max", MAX_WEIGHT));
    }

    fn agg_row(
        index: usize,
        ok: bool,
        session: Option<f64>,
        weekly: Option<f64>,
    ) -> KeyUsage {
        KeyUsage {
            index,
            suffix: format!("sfx{index}"),
            ok,
            status: ok.then_some(200),
            session,
            weekly,
            session_pct: session.map(pct),
            weekly_pct: weekly.map(pct),
            session_resets_at: None,
            weekly_resets_at: None,
            included_usd: None,
            allowance_usd: None,
            purchased_usd: None,
            error: (!ok).then(|| "test failure".to_string()),
        }
    }

    fn agg_snap(keys: Vec<KeyUsage>) -> UsageSnapshot {
        UsageSnapshot {
            fetched_at: Instant::now(),
            keys,
        }
    }

    #[test]
    fn aggregate_weights_by_tier() {
        // The tiers still weight by plan-cap multiples: legacy plans'
        // remaining_percent is a fraction of the *account's own* plan cap,
        // so a pro key at 81% has burned far more capacity than a free key
        // at 81%. free(1) at 3.7% + max(250) at 81%:
        // (0.037*1 + 0.81*250) / 251 = 0.807 — the max key dominates, and
        // the number stays in [0, 1].
        let snap = agg_snap(vec![
            agg_row(0, true, Some(0.037), Some(0.007)),
            agg_row(1, true, Some(0.81), Some(0.42)),
        ]);
        let a = aggregate(&snap, &[1, 10]);
        assert_eq!(a.session, Some(0.807), "(0.037*1 + 0.81*250) / 251");
        assert_eq!(a.weekly, Some(0.418), "(0.007*1 + 0.42*250) / 251");
    }

    #[test]
    fn aggregate_averages_across_tiers_without_fp_noise() {
        // The classic fp trap: 0.037 + 0.81 raw-sums to
        // 0.8470000000000001; round3 must render the honest mean 0.424
        // ((0.037 + 0.81) / 2 with equal weights).
        let snap = agg_snap(vec![
            agg_row(0, true, Some(0.037), None),
            agg_row(1, true, Some(0.81), None),
        ]);
        let a = aggregate(&snap, &[1, 1]);
        assert_eq!(a.session, Some(0.424));
        assert_eq!(a.weekly, None, "window nobody reported stays null");
    }

    #[test]
    fn aggregate_excludes_error_rows_and_nulls_when_all_fail() {
        // Only the ok row contributes, and a single contributor's
        // weighted mean is exactly its own fraction.
        let snap = agg_snap(vec![
            agg_row(0, false, None, None),
            agg_row(1, true, Some(0.5), None),
        ]);
        let a = aggregate(&snap, &[3, 3]);
        assert_eq!(
            a.session,
            Some(0.5),
            "0.5 * pro weight / pro weight, error row skipped"
        );
        assert_eq!(a.weekly, None);
        // No ok row at all → both windows null, never 0.
        let snap = agg_snap(vec![agg_row(0, false, None, None)]);
        let a = aggregate(&snap, &[1]);
        assert_eq!(a, UsageAggregate::default());
    }

    #[test]
    fn aggregate_averages_window_only_over_rows_that_report_it() {
        // Mixed report: key 0 has session only, key 1 weekly only — each
        // window averages over exactly its contributors ("never
        // fabricate"), and the per-window denominator follows them.
        let snap = agg_snap(vec![
            agg_row(0, true, Some(0.4), None),
            agg_row(1, true, None, Some(0.2)),
        ]);
        let a = aggregate(&snap, &[1, 1]);
        assert_eq!(a.session, Some(0.4));
        assert_eq!(a.weekly, Some(0.2));
    }

    #[test]
    fn aggregate_reset_is_unanimous_value() {
        // Same reset on all reporting keys of a window → it surfaces.
        let mut keys = vec![
            agg_row(0, true, Some(0.4), Some(0.2)),
            agg_row(1, true, Some(0.4), Some(0.2)),
        ];
        keys[0].session_resets_at = Some("2026-10-09T00:00:00Z".into());
        keys[1].session_resets_at = Some("2026-10-09T00:00:00Z".into());
        keys[0].weekly_resets_at = Some("2026-10-12T00:00:00Z".into());
        // Key 1's weekly resets_at missing: weekly has no unanimous value.
        let a = aggregate(&agg_snap(keys), &[1, 1]);
        assert_eq!(a.session_resets_at.as_deref(), Some("2026-10-09T00:00:00Z"));
        assert_eq!(a.weekly_resets_at, None);
        // Split values likewise null the field.
        let mut keys = vec![agg_row(0, true, Some(0.4), None), agg_row(1, true, Some(0.4), None)];
        keys[0].session_resets_at = Some("2026-10-09T00:00:00Z".into());
        keys[1].session_resets_at = Some("2026-10-10T00:00:00Z".into());
        let a = aggregate(&agg_snap(keys), &[1, 1]);
        assert_eq!(a.session_resets_at, None, "accounts reset on their own schedules");
    }

    #[test]
    fn aggregate_default_weight_is_free_for_short_slices() {
        // Defensive: a shorter concurrency slice must not panic; missing
        // entries fall back to free weight.
        let snap = agg_snap(vec![agg_row(0, true, Some(0.037), Some(0.007))]);
        let a = aggregate(&snap, &[]);
        assert_eq!(a.session, Some(0.037));
        assert_eq!(a.weekly, Some(0.007));
    }

    #[test]
    fn aggregate_includes_keys_on_cooldown() {
        // The pin for "aggregate covers cooldown keys": usage fetching is
        // health-blind, so a key cooling down after a 429 still has its
        // usage fetched and must contribute to the mean.
        let pool = Arc::new(Pool::new(
            vec![("omk-usage-cd01".into(), 3), ("omk-usage-cd02".into(), 1)],
            4,
            false,
        ));
        pool.mark_cooldown(0, Duration::from_secs(60), "429 test");
        let t = UsageTracker::new(pool, "https://ollama.com").with_fetch(|i, _, _| {
            Ok(serde_json::from_str::<UsagePayload>(&if i == 0 {
                legacy_body(50.0, 80.0, "2026-10-09T00:00:00Z")
            } else {
                legacy_body(90.0, 90.0, "2026-10-09T00:00:00Z")
            })
            .unwrap())
        });
        let snap = t.get();
        assert!(snap.keys[0].ok, "cooldown key's usage fetch must succeed");
        let a = aggregate(&snap, &[3, 1]);
        // 1-remaining: key0 0.5/0.2, key1 0.1/0.1 →
        // (0.5*50 + 0.1*1) / 51 = 0.492; (0.2*50 + 0.1*1) / 51 = 0.198.
        assert_eq!(a.session, Some(0.492));
        assert_eq!(a.weekly, Some(0.198));
        assert_eq!(a.session_resets_at.as_deref(), Some("2026-10-09T00:00:00Z"));
    }

    #[test]
    fn tracker_maps_remaining_to_used_fraction() {
        // End-to-end through the seam: remaining_percent 95.56 → used
        // 0.044 → pct 4.4; resets_at rides into the row.
        let pool = Arc::new(Pool::new(vec![("omk-usage-pct1".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_fetch(|_, _, _| {
                Ok(serde_json::from_str::<UsagePayload>(
                    &legacy_body(95.56, 49.67, "2026-10-09T00:00:00Z"),
                )
                .unwrap())
            });
        let snap = t.get();
        let row = &snap.keys[0];
        assert!(row.ok);
        assert_eq!(row.session.unwrap(), 0.044, "round3 precision cut");
        assert_eq!(row.session_pct.unwrap(), 4.4);
        assert!(row.weekly.is_some() && row.weekly_pct.unwrap() > 49.0);
        assert_eq!(row.session_resets_at.as_deref(), Some("2026-10-09T00:00:00Z"));
        assert_eq!(row.weekly_resets_at.as_deref(), Some("2026-10-09T00:00:00Z"));
    }

    #[test]
    fn credit_plan_row_carries_usd_not_fractions() {
        let pool = Arc::new(Pool::new(vec![("omk-credit001".into(), 1)], 4, false));
        let t = UsageTracker::new(pool, "https://ollama.com")
            .with_fetch(|_, _, _| {
                Ok(serde_json::from_str::<UsagePayload>(
                    r#"{"included":{"balance_usd":72.5,"allowance_usd":100},"purchased":{"balance_usd":25}}"#,
                )
                .unwrap())
            });
        let snap = t.get();
        let row = &snap.keys[0];
        assert!(row.ok);
        assert_eq!(row.session, None, "credit plans have no percent windows");
        assert_eq!(row.weekly, None);
        assert_eq!(row.included_usd, Some(72.5));
        assert_eq!(row.allowance_usd, Some(100.0));
        assert_eq!(row.purchased_usd, Some(25.0));
    }
}
