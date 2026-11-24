//! Model/time usage statistics from Ollama Cloud's documented **cloud
//! usage** endpoint.
//!
//! `GET https://ollama.com/api/usage?range=…&group_by=model`
//! (docs.ollama.com/api/cloud-usage) answers, per account, with request
//! counts, tokens and USD cost in UTC buckets — hourly for `24h`, daily
//! for `7d`/`30d` — and, with `group_by=model`, a per-model breakdown in
//! `totals` and in every bucket. This is the only documented source for
//! "which models were used, when, and how much"; the finest resolution
//! is one hour and there is no per-request log.
//!
//! Everything here is **account-scoped**, not key-scoped: one fetch with
//! any key of an account returns that account's usage. ollamux therefore
//! fetches once per distinct `@account` label (from the keys file) and
//! fans the result back out per account in `/_usage`. Upstream has no
//! account identifier and the API offers no per-key breakdown yet, so
//! the keys file's labels are the source of truth for attribution.
//!
//! Rate limiting matches the balance endpoint (10 requests per minute
//! per user, shared across that user's keys and devices; docs recommend
//! polling once per minute). One fetch per account per range stays
//! within that for friend-group pools of up to ~10 accounts; a 429
//! carries the last good row (`as_of`) and sits the account out for its
//! `Retry-After` (capped at 5 min). Decoding is tolerant: shape drift
//! becomes a per-account error string, never a panic or a 5xx.
//!
//! Introspection is read-only: no pool slots, no health markings, and
//! `/_keys` never triggers a stats fetch.

use crate::pool::{AccountInfo, Pool, State};
use crate::usage::FetchError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Serve-at-most-this-age before a refresh is considered. The docs
/// recommend polling once per minute.
pub const STATS_TTL: Duration = Duration::from_secs(60);
/// Minimum interval between upstream attempts per (account, range).
const MIN_REFRESH: Duration = Duration::from_secs(5);
/// Backoff after a 429 without a usable `Retry-After`.
const DEFAULT_BACKOFF: Duration = Duration::from_secs(60);
/// Cap on a 429 backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);
/// Upstream timeouts (bounded; never a whole-request timeout).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(10);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The three ranges the endpoint supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Range {
    /// 24 hours, hourly buckets.
    H24,
    /// 7 days, daily buckets.
    D7,
    /// 30 days, daily buckets.
    D30,
}

impl Range {
    pub fn parse(s: &str) -> Option<Range> {
        match s {
            "24h" => Some(Range::H24),
            "7d" => Some(Range::D7),
            "30d" => Some(Range::D30),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Range::H24 => "24h",
            Range::D7 => "7d",
            Range::D30 => "30d",
        }
    }
}

// ---------------------------------------------------------------------------
// Wire model (tolerant decode of the documented /api/usage payload)
// ---------------------------------------------------------------------------

/// Counters shared by totals, buckets, groups and the remainder.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Metrics {
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub usage_usd: f64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

impl Metrics {
    fn add(&mut self, other: &Metrics) {
        self.request_count += other.request_count;
        self.usage_usd += other.usage_usd;
        self.input_tokens += other.input_tokens;
        self.cached_input_tokens += other.cached_input_tokens;
        self.output_tokens += other.output_tokens;
    }
}

/// One model's slice of usage.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Group {
    #[serde(default)]
    pub model: String,
    #[serde(flatten)]
    pub metrics: Metrics,
}

/// A totals block or a bucket's payload: metrics plus the optional model
/// groups and the remainder (usage from groups outside upstream's
/// top-10 cap).
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Summary {
    #[serde(flatten)]
    pub metrics: Metrics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<Group>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remainder: Option<Metrics>,
}

impl Summary {
    /// Sum another summary into this one, merging model groups by name.
    fn merge(&mut self, other: &Summary) {
        self.metrics.add(&other.metrics);
        for g in &other.groups {
            if let Some(existing) = self.groups.iter_mut().find(|x| x.model == g.model) {
                existing.metrics.add(&g.metrics);
            } else {
                self.groups.push(g.clone());
            }
        }
        if let Some(r) = &other.remainder {
            match &mut self.remainder {
                Some(existing) => existing.add(r),
                None => self.remainder = Some(r.clone()),
            }
        }
    }
}

/// One UTC hour or day of usage.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Bucket {
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub until: String,
    /// Present and true for the current, still-in-progress bucket.
    #[serde(default)]
    pub partial: bool,
    #[serde(flatten)]
    pub summary: Summary,
}

/// Decoded `/api/usage` payload. Everything optional/defaulted so shape
/// drift degrades to a per-account error, never a panic.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UsageResponse {
    #[serde(default)]
    pub range: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub granularity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_by: Option<String>,
    #[serde(default)]
    pub from: String,
    #[serde(default)]
    pub until: String,
    #[serde(default)]
    pub totals: Summary,
    #[serde(default)]
    pub buckets: Vec<Bucket>,
    /// Absorb future top-level fields instead of failing the decode.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl UsageResponse {
    /// A documented response carries the window and at least the
    /// zero-filled buckets. Anything else is shape drift.
    fn plausible(&self) -> bool {
        !self.from.is_empty()
            && !self.until.is_empty()
            && !self.granularity.is_empty()
            && !self.buckets.is_empty()
    }
}

fn parse_payload(body: &str) -> Result<UsageResponse, String> {
    // serde error text embeds offending values from the body; only the
    // location is relayed, never content (a hostile upstream could echo
    // the Authorization secret into a wrong-typed field).
    serde_json::from_str::<UsageResponse>(body)
        .map(|r| {
            if !r.plausible() {
                return Err("endpoint changed (no usage data in payload)".to_string());
            }
            Ok(r)
        })
        .unwrap_or_else(|_| Err("endpoint changed (unexpected payload)".to_string()))
}

// ---------------------------------------------------------------------------
// Fetch + cache
// ---------------------------------------------------------------------------

type FetchFn = dyn Fn(&str, &[usize], Range) -> Result<UsageResponse, FetchError> + Send + Sync;

/// One account's stats result for a range.
#[derive(Debug, Clone)]
pub struct AccountStats {
    pub account: String,
    pub keys: usize,
    pub ok: bool,
    /// Unix seconds when carried data was originally fetched (transient
    /// failure or 429 backoff); absent on fresh rows.
    pub as_of: Option<u64>,
    pub error: Option<String>,
    pub usage: Option<UsageResponse>,
    /// When these numbers were fetched (for the section's age). Carried
    /// rows keep their original fetch instant.
    pub fetched_at: Instant,
}

#[derive(Default)]
struct CellState {
    snapshot: Option<Arc<AccountStats>>,
    fetched_at: Option<Instant>,
    last_attempt: Option<Instant>,
    backoff_until: Option<Instant>,
}

#[derive(Default)]
struct Cell {
    state: Mutex<CellState>,
}

/// Combined per-account snapshot for one range.
#[derive(Debug)]
pub struct StatsSnapshot {
    pub range: Range,
    /// When this collection ran (freshness reference).
    pub fetched_at: Instant,
    pub accounts: Vec<AccountStats>,
}

impl StatsSnapshot {
    pub fn updated_unix(&self) -> u64 {
        // Oldest contributing fetch: if every account was served from
        // cache, age_s should show that, not 0.
        let oldest = self
            .accounts
            .iter()
            .map(|a| a.fetched_at)
            .min()
            .unwrap_or(self.fetched_at);
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|now| now.as_secs().saturating_sub(oldest.elapsed().as_secs()))
            .unwrap_or(0)
    }

    /// Sum every ok account's totals into one pool-wide summary, merging
    /// model groups by name. Never sums per key: duplicate keys of one
    /// account would double-count.
    pub fn pool_totals(&self) -> Summary {
        let mut out = Summary::default();
        for a in &self.accounts {
            if let Some(u) = &a.usage {
                out.merge(&u.totals);
            }
        }
        out
    }

    /// The window metadata from the first ok account (all accounts see
    /// the same requested range/granularity).
    pub fn window(&self) -> Option<&UsageResponse> {
        self.accounts.iter().find_map(|a| a.usage.as_ref())
    }
}

pub struct StatsTracker {
    pool: Arc<Pool>,
    cells: Mutex<HashMap<(String, Range), Arc<Cell>>>,
    ttl: Duration,
    min_refresh: Duration,
    default_backoff: Duration,
    fetch: Box<FetchFn>,
}

impl StatsTracker {
    pub fn new(pool: Arc<Pool>, upstream: &str) -> StatsTracker {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .redirects(0)
            .build();
        let agent = Arc::new(agent);
        let upstream = upstream.trim_end_matches('/').to_string();
        let pool_for_fetch = pool.clone();
        let agent_for_fetch = agent.clone();
        let base = upstream.clone();
        StatsTracker {
            pool,
            cells: Mutex::new(HashMap::new()),
            ttl: STATS_TTL,
            min_refresh: MIN_REFRESH,
            default_backoff: DEFAULT_BACKOFF,
            fetch: Box::new(move |_account, keys, range| {
                // Healthy keys first; rotate on auth rejection only (the
                // rate limit and the upstream are shared by the account).
                let mut order = keys.to_vec();
                order.sort_by_key(|&i| (pool_for_fetch.state_of(i) != State::Up, i));
                let mut last_auth = None;
                for i in order {
                    match fetch_usage_http(
                        &agent_for_fetch,
                        &base,
                        pool_for_fetch.secret_of(i),
                        range,
                    ) {
                        Ok(r) => return Ok(r),
                        Err(FetchError::Status(401 | 403)) => {
                            last_auth = Some(FetchError::Status(401));
                        }
                        Err(other) => return Err(other),
                    }
                }
                Err(last_auth.unwrap_or_else(|| FetchError::Network("no key for account".into())))
            }),
        }
    }

    #[doc(hidden)]
    pub fn with_ttl(mut self, ttl: Duration) -> StatsTracker {
        self.ttl = ttl;
        self
    }

    #[doc(hidden)]
    pub fn with_min_refresh(mut self, d: Duration) -> StatsTracker {
        self.min_refresh = d;
        self
    }

    #[doc(hidden)]
    pub fn with_default_backoff(mut self, d: Duration) -> StatsTracker {
        self.default_backoff = d;
        self
    }

    #[doc(hidden)]
    pub fn with_fetch<F>(mut self, f: F) -> StatsTracker
    where
        F: Fn(&str, &[usize], Range) -> Result<UsageResponse, FetchError> + Send + Sync + 'static,
    {
        self.fetch = Box::new(f);
        self
    }

    /// Cached-or-fresh collection. `force` bypasses the TTL but not the
    /// per-cell min-interval guard.
    pub fn get(&self, range: Range, force: bool) -> Arc<StatsSnapshot> {
        let accounts = self.pool.accounts();
        let mut cells: Vec<(AccountInfo, Arc<Cell>)> = Vec::new();
        {
            let mut map = lock(&self.cells);
            for a in accounts {
                let key = (a.name.clone(), range);
                let cell = map
                    .entry(key)
                    .or_insert_with(|| Arc::new(Cell::default()))
                    .clone();
                cells.push((a, cell));
            }
        }
        let results: Vec<AccountStats> = std::thread::scope(|s| {
            let handles: Vec<_> = cells
                .into_iter()
                .map(|(a, cell)| {
                    let fetch = &self.fetch;
                    s.spawn(move || self.account_stats(&a, &cell, range, force, fetch))
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().unwrap_or_else(|_| AccountStats {
                        account: "?".into(),
                        keys: 0,
                        ok: false,
                        as_of: None,
                        error: Some("stats worker panicked".into()),
                        usage: None,
                        fetched_at: Instant::now(),
                    })
                })
                .collect()
        });
        Arc::new(StatsSnapshot {
            range,
            fetched_at: Instant::now(),
            accounts: results,
        })
    }

    fn account_stats(
        &self,
        a: &AccountInfo,
        cell: &Cell,
        range: Range,
        force: bool,
        fetch: &FetchFn,
    ) -> AccountStats {
        let mut st = lock(&cell.state);
        // Fresh cache wins unless the caller explicitly forced. Forced
        // refreshes still honor the min-interval guard below.
        if !force {
            if let (Some(snap), Some(at)) = (&st.snapshot, st.fetched_at) {
                if at.elapsed() < self.ttl {
                    return (**snap).clone();
                }
            }
        }
        let now = Instant::now();
        if st.backoff_until.is_some_and(|until| until > now) {
            return self.carried_or_failed(&mut st, a, "rate limited (upstream 429)", true);
        }
        if st
            .last_attempt
            .is_some_and(|last| last.elapsed() < self.min_refresh)
        {
            return self.carried_or_failed(&mut st, a, "refresh throttled", true);
        }
        st.last_attempt = Some(now);
        match fetch(&a.name, &a.key_indices, range) {
            Ok(usage) => {
                let stats = AccountStats {
                    account: a.name.clone(),
                    keys: a.key_count(),
                    ok: true,
                    as_of: None,
                    error: None,
                    usage: Some(usage),
                    fetched_at: now,
                };
                let arc = Arc::new(stats.clone());
                st.snapshot = Some(arc);
                st.fetched_at = Some(now);
                st.backoff_until = None;
                stats
            }
            Err(FetchError::RateLimited(ra)) => {
                let backoff = ra.unwrap_or(self.default_backoff).min(MAX_BACKOFF);
                st.backoff_until = Some(now + backoff);
                self.carried_or_failed(&mut st, a, "rate limited (upstream 429)", true)
            }
            Err(FetchError::Status(code)) => {
                let transient = (500..=599).contains(&code);
                let reason = match code {
                    401 | 403 => "unauthorized".to_string(),
                    404 => "endpoint gone (upstream 404)".to_string(),
                    other => format!("upstream HTTP {other}"),
                };
                self.carried_or_failed(&mut st, a, &reason, transient)
            }
            Err(FetchError::Network(e)) => {
                self.carried_or_failed(&mut st, a, &format!("network: {e}"), true)
            }
            Err(FetchError::Drift(msg)) => AccountStats {
                account: a.name.clone(),
                keys: a.key_count(),
                ok: false,
                as_of: None,
                error: Some(msg),
                usage: None,
                fetched_at: now,
            },
        }
    }

    fn carried_or_failed(
        &self,
        st: &mut CellState,
        a: &AccountInfo,
        reason: &str,
        transient: bool,
    ) -> AccountStats {
        if transient {
            if let (Some(snap), Some(at)) = (&st.snapshot, st.fetched_at) {
                let mut carried = (**snap).clone();
                carried.as_of = Some(carried.as_of.unwrap_or_else(|| unix_secs(at)));
                carried.fetched_at = at;
                return carried;
            }
        }
        AccountStats {
            account: a.name.clone(),
            keys: a.key_count(),
            ok: false,
            as_of: None,
            error: Some(reason.to_string()),
            usage: None,
            fetched_at: Instant::now(),
        }
    }
}

fn unix_secs(at: Instant) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|now| now.as_secs().saturating_sub(at.elapsed().as_secs()))
        .unwrap_or(0)
}

fn retry_after_seconds(resp: &ureq::Response) -> Option<Duration> {
    let v = resp.header("Retry-After")?;
    let secs: u64 = v.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

/// One keyed HTTP GET of `/api/usage` decoded tolerantly.
fn fetch_usage_http(
    agent: &ureq::Agent,
    upstream: &str,
    secret: &str,
    range: Range,
) -> Result<UsageResponse, FetchError> {
    let url = format!(
        "{upstream}/api/usage?range={}&group_by=model",
        range.as_str()
    );
    let resp = agent
        .get(&url)
        .set("Authorization", &format!("Bearer {secret}"))
        .set("Accept", "application/json")
        .call()
        .map_err(|e| match e {
            ureq::Error::Status(429, resp) => FetchError::RateLimited(retry_after_seconds(&resp)),
            ureq::Error::Status(code, _) => FetchError::Status(code),
            other => FetchError::Network(other.to_string()),
        })?;
    if resp.status() != 200 {
        return Err(FetchError::Status(resp.status()));
    }
    let body = resp
        .into_string()
        .map_err(|e| FetchError::Network(e.to_string()))?;
    parse_payload(&body).map_err(FetchError::Drift)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KeyEntry;

    fn pool(entries: &[(&str, u32, &str)]) -> Arc<Pool> {
        Arc::new(Pool::from_entries(
            entries
                .iter()
                .map(|(k, c, a)| KeyEntry::new(*k, *c, *a))
                .collect(),
            4,
            false,
        ))
    }

    fn body(model: &str) -> String {
        format!(
            r#"{{"range":"7d","scope":"self","granularity":"day",
                 "from":"2026-09-24T00:00:00Z","until":"2026-10-01T02:30:00Z",
                 "totals":{{"request_count":4,"usage_usd":0.0052,
                            "input_tokens":24000,"cached_input_tokens":0,
                            "output_tokens":3200,
                            "groups":[{{"model":"{model}","request_count":4,
                                        "usage_usd":0.0052,"input_tokens":24000,
                                        "cached_input_tokens":0,"output_tokens":3200}}],
                            "remainder":{{"request_count":1,"usage_usd":0.001}}}},
                 "buckets":[{{"from":"2026-09-30T12:00:00Z","until":"2026-09-30T13:00:00Z",
                              "request_count":4,"usage_usd":0.0052,"input_tokens":24000,
                              "cached_input_tokens":0,"output_tokens":3200,
                              "groups":[{{"model":"{model}","request_count":4,
                                          "usage_usd":0.0052,"input_tokens":24000,
                                          "cached_input_tokens":0,"output_tokens":3200}}]}}]}}"#
        )
    }

    #[test]
    fn decodes_documented_payload_with_groups() {
        let r: UsageResponse = serde_json::from_str(&body("gpt-oss:120b")).unwrap();
        assert!(r.plausible());
        assert_eq!(r.range, "7d");
        assert_eq!(r.granularity, "day");
        assert_eq!(r.buckets.len(), 1);
        assert_eq!(r.totals.metrics.request_count, 4);
        assert_eq!(r.totals.groups[0].model, "gpt-oss:120b");
        assert_eq!(r.totals.groups[0].metrics.output_tokens, 3200);
        assert_eq!(r.totals.remainder.as_ref().unwrap().request_count, 1);
    }

    #[test]
    fn empty_or_drifted_payload_is_not_plausible() {
        assert!(
            !serde_json::from_str::<UsageResponse>("{}")
                .unwrap()
                .plausible()
        );
        let err = parse_payload(r#"{"hello":"world"}"#).unwrap_err();
        assert!(err.contains("endpoint changed"));
    }

    #[test]
    fn one_fetch_per_account_and_key_rows_share_it() {
        let p = pool(&[("k1", 1, "alice"), ("k2", 1, "alice"), ("k3", 1, "bob")]);
        let t = StatsTracker::new(p, "https://ollama.com").with_fetch(|account, _, _| {
            Ok(serde_json::from_str(&body(&format!("{account}-model"))).unwrap())
        });
        let snap = t.get(Range::D7, false);
        assert_eq!(snap.accounts.len(), 2);
        let alice = snap.accounts.iter().find(|a| a.account == "alice").unwrap();
        assert_eq!(alice.keys, 2);
        assert!(alice.ok);
        let bob = snap.accounts.iter().find(|a| a.account == "bob").unwrap();
        assert_eq!(bob.keys, 1);
        let totals = snap.pool_totals();
        assert_eq!(totals.metrics.request_count, 8, "two accounts summed");
        assert_eq!(totals.groups.len(), 2, "groups merged by model");
    }

    #[test]
    fn ttl_caches_then_refresh_forces() {
        static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let p = pool(&[("k1", 1, "alice")]);
        let t = StatsTracker::new(p, "https://ollama.com")
            .with_ttl(Duration::from_secs(60))
            .with_min_refresh(Duration::ZERO)
            .with_fetch(|_, _, _| {
                CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(serde_json::from_str(&body("m")).unwrap())
            });
        let _ = t.get(Range::D7, false);
        let _ = t.get(Range::D7, false);
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 1);
        let _ = t.get(Range::D7, true);
        assert_eq!(CALLS.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn transient_failure_carries_last_good_row() {
        static FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        let p = pool(&[("k1", 1, "alice")]);
        let t = StatsTracker::new(p, "https://ollama.com")
            .with_ttl(Duration::ZERO)
            .with_min_refresh(Duration::ZERO)
            .with_fetch(|_, _, _| {
                if FAIL.load(std::sync::atomic::Ordering::Relaxed) {
                    Err(FetchError::Network("boom".into()))
                } else {
                    Ok(serde_json::from_str(&body("m")).unwrap())
                }
            });
        let first = t.get(Range::D7, false);
        assert!(first.accounts[0].ok);
        FAIL.store(true, std::sync::atomic::Ordering::Relaxed);
        let second = t.get(Range::D7, false);
        assert!(second.accounts[0].ok);
        assert!(second.accounts[0].as_of.is_some());
        assert_eq!(second.accounts[0].usage.as_ref().unwrap().range, "7d");
    }

    #[test]
    fn fetch_seam_is_account_scoped() {
        // One call covers the whole account: the seam receives every
        // member key so the real implementation can rotate on 401/403.
        let p = pool(&[("k1", 1, "alice"), ("k2", 1, "alice")]);
        let t = StatsTracker::new(p, "https://ollama.com").with_fetch(|account, keys, _| {
            assert_eq!(account, "alice");
            assert_eq!(keys, &[0, 1][..]);
            Ok(serde_json::from_str(&body("m")).unwrap())
        });
        let snap = t.get(Range::D7, false);
        assert!(snap.accounts[0].ok);
        assert_eq!(snap.accounts[0].keys, 2);
    }

    #[test]
    fn range_parsing_and_labels() {
        assert_eq!(Range::parse("24h"), Some(Range::H24));
        assert_eq!(Range::parse("7d"), Some(Range::D7));
        assert_eq!(Range::parse("30d"), Some(Range::D30));
        assert_eq!(Range::parse("1d"), None);
        assert_eq!(Range::H24.as_str(), "24h");
    }
}
