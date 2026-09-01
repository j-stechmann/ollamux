//! The key pool: per-key slot semaphores, health, and selection.
//!
//! Locking topology (from design review):
//! - No pool-level lock at all. Each key owns `Mutex<Health>` and
//!   `Mutex<u32>` slots; the round-robin tie-breaker is an atomic.
//! - Waiting happens only on a key's own condvar against its own slot
//!   mutex — never across objects, so no lost wakeups or inversion.
//! - Usage-derived state (quota-aware routing) is likewise lock-free:
//!   one `AtomicUsize` threshold and an `AtomicU64` over-quota bitmask,
//!   both written by the usage tracker and read on the request hot path.

use crate::affinity::AffinityMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Cooldown applied after a 429 (or when Retry-After is absent/unparseable).
pub const COOLDOWN_429: Duration = Duration::from_secs(60);
/// Cooldown for keys marked dead is effectively permanent (until restart);
/// kept as a named constant for clarity in `/_keys` output.
#[allow(dead_code)]
pub const COOLDOWN_DEAD: Duration = Duration::from_secs(300);
/// Failures (5xx / network / non-auth 403) before a key is cooled down.
pub const STRIKES_TO_COOL: u32 = 3;
/// Atomic encoding for "quota-aware routing disabled".
const THRESHOLD_DISABLED: usize = 0;
/// Entries in the prompt-cache affinity pin map. Identities are u64
/// conversation prefixes; a localhost single-user proxy will essentially
/// never reach this, and eviction is harmless (one cache miss, self-heals).
const AFFINITY_CAP: usize = 4096;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic elsewhere must not wedge the proxy forever (review #12a).
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Up,
    Cooldown,
    Dead,
}

#[derive(Debug)]
struct Health {
    state: State,
    cooldown_until: Option<Instant>,
    /// Consecutive strikes; reset on any confirmed upstream success.
    strikes: u32,
    fails: u64,
    successes: u64,
    last_error: Option<String>,
}

pub struct KeyState {
    /// Per-key concurrency limit (`KEY:N`, default 3).
    pub concurrency: u32,
    held: Mutex<u32>,
    /// Waiters currently blocked on this key (bounded admissions).
    waiters: Mutex<u32>,
    /// Max simultaneous waiters on this key; over this => immediate 429.
    pub waiter_cap: u32,
    cv: Condvar,
    health: Mutex<Health>,
}

impl KeyState {
    fn new(concurrency: u32, waiter_cap: u32) -> Self {
        KeyState {
            concurrency: concurrency.max(1),
            held: Mutex::new(0),
            waiters: Mutex::new(0),
            waiter_cap,
            cv: Condvar::new(),
            health: Mutex::new(Health {
                state: State::Up,
                cooldown_until: None,
                strikes: 0,
                fails: 0,
                successes: 0,
                last_error: None,
            }),
        }
    }

    fn in_use(&self) -> u32 {
        *lock(&self.held)
    }

    /// Effective state; cooldowns expire lazily on observation.
    fn state(&self) -> State {
        let mut h = lock(&self.health);
        if h.state == State::Cooldown {
            match h.cooldown_until {
                Some(t) if t > Instant::now() => State::Cooldown,
                _ => {
                    h.state = State::Up;
                    h.cooldown_until = None;
                    State::Up
                }
            }
        } else {
            h.state
        }
    }
}

pub struct Pool {
    keys: Vec<String>,
    states: Vec<KeyState>,
    /// Round-robin tie-breaker: last successful key.
    rr: AtomicUsize,
    verbose: bool,
    /// Quota-aware routing threshold, in tenths of a percent (e.g. 800 =
    /// 80%). 0 = disabled. Written once at startup (Relaxed is enough).
    usage_threshold_x10: AtomicUsize,
    /// Bit i set = key i's latest known session usage is at/over the
    /// threshold. Only 64 bits exist, so keys at/over index 64 are never
    /// demoted rather than aliased onto another key's bit. A 0 value
    /// must never exclude keys from `candidates()` (data absent = no
    /// demotion), so it doubles as "no usage known".
    over_quota_mask: AtomicU64,
    /// Prompt-cache affinity (on by default; `--no-affinity` disables).
    /// Write-once at startup like the usage threshold.
    affinity_enabled: AtomicBool,
    /// identity -> key that last admitted it (see affinity.rs). Consulted
    /// by `admit_affine`; advisory only.
    affinity: AffinityMap,
}

/// RAII permit: one of the key's concurrency slots. Held from dispatch
/// until the last response byte (or disconnect / mid-stream death).
/// Drop frees the slot and wakes exactly one waiter on that key.
pub struct Permit<'a> {
    pool: &'a Pool,
    key: usize,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let ks = &self.pool.states[self.key];
        {
            let mut held = lock(&ks.held);
            *held = held.saturating_sub(1);
        }
        ks.cv.notify_one();
    }
}

#[derive(Debug)]
pub struct Reject {
    pub status: u16,
    pub reason: &'static str,
    /// Seconds the client should wait (max remaining cooldown) if all keys
    /// are cooling; `None` otherwise.
    pub retry_after_s: Option<u64>,
}

#[derive(Debug)]
pub enum WaitRejected {
    Full,
    Timeout,
}

/// Snapshot of one key for `/_keys`.
#[derive(Debug, serde::Serialize)]
pub struct KeyInfo {
    pub index: usize,
    pub suffix: String,
    pub state: State,
    pub concurrency: u32,
    pub in_use: u32,
    pub waiters: u32,
    pub cooldown_left_s: Option<u64>,
    pub strikes: u32,
    pub fails: u64,
    pub successes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Latest known usage (rendered only when a usage snapshot exists).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageBrief>,
}

/// Usage figures embedded in `/_keys`. Raw fractions are primary (the
/// upstream's own unit); percents are one-decimal mirrors for humans,
/// clamped to [0, 100] the same way /_usage renders them. A window the
/// snapshot does not carry stays absent (field omitted) — it is never
/// fabricated from the other window.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageBrief {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weekly_pct: Option<f64>,
    /// True when the key's session usage is at/over the configured
    /// quota-aware threshold (always false when routing is not enabled).
    pub over_quota: bool,
}

/// One-decimal percent mirror of a fraction, clamped to [0, 100] —
/// identical to usage::pct so /_keys and /_usage can never disagree.
fn pct(f: f64) -> f64 {
    (f.clamp(0.0, 1.0) * 1000.0).round() / 10.0
}

impl Pool {
    pub fn new(keys_with_concurrency: Vec<(String, u32)>, waiter_cap: u32, verbose: bool) -> Pool {
        let keys = keys_with_concurrency
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        let states = keys_with_concurrency
            .iter()
            .map(|(_, c)| KeyState::new(*c, waiter_cap))
            .collect();
        Pool {
            keys,
            states,
            rr: AtomicUsize::new(0),
            verbose,
            usage_threshold_x10: AtomicUsize::new(THRESHOLD_DISABLED),
            over_quota_mask: AtomicU64::new(0),
            affinity_enabled: AtomicBool::new(true),
            affinity: AffinityMap::new(AFFINITY_CAP),
        }
    }

    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Alias kept for readability at call sites that mean "key count".
    pub fn total_keys(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn total_slots(&self) -> u32 {
        self.states.iter().map(|s| s.concurrency).sum()
    }

    /// Whether verbose stderr logging is enabled.
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    pub fn suffix_of(&self, key: usize) -> String {
        crate::config::suffix(&self.keys[key])
    }

    fn log(&self, msg: String) {
        if self.verbose {
            eprintln!("ollamux: {msg}");
        }
    }

    pub fn state_of(&self, key: usize) -> State {
        self.states[key].state()
    }

    // ----- usage (quota-aware routing) -----

    /// Enable quota-aware routing with a threshold in whole percent
    /// (e.g. 80). Called at startup; 0 disables.
    pub fn set_usage_threshold(&self, pct: u32) {
        self.usage_threshold_x10
            .store(pct as usize * 10, Ordering::Relaxed);
    }

    /// Threshold in tenths of a percent (0 = disabled).
    #[cfg(test)]
    fn usage_threshold_x10(&self) -> usize {
        self.usage_threshold_x10.load(Ordering::Relaxed)
    }

    /// Receive a fresh usage snapshot from the tracker: rebuild the
    /// over-quota bitmask atomically. Read-only with respect to key
    /// health — usage never marks, cools, or settles keys.
    pub fn publish_usage(&self, snap: &crate::usage::UsageSnapshot) {
        let threshold = self.usage_threshold_x10.load(Ordering::Relaxed);
        let mut mask = 0u64;
        if threshold != THRESHOLD_DISABLED {
            for k in &snap.keys {
                // Keys beyond bit 63 cannot be represented in the mask;
                // skip them (never demoted) instead of folding several
                // keys onto one bit or shifting out of range.
                if k.index >= u64::BITS as usize {
                    continue;
                }
                if let Some(session) = k.session {
                    // Threshold is in tenths of a percent; usage is a
                    // 0.0–1.0 fraction (compare in tenths of a percent:
                    // session * 1000 >= threshold).
                    if (session * 1000.0).clamp(0.0, 1000.0) as usize >= threshold {
                        mask |= 1u64 << k.index;
                    }
                }
            }
        }
        self.over_quota_mask.store(mask, Ordering::Relaxed);
    }

    /// Latest usage rendering per key for `/_keys`: a pure in-memory read
    /// of the optional snapshot — never triggers a fetch. Windows absent
    /// from the snapshot stay absent (never substituted from the other
    /// window, matching /_usage's honest rendering of partial data).
    pub fn usage_briefs(&self, snap: &crate::usage::UsageSnapshot) -> Vec<Option<UsageBrief>> {
        let threshold = self.usage_threshold_x10.load(Ordering::Relaxed);
        snap.keys
            .iter()
            .map(|k| {
                if k.session.is_none() && k.weekly.is_none() {
                    return None;
                }
                Some(UsageBrief {
                    session: k.session,
                    weekly: k.weekly,
                    session_pct: k.session.map(pct),
                    weekly_pct: k.weekly.map(pct),
                    over_quota: threshold != THRESHOLD_DISABLED
                        && k.session
                            .is_some_and(|s| (s * 1000.0).clamp(0.0, 1000.0) as usize >= threshold),
                })
            })
            .collect()
    }

    /// True if at least one key is selectable right now.
    pub fn healthy_any(&self) -> bool {
        self.states.iter().any(|st| st.state() == State::Up)
    }

    /// Whether `key` is currently demoted by quota-aware routing. Mask-
    /// derived (routing-time semantics, identical to `candidates()`), not
    /// snapshot-derived: threshold disabled → never; index ≥ 64 → never
    /// (no mask bit exists); otherwise the mask bit decides.
    pub fn is_over_quota(&self, key: usize) -> bool {
        if self.usage_threshold_x10.load(Ordering::Relaxed) == THRESHOLD_DISABLED {
            return false;
        }
        if key >= u64::BITS as usize {
            return false;
        }
        (self.over_quota_mask.load(Ordering::Relaxed) >> key) & 1 == 1
    }

    // ----- prompt-cache affinity -----

    /// Disable prompt-cache affinity (`--no-affinity` / env). Write-once at
    /// startup, mirroring `set_usage_threshold`.
    pub fn set_affinity_enabled(&self, on: bool) {
        self.affinity_enabled.store(on, Ordering::Relaxed);
    }

    pub fn affinity_enabled(&self) -> bool {
        self.affinity_enabled.load(Ordering::Relaxed)
    }

    /// Pinned key for `id`, if affinity is on and the pin exists. The
    /// returned key is *not* health-checked here — callers re-check state
    /// under the pool's normal races (same as candidates()).
    pub fn pinned_key(&self, id: u64) -> Option<usize> {
        if !self.affinity_enabled() {
            return None;
        }
        self.affinity.get(id)
    }

    /// Whether `id` currently has a pin that could serve *right now* —
    /// Up, not over-quota, and a free slot: the same usability test
    /// `admit_affine` applies before honoring a pin. Read-only (no slot
    /// is taken); races with concurrent admissions are inherent and
    /// harmless — this only feeds the advisory X-Ollamux-Affinity header.
    pub fn pin_usable(&self, id: u64) -> bool {
        let Some(key) = self.pinned_key(id) else {
            return false;
        };
        let ks = &self.states[key];
        ks.state() == State::Up && !self.is_over_quota(key) && ks.in_use() < ks.concurrency
    }

    /// Pin (or re-pin) an identity to a key. Called at admission time (so
    /// parallel same-conversation requests share the pin even while the
    /// first response streams) and again on confirmed 2xx success.
    pub fn pin(&self, id: u64, key: usize) {
        if self.affinity_enabled() {
            self.affinity.put(id, key);
        }
    }

    /// Unpin `id` iff it points at `key`: a retryable failure (429/401/5xx)
    /// on the pinned key must not deterministically re-select the flapping
    /// key for every subsequent same-conversation request until it cools.
    pub fn unpin(&self, id: u64, key: usize) {
        self.affinity.remove_if(id, key);
    }

    /// The secret for key `i`. Only used to build the Authorization header.
    pub fn secret_of(&self, key: usize) -> &str {
        &self.keys[key]
    }

    // ----- health transitions -----

    /// Invalid credential: dead until restart.
    pub fn mark_dead(&self, key: usize, reason: &str) {
        let st = &self.states[key];
        let mut h = lock(&st.health);
        h.state = State::Dead;
        h.cooldown_until = None;
        h.strikes = 0;
        h.fails += 1;
        h.last_error = Some(reason.to_string());
        drop(h);
        self.log(format!(
            "key #{} {}: DEAD ({})",
            key,
            self.suffix_of(key),
            reason
        ));
    }

    pub fn mark_cooldown(&self, key: usize, dur: Duration, reason: &str) {
        let st = &self.states[key];
        let mut h = lock(&st.health);
        h.state = State::Cooldown;
        h.cooldown_until = Some(Instant::now() + dur);
        h.last_error = Some(reason.to_string());
        h.fails += 1;
        let left = h
            .cooldown_until
            .unwrap()
            .saturating_duration_since(Instant::now());
        drop(h);
        self.log(format!(
            "key #{} {}: cooldown {}s ({})",
            key,
            self.suffix_of(key),
            left.as_secs(),
            reason
        ));
    }

    pub fn mark_strike(&self, key: usize, reason: &str) {
        let st = &self.states[key];
        let strikes = {
            let mut h = lock(&st.health);
            h.strikes += 1;
            h.fails += 1;
            h.last_error = Some(reason.to_string());
            h.strikes
        };
        if strikes >= STRIKES_TO_COOL {
            self.mark_cooldown(key, COOLDOWN_429, reason);
            lock(&st.health).strikes = 0;
        }
    }

    /// A confirmed upstream success: clears strikes/cooldown and counts.
    pub fn settle(&self, key: usize, ok: bool) {
        let st = &self.states[key];
        let mut h = lock(&st.health);
        if ok {
            h.state = State::Up;
            h.cooldown_until = None;
            h.strikes = 0;
            h.successes += 1;
        }
        drop(h);
        if ok {
            self.rr.store(key, Ordering::Relaxed);
        }
    }

    // ----- introspection -----

    /// Time until the soonest cooldown expires (Retry-After on overload).
    pub fn next_retry_in(&self) -> Option<Duration> {
        self.states
            .iter()
            .filter_map(|st| {
                let h = lock(&st.health);
                if h.state == State::Cooldown {
                    h.cooldown_until
                } else {
                    None
                }
            })
            .min()
            .map(|t| t.saturating_duration_since(Instant::now()))
    }

    pub fn info(&self) -> Vec<KeyInfo> {
        self.states
            .iter()
            .enumerate()
            .map(|(i, st)| {
                let state = st.state();
                let h = lock(&st.health);
                KeyInfo {
                    index: i,
                    suffix: self.suffix_of(i),
                    state,
                    concurrency: st.concurrency,
                    in_use: st.in_use(),
                    waiters: *lock(&st.waiters),
                    cooldown_left_s: if state == State::Cooldown {
                        h.cooldown_until
                            .map(|t| t.saturating_duration_since(Instant::now()).as_secs() + 1)
                    } else {
                        None
                    },
                    strikes: h.strikes,
                    fails: h.fails,
                    successes: h.successes,
                    last_error: h.last_error.clone(),
                    usage: None,
                }
            })
            .collect()
    }

    /// `info()` with usage columns joined in from a snapshot (used by
    /// `/_keys` when a usage snapshot exists). Joined by index rather
    /// than zipped: a short snapshot (defensive only — keys are fixed at
    /// startup) leaves the extra keys' usage absent instead of silently
    /// truncating the /_keys output.
    pub fn info_with_usage(
        &self,
        snap: &crate::usage::UsageSnapshot,
    ) -> Vec<(KeyInfo, Option<UsageBrief>)> {
        let briefs = self.usage_briefs(snap);
        self.info()
            .into_iter()
            .enumerate()
            .map(|(i, info)| {
                let usage = briefs.get(i).cloned().flatten();
                (info, usage)
            })
            .collect()
    }

    // ----- selection & admission -----

    /// Selectable keys right now (State::Up), least-loaded first; the
    /// round-robin counter rotates exact ties. Load = in_use/concurrency.
    /// When quota-aware routing is enabled, keys whose latest known
    /// session usage is at/over the threshold are demoted behind fresh
    /// ones — demoted, never excluded: an over-quota key still serves if
    /// no fresh key has a free slot, preserving the "never exclude" rule.
    pub fn candidates(&self) -> Vec<usize> {
        let n = self.states.len().max(1) as u64;
        let rr = self.rr.load(Ordering::Relaxed) as u64;
        let demote_mask = self.over_quota_mask.load(Ordering::Relaxed);
        let mut cands: Vec<usize> = (0..self.states.len())
            .filter(|&i| self.states[i].state() == State::Up)
            .collect();
        cands.sort_by_key(|&i| {
            let ks = &self.states[i];
            let load_x1024 = (ks.in_use() as u64 * 1024) / ks.concurrency as u64;
            // Mask bit only exists for indices < 64 (see publish_usage):
            // higher keys are never demoted, never panics on debug shifts.
            let over = if i < u64::BITS as usize {
                (demote_mask >> i) & 1
            } else {
                0
            };
            let ord = (i as u64 + rr) % n;
            (over, load_x1024, ord)
        });
        cands
    }

    /// Take a slot on `key` without blocking, if one is free.
    pub fn try_acquire(&self, key: usize) -> Option<Permit<'_>> {
        let ks = &self.states[key];
        let mut held = lock(&ks.held);
        if *held < ks.concurrency {
            *held += 1;
            drop(held);
            Some(Permit { pool: self, key })
        } else {
            None
        }
    }

    /// Block on one key's condvar until a slot frees or `timeout` elapses.
    /// Per-key wait cap bounded; FIFO by wakeup order (notify_one).
    pub fn acquire_on(&self, key: usize, timeout: Duration) -> Result<Permit<'_>, WaitRejected> {
        let ks = &self.states[key];
        {
            let mut waiters = lock(&ks.waiters);
            if *waiters >= ks.waiter_cap {
                return Err(WaitRejected::Full);
            }
            *waiters += 1;
        }
        let deadline = Instant::now() + timeout;
        let result = loop {
            if let Some(p) = self.try_acquire(key) {
                break Ok(p);
            }
            let now = Instant::now();
            if now >= deadline {
                break Err(WaitRejected::Timeout);
            }
            // Timeout granularity: min(remaining, 250ms) keeps responsiveness
            // bounded without busy-spinning (review #10: always re-check
            // acquire after waking).
            let chunk = deadline.min(now + Duration::from_millis(250)) - now;
            let held = lock(&ks.held);
            let _ = ks
                .cv
                .wait_timeout_while(held, chunk, |h| *h >= ks.concurrency);
        };
        {
            let mut waiters = lock(&ks.waiters);
            *waiters = waiters.saturating_sub(1);
        }
        result
    }

    /// Admit one request: pick the best healthy key and take (or wait for)
    /// one of its slots. The request body is buffered *before* this call, so
    /// bounded admission bounds memory only from here on; the 16 MiB body
    /// cap plus the (worker-count × body cap) bound covers the pre-admission
    /// phase. Admission deliberately does NOT touch key health: strikes and
    /// cooldowns may only be cleared by a confirmed upstream success
    /// (`settle`), otherwise a permanently-failing key would have its
    /// strike counter wiped by every new arrival.
    pub fn admit(&self, timeout: Duration) -> Result<(Permit<'_>, usize), Reject> {
        if self.is_empty() {
            return Err(Reject {
                status: 503,
                reason: "ollamux has no API keys configured; set OLLAMUX_KEYS or create ~/.config/ollamux/keys (one key per line, https://ollama.com/settings/keys), then restart",
                retry_after_s: None,
            });
        }
        // All dead?
        if self.states.iter().all(|st| st.state() == State::Dead) {
            return Err(Reject {
                status: 403,
                reason: "all ollamux API keys were rejected by https://ollama.com as invalid (401/403): check the keys at https://ollama.com/settings/keys and restart ollamux — dead keys stay dead until restart. Per-key state: GET /_keys",
                retry_after_s: None,
            });
        }
        let cands = self.candidates();
        let cands = if cands.is_empty() {
            // Racing with concurrent health transitions (another worker just
            // marked the last Up key dead/cooling): re-read the pool state
            // once and classify honestly instead of panicking.
            if self.states.iter().all(|st| st.state() == State::Dead) {
                return Err(Reject {
                    status: 403,
                    reason: "all ollamux API keys were rejected by https://ollama.com as invalid (401/403): check the keys at https://ollama.com/settings/keys and restart ollamux — dead keys stay dead until restart. Per-key state: GET /_keys",
                    retry_after_s: None,
                });
            }
            let secs = self
                .next_retry_in()
                .map(|d| d.as_secs().max(1))
                .unwrap_or(2);
            return Err(Reject {
                status: 429,
                reason: "all ollamux keys are cooling down (rate-limited by https://ollama.com) and recover automatically; wait for the Retry-After header, then retry. Per-key state: GET /_keys",
                retry_after_s: Some(secs),
            });
        } else {
            cands
        };
        for &key in &cands {
            if let Some(p) = self.try_acquire(key) {
                return Ok((p, key));
            }
        }
        // All Up keys at capacity: wait on the least-loaded one.
        match self.acquire_on(cands[0], timeout) {
            Ok(p) => Ok((p, cands[0])),
            Err(WaitRejected::Full) => Err(Reject {
                status: 429,
                reason: "ollamux is overloaded: every key is at its concurrency limit and the wait queue is full; raise per-key concurrency (KEY:N in the keys file) or add more keys. Retry shortly",
                retry_after_s: Some(2),
            }),
            Err(WaitRejected::Timeout) => Err(Reject {
                status: 429,
                reason: "ollamux is overloaded: the request queued too long for a free key slot; raise per-key concurrency (KEY:N in the keys file) or reduce parallel requests. Retry via Retry-After",
                retry_after_s: Some(1),
            }),
        }
    }

    /// Affinity-aware admission: try the pinned key first (only when it can
    /// serve *right now* — Up, not over-quota, free slot), else fall through
    /// to plain `admit()` unchanged. Deliberately no bounded wait on the
    /// pinned key: while the pin frees up, another key may be fully idle,
    /// and 2s+ of guaranteed added latency for a maybe-warm cache is the
    /// wrong trade for an interactive proxy. A miss costs one cold prefix,
    /// then the success re-pin heals it.
    ///
    /// On success the identity is pinned to the admitted key immediately
    /// (not only after the upstream succeeds): streaming responses can hold
    /// a slot for minutes, and a parallel same-conversation request must
    /// share the pin, not miss it and split the upstream cache.
    pub fn admit_affine(
        &self,
        timeout: Duration,
        id: Option<u64>,
    ) -> Result<(Permit<'_>, usize), Reject> {
        if let Some(id) = id {
            if let Some(key) = self.pinned_key(id) {
                let usable = self.states[key].state() == State::Up && !self.is_over_quota(key);
                if usable {
                    if let Some(p) = self.try_acquire(key) {
                        self.pin(id, key);
                        return Ok((p, key));
                    }
                }
            }
        }
        let (p, key) = self.admit(timeout)?;
        if let Some(id) = id {
            self.pin(id, key);
        }
        Ok((p, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(n: u32, conc: u32) -> Pool {
        Pool::new(
            (0..n).map(|i| (format!("omk-test{i:04}"), conc)).collect(),
            32,
            false,
        )
    }

    #[test]
    fn try_acquire_is_bounded_and_raii_releases() {
        let p = pool(1, 2);
        let a = p.try_acquire(0).unwrap();
        let b = p.try_acquire(0).unwrap();
        assert!(p.try_acquire(0).is_none());
        assert_eq!(p.states[0].in_use(), 2);
        drop(b);
        assert_eq!(p.states[0].in_use(), 1);
        assert!(p.try_acquire(0).is_some());
        drop(a);
    }

    #[test]
    fn least_loaded_candidate_wins() {
        let p = pool(2, 2);
        let _a = p.try_acquire(0).unwrap(); // key 0 half busy
        assert_eq!(p.candidates()[0], 1); // key 1 free
    }

    #[test]
    fn cooldown_expires_lazily() {
        let p = pool(1, 1);
        p.mark_cooldown(0, Duration::from_millis(10), "test");
        assert_eq!(p.state_of(0), State::Cooldown);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(p.state_of(0), State::Up);
    }

    #[test]
    fn strikes_lead_to_cooldown_and_success_resets() {
        let p = pool(1, 1);
        p.mark_strike(0, "e1");
        p.mark_strike(0, "e2");
        assert_eq!(p.state_of(0), State::Up);
        p.mark_strike(0, "e3");
        assert_eq!(p.state_of(0), State::Cooldown);
        std::thread::sleep(Duration::from_millis(20));
        // settle(ok) clears everything
        p.settle(0, true);
        let h = lock(&p.states[0].health);
        assert_eq!(h.strikes, 0);
        assert_eq!(h.state, State::Up);
    }

    #[test]
    fn dead_key_is_excluded_until_restart() {
        let p = pool(2, 1);
        p.mark_dead(0, "401");
        assert_eq!(p.state_of(0), State::Dead);
        // candidates skip it even after time passes
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(p.candidates(), vec![1]);
    }

    #[test]
    fn admit_bounded_by_concurrency_then_waits() {
        let p = pool(1, 1);
        {
            let (permit, key) = p.admit(Duration::from_millis(50)).unwrap();
            assert_eq!(key, 0);
            // Second admit has no free slot and must time out.
            let err = match p.admit(Duration::from_millis(50)) {
                Err(e) => e,
                Ok(_) => panic!("second admit should be rejected (no free slot)"),
            };
            assert_eq!(err.status, 429);
            drop(permit);
        }
        // Slot free again.
        assert!(p.admit(Duration::from_millis(50)).is_ok());
    }

    #[test]
    fn admit_all_dead_is_403_all_cooling_is_429() {
        let p = pool(2, 1);
        p.mark_dead(0, "x");
        p.mark_dead(1, "x");
        let err = match p.admit(Duration::from_millis(10)) {
            Err(e) => e,
            Ok(_) => panic!("all-dead pool must reject"),
        };
        assert_eq!(err.status, 403);

        let p = pool(1, 1);
        p.mark_cooldown(0, Duration::from_millis(500), "429");
        let err = match p.admit(Duration::from_millis(10)) {
            Err(e) => e,
            Ok(_) => panic!("all-cooling pool must reject"),
        };
        assert_eq!(err.status, 429);
        assert!(err.retry_after_s.unwrap() >= 1);
    }

    #[test]
    fn acquire_on_respects_waiter_cap() {
        // Waiter cap is the per-key bound on blocked requests: waiters that
        // fit may block; excess ones get `Full` immediately.
        let p = std::sync::Arc::new(Pool::new(
            vec![("omk-cap-test".into(), 1)],
            2, // waiter_cap = 2
            false,
        ));
        let _slot = p.try_acquire(0).unwrap(); // hold the only slot

        // 2 waiters block (then time out); the 3rd gets Full immediately.
        // Results go through a channel: a Permit borrows the pool, so it
        // cannot cross the thread boundary.
        let (tx, rx) = std::sync::mpsc::channel::<&'static str>();
        for _ in 0..2 {
            let p = std::sync::Arc::clone(&p);
            let tx = tx.clone();
            std::thread::spawn(move || {
                let verdict = match p.acquire_on(0, Duration::from_millis(200)) {
                    Ok(_) => "acquired",
                    Err(WaitRejected::Timeout) => "timeout",
                    Err(WaitRejected::Full) => "full",
                };
                tx.send(verdict).unwrap();
            });
        }
        drop(tx);
        std::thread::sleep(Duration::from_millis(30));
        let w3 = (*p).acquire_on(0, Duration::from_millis(30));
        assert!(
            matches!(w3, Err(WaitRejected::Full)),
            "excess waiter must be rejected"
        );
        assert_eq!(*lock(&p.states[0].waiters), 2);
        let mut verdicts: Vec<&str> = rx.iter().collect();
        verdicts.sort_unstable();
        assert_eq!(verdicts, vec!["timeout", "timeout"]);
        assert_eq!(*lock(&p.states[0].waiters), 0, "no waiter leaks");
    }

    #[test]
    fn concurrent_admit_respects_total_slots() {
        let p = std::sync::Arc::new(pool(1, 3));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let p = p.clone();
                std::thread::spawn(move || {
                    if let Ok((permit, _)) = p.admit(Duration::from_millis(500)) {
                        std::thread::sleep(Duration::from_millis(20));
                        drop(permit);
                        true
                    } else {
                        false
                    }
                })
            })
            .collect();
        let ok = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert!(
            ok >= 8,
            "most requests should eventually get a slot (got {ok})"
        );
        assert_eq!(p.states[0].in_use(), 0, "all permits released");
    }

    #[test]
    fn concurrent_permits_respect_limit() {
        let p = std::sync::Arc::new(pool(1, 3));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let p = p.clone();
                std::thread::spawn(move || {
                    let (permit, _) = p.admit(Duration::from_millis(500)).unwrap();
                    std::thread::sleep(Duration::from_millis(20));
                    drop(permit);
                    p.settle(0, true);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(p.states[0].in_use(), 0);
        assert!(p.states[0].health.lock().unwrap().successes >= 16);
    }

    #[test]
    fn admission_does_not_reset_strikes() {
        // A key that keeps failing must reach the cooldown even under
        // steady arrival of new requests: taking a slot (admission) must
        // never wipe strikes — only a confirmed success may.
        let p = pool(1, 8);
        let (_permit, _) = p.admit(Duration::from_millis(100)).unwrap();
        p.mark_strike(0, "boom1");
        let (_permit2, _) = p.admit(Duration::from_millis(100)).unwrap();
        p.mark_strike(0, "boom2");
        assert_eq!(p.state_of(0), State::Up);
        // Admission between strikes; strikes must still be 2.
        let (_permit3, _) = p.admit(Duration::from_millis(100)).unwrap();
        assert_eq!(lock(&p.states[0].health).strikes, 2);
        p.mark_strike(0, "boom3");
        assert_eq!(
            p.state_of(0),
            State::Cooldown,
            "3 strikes must cool the key"
        );
        assert_eq!(lock(&p.states[0].health).strikes, 0);
        assert_eq!(lock(&p.states[0].health).successes, 0);
    }

    // ----- quota-aware routing -----

    fn usage_snap(entries: &[(usize, Option<f64>)]) -> crate::usage::UsageSnapshot {
        crate::usage::UsageSnapshot {
            fetched_at: std::time::Instant::now(),
            keys: entries
                .iter()
                .map(|&(i, session)| crate::usage::KeyUsage {
                    index: i,
                    suffix: format!("sfx{i}"),
                    ok: session.is_some(),
                    status: session.map(|_| 200),
                    session,
                    weekly: session,
                    session_pct: session.map(|s| (s * 1000.0).round() / 10.0),
                    weekly_pct: session.map(|s| (s * 1000.0).round() / 10.0),
                    models: Vec::new(),
                    cost: None,
                    error: None,
                })
                .collect(),
        }
    }

    fn two_key_pool() -> Pool {
        Pool::new(
            vec![("omk-quota-key1".into(), 4), ("omk-quota-key2".into(), 4)],
            4,
            false,
        )
    }

    #[test]
    fn usage_publish_is_health_read_only() {
        let p = two_key_pool();
        p.publish_usage(&usage_snap(&[(0, Some(0.99)), (1, Some(0.01))]));
        // Usage alone must never mark/cooldown/settle anything.
        assert_eq!(p.state_of(0), State::Up);
        assert_eq!(p.state_of(1), State::Up);
        assert_eq!(lock(&p.states[0].health).fails, 0);
        assert_eq!(lock(&p.states[1].health).successes, 0);
    }

    #[test]
    fn over_threshold_key_demotes_but_never_excluded() {
        let p = two_key_pool();
        p.set_usage_threshold(80);
        assert_eq!(p.usage_threshold_x10(), 800);
        // Key 0 fresh and idle must win over an over-quota key even if
        // the over-quota key has had more success (rr tie-break).
        p.publish_usage(&usage_snap(&[(0, Some(0.10)), (1, Some(0.95))]));
        p.settle(1, true); // rr favors key 1
        let cands = p.candidates();
        assert_eq!(cands, vec![0, 1], "over-quota key sorts last");

        // Only the over-quota key is selectable: it is demoted, never
        // excluded (disabling the threshold restores plain ordering).
        p.mark_cooldown(0, Duration::from_secs(60), "test");
        let cands = p.candidates();
        assert_eq!(cands, vec![1], "over-quota key still serves when alone");

        // Data absent → mask empty → order unchanged.
        p.publish_usage(&usage_snap(&[(0, None), (1, None)]));
        p.mark_cooldown(0, Duration::ZERO, "expired");
        let cands = p.candidates();
        assert_eq!(cands.len(), 2, "absent data excludes nobody");
        assert!(cands.contains(&0) && cands.contains(&1));
    }

    #[test]
    fn threshold_disabled_ignores_usage_data() {
        let p = two_key_pool();
        // No set_usage_threshold call: publishing an all-over-quota
        // snapshot must not change ordering at all.
        p.publish_usage(&usage_snap(&[(0, Some(1.0)), (1, Some(1.0))]));
        p.settle(1, true);
        // Both keys load 0 → rr tie-break decides; no crash, no exclusion.
        let cands = p.candidates();
        assert_eq!(cands.len(), 2);
        // And the mask itself stays empty.
        assert_eq!(p.over_quota_mask.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn usage_briefs_render_and_over_quota_flag() {
        let p = two_key_pool();
        p.set_usage_threshold(80);
        // Failure rows (ok=false, no numbers) render as None.
        let snap = usage_snap(&[(0, None), (1, Some(0.812))]);
        let briefs = p.usage_briefs(&snap);
        assert!(briefs[0].is_none());
        let b = briefs[1].as_ref().unwrap();
        assert_eq!(b.session, Some(0.812));
        assert_eq!(b.session_pct, Some(81.2));
        assert!(b.over_quota, "81.2% >= 80% threshold");
    }

    #[test]
    fn usage_briefs_never_fabricate_missing_windows() {
        let p = two_key_pool();
        // Session-only row: weekly must stay absent, not copy session.
        // (usage_snap's helper mirrors weekly=session, so build the row
        // by hand to model upstream drift honestly.)
        let mut row = usage_snap(&[(0, None)]).keys.remove(0);
        row.session = Some(0.4);
        row.weekly = None;
        row.ok = true;
        let snap = crate::usage::UsageSnapshot {
            fetched_at: std::time::Instant::now(),
            keys: vec![row],
        };
        let briefs = p.usage_briefs(&snap);
        let b = briefs[0].as_ref().unwrap();
        assert_eq!(b.session, Some(0.4));
        assert_eq!(b.weekly, None);
        assert_eq!(b.weekly_pct, None);
        // Weekly-only row: symmetric.
        let mut row = usage_snap(&[(0, None)]).keys.remove(0);
        row.weekly = Some(0.2);
        row.ok = true;
        let snap = crate::usage::UsageSnapshot {
            fetched_at: std::time::Instant::now(),
            keys: vec![row],
        };
        let briefs = p.usage_briefs(&snap);
        let b = briefs[0].as_ref().unwrap();
        assert_eq!(b.session, None);
        assert_eq!(b.weekly, Some(0.2));
        assert!(!b.over_quota, "session absent → never over quota");
    }

    #[test]
    fn over_quota_mask_ignores_keys_beyond_64_bits() {
        // Pool with 65 keys; the last one is over quota. Before the
        // bounds check this folded bit 64 onto bit 0 (release) or
        // panicked on the shift (debug).
        let keys: Vec<(String, u32)> = (0..65).map(|i| (format!("omk-wide{i:04}"), 2)).collect();
        let p = Pool::new(keys, 4, false);
        p.set_usage_threshold(80);
        let entries: Vec<(usize, Option<f64>)> = (0..65)
            .map(|i| {
                // Only key 64 is over threshold; bit 63 is the highest
                // representable one and stays under threshold here.
                if i == 64 {
                    (i, Some(0.99))
                } else {
                    (i, Some(0.10))
                }
            })
            .collect();
        p.publish_usage(&usage_snap(&entries));
        // No aliasing: only bits < 64 may be set, and key 0 must be
        // untouched even though key 64 is over quota.
        let mask = p.over_quota_mask.load(Ordering::Relaxed);
        assert_eq!(mask, 0, "no over-quota key below 64 → empty mask");
        // candidates() must not panic (debug >> overflow) for any index.
        let cands = p.candidates();
        assert_eq!(cands.len(), 65);
        assert_eq!(cands[0], 0, "key 0 must not inherit key 64's demotion");
    }

    // ----- prompt-cache affinity -----

    #[test]
    fn pin_usable_mirrors_admit_affine_usability() {
        let p = pool(2, 1);
        // No pin yet: not usable.
        assert!(!p.pin_usable(5));
        let (permit, key) = p.admit_affine(Duration::from_millis(50), Some(5)).unwrap();
        // Pin exists and key is Up with (this permit just taken, but
        // concurrency is 1 — the pinned key is now full).
        if key == 0 {
            assert!(!p.pin_usable(5), "pinned key full → not usable");
        }
        drop(permit);
        assert!(p.pin_usable(5), "pin Up + free slot → usable");
        // Cooldown makes the pin unusable.
        p.mark_cooldown(key, Duration::from_millis(50), "test");
        assert!(!p.pin_usable(5), "cooling pinned key → not usable");
        // Dead makes the pin unusable.
        p.mark_dead(key, "test");
        assert!(!p.pin_usable(5), "dead pinned key → not usable");
    }

    #[test]
    fn pin_usable_false_when_over_quota_or_disabled() {
        let p = two_key_pool();
        p.set_usage_threshold(80);
        p.publish_usage(&usage_snap(&[(0, Some(0.95)), (1, Some(0.10))]));
        p.pin(11, 0);
        assert!(p.is_over_quota(0));
        assert!(!p.pin_usable(11), "over-quota pinned key → not usable");
        p.set_affinity_enabled(false);
        assert!(!p.pin_usable(11), "affinity disabled → no usable pin");
        p.set_affinity_enabled(true);
        p.publish_usage(&usage_snap(&[(0, Some(0.10))]));
        assert!(p.pin_usable(11), "back under quota + enabled → usable");
    }

    #[test]
    fn affine_pin_hit_reuses_pinned_key() {
        let p = pool(2, 2);
        // First admission pins (rr favors key 0 on a fresh pool).
        let (permit, key) = p.admit_affine(Duration::from_millis(50), Some(42)).unwrap();
        drop(permit);
        // Second admission with the same identity must land on the same
        // key even though both keys are fresh (a plain admit would
        // tie-break by load/rr, not by history).
        let (_permit2, key2) = p.admit_affine(Duration::from_millis(50), Some(42)).unwrap();
        assert_eq!(key, key2, "same identity must pin to the same key");
        // A different identity is not pinned: fresh pool, rr now favors
        // the other key after settle-less success (rr unchanged) — the
        // load/rr ordering decides, not the map.
        let (_p3, key3) = p.admit_affine(Duration::from_millis(50), Some(43)).unwrap();
        assert_ne!(key3, key, "different identity must not inherit the pin");
    }

    #[test]
    fn affine_falls_back_when_pinned_dead_or_cooling() {
        let p = pool(2, 1);
        let (_a, key) = p.admit_affine(Duration::from_millis(50), Some(7)).unwrap();
        drop(_a);
        p.mark_dead(key, "test 401");
        // Pinned key is dead: must fall through to plain admit and pick
        // the survivor.
        let (_b, key2) = p.admit_affine(Duration::from_millis(50), Some(7)).unwrap();
        assert_ne!(key2, key, "dead pinned key must not be selected");
        // And the fallback re-pinned the identity to the survivor.
        drop(_b);
        let (_c, key3) = p.admit_affine(Duration::from_millis(50), Some(7)).unwrap();
        assert_eq!(key2, key3, "fallback must re-pin to the serving key");

        // Same shape with a cooldown (recoverable, not dead).
        let p2 = pool(2, 1);
        let (_d, k1) = p2.admit_affine(Duration::from_millis(50), Some(9)).unwrap();
        drop(_d);
        p2.mark_cooldown(k1, Duration::from_millis(500), "429");
        let (_e, k2) = p2.admit_affine(Duration::from_millis(50), Some(9)).unwrap();
        assert_ne!(k2, k1, "cooling pinned key must not be selected");
    }

    #[test]
    fn affine_falls_back_when_pinned_at_capacity() {
        let p = pool(2, 1);
        let (_a, pinned) = p.admit_affine(Duration::from_millis(50), Some(5)).unwrap();
        // Pinned key's only slot is held: same identity must not block on
        // it — the other key's free slot wins immediately.
        let (_b, other) = p.admit_affine(Duration::from_millis(50), Some(5)).unwrap();
        assert_ne!(other, pinned, "full pinned key must fall through");
        // Re-pin moved to the fallback key (admission-time pin).
        drop(_b);
        let (_c, again) = p.admit_affine(Duration::from_millis(50), Some(5)).unwrap();
        assert_eq!(again, other, "fallback re-pins the identity");
        drop(_c);
        drop(_a);
        // All free again: identity still points at the fallback key.
        let (_d, after) = p.admit_affine(Duration::from_millis(50), Some(5)).unwrap();
        assert_eq!(after, other, "re-pin survives while the pin is valid");
    }

    #[test]
    fn affine_respects_over_quota_demotion() {
        let p = two_key_pool();
        p.set_usage_threshold(80);
        p.publish_usage(&usage_snap(&[(0, Some(0.95)), (1, Some(0.10))]));
        // Pin to the over-quota key 0.
        p.pin(11, 0);
        assert!(p.is_over_quota(0));
        assert!(!p.is_over_quota(1));
        // Over-quota pinned key must not win while a fresh key exists —
        // affinity never overrides quota-aware demotion.
        let (_a, key) = p.admit_affine(Duration::from_millis(50), Some(11)).unwrap();
        assert_eq!(key, 1, "over-quota pin must fall through to fresh key");
        // Demote-never-exclude: with key 1 gone, the over-quota pinned
        // key still serves.
        p.mark_dead(1, "test");
        let (_b, key2) = p.admit_affine(Duration::from_millis(50), Some(11)).unwrap();
        assert_eq!(key2, 0, "over-quota key still serves when alone");
    }

    #[test]
    fn affine_threshold_disabled_never_blocks_pin() {
        let p = two_key_pool();
        // No set_usage_threshold: mask stays 0 even when usage is
        // published; a pinned key must still be used.
        p.publish_usage(&usage_snap(&[(0, Some(1.0)), (1, Some(0.0))]));
        assert!(!p.is_over_quota(0), "disabled threshold → never over quota");
        p.pin(3, 0);
        let (_a, key) = p.admit_affine(Duration::from_millis(50), Some(3)).unwrap();
        assert_eq!(key, 0, "pin must hold without quota-aware routing");
    }

    #[test]
    fn is_over_quota_ignores_keys_beyond_64_bits() {
        let keys: Vec<(String, u32)> = (0..65).map(|i| (format!("omk-aff{i:04}"), 2)).collect();
        let p = Pool::new(keys, 4, false);
        p.set_usage_threshold(80);
        let entries: Vec<(usize, Option<f64>)> = (0..65)
            .map(|i| (i, Some(if i == 64 { 0.99 } else { 0.10 })))
            .collect();
        p.publish_usage(&usage_snap(&entries));
        assert!(
            !p.is_over_quota(64),
            "index 64 has no mask bit → never over quota"
        );
        assert!(!p.is_over_quota(0));
        // And admission must not panic for a pinned high index.
        p.pin(1, 64);
        let (_a, _k) = p.admit_affine(Duration::from_millis(50), Some(1)).unwrap();
    }

    #[test]
    fn affine_disabled_ignores_map() {
        let p = pool(2, 2);
        let (_a, _key) = p.admit_affine(Duration::from_millis(50), Some(21)).unwrap();
        drop(_a);
        p.set_affinity_enabled(false);
        assert!(!p.affinity_enabled());
        // Pin still in the map, but affinity off: with both keys fresh the
        // rr tie-break repeats the same pick a plain admit() would make.
        // To prove the pin is ignored, saturate the would-be pinned key:
        // with affinity ON a same-identity request would block/fall to the
        // other key only via the map; with it OFF, plain admit() must
        // prefer the free key instantly.
        let p = pool(2, 1);
        let (_a, _key) = p.admit_affine(Duration::from_millis(50), Some(21)).unwrap();
        p.set_affinity_enabled(false);
        drop(_a);
        // Direct proof: pin an identity to key 1 while key 0 is the
        // natural (rr/least-loaded) pick, then disable affinity — the
        // natural pick must win.
        let p2 = two_key_pool();
        p2.pin(99, 1);
        p2.set_affinity_enabled(false);
        let (_c, natural) = p2
            .admit_affine(Duration::from_millis(50), Some(99))
            .unwrap();
        assert_eq!(natural, 0, "disabled affinity: plain admit order wins");
    }

    #[test]
    fn affine_concurrent_same_identity_respects_slots() {
        let p = std::sync::Arc::new(pool(2, 1));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = std::sync::Arc::clone(&p);
                std::thread::spawn(move || {
                    let (permit, _key) = p
                        .admit_affine(Duration::from_millis(500), Some(77))
                        .unwrap_or_else(|e| panic!("admit failed: {e:?}"));
                    std::thread::sleep(Duration::from_millis(10));
                    drop(permit);
                    true
                })
            })
            .collect();
        let mut total = 0;
        for h in handles {
            h.join().unwrap();
            total += 1;
        }
        assert_eq!(total, 8);
        assert_eq!(p.states[0].in_use() + p.states[1].in_use(), 0);
        // Map has exactly the one identity, pointing at one of the keys.
        assert_eq!(p.affinity.len(), 1);
    }

    #[test]
    fn unpin_only_affects_current_pin() {
        let p = pool(2, 1);
        p.pin(1, 0);
        p.unpin(1, 0);
        assert_eq!(p.pinned_key(1), None);
        // Re-pin; unpinning with the wrong key must not drop it.
        p.pin(1, 1);
        p.unpin(1, 0);
        assert_eq!(p.pinned_key(1), Some(1));
    }

    #[test]
    fn affine_pin_survives_unrelated_identity_churn() {
        let p = pool(1, 1);
        for i in 0..50 {
            p.pin(i, 0);
        }
        assert!(p.pinned_key(49).is_some());
        assert!(p.pinned_key(0).is_some(), "well under cap: nothing evicted");
        assert_eq!(p.affinity.len(), 50);
    }
}
