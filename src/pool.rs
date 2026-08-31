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

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    /// threshold. A 0 value must never exclude keys from `candidates()`
    /// (data absent = no demotion), so it doubles as "no usage known".
    over_quota_mask: AtomicU64,
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
/// upstream's own unit); percents are one-decimal mirrors for humans.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageBrief {
    pub session: f64,
    pub weekly: f64,
    pub session_pct: f64,
    pub weekly_pct: f64,
    /// True when the key's session usage is at/over the configured
    /// quota-aware threshold (always false when routing is not enabled).
    pub over_quota: bool,
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
                if let Some(session) = k.session {
                    // Threshold is in tenths of a percent; usage is a
                    // 0.0–1.0 fraction (compare in tenths of a percent:
                    // session * 1000 >= threshold).
                    if (session * 1000.0).clamp(0.0, 1000.0) as usize >= threshold {
                        mask |= 1u64 << k.index.min(63);
                    }
                }
            }
        }
        self.over_quota_mask.store(mask, Ordering::Relaxed);
    }

    /// Latest usage rendering per key for `/_keys`: a pure in-memory read
    /// of the optional snapshot — never triggers a fetch.
    pub fn usage_briefs(&self, snap: &crate::usage::UsageSnapshot) -> Vec<Option<UsageBrief>> {
        let threshold = self.usage_threshold_x10.load(Ordering::Relaxed);
        snap.keys
            .iter()
            .map(|k| {
                let (session, weekly) = match (k.session, k.weekly) {
                    (Some(s), Some(w)) => (s, w),
                    (Some(s), None) => (s, s),
                    (None, Some(w)) => (w, w),
                    (None, None) => return None,
                };
                Some(UsageBrief {
                    session,
                    weekly,
                    session_pct: (session * 1000.0).round() / 10.0,
                    weekly_pct: (weekly * 1000.0).round() / 10.0,
                    over_quota: threshold != THRESHOLD_DISABLED
                        && (session * 1000.0).clamp(0.0, 1000.0) as usize >= threshold,
                })
            })
            .collect()
    }

    /// True if at least one key is selectable right now.
    pub fn healthy_any(&self) -> bool {
        self.states.iter().any(|st| st.state() == State::Up)
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
    /// `/_keys` when a usage snapshot exists).
    pub fn info_with_usage(
        &self,
        snap: &crate::usage::UsageSnapshot,
    ) -> Vec<(KeyInfo, Option<UsageBrief>)> {
        let briefs = self.usage_briefs(snap);
        self.info().into_iter().zip(briefs).collect()
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
            let over = (demote_mask >> i) & 1;
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
        assert_eq!(b.session, 0.812);
        assert_eq!(b.session_pct, 81.2);
        assert!(b.over_quota, "81.2% >= 80% threshold");
    }
}
