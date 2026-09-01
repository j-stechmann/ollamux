//! Prompt-cache affinity: conversation identity extraction and the pin map.
//!
//! Ollama Cloud caches prompt prefixes server-side, per account (i.e. per
//! API key). ollamux's normal least-loaded/round-robin routing sends
//! successive requests of the same conversation to different keys and
//! thereby defeats that cache. The pin map here lets the pool route
//! requests with the same conversation identity to the key that already
//! warmed the upstream cache.
//!
//! Identity is content-addressed, mirroring how prefix caches work: for
//! chat-shaped requests, `model` plus every *leading* system message plus
//! the first non-system message — messages arrays only append in a
//! conversation, so that prefix is byte-identical across turns. For
//! prompt-shaped requests, `model` plus the first 8 KiB of the decoded
//! `prompt`. Identity is hashed with FNV-1a 64-bit (hand-rolled: no new
//! dependency for a value that only ever routes traffic; a collision at
//! realistic map sizes is ~1e-13 and merely shares a warm key).
//!
//! The map is strictly advisory state: an eviction, a stale pin, or a race
//! costs at most one cache miss and then self-heals (the next success
//! re-pins). Nothing here can ever change which responses a client sees —
//! only which key serves them.

use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};

/// Hash-input cap for the chat prefix (serialized). Bounds hash input per
/// request on bodies with huge first messages (base64 images) — the
/// message is still fully serialized before truncation, so this bounds
/// hashing, not serialization cost. Identical to the `prompt` cap's role.
/// Pinning a prefix beyond this is not meaningful — such bodies are
/// re-ingested slowly upstream anyway.
const MAX_PREFIX_BYTES: usize = 64 * 1024;
/// Hash-input cap for the decoded `prompt` string.
const MAX_PROMPT_BYTES: usize = 8 * 1024;

/// Endpoint families that get affinity, by sub_path prefix (the `route()`
/// caller has already stripped the leading slash). Everything else —
/// embeddings, no-auth GETs, unknown paths — never pins.
const AFFINE_PREFIXES: &[&str] = &[
    "api/chat",
    "api/generate",
    "v1/chat/completions",
    "v1/completions",
];

pub fn is_affine_path(sub_path: &str) -> bool {
    AFFINE_PREFIXES.contains(&sub_path)
}

// ---------------------------------------------------------------------------
// FNV-1a 64-bit
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// FNV-1a 64: tiny, dependency-free, deterministic across runs/versions.
/// A collision only pins two unrelated conversations to the same key —
/// a shared warm key, never a wrong response.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    // Final avalanche fold (xor high half into low): blunts the laziest
    // collision classes of bare FNV without a full mixer.
    h ^= h >> 32;
    h
}

fn hash_parts(parts: &[&[u8]]) -> u64 {
    let mut h = FNV_OFFSET;
    for part in parts {
        // Length-prefix each part so part boundaries cannot alias.
        h ^= u64::try_from(part.len()).unwrap_or(u64::MAX);
        h = h.wrapping_mul(FNV_PRIME);
        for &b in *part {
            h ^= u64::from(b);
            h = h.wrapping_mul(FNV_PRIME);
        }
    }
    h ^= h >> 32;
    h
}

// ---------------------------------------------------------------------------
// Identity extraction
// ---------------------------------------------------------------------------

/// Conversation identity from an already-parsed request body. `None` = no
/// affinity for this request (non-affine path, unparseable body, or missing
/// model/prompt fields). Called after the single body parse in proxy.rs —
/// no re-parse here.
///
/// Chat: hash of (path family, model, every leading system message, first
/// non-system message) — whole message objects, so `content` as string vs.
/// part-array and Ollama's separate `images` field are all covered.
/// Generate/completions: hash of (path family, model, first 8 KiB of the
/// decoded prompt value — serialized if it is not a string).
pub fn identity_from(parsed: Option<&Value>, sub_path: &str) -> Option<u64> {
    if !is_affine_path(sub_path) {
        return None;
    }
    let v = parsed?;
    let is_chat = sub_path.ends_with("chat") || sub_path.ends_with("chat/completions");
    let model = v.get("model").and_then(Value::as_str)?;
    if model.is_empty() {
        return None;
    }
    let family = if sub_path.starts_with("v1/") {
        "v1"
    } else {
        "api"
    };
    if is_chat {
        let msgs = v.get("messages").and_then(Value::as_array)?;
        if msgs.is_empty() {
            return None;
        }
        // Prefix = leading system messages + first non-system message.
        let cut = msgs
            .iter()
            .position(|m| m.get("role").and_then(Value::as_str) != Some("system"))
            .unwrap_or(msgs.len());
        if cut == msgs.len() {
            // Only system messages: nothing user-shaped to anchor the
            // conversation; a pure system prompt is a shared prefix, not a
            // conversation identity.
            return None;
        }
        let mut seed = hash_parts(&[family.as_bytes(), model.as_bytes()]);
        for m in &msgs[..=cut] {
            // Hash the serialized whole-object form (serde_json Value
            // serialization is canonical here: sorted keys, no whitespace)
            // rather than extracted .content strings — shape-agnostic and
            // stable for the same client.
            let mut buf = Vec::new();
            serde_json::to_writer(&mut buf, m).ok()?;
            let take = &buf[..buf.len().min(MAX_PREFIX_BYTES)];
            seed ^= fnv1a64(take);
            seed = seed.wrapping_mul(FNV_PRIME);
        }
        Some(seed)
    } else {
        let prompt = v.get("prompt")?;
        // prompt may be a string, array of strings, or token array
        // (OpenAI legacy shapes): serialize whatever is there.
        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, prompt).ok()?;
        let take = &buf[..buf.len().min(MAX_PROMPT_BYTES)];
        Some(hash_parts(&[family.as_bytes(), model.as_bytes(), take]))
    }
}

// ---------------------------------------------------------------------------
// Pin map
// ---------------------------------------------------------------------------

/// Bounded identity→key map with LRU eviction. A leaf lock: guards are
/// never held across any blocking pool call (see admit_affine).
pub struct AffinityMap {
    inner: Mutex<LruMap>,
}

struct LruMap {
    map: HashMap<u64, usize>,
    lru: VecDeque<u64>,
    cap: usize,
}

impl AffinityMap {
    pub fn new(cap: usize) -> AffinityMap {
        AffinityMap {
            inner: Mutex::new(LruMap {
                map: HashMap::new(),
                lru: VecDeque::with_capacity(cap.min(1024)),
                cap: cap.max(1),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, LruMap> {
        // Poison tolerance mirrors pool.rs/usage.rs: a panic elsewhere must
        // not wedge routing.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Latest known key for `id`, refreshing its recency.
    pub fn get(&self, id: u64) -> Option<usize> {
        let mut l = self.lock();
        let key = *l.map.get(&id)?;
        // LRU touch: move to the back of the deque.
        if let Some(pos) = l.lru.iter().position(|&k| k == id) {
            l.lru.remove(pos);
        }
        l.lru.push_back(id);
        Some(key)
    }

    /// Pin (or re-pin) `id` to `key`.
    pub fn put(&self, id: u64, key: usize) {
        let mut l = self.lock();
        if !l.map.contains_key(&id) && l.map.len() >= l.cap {
            // Evict LRU. The deque and map are only ever mutated under this
            // one lock, so they cannot diverge.
            if let Some(old) = l.lru.pop_front() {
                if l.map.remove(&old).is_none() {
                    // Defensive: a stale deque entry (should not happen)
                    // must not loop forever dropping entries.
                    l.map.clear();
                    l.lru.clear();
                }
            }
        }
        if l.map.insert(id, key).is_none() {
            l.lru.push_back(id);
        } else {
            // Existing entry: refresh recency.
            if let Some(pos) = l.lru.iter().position(|&k| k == id) {
                l.lru.remove(pos);
            }
            l.lru.push_back(id);
        }
    }

    /// Drop `id` iff it currently points at `key` (used on retryable
    /// failures: unpin a flapping key so the next request re-selects).
    pub fn remove_if(&self, id: u64, key: usize) {
        let mut l = self.lock();
        if l.map.get(&id) == Some(&key) {
            l.map.remove(&id);
            if let Some(pos) = l.lru.iter().position(|&k| k == id) {
                l.lru.remove(pos);
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(body: &str) -> Option<Value> {
        serde_json::from_str(body).ok()
    }

    #[test]
    fn fnv1a64_is_stable_and_order_sensitive() {
        // Pinned FNV-1a vectors (with the avalanche fold applied).
        assert_eq!(
            fnv1a64(b""),
            0xcbf29ce484222325 ^ (0xcbf29ce484222325 >> 32)
        );
        assert_eq!(
            fnv1a64(b"a"),
            0xaf63dc4c8601ec8c ^ (0xaf63dc4c8601ec8c >> 32)
        );
        assert_ne!(fnv1a64(b"abc"), fnv1a64(b"acb"));
        assert_ne!(fnv1a64(&[0u8]), fnv1a64(&[0u8, 0u8]));
    }

    #[test]
    fn same_conversation_across_turns_stable() {
        // Turn 1: [sys, user1]. Turn 2: [sys, user1, asst1, user2] —
        // append-only clients echo the prefix byte-identically.
        let turn1 = v(r#"{"model":"gpt-oss:120b","messages":[
                {"role":"system","content":"You are terse."},
                {"role":"user","content":"hi"}],
                "stream":true}"#)
        .unwrap();
        let turn2 = v(r#"{"model":"gpt-oss:120b","messages":[
                {"role":"system","content":"You are terse."},
                {"role":"user","content":"hi"},
                {"role":"assistant","content":"Hello."},
                {"role":"user","content":"more"}],
                "stream":true}"#)
        .unwrap();
        assert_eq!(
            identity_from(Some(&turn1), "api/chat"),
            identity_from(Some(&turn2), "api/chat")
        );
    }

    #[test]
    fn multi_system_prefix_and_part_array_content() {
        // opencode-style multi-system prompts + content as part array:
        // whole-object hashing must stay stable across turns.
        let turn1 = v(r#"{"model":"m","messages":[
                {"role":"system","content":"s1"},
                {"role":"system","content":"s2"},
                {"role":"user","content":[{"type":"text","text":"q"}]}]}"#)
        .unwrap();
        let turn2 = v(r#"{"model":"m","messages":[
                {"role":"system","content":"s1"},
                {"role":"system","content":"s2"},
                {"role":"user","content":[{"type":"text","text":"q"}]},
                {"role":"assistant","content":[{"type":"text","text":"a"}]},
                {"role":"user","content":"again"}]}"#)
        .unwrap();
        assert_eq!(
            identity_from(Some(&turn1), "v1/chat/completions"),
            identity_from(Some(&turn2), "v1/chat/completions")
        );
    }

    #[test]
    fn different_conversations_diverge() {
        let a = v(r#"{"model":"m","messages":[{"role":"user","content":"a"}]}"#).unwrap();
        let b = v(r#"{"model":"m","messages":[{"role":"user","content":"b"}]}"#).unwrap();
        let sys = v(
            r#"{"model":"m","messages":[{"role":"system","content":"s"},{"role":"user","content":"a"}]}"#,
        )
        .unwrap();
        assert_ne!(
            identity_from(Some(&a), "api/chat"),
            identity_from(Some(&b), "api/chat")
        );
        assert_ne!(
            identity_from(Some(&a), "api/chat"),
            identity_from(Some(&sys), "api/chat")
        );
        // Different model = different upstream prompt = different identity.
        let other_model =
            v(r#"{"model":"other","messages":[{"role":"user","content":"a"}]}"#).unwrap();
        assert_ne!(
            identity_from(Some(&a), "api/chat"),
            identity_from(Some(&other_model), "api/chat")
        );
    }

    #[test]
    fn stream_flag_and_params_do_not_split_identity() {
        let stream_on = v(r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true,"temperature":0.7}"#).unwrap();
        let stream_off = v(r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":false,"temperature":0.2}"#).unwrap();
        assert_eq!(
            identity_from(Some(&stream_on), "api/chat"),
            identity_from(Some(&stream_off), "api/chat")
        );
        // ...but a changed prefix does.
        let changed_sys = v(r#"{"model":"m","messages":[{"role":"system","content":"different"},{"role":"user","content":"x"}],"stream":true}"#).unwrap();
        assert_ne!(
            identity_from(Some(&stream_on), "api/chat"),
            identity_from(Some(&changed_sys), "api/chat")
        );
    }

    #[test]
    fn generate_hashes_prompt_prefix() {
        let a = v(r#"{"model":"m","prompt":"hello world"}"#).unwrap();
        let same = v(r#"{"model":"m","prompt":"hello world","stream":true}"#).unwrap();
        let longer = v(r#"{"model":"m","prompt":"hello world!"}"#).unwrap();
        assert_eq!(
            identity_from(Some(&a), "api/generate"),
            identity_from(Some(&same), "api/generate")
        );
        assert_ne!(
            identity_from(Some(&a), "api/generate"),
            identity_from(Some(&longer), "api/generate")
        );
    }

    #[test]
    fn prompt_cap_is_8kib() {
        let big = "x".repeat(20 * 1024);
        let body = format!(r#"{{"model":"m","prompt":"{big}"}}"#);
        let body = v(&body).unwrap();
        let truncated = format!(
            r#"{{"model":"m","prompt":"{}…"}}"#,
            "x".repeat(MAX_PROMPT_BYTES)
        );
        let truncated = v(&truncated).unwrap();
        // First 8 KiB identical → same identity even though the full
        // prompts differ beyond the cap.
        assert_eq!(
            identity_from(Some(&body), "v1/completions"),
            identity_from(Some(&truncated), "v1/completions")
        );
    }

    #[test]
    fn surfaces_and_paths_gate_affinity() {
        let body = v(r#"{"model":"m","messages":[{"role":"user","content":"x"}]}"#).unwrap();
        // Cross-surface never collapses: api vs v1 family is in the hash.
        assert_ne!(
            identity_from(Some(&body), "api/chat"),
            identity_from(Some(&body), "v1/chat/completions")
        );
        // Non-affine paths → None even with a perfect body.
        assert_eq!(identity_from(Some(&body), "api/embed"), None);
        assert_eq!(identity_from(Some(&body), "api/tags"), None);
        // Same family, different path prefix → still one family value.
        assert_eq!(
            identity_from(Some(&body), "api/chat"),
            identity_from(Some(&body), "api/chat") // identical call
        );
    }

    #[test]
    fn unparseable_or_missing_fields_give_none() {
        assert_eq!(identity_from(None, "api/chat"), None);
        let garbage: Option<Value> = serde_json::from_str("not json").ok();
        assert_eq!(identity_from(garbage.as_ref(), "api/chat"), None);
        let no_model = v(r#"{"messages":[{"role":"user","content":"x"}]}"#).unwrap();
        assert_eq!(identity_from(Some(&no_model), "api/chat"), None);
        let no_msgs = v(r#"{"model":"m"}"#).unwrap();
        assert_eq!(identity_from(Some(&no_msgs), "api/chat"), None);
        let empty_msgs = v(r#"{"model":"m","messages":[]}"#).unwrap();
        assert_eq!(identity_from(Some(&empty_msgs), "api/chat"), None);
        let system_only =
            v(r#"{"model":"m","messages":[{"role":"system","content":"s"}]}"#).unwrap();
        assert_eq!(identity_from(Some(&system_only), "api/chat"), None);
        let no_prompt = v(r#"{"model":"m"}"#).unwrap();
        assert_eq!(identity_from(Some(&no_prompt), "api/generate"), None);
        let empty_model = v(r#"{"model":"","prompt":"x"}"#).unwrap();
        assert_eq!(identity_from(Some(&empty_model), "api/generate"), None);
    }

    #[test]
    fn prompt_accepts_non_string_shapes() {
        let arr = v(r#"{"model":"m","prompt":["one","two"]}"#).unwrap();
        let tokens = v(r#"{"model":"m","prompt":[1,2,3]}"#).unwrap();
        assert!(identity_from(Some(&arr), "v1/completions").is_some());
        assert!(identity_from(Some(&tokens), "v1/completions").is_some());
        assert_ne!(
            identity_from(Some(&arr), "v1/completions"),
            identity_from(Some(&tokens), "v1/completions")
        );
    }

    #[test]
    fn map_put_get_and_lru_eviction() {
        let m = AffinityMap::new(2);
        m.put(1, 0);
        m.put(2, 1);
        assert_eq!(m.get(1), Some(0));
        // Touch 1 (LRU refresh), then insert 3: 2 must be evicted, 1 kept.
        m.get(1);
        m.put(3, 2);
        assert_eq!(m.get(2), None, "LRU entry evicted");
        assert_eq!(m.get(1), Some(0));
        assert_eq!(m.get(3), Some(2));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn map_remove_if_only_drops_current_pin() {
        let m = AffinityMap::new(4);
        m.put(7, 0);
        // Stale key: not removed.
        m.remove_if(7, 3);
        assert_eq!(m.get(7), Some(0));
        // Current key: removed.
        m.remove_if(7, 0);
        assert_eq!(m.get(7), None);
        // Re-put after removal works and does not grow beyond cap.
        m.put(7, 1);
        assert_eq!(m.get(7), Some(1));
    }

    #[test]
    fn map_repin_updates_value() {
        let m = AffinityMap::new(4);
        m.put(1, 0);
        m.put(1, 1);
        assert_eq!(m.get(1), Some(1));
        assert_eq!(m.len(), 1, "re-pin must not duplicate the entry");
    }
}
