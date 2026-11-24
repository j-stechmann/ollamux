//! Key discovery and parsing.
//!
//! Keys come from, in order of precedence:
//!   1. `OLLAMUX_KEYS` env var (newlines or commas separate keys)
//!   2. `$XDG_CONFIG_HOME/ollamux/keys`, defaulting to `~/.config/ollamux/keys`
//!
//! One key per line, `KEY[:N]@ACCOUNT`, where the account label is
//! **required**. `KEY:N` sets concurrency against that account's
//! concurrent-model limit (free=1, pro=3, max=10); keys sharing one
//! account share one upstream balance/usage pool. Blank lines are
//! ignored, `#` starts a comment (whole-line or trailing), and
//! leading/trailing whitespace is trimmed.
//!
//! Examples:
//!
//! ```text
//! alice_key_one...:1@alice
//! alice_key_two...:1@alice
//! bob_key...:10@bob
//! ```

use std::fs;
use std::path::{Path, PathBuf};

/// Default per-key concurrency. Chosen for Pro-tier keys; Free-tier users
/// should write `KEY:1`, Max users `KEY:10`.
pub const DEFAULT_CONCURRENCY: u32 = 3;

/// Longest accepted account label, in bytes.
const MAX_ACCOUNT_LEN: usize = 64;

/// One parsed key. `account` groups keys that belong to the same
/// ollama.com account (usage/balance are account-scoped: one fetch per
/// distinct account, not per key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEntry {
    pub secret: String,
    pub concurrency: u32,
    pub account: String,
}

impl KeyEntry {
    pub fn new(secret: impl Into<String>, concurrency: u32, account: impl Into<String>) -> Self {
        KeyEntry {
            secret: secret.into(),
            concurrency: concurrency.max(1),
            account: account.into(),
        }
    }
}

#[derive(Debug)]
pub struct Keys {
    pub entries: Vec<KeyEntry>,
    /// Directory the keys file lives in, for the warning message.
    source: PathBuf,
}

#[derive(Debug)]
pub enum ParseError {
    /// Neither OLLAMUX_KEYS nor a keys file was found.
    NotFound(PathBuf),
    /// A line parsed but produced no usable key.
    BadLine(String),
    /// A line had no `@ACCOUNT` label (required since 0.9.0).
    MissingAccount(String),
    /// `@ACCOUNT` was empty or used characters outside the allowed set.
    BadAccount {
        line: String,
        account: String,
    },
    /// `KEY:N` suffix where N is not a valid concurrency count (u32, >=1).
    BadConcurrency {
        line: String,
        tail: String,
    },
    /// A keys source was found but yielded zero keys (all lines blank or
    /// `#` comments, or the env var held nothing usable).
    Empty(String),
    Io(std::io::Error),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::NotFound(p) => write!(
                f,
                "no keys found: set OLLAMUX_KEYS or create {} (one `KEY[:N]@ACCOUNT` per line)",
                p.display()
            ),
            ParseError::BadLine(line) => {
                write!(f, "unparseable key line: {:?}", line)
            }
            ParseError::MissingAccount(line) => write!(
                f,
                "missing @ACCOUNT label in key line (want `KEY[:N]@ACCOUNT`): {line:?}"
            ),
            ParseError::BadAccount { line, account } => write!(
                f,
                "invalid account label {account:?} in key line (want 1-{MAX_ACCOUNT_LEN} chars of A-Z a-z 0-9 . _ -): {line:?}"
            ),
            ParseError::BadConcurrency { line, tail } => write!(
                f,
                "invalid concurrency {tail:?} in key line (want `KEY:N@ACCOUNT`, N = 1..=u32::MAX): {line:?}"
            ),
            ParseError::Empty(msg) => write!(f, "no keys: {msg}"),
            ParseError::Io(e) => write!(f, "reading keys: {e}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl Keys {
    /// Load keys from env or file. `display_name` is unused legacy.
    pub fn load(explicit_path: Option<&Path>, display_name: &str) -> Result<Self, ParseError> {
        let _ = display_name;
        Self::from_source(explicit_path)
    }

    fn from_source(explicit_path: Option<&Path>) -> Result<Self, ParseError> {
        if let Some(path) = explicit_path {
            let text = fs::read_to_string(path).map_err(ParseError::Io)?;
            let mut keys = Self::parse(&text, Some(path))?;
            keys =
                Self::require_nonempty(keys, &format!("keys file {} has no keys", path.display()))?;
            keys.source = path.to_path_buf();
            return Ok(keys);
        }
        if let Ok(raw) = std::env::var("OLLAMUX_KEYS") {
            let keys = Self::parse(&raw, None)?;
            if keys.entries.is_empty() {
                return Err(ParseError::Empty("OLLAMUX_KEYS is set but empty".into()));
            }
            return Ok(keys);
        }
        let path = config_path()?;
        match fs::read_to_string(&path) {
            Ok(text) => {
                let mut keys = Self::parse(&text, Some(&path))?;
                keys = Self::require_nonempty(
                    keys,
                    &format!("keys file {} has no keys", path.display()),
                )?;
                keys.source = path;
                Ok(keys)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(ParseError::NotFound(path)),
            Err(e) => Err(ParseError::Io(e)),
        }
    }

    /// A keys file that yields zero entries is almost certainly a mistake
    /// (commented-out everything, wrong path, wrong file); refuse to start
    /// rather than serving 503s for every request.
    fn require_nonempty(keys: Keys, msg: &str) -> Result<Keys, ParseError> {
        if keys.entries.is_empty() {
            Err(ParseError::Empty(msg.to_string()))
        } else {
            Ok(keys)
        }
    }

    /// Parse the textual format. `warn_source` emits a world-readable
    /// warning when parsing from a real file.
    pub fn parse(text: &str, warn_source: Option<&Path>) -> Result<Self, ParseError> {
        let mut entries: Vec<KeyEntry> = Vec::new();
        for raw in text.lines() {
            let raw = raw.trim();
            if raw.is_empty() || raw.starts_with('#') {
                continue;
            }
            // Trailing `#` starts a comment. Account labels cannot contain
            // `#`, and API keys are opaque tokens; the convenience of
            // annotating key lines outweighs the edge case.
            let uncommented = raw.split_once('#').map_or(raw, |(before, _)| before.trim());
            if uncommented.is_empty() {
                continue;
            }
            // `OLLAMUX_KEYS` uses commas or newlines; a file may use either.
            for token in uncommented.split(',') {
                let line = token.trim();
                if line.is_empty() {
                    continue;
                }
                // The account label is the last `@`-suffixed token and is
                // required: usage/balance are account-scoped, so an
                // unlabelled key cannot be attributed correctly.
                let (key_part, account) = line
                    .rsplit_once('@')
                    .ok_or_else(|| ParseError::MissingAccount(line.to_string()))?;
                let account = account.trim();
                if !valid_account(account) {
                    return Err(ParseError::BadAccount {
                        line: line.to_string(),
                        account: account.to_string(),
                    });
                }
                let (key, conc) = match key_part.trim().rsplit_once(':') {
                    // `KEY:N` — only split when the tail is exactly
                    // decimal digits (a colon inside a key is not a
                    // splitter).
                    Some((k, n))
                        if !k.is_empty()
                            && !n.is_empty()
                            && n.bytes().all(|b| b.is_ascii_digit()) =>
                    {
                        let conc = match n.parse::<u32>() {
                            Ok(c) => c.max(1),
                            Err(_) => {
                                return Err(ParseError::BadConcurrency {
                                    line: line.to_string(),
                                    tail: n.to_string(),
                                });
                            }
                        };
                        (k.trim(), conc)
                    }
                    _ => (key_part.trim(), DEFAULT_CONCURRENCY),
                };
                if key.is_empty() {
                    return Err(ParseError::BadLine(line.to_string()));
                }
                if entries.iter().any(|e| e.secret == key) {
                    return Err(ParseError::BadLine(format!(
                        "duplicate key entry: …{}",
                        suffix(key)
                    )));
                }
                entries.push(KeyEntry::new(key, conc, account));
            }
        }
        if let Some(src) = warn_source {
            Keys::warn_if_world_readable(src);
        }
        Ok(Keys {
            entries,
            source: PathBuf::new(),
        })
    }

    fn warn_if_world_readable(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = fs::metadata(path) {
                let mode = meta.permissions().mode();
                if mode & 0o077 != 0 {
                    eprintln!(
                        "ollamux: warning: {} is readable by others (mode {:o}); fix: chmod 600 {}",
                        path.display(),
                        mode & 0o777,
                        path.display()
                    );
                }
            }
        }
    }

    /// Key suffixes only, for banner and logs. Secrets never leave here.
    pub fn suffixes(&self) -> Vec<String> {
        self.entries.iter().map(|e| suffix(&e.secret)).collect()
    }

    /// Distinct account labels, in first-seen order.
    pub fn accounts(&self) -> Vec<String> {
        let mut seen: Vec<String> = Vec::new();
        for e in &self.entries {
            if !seen.iter().any(|a| a == &e.account) {
                seen.push(e.account.clone());
            }
        }
        seen
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub fn suffix(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    let start = chars.len().saturating_sub(4);
    chars[start..].iter().collect()
}

/// A usable account label: 1..=64 bytes of `A-Z a-z 0-9 . _ -`. Kept
/// deliberately narrow so labels are shell-safe, URL-query-safe and
/// unambiguous inside `OLLAMUX_KEYS` (which is comma-separated).
fn valid_account(a: &str) -> bool {
    !a.is_empty()
        && a.len() <= MAX_ACCOUNT_LEN
        && a.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn config_path() -> Result<PathBuf, ParseError> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return Ok(PathBuf::from(xdg).join("ollamux/keys"));
        }
    }
    let home = std::env::var("HOME").map_err(|_| {
        ParseError::Empty("HOME not set and OLLAMUX_KEYS unset; cannot locate keys".into())
    })?;
    Ok(PathBuf::from(home).join(".config/ollamux/keys"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(k: &Keys, i: usize) -> &KeyEntry {
        &k.entries[i]
    }

    #[test]
    fn parses_lines_comments_and_whitespace() {
        let k = Keys::parse(" \n # comment\n  key-a:5@alice  \n\nkey-b@alice\n", None).unwrap();
        assert_eq!(
            k.entries,
            vec![
                KeyEntry::new("key-a", 5, "alice"),
                KeyEntry::new("key-b", DEFAULT_CONCURRENCY, "alice"),
            ]
        );
        assert_eq!(k.len(), 2);
        assert_eq!(k.accounts(), vec!["alice".to_string()]);
    }

    #[test]
    fn colon_only_splits_on_digit_tail() {
        let k = Keys::parse("key-abc:3@alice", None).unwrap();
        assert_eq!(entry(&k, 0).concurrency, 3);
        assert_eq!(entry(&k, 0).account, "alice");
        // A colon in the middle of a key is not a splitter.
        let k = Keys::parse("key-abc:def@alice", None).unwrap();
        assert_eq!(entry(&k, 0).secret, "key-abc:def");
        assert_eq!(entry(&k, 0).concurrency, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn concurrency_floor_is_one() {
        let k = Keys::parse("key-a:0@alice", None).unwrap();
        assert_eq!(entry(&k, 0).concurrency, 1);
    }

    #[test]
    fn account_label_is_required() {
        let err = Keys::parse("key-a:3", None).unwrap_err();
        assert!(matches!(err, ParseError::MissingAccount(_)));
        assert!(err.to_string().contains("@ACCOUNT"));
    }

    #[test]
    fn account_label_charset_is_enforced() {
        for bad in [
            "key-a@",
            "key-a@bad account",
            "key-a@bad/account",
            "key-a@bad,account",
        ] {
            let err = Keys::parse(bad, None).unwrap_err();
            assert!(
                matches!(
                    err,
                    ParseError::BadAccount { .. } | ParseError::MissingAccount(_)
                ),
                "{bad:?} must be rejected, got {err:?}"
            );
        }
        // `@` splits at the last occurrence, so a key may contain one.
        let k = Keys::parse("key@fragment:1@alice", None).unwrap();
        assert_eq!(entry(&k, 0).secret, "key@fragment");
        assert_eq!(entry(&k, 0).account, "alice");
    }

    #[test]
    fn multiple_keys_share_one_account() {
        let k = Keys::parse("a:3@alice\nb:3@alice\nc:10@bob\n", None).unwrap();
        assert_eq!(k.accounts(), vec!["alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn commas_separate_keys_too() {
        // OLLAMUX_KEYS is documented as newline/comma-separated.
        let k = Keys::parse("a:1@alice, b:3@bob", None).unwrap();
        assert_eq!(k.len(), 2);
        assert_eq!(entry(&k, 0).account, "alice");
        assert_eq!(entry(&k, 1).account, "bob");
        assert_eq!(entry(&k, 1).concurrency, 3);
    }

    #[test]
    fn trailing_comments_are_ignored() {
        let k = Keys::parse("a:1@alice  # free tier\nb@bob # pro\n", None).unwrap();
        assert_eq!(k.len(), 2);
        assert_eq!(entry(&k, 0).account, "alice");
        assert_eq!(entry(&k, 0).concurrency, 1);
        assert_eq!(entry(&k, 1).account, "bob");
    }

    #[test]
    fn suffix_is_last_four() {
        assert_eq!(suffix("key-abcdefgh"), "efgh");
        assert_eq!(suffix("abc"), "abc");
    }

    #[test]
    fn suffix_never_panics_on_multibyte() {
        assert_eq!(suffix("aébcd"), "ébcd");
        assert_eq!(suffix("日本語テスト"), "語テスト");
        assert_eq!(suffix("héllo"), "éllo");
    }

    #[test]
    fn oversized_concurrency_is_an_error_not_a_silent_default() {
        let err = Keys::parse("key-a:99999999999@alice", None).unwrap_err();
        assert!(matches!(err, ParseError::BadConcurrency { .. }));
    }

    #[test]
    fn duplicate_keys_are_an_error() {
        let err = Keys::parse("key-a@alice\nkey-b@alice\nkey-a@alice\n", None).unwrap_err();
        assert!(matches!(err, ParseError::BadLine(_)));
        let ok = Keys::parse("key-a@alice\nkey-b@alice\n", None).unwrap();
        assert_eq!(ok.len(), 2);
    }

    #[test]
    fn empty_parse_is_ok_but_from_file_is_rejected() {
        // parse() itself is permissive (used on env var with its own check);
        // the file loader enforces nonempty.
        let k = Keys::parse("# only comments\n", None).unwrap();
        assert!(k.is_empty());
        assert!(Keys::load(Some(Path::new("/nonexistent-ollamux-keys")), "").is_err());
    }
}
