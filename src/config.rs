//! Key discovery and parsing.
//!
//! Keys come from, in order of precedence:
//!   1. `OMLX_KEYS` env var (newlines or commas separate keys)
//!   2. `$XDG_CONFIG_HOME/omlx/keys`, defaulting to `~/.config/omlx/keys`
//!
//! One key per line. `KEY:MODE` sets per-key concurrency (Ollama Cloud's
//! concurrent-model limit: free=1, pro=3, max=10). Blank lines and `#`
//! comments are ignored; leading/trailing whitespace is trimmed.

use std::fs;
use std::path::{Path, PathBuf};

/// Default per-key concurrency. Chosen for Pro-tier keys; Free-tier users
/// should write `KEY:1`, Max users `KEY:10`.
pub const DEFAULT_CONCURRENCY: u32 = 3;

#[derive(Debug)]
pub struct Keys {
    /// (secret, per-key concurrency slots)
    pub entries: Vec<(String, u32)>,
    /// Directory the keys file lives in, for the warning message.
    source: PathBuf,
}

#[derive(Debug)]
pub enum ParseError {
    /// Neither OMLX_KEYS nor a keys file was found.
    NotFound(PathBuf),
    /// A line parsed but produced no usable key.
    BadLine(String),
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
                "no keys found: set OMLX_KEYS or create {} (one API key per line)",
                p.display()
            ),
            ParseError::BadLine(line) => {
                write!(f, "unparseable key line: {:?}", line)
            }
            ParseError::BadConcurrency { line, tail } => write!(
                f,
                "invalid concurrency {tail:?} in key line (want `KEY:N`, N = 1..=u32::MAX): {line:?}"
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
        if let Ok(raw) = std::env::var("OMLX_KEYS") {
            let keys = Self::parse(&raw, None)?;
            if keys.entries.is_empty() {
                return Err(ParseError::Empty("OMLX_KEYS is set but empty".into()));
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
        let mut entries: Vec<(String, u32)> = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, conc) = match line.rsplit_once(':') {
                // `KEY:N` — only split when the tail is exactly decimal
                // digits (a colon in the middle of a key is not a splitter).
                Some((k, n))
                    if !k.is_empty() && !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) =>
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
                _ => (line, DEFAULT_CONCURRENCY),
            };
            if entries.iter().any(|(k, _)| *k == key) {
                return Err(ParseError::BadLine(format!(
                    "duplicate key entry: …{}",
                    suffix(key)
                )));
            }
            entries.push((key.to_string(), conc));
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
                        "omlx: warning: {} is readable by others (mode {:o}); fix: chmod 600 {}",
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
        self.entries.iter().map(|(k, _)| suffix(k)).collect()
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

fn config_path() -> Result<PathBuf, ParseError> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return Ok(PathBuf::from(xdg).join("omlx/keys"));
        }
    }
    let home = std::env::var("HOME").map_err(|_| {
        ParseError::Empty("HOME not set and OMLX_KEYS unset; cannot locate keys".into())
    })?;
    Ok(PathBuf::from(home).join(".config/omlx/keys"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lines_comments_and_whitespace() {
        let k = Keys::parse(" \n # comment\n  key-a:5  \n\nkey-b\n", None).unwrap();
        assert_eq!(
            k.entries,
            vec![("key-a".into(), 5), ("key-b".into(), DEFAULT_CONCURRENCY)]
        );
        assert_eq!(k.len(), 2);
    }

    #[test]
    fn colon_only_splits_on_digit_tail() {
        let k = Keys::parse("key-abc:3", None).unwrap();
        assert_eq!(k.entries.first().unwrap().1, 3);
        // A colon in the middle of a key is not a splitter.
        let k = Keys::parse("key-abc:def", None).unwrap();
        assert_eq!(k.entries.first().unwrap().0, "key-abc:def");
        assert_eq!(k.entries.first().unwrap().1, DEFAULT_CONCURRENCY);
    }

    #[test]
    fn concurrency_floor_is_one() {
        let k = Keys::parse("key-a:0", None).unwrap();
        assert_eq!(k.entries.first().unwrap().1, 1);
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
        let err = Keys::parse("key-a:99999999999", None).unwrap_err();
        assert!(matches!(err, ParseError::BadConcurrency { .. }));
    }

    #[test]
    fn duplicate_keys_are_an_error() {
        let err = Keys::parse("key-a\nkey-b\nkey-a\n", None).unwrap_err();
        assert!(matches!(err, ParseError::BadLine(_)));
        let ok = Keys::parse("key-a\nkey-b\n", None).unwrap();
        assert_eq!(ok.len(), 2);
    }

    #[test]
    fn empty_parse_is_ok_but_from_file_is_rejected() {
        // parse() itself is permissive (used on env var with its own check);
        // the file loader enforces nonempty.
        let k = Keys::parse("# only comments\n", None).unwrap();
        assert!(k.is_empty());
        assert!(Keys::load(Some(Path::new("/nonexistent-omlx-keys")), "").is_err());
    }
}
