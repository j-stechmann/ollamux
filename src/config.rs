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
            let mut keys = Self::parse(&text, None)?;
            keys.source = path.to_path_buf();
            return Ok(keys);
        }
        if let Ok(raw) = std::env::var("OMLX_KEYS") {
            let keys = Self::parse(&raw, None)?;
            if keys.entries.is_empty() {
                return Err(ParseError::BadLine("OMLX_KEYS is set but empty".into()));
            }
            return Ok(keys);
        }
        let path = config_path()?;
        match fs::read_to_string(&path) {
            Ok(text) => {
                let mut keys = Self::parse(&text, None)?;
                keys.source = path;
                Ok(keys)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(ParseError::NotFound(path)),
            Err(e) => Err(ParseError::Io(e)),
        }
    }

    /// Parse the textual format. `warn_source` is only used to emit a
    /// world-readable warning when parsing from a real file.
    pub fn parse(text: &str, warn_source: Option<&Path>) -> Result<Self, ParseError> {
        let mut entries = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, conc) = match line.rsplit_once(':') {
                // `KEY:N` — only split when the tail is a plausible count.
                Some((k, n))
                    if !k.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && !n.is_empty() =>
                {
                    (k.trim(), n.parse::<u32>().unwrap_or(DEFAULT_CONCURRENCY))
                }
                _ => (line, DEFAULT_CONCURRENCY),
            };
            entries.push((key.to_string(), conc.max(1)));
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
    let n = key.len().min(4);
    key[key.len() - n..].to_string()
}

fn config_path() -> Result<PathBuf, ParseError> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return Ok(PathBuf::from(xdg).join("omlx/keys"));
        }
    }
    let home = std::env::var("HOME").map_err(|_| {
        ParseError::BadLine("HOME not set and OMLX_KEYS unset; cannot locate keys".into())
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
}
