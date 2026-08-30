//! Integration tests through the real handler and a real tiny_http server.
//!
//! Hermetic by default: `/api/*` requests go to a local upstream spawned in
//! this process. The historical test hit real ollama.com; that variant now
//! requires `--features net` (it depends on live upstream behavior).

use ollamux::proxy::Server;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn pool_with(keys: &[(&str, u32)]) -> Arc<ollamux::Pool> {
    Arc::new(ollamux::Pool::new(
        keys.iter().map(|(k, c)| (k.to_string(), *c)).collect(),
        32,
        false,
    ))
}

/// Spawn the real Server proxying to `upstream`; returns base URL.
fn spawn_server(pool: Arc<ollamux::Pool>, upstream: &str) -> String {
    let server = Server::with_upstream(pool, upstream);
    let tiny = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let addr = match tiny.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("expected IP listener"),
    };
    let _upstream = upstream.to_string();
    std::thread::spawn(move || {
        while let Ok(req) = tiny.recv() {
            server.handle(req);
        }
    });
    format!("http://{addr}")
}

fn headers_of(r: &ureq::Response) -> Vec<(String, String)> {
    r.headers_names()
        .into_iter()
        .flat_map(|n| {
            r.all(&n)
                .into_iter()
                .map(move |v| (n.clone(), v.to_string()))
        })
        .collect()
}

fn get(url: &str, path: &str) -> (u16, String, Vec<(String, String)>) {
    let resp = match ureq::get(&format!("{url}{path}")).call() {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => panic!("request failed: {e}"),
    };
    let status = resp.status();
    let headers = headers_of(&resp);
    (status, resp.into_string().unwrap(), headers)
}

fn post(url: &str, path: &str, body: &str) -> (u16, String) {
    let (status, body, _) = post_with_headers(url, path, body);
    (status, body)
}

fn post_with_headers(url: &str, path: &str, body: &str) -> (u16, String, Vec<(String, String)>) {
    match ureq::post(&format!("{url}{path}")).send_string(body) {
        Ok(r) => {
            let headers = headers_of(&r);
            let status = r.status();
            (status, r.into_string().unwrap(), headers)
        }
        Err(ureq::Error::Status(code, r)) => {
            let headers = headers_of(&r);
            (code, r.into_string().unwrap(), headers)
        }
        Err(e) => panic!("request failed: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Local upstream: a tiny thread-per-connection server on 127.0.0.1:0 that
// records requests and produces scripted responses.
// ---------------------------------------------------------------------------

pub struct Upstream {
    pub url: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<RecordedRequest>>>,
}

#[derive(Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path_with_query: String,
    pub auth: Option<String>,
    pub body: String,
}

impl Upstream {
    /// resp_for: given the request path-with-query, return (status, body).
    pub fn spawn(
        resp_for: impl Fn(&str) -> (u16, String, String) + Send + Sync + 'static,
    ) -> Upstream {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reqs = requests.clone();
        let resp_for = std::sync::Arc::new(resp_for);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let reqs = reqs.clone();
                let resp_for = resp_for.clone();
                std::thread::spawn(move || {
                    let (method, path, auth, body) = read_http_request(&mut stream);
                    reqs.lock().unwrap().push(RecordedRequest {
                        method,
                        path_with_query: path.clone(),
                        auth,
                        body,
                    });
                    let (status, reason, body) = resp_for(&path);
                    let payload = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(payload.as_bytes());
                });
            }
        });
        Upstream { url, requests }
    }

    pub fn recorded(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

/// Minimal HTTP/1.1 request reader for the local upstream.
fn read_http_request(stream: &mut std::net::TcpStream) -> (String, String, Option<String>, String) {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    // Read until end of headers.
    loop {
        match stream.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let first = lines.next().unwrap_or_default().to_string();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut auth = None;
    let mut content_length = 0usize;
    for line in lines {
        if let Some(v) = line.strip_prefix("Authorization: ") {
            auth = Some(v.trim().to_string());
        }
        if let Some(v) = line.strip_prefix("Content-Length: ") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let _ = stream.read_exact(&mut body);
    }
    (
        method,
        path,
        auth,
        String::from_utf8_lossy(&body).into_owned(),
    )
}

// ---------------------------------------------------------------------------
// Endpoint tests (fully hermetic)
// ---------------------------------------------------------------------------

#[test]
fn keys_health_and_404_endpoints() {
    let up = Upstream::spawn(|_| (200u16, "OK".into(), "{}".into()));
    let addr = spawn_server(pool_with(&[("omk-abcd1234", 1)]), &up.url);

    let (status, body, _) = get(&addr, "/_keys");
    assert_eq!(status, 200, "body: {body}");
    let keys: serde_json::Value = serde_json::from_str(&body).unwrap();
    let arr = keys.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["suffix"], "1234");
    assert_eq!(arr[0]["concurrency"], 1);
    assert_eq!(arr[0]["state"], "up");
    // The full secret must never appear.
    assert!(!body.contains("omk-abcd1234"));

    let (status, body, _) = get(&addr, "/_health");
    assert_eq!(status, 200);
    assert!(body.contains("\"ok\":true"), "health body: {body}");
    let health: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(health["service"], "ollamux");
    assert_eq!(health["version"], ollamux::VERSION);

    let (status, body, _) = get(&addr, "/nonsense");
    assert_eq!(status, 404);
    assert!(body.contains("not a local Ollama"), "404 body: {body}");
    // Agent-friendly guidance: says what it IS, and how to fix confusion.
    assert!(body.contains("Ollama Cloud"), "404 body: {body}");
    assert!(body.contains("11434"), "404 body: {body}");
    assert!(body.contains("/api/*"), "404 body: {body}");
}

// Every response must self-identify: errors should be attributable to
// ollamux (vs a real Ollama server or a CDN) by header alone.
#[test]
fn all_responses_carry_identity_header() {
    // /api/generate streams a 200; every other path 404s (relayed verbatim).
    let up = Upstream::spawn(|path| {
        if path.starts_with("/api/generate") {
            (200u16, "OK".into(), "line1\nline2\n".into())
        } else {
            (
                404u16,
                "Not Found".into(),
                r#"{"error":"upstream 404"}"#.into(),
            )
        }
    });
    let addr = spawn_server(pool_with(&[("omk-ident001", 1)]), &up.url);

    for path in ["/_health", "/nonsense"] {
        let (_, _, headers) = get(&addr, path);
        assert!(
            headers
                .iter()
                .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
            "GET {path} must carry the X-Ollamux identity header: {headers:?}"
        );
    }

    // Success path (no-auth endpoint) and relayed upstream error too.
    let (_, _, headers) = get(&addr, "/api/tags");
    assert!(
        headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
        "relayed success must carry X-Ollamux: {headers:?}"
    );
    let (_, _, headers) = get(&addr, "/_doesnotexist");
    assert!(
        headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
        "404 must carry X-Ollamux: {headers:?}"
    );

    // Streaming responses are hand-framed in stream_chunked; the identity
    // header must not drift from identity_header().
    let (status, body, headers) =
        post_with_headers(&addr, "/api/generate", r#"{"model":"x","stream":true}"#);
    assert_eq!(status, 200);
    assert_eq!(body, "line1\nline2\n");
    assert!(
        headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
        "streaming response must carry X-Ollamux: {headers:?}"
    );

    let (status, body, headers) = post_with_headers(&addr, "/api/show", r#"{"model":"x"}"#);
    assert_eq!(status, 404, "upstream 404 is not a key signal; relayed");
    assert!(
        headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
        "relayed upstream error must carry X-Ollamux: {headers:?}"
    );
    assert!(
        body.contains("upstream 404"),
        "body relayed verbatim: {body}"
    );
}

// H3: /api/tags etc. must be credential-less and must not mark key health.
#[test]
fn no_auth_paths_skip_credentials_and_slots() {
    let up = Upstream::spawn(|path| {
        assert!(path.starts_with("/api/tags"), "unexpected path {path}");
        (200, "OK".into(), r#"{"models":[]}"#.into())
    });
    let addr = spawn_server(pool_with(&[("omk-notags01", 1)]), &up.url);

    let (status, body, headers) = get(&addr, "/api/tags");
    assert_eq!(status, 200, "body: {body}");
    assert!(
        !headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("x-ollamux-key")),
        "no-auth responses must not claim a key"
    );

    let reqs = up.recorded();
    assert_eq!(reqs.len(), 1);
    assert!(
        reqs[0].auth.is_none(),
        "/api/tags must be sent without an Authorization header"
    );

    // No slot consumed (would show as in_use>0 only mid-flight) and, more
    // importantly, key health untouched even though upstream would 401 any
    // bogus credential.
    let info = serde_json::to_value(spawn_info(&addr)).unwrap();
    assert_eq!(info[0]["state"], "up");
}

fn spawn_info(addr: &str) -> serde_json::Value {
    let (status, body, _) = get(addr, "/_keys");
    assert_eq!(status, 200);
    serde_json::from_str(&body).unwrap()
}

// M2: query strings must reach the upstream.
#[test]
fn query_strings_are_forwarded() {
    up_spawn_and_check_query();
}

fn up_spawn_and_check_query() {
    let seen: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
    let seen2 = seen.clone();
    let up = Upstream::spawn(move |path| {
        *seen2.lock().unwrap() = Some(path.to_string());
        (200, "OK".into(), "{}".into())
    });
    let addr = spawn_server(pool_with(&[("omk-query0001", 1)]), &up.url);
    let _ = get(&addr, "/api/tags?foo=bar&z=1");
    let recorded = up.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].path_with_query, "/api/tags?foo=bar&z=1",
        "query string must not be dropped"
    );
    drop(seen);
}

// H4: relayed upstream error bodies must arrive verbatim (>T would be truncation).
#[test]
fn relayed_4xx_bodies_are_verbatim() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let body_lines = "x".repeat(600); // > SNIPPET (256)
    let payload = format!(r#"{{"error":"{body_lines}"}}"#);
    let up = Upstream::spawn(move |_| {
        CALLS.fetch_add(1, Ordering::Relaxed);
        (400, "Bad Request".into(), payload.clone())
    });
    let addr = spawn_server(pool_with(&[("omk-passthru1", 1)]), &up.url);
    let (status, body) = post(&addr, "/api/generate", r#"{"model":"x"}"#);
    assert_eq!(status, 400);
    assert!(
        body.contains(&"x".repeat(600)),
        "relayed body must not be truncated (len {})",
        body.len()
    );
    // Status relayed is not a key-health signal: key stays up.
    let info = spawn_info(&addr);
    assert_eq!(info[0]["state"], "up");
}

// Error shape per surface, using hermetic upstream failures.
#[test]
fn surface_error_shapes_on_total_failure() {
    // Upstream unreachable → after budget, 502 with the surface's shape.
    let addr = spawn_server(
        pool_with(&[("omk-shape0001", 1)]),
        "http://127.0.0.1:1", // nothing listens there
    );
    let (status, body) = post(&addr, "/api/chat", r#"{"model":"x"}"#);
    assert_eq!(status, 502, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["error"].is_string(), "ollama-style error: {body}");
    // Guidance: where to look next.
    assert!(
        v["error"].as_str().unwrap().contains("/_keys"),
        "502 must point at /_keys: {body}"
    );

    let (status, body) = post(&addr, "/v1/chat/completions", r#"{"model":"x"}"#);
    assert_eq!(status, 502, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        v["error"]["message"].is_string(),
        "openai-style error: {body}"
    );
    assert!(
        v["error"]["message"].as_str().unwrap().contains("/_keys"),
        "502 must point at /_keys: {body}"
    );
}

// 429 from upstream cools the key and rotates to the healthy one.
#[test]
fn rate_limited_key_rotates_and_cools() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let up = Upstream::spawn(move |_| {
        // First call: 429 (cools whichever key was chosen); retry lands on
        // the other key and succeeds — failover before the first byte.
        if CALLS.fetch_add(1, Ordering::Relaxed) == 0 {
            (
                429,
                "Too Many Requests".into(),
                r#"{"error":"rate limited"}"#.into(),
            )
        } else {
            (200, "OK".into(), r#"{"ok":true}"#.into())
        }
    });
    let addr = spawn_server(
        pool_with(&[("omk-cool00001", 2), ("omk-cool00002", 2)]),
        &up.url,
    );
    let (status, body) = post(&addr, "/api/generate", r#"{"model":"x"}"#);
    assert_eq!(status, 200, "retry on second key must succeed: {body}");

    // The 429'd key is cooling down in /_keys (60s default > test horizon).
    let info = spawn_info(&addr);
    let cooling = info.as_array().unwrap();
    assert_eq!(cooling.len(), 2);
    assert!(
        cooling.iter().any(|k| k["state"] == "cooldown"),
        "the 429'd key must be cooling: {info}"
    );

    // Pool-level: first key cooled, second serves.
    let pool = pool_with(&[("omk-cool00003", 1), ("omk-cool00004", 1)]);
    let (permit, key0) = pool.admit(Duration::from_secs(1)).unwrap();
    pool.mark_cooldown(key0, Duration::from_millis(50), "429 rate limited");
    drop(permit);
    let (_, key1) = pool.admit(Duration::from_secs(1)).unwrap();
    assert_ne!(key0, key1, "failover must rotate to the healthy key");
}

// Pool-level: admission must not reset strikes (M1) — mirrored in unit tests.
#[test]
fn all_dead_surfaces_403_hermetically() {
    let up = Upstream::spawn(|_| (401, "Unauthorized".into(), "Unauthorized".into()));
    let addr = spawn_server(pool_with(&[("omk-dead00001", 1)]), &up.url);
    let (status, body) = post(&addr, "/api/chat", r#"{"model":"x"}"#);
    assert_eq!(status, 403); // all keys dead after the 401
    let info = spawn_info(&addr);
    assert_eq!(info[0]["state"], "dead");
    // Agent guidance: says the keys are invalid and how to recover.
    assert!(body.contains("restart"), "403 must mention restart: {body}");
    assert!(body.contains("/_keys"), "403 must point at /_keys: {body}");
}

#[test]
fn streaming_passes_through_chunked() {
    let up = Upstream::spawn(|_| (200, "OK".into(), "line1\nline2\n".into()));
    let addr = spawn_server(pool_with(&[("omk-stream001", 1)]), &up.url);
    let (status, body) = post(&addr, "/api/generate", r#"{"model":"x","stream":true}"#);
    assert_eq!(status, 200);
    assert_eq!(body, "line1\nline2\n");
}

#[cfg(feature = "net")]
#[test]
fn net_failover_money_test() {
    // One-key pool; bogus key: upstream 401 marks the key dead, retry
    // exhausts, proxy answers the all-dead 403 in the surface's shape.
    let addr = spawn_server(pool_with(&[("omk-money0004", 1)]), "https://ollama.com");
    let (status, body) = post(&addr, "/api/chat", r#"{"model":"x"}"#);
    assert_eq!(status, 403, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["error"].is_string(), "ollama-style error: {body}");

    let (status2, body2) = post(&addr, "/v1/chat/completions", r#"{"model":"x"}"#);
    // The key is already dead from the previous call — admission 403.
    assert_eq!(status2, 403, "body: {body2}");
    let v: serde_json::Value = serde_json::from_str(&body2).unwrap();
    assert!(
        v["error"]["message"].is_string(),
        "openai-style error: {body2}"
    );
}
