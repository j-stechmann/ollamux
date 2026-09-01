//! Integration tests through the real handler and a real tiny_http server.
//!
//! Hermetic by default: `/api/*` requests go to a local upstream spawned in
//! this process. The historical test hit real ollama.com; that variant now
//! requires `--features net` (it depends on live upstream behavior).

use ollamux::proxy::Server;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn pool_with(keys: &[(&str, u32)]) -> Arc<ollamux::Pool> {
    Arc::new(ollamux::Pool::new(
        keys.iter().map(|(k, c)| (k.to_string(), *c)).collect(),
        32,
        false,
    ))
}

/// Spawn the real Server proxying to `upstream`; returns (base URL, pool).
fn spawn_server(pool: Arc<ollamux::Pool>, upstream: &str) -> (String, Arc<ollamux::Pool>) {
    let server = Server::with_upstream(pool.clone(), upstream);
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
    (format!("http://{addr}"), pool)
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
    /// (condvar, completion counter) for wait_for_count.
    done: Arc<(std::sync::Condvar, std::sync::Mutex<usize>)>,
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
        Self::spawn_auth(move |path, _| resp_for(path))
    }

    /// Like spawn, but the responder also sees the Authorization header
    /// (None when absent) — needed to script per-key usage responses.
    /// resp_for: given (path-with-query, auth), return (status, body).
    pub fn spawn_auth(
        resp_for: impl Fn(&str, Option<&str>) -> (u16, String, String) + Send + Sync + 'static,
    ) -> Upstream {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let done = Arc::new((std::sync::Condvar::new(), std::sync::Mutex::new(0usize)));
        let reqs = requests.clone();
        let resp_for = std::sync::Arc::new(resp_for);
        let accept_done = done.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let reqs = reqs.clone();
                let resp_for = resp_for.clone();
                let done = accept_done.clone();
                std::thread::spawn(move || {
                    let (method, path, auth, body) = read_http_request(&mut stream);
                    let auth_for_resp = Arc::new(std::sync::Mutex::new(auth.clone()));
                    reqs.lock().unwrap().push(RecordedRequest {
                        method,
                        path_with_query: path.clone(),
                        auth,
                        body,
                    });
                    {
                        let (cv, count) = &*done;
                        let mut n = count.lock().unwrap();
                        *n += 1;
                        cv.notify_all();
                    }
                    let auth_snap = auth_for_resp.lock().unwrap().clone();
                    let (status, reason, body) = resp_for(&path, auth_snap.as_deref());
                    let payload = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(payload.as_bytes());
                });
            }
        });
        Upstream {
            url,
            requests,
            done,
        }
    }

    pub fn recorded(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    /// Block until at least `n` requests have been recorded (bounded).
    pub fn wait_for_count(&self, n: usize, timeout: Duration) -> usize {
        let (cv, count) = &*self.done;
        let deadline = std::time::Instant::now() + timeout;
        let mut guard = count.lock().unwrap();
        loop {
            if *guard >= n {
                return *guard;
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return *guard;
            }
            let (g, _t) = cv
                .wait_timeout(guard, left.min(Duration::from_millis(50)))
                .unwrap();
            guard = g;
        }
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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-abcd1234", 1)]), &up.url);

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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-ident001", 1)]), &up.url);

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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-notags01", 1)]), &up.url);

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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-query0001", 1)]), &up.url);
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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-passthru1", 1)]), &up.url);
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
    let (addr, _pool) = spawn_server(
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
    let (addr, _pool) = spawn_server(
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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-dead00001", 1)]), &up.url);
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
    let (addr, _pool) = spawn_server(pool_with(&[("omk-stream001", 1)]), &up.url);
    let (status, body) = post(&addr, "/api/generate", r#"{"model":"x","stream":true}"#);
    assert_eq!(status, 200);
    assert_eq!(body, "line1\nline2\n");
}

// ---------------------------------------------------------------------------
// /_usage and usage-aware routing (hermetic): the local upstream scripts
// GET /api/usage per Authorization header.
// ---------------------------------------------------------------------------

fn usage_body(session: f64, weekly: f64, model: &str, count: u64, cost: &str) -> String {
    format!(
        r#"{{"limits":{{"session":{{"usage":{session},"models":[{{"name":"{model}","request_count":{count}}}]}},"weekly":{{"usage":{weekly}}}}},"activity":{{"cost":"{cost}"}}}}"#
    )
}

#[test]
fn usage_endpoint_aggregates_per_key_without_secrets() {
    let payload_a = usage_body(0.037, 0.007, "gpt-oss:120b", 42, "$1.23");
    let payload_b = usage_body(0.81, 0.42, "qwen3-coder:480b", 7, "$0.10");
    let up = Upstream::spawn_auth(move |path, auth| {
        assert!(path.starts_with("/api/usage"), "unexpected path {path}");
        match auth {
            Some(a) if a.contains("abcd1234") => (200, "OK".into(), payload_a.clone()),
            Some(a) if a.contains("efgh5678") => (200, "OK".into(), payload_b.clone()),
            other => panic!("unexpected auth {other:?}"),
        }
    });
    let (addr, _pool) = spawn_server(
        pool_with(&[("omk-abcd1234", 1), ("omk-efgh5678", 1)]),
        &up.url,
    );

    let (status, body, headers) = post_with_headers(&addr, "/_usage", "");
    assert_eq!(status, 200, "body: {body}");
    assert!(
        headers
            .iter()
            .any(|(n, v)| n.eq_ignore_ascii_case("x-ollamux") && v.contains("ollamux/")),
        "identity header present: {headers:?}"
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["keys"].as_array().unwrap().len(), 2, "{body}");
    let updated = v["updated"].as_u64().expect("updated unix ts");
    assert!(updated > 0);
    assert!(v["age_s"].is_u64(), "{body}");
    assert_eq!(v["stale"], false, "{body}");

    // Per-key rows: ok/error vocabulary, suffixes only.
    let rows = v["keys"].as_array().unwrap();
    let row_a = rows.iter().find(|r| r["suffix"] == "1234").unwrap();
    assert_eq!(row_a["ok"], true, "{body}");
    assert_eq!(row_a["session"], 0.037);
    assert_eq!(row_a["session_pct"], 3.7);
    assert_eq!(row_a["weekly_pct"], 0.7);
    assert_eq!(row_a["models"][0]["request_count"], 42);
    assert_eq!(row_a["cost"], json_str("$1.23"));
    let row_b = rows.iter().find(|r| r["suffix"] == "5678").unwrap();
    assert_eq!(row_b["session_pct"], 81.0);

    // No secret ever.
    assert!(!body.contains("omk-abcd1234"), "secret leaked: {body}");
    assert!(!body.contains("omk-efgh5678"), "secret leaked: {body}");
}

fn json_str(s: &str) -> serde_json::Value {
    serde_json::Value::String(s.to_string())
}

#[test]
fn usage_ttl_caches_and_refresh_flag_bypasses() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let up = Upstream::spawn_auth(move |_, _| {
        CALLS.fetch_add(1, Ordering::Relaxed);
        (200, "OK".into(), usage_body(0.1, 0.2, "m", 1, "$0.01"))
    });
    let (addr, _pool) = spawn_server(pool_with(&[("omk-ttlcache001", 1)]), &up.url);

    let _ = post(&addr, "/_usage", "");
    assert_eq!(up.wait_for_count(1, std::time::Duration::from_secs(5)), 1);
    // Second call inside the TTL: served from cache, no new upstream hit.
    let _ = post(&addr, "/_usage", "");
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(
        CALLS.load(Ordering::Relaxed),
        1,
        "TTL must suppress refetch"
    );

    // ?refresh=1 forces a second round. MIN_REFRESH (5s) deliberately
    // gates the forced path too — assert the fetch count stays at 1 and
    // that a second ?refresh within the window does not hammer upstream.
    let (status, body, _) = post_with_headers(&addr, "/_usage?refresh=1", "");
    assert_eq!(status, 200, "{body}");
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(
        CALLS.load(Ordering::Relaxed),
        1,
        "?refresh=1 inside MIN_REFRESH must not refetch"
    );
}

#[test]
fn keys_endpoint_never_fetches_but_embeds_snapshot() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let up = Upstream::spawn_auth(move |_, _| {
        CALLS.fetch_add(1, Ordering::Relaxed);
        (200, "OK".into(), usage_body(0.5, 0.25, "m", 3, "$3.21"))
    });
    let (addr, _pool) = spawn_server(pool_with(&[("omk-keysembed1", 1)]), &up.url);

    // Pure in-memory read: no snapshot yet → no usage key, no upstream call.
    let (status, body, _) = get(&addr, "/_keys");
    assert_eq!(status, 200);
    let info: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(info[0].get("usage").is_none(), "no snapshot yet: {body}");
    assert_eq!(CALLS.load(Ordering::Relaxed), 0, "/_keys must never fetch");

    // A /_usage call populates the snapshot…
    let _ = post(&addr, "/_usage", "");
    assert_eq!(up.wait_for_count(1, std::time::Duration::from_secs(2)), 1);

    // …and the next /_keys read embeds it (still zero extra upstream calls).
    let (_, body2, _) = get(&addr, "/_keys");
    let info2: serde_json::Value = serde_json::from_str(&body2).unwrap();
    assert_eq!(info2[0]["usage"]["session"], 0.5, "{body2}");
    assert_eq!(info2[0]["usage"]["session_pct"], 50.0);
    assert_eq!(info2[0]["usage"]["weekly_pct"], 25.0, "{body2}");
    assert_eq!(CALLS.load(Ordering::Relaxed), 1);
    assert_eq!(info2[0]["state"], "up", "usage must not touch key health");
}

#[test]
fn usage_payload_drift_is_reported_per_key_still_200() {
    let up = Upstream::spawn_auth(|_, _| (200, "OK".into(), r#"{"hello":"world"}"#.into()));
    let (addr, _pool) = spawn_server(pool_with(&[("omk-drift00001", 1)]), &up.url);
    let (status, body, _) = post_with_headers(&addr, "/_usage", "");
    assert_eq!(status, 200, "introspection stays 200 on drift: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let row = &v["keys"][0];
    assert_eq!(row["ok"], false);
    assert!(
        row["error"].as_str().unwrap().contains("endpoint changed"),
        "{body}"
    );
}

#[test]
fn usage_auth_failure_is_reported_never_marks_dead() {
    let up = Upstream::spawn_auth(move |_, auth| match auth {
        Some(a) if a.contains("unauth401") => (401, "Unauthorized".into(), String::new()),
        _ => (200, "OK".into(), usage_body(0.1, 0.1, "m", 1, "$0")),
    });
    let (addr, _pool) = spawn_server(pool_with(&[("omk-unauth401xx", 1)]), &up.url);
    let (status, body, _) = post_with_headers(&addr, "/_usage", "");
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["keys"][0]["ok"], false);
    assert_eq!(v["keys"][0]["error"], json_str("unauthorized"));
    // Usage introspection is read-only: the key stays up in /_keys.
    let info = spawn_info(&addr);
    assert_eq!(info[0]["state"], "up", "401 on usage must not mark_dead");
    // The Bearer header reached upstream as expected (sanity on the seam).
    let _ = up.wait_for_count(1, Duration::from_secs(5));
    let reqs = up.recorded();
    assert_eq!(reqs.len(), 1, "one usage fetch recorded");
    assert_eq!(
        reqs[0].auth.as_deref(),
        Some("Bearer omk-unauth401xx"),
        "usage fetch must send Bearer <full key>"
    );
}

#[test]
fn usage_upstream_error_body_is_never_relayed() {
    // A malicious/broken upstream echoing the Bearer secret must not leak
    // through /_usage: only generated suffix-safe error strings appear.
    let up = Upstream::spawn_auth(move |_, auth| {
        let echoed = auth.unwrap_or_default();
        (500, "Server Error".into(), format!("error with {echoed}"))
    });
    let (addr, _pool) = spawn_server(pool_with(&[("omk-secret1234", 1)]), &up.url);
    let (status, body, _) = post_with_headers(&addr, "/_usage", "");
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["keys"][0]["ok"], false);
    assert_eq!(v["keys"][0]["error"], json_str("upstream HTTP 500"));
    assert!(
        !body.contains("omk-secret1234"),
        "upstream error text must not be relayed: {body}"
    );
}

#[test]
fn usage_all_keys_failing_still_answers_200() {
    // Nothing listens on the upstream port: every fetch takes the network
    // error path. /_usage is observability: never 5xx, report per key.
    let (addr, _pool) = spawn_server(pool_with(&[("omk-noreach001", 1)]), "http://127.0.0.1:1");
    let (status, body) = post(&addr, "/_usage", "");
    assert_eq!(status, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["keys"][0]["ok"], false);
    let err = v["keys"][0]["error"].as_str().unwrap();
    assert!(err.starts_with("network:"), "{body}");
}

#[test]
fn usage_endpoint_method_and_query_handling() {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let up = Upstream::spawn_auth(move |_, _| {
        CALLS.fetch_add(1, Ordering::Relaxed);
        (200, "OK".into(), usage_body(0.1, 0.2, "m", 1, "$0.01"))
    });
    let (addr, _pool) = spawn_server(pool_with(&[("omk-methodtest1", 1)]), &up.url);

    // GET works (the documented form)…
    let (status, _, _) = get(&addr, "/_usage");
    assert_eq!(status, 200);
    assert_eq!(up.wait_for_count(1, std::time::Duration::from_secs(2)), 1);

    // …POST hits the same read-only introspection route (harmless).
    let (status, _, _) = post_with_headers(&addr, "/_usage", "");
    assert_eq!(status, 200);
    // Unknown query params other than refresh are ignored.
    let (status, _, _) = get(&addr, "/_usage?foo=bar");
    assert_eq!(status, 200);
    let _ = CALLS.load(Ordering::Relaxed);
}

#[test]
fn usage_aware_routing_demotes_over_quota_key() {
    // Two keys: key "…aaaa" serves 95% (over the 80% threshold), key
    // "…zzzz" 10%. Quota-aware candidates must put zzzz first.
    let payload_a = usage_body(0.95, 0.1, "m", 1, "$0");
    let payload_z = usage_body(0.05, 0.1, "m", 1, "$0");
    let up = Upstream::spawn_auth(move |_, auth| match auth {
        Some(a) if a.contains("quota0001") => (200, "OK".into(), payload_a.clone()),
        Some(a) if a.contains("quota0002") => (200, "OK".into(), payload_z.clone()),
        other => panic!("unexpected auth {other:?}"),
    });
    let pool = Arc::new(ollamux::Pool::new(
        vec![
            ("omk-quota0001".to_string(), 2),
            ("omk-quota0002".to_string(), 2),
        ],
        4,
        false,
    ));
    pool.set_usage_threshold(80);
    let (addr, _pool) = spawn_server(pool.clone(), &up.url);

    // Fill the snapshot (both keys fetched in one fan-out).
    let (status, body) = post(&addr, "/_usage", "");
    assert_eq!(status, 200, "{body}");
    assert_eq!(up.wait_for_count(2, std::time::Duration::from_secs(2)), 2);

    // Both keys fresh+idle: the over-quota key must sort last. Round-robin
    // would otherwise alternate, so run several admissions.
    let mut firsts = std::collections::HashMap::new();
    for _ in 0..8 {
        let (_p, k) = pool
            .admit(Duration::from_millis(200))
            .unwrap_or_else(|e| panic!("admit failed: {e:?}"));
        *firsts.entry(k).or_insert(0u32) += 1;
        drop(_p);
    }
    assert_eq!(
        firsts.get(&0),
        None,
        "over-quota key must never be chosen while a fresh key is free"
    );

    // Threshold disabled mid-flight? (Not possible at runtime; but a pool
    // without a threshold ignores the same data.)
    let pool_plain = pool_with(&[("omk-quota0001", 1), ("omk-quota0002", 1)]);
    pool_plain.publish_usage(&{
        // Fabricate a snapshot directly: over-quota on key 0 only.
        ollamux::UsageSnapshot::for_test(
            vec![
                ollamux::KeyUsage::for_test(0, Some(0.99)),
                ollamux::KeyUsage::for_test(1, Some(0.01)),
            ],
            std::time::Instant::now(),
        )
    });
    let cands = pool_plain.candidates();
    assert_eq!(cands.len(), 2, "no data/flag → nobody excluded");
}

// ---------------------------------------------------------------------------
// Prompt-cache affinity: same conversation prefix pins to one key so
// ollama.com's per-account prompt cache stays warm.
// ---------------------------------------------------------------------------

const CONVO_A: &str = r#"{"model":"gpt-oss:120b","messages":[
    {"role":"system","content":"You are terse."},
    {"role":"user","content":"hi"}]}"#;
const CONVO_A_TURN2: &str = r#"{"model":"gpt-oss:120b","messages":[
    {"role":"system","content":"You are terse."},
    {"role":"user","content":"hi"},
    {"role":"assistant","content":"Hello."},
    {"role":"user","content":"more"}]}"#;

fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn suffix_of_auth(auth: &str) -> String {
    // Keys are "omk-<name>"; the pool reports the last four characters.
    auth.chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

#[test]
fn affinity_same_conversation_same_key() {
    // Two keys; upstream 200s everything. Turn 1 pins (miss), turn 2 and
    // the parallel-shot re-request must reuse the same key (hit).
    let up = Upstream::spawn(move |_| {
        (
            200,
            "OK".into(),
            r#"{"model":"gpt-oss:120b","message":{"role":"assistant","content":"ok"},"done":true}"#
                .into(),
        )
    });
    let (addr, _pool) = spawn_server(
        pool_with(&[("omk-affkey001", 4), ("omk-affkey002", 4)]),
        &up.url,
    );

    let (s1, _, h1) = post_with_headers(&addr, "/api/chat", CONVO_A);
    assert_eq!(s1, 200);
    let (s2, _, h2) = post_with_headers(&addr, "/api/chat", CONVO_A_TURN2);
    assert_eq!(s2, 200);

    let key1 = header_value(&h1, "x-ollamux-key").expect("key header");
    let key2 = header_value(&h2, "x-ollamux-key").expect("key header");
    assert_eq!(key1, key2, "same conversation must pin to one key");

    let aff1 = header_value(&h1, "x-ollamux-affinity").expect("affinity header");
    let aff2 = header_value(&h2, "x-ollamux-affinity").expect("affinity header");
    assert_eq!(aff1, "miss", "first request of a conversation: {aff1}");
    assert_eq!(aff2, "hit", "second request must consult the pin: {aff2}");

    // And the upstream really saw one Authorization across both turns.
    let auths: Vec<String> = up
        .recorded()
        .iter()
        .filter_map(|r| r.auth.clone())
        .collect();
    assert_eq!(auths.len(), 2, "{auths:?}");
    assert_eq!(auths[0], auths[1], "both turns served by the same key");
}

#[test]
fn affinity_pinned_key_death_repins_to_survivor() {
    // Key "…d1e2" always 401s (dead), key "…a9b8" 200s. First request
    // fails over to the survivor; the pin follows, so the second request
    // goes straight to the survivor (retries=0, no new 401).
    let up = Upstream::spawn_auth(move |_, auth| match auth {
        Some(a) if a.contains("d1e2") => (
            401,
            "Unauthorized".into(),
            r#"{"error":"Unauthorized"}"#.into(),
        ),
        _ => (
            200,
            "OK".into(),
            r#"{"done":true,"message":{"role":"assistant","content":"ok"}}"#.into(),
        ),
    });
    let (addr, _pool) = spawn_server(
        pool_with(&[("omk-deadd1e2", 1), ("omk-alivea9b8", 1)]),
        &up.url,
    );

    let (s1, _, h1) = post_with_headers(&addr, "/api/chat", CONVO_A);
    assert_eq!(s1, 200, "failover must reach the survivor");
    let retries1 = header_value(&h1, "x-ollamux-retries").unwrap_or_default();
    assert_eq!(retries1, "1", "one rotation to the survivor");

    let (s2, _, h2) = post_with_headers(&addr, "/api/chat", CONVO_A_TURN2);
    assert_eq!(s2, 200);
    let retries2 = header_value(&h2, "x-ollamux-retries").unwrap_or_default();
    assert_eq!(retries2, "0", "re-pinned: served directly by the survivor");

    // Exactly one 401 was recorded (the initial pin attempt); the second
    // request did not revisit the dead key.
    let deads = up
        .recorded()
        .iter()
        .filter(|r| r.auth.as_deref().is_some_and(|a| a.contains("d1e2")))
        .count();
    assert_eq!(deads, 1, "dead key must not be retried after re-pin");

    // Header says miss: the pin existed but the dead key could not serve,
    // so the request fell through to the survivor (hit = usable pin).
    let aff1 = header_value(&h1, "x-ollamux-affinity").unwrap_or_default();
    assert_eq!(aff1, "miss");
}

#[test]
fn affinity_busy_pinned_key_falls_through_then_returns() {
    // Deterministic via slot occupation: pin lands on key 0; with key 0's
    // slot occupied the same conversation must be served by key 1 (no
    // waiting on the pinned key), and with both free again the pin wins.
    let up = Upstream::spawn(move |_| {
        (
            200,
            "OK".into(),
            r#"{"done":true,"message":{"role":"assistant","content":"ok"}}"#.into(),
        )
    });
    let (addr, pool) = spawn_server(
        pool_with(&[("omk-busykey01", 1), ("omk-busykey02", 1)]),
        &up.url,
    );

    // Warm the pin onto whichever key serves first.
    let (s1, _, h1) = post_with_headers(&addr, "/api/chat", CONVO_A);
    assert_eq!(s1, 200);
    let pinned_suffix = header_value(&h1, "x-ollamux-key").unwrap();
    let pinned_idx = if pinned_suffix == suffix_of_auth("omk-busykey01") {
        0
    } else {
        1
    };
    let other_idx = 1 - pinned_idx;

    // Occupy the pinned key's only slot at the pool level. The worker
    // thread frees its permit shortly after the response bytes reach us,
    // so poll briefly rather than assuming it is already free.
    let deadline = Instant::now() + Duration::from_secs(5);
    let _hold = loop {
        if let Some(permit) = pool.try_acquire(pinned_idx) {
            break permit;
        }
        assert!(
            Instant::now() < deadline,
            "pinned key slot never freed (permit leak?)"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    let (s2, _, h2) = post_with_headers(&addr, "/api/chat", CONVO_A_TURN2);
    assert_eq!(s2, 200);
    let served = header_value(&h2, "x-ollamux-key").unwrap();
    assert_eq!(
        served,
        suffix_of_auth(if other_idx == 0 {
            "omk-busykey01"
        } else {
            "omk-busykey02"
        }),
        "busy pinned key must fall through, not wait: got {served}, pin was {pinned_suffix}"
    );
    assert_eq!(
        header_value(&h2, "x-ollamux-affinity").unwrap(),
        "miss",
        "pinned key had no free slot: not a usable pin, not a hit"
    );
    drop(_hold);

    // Both slots free again: the pin (now on other_idx) is used directly.
    let (s3, _, h3) = post_with_headers(&addr, "/api/chat", CONVO_A_TURN2);
    assert_eq!(s3, 200);
    assert_eq!(
        header_value(&h3, "x-ollamux-key").unwrap(),
        served,
        "re-pinned identity must win over rr once its key is free"
    );
    assert_eq!(header_value(&h3, "x-ollamux-affinity").unwrap(), "hit");
}

#[test]
fn no_affinity_disables_pinning() {
    let up = Upstream::spawn(move |_| {
        (
            200,
            "OK".into(),
            r#"{"done":true,"message":{"role":"assistant","content":"ok"}}"#.into(),
        )
    });
    let (addr, pool) = spawn_server(
        pool_with(&[("omk-offkey001", 4), ("omk-offkey002", 4)]),
        &up.url,
    );
    pool.set_affinity_enabled(false);

    // With affinity off, a saturated-key fallback must not stick: serve on
    // key A, free everything, request again — plain admit() order rules,
    // so the same conversation may land on either key (assert only that
    // the header reports off and no pin overrides least-loaded).
    let (s1, _, h1) = post_with_headers(&addr, "/api/chat", CONVO_A);
    assert_eq!(s1, 200);
    let aff1 = header_value(&h1, "x-ollamux-affinity").unwrap();
    assert_eq!(aff1, "off", "disabled affinity must say off");

    // Pin the map manually (as if it had been used before) and prove the
    // disabled pool ignores it.
    pool.pin(12345, 1);
    let (s2, _, h2) = post_with_headers(&addr, "/api/chat", CONVO_A);
    assert_eq!(s2, 200);
    assert_eq!(
        header_value(&h2, "x-ollamux-affinity").unwrap(),
        "off",
        "header must stay off when disabled"
    );
    let key2 = header_value(&h2, "x-ollamux-key").unwrap();
    assert_eq!(
        key2,
        suffix_of_auth("omk-offkey001"),
        "fresh two-key pool, plain admit: least-loaded/rr picks key 0 regardless of the pin"
    );
}

#[test]
fn affinity_header_surface_matrix() {
    // Present (with key) on: buffered 200, streaming 200, relayed 4xx.
    // Absent on: no-auth GET, /_keys, /_health, hintful 404.
    let up = Upstream::spawn(move |path| match path {
        p if p.starts_with("/api/chat") => (
            400,
            "Bad Request".into(),
            r#"{"error":"bad"}"#.into(), // relayed verbatim (client's fault)
        ),
        _ => (200, "OK".into(), r#"{"models":[]}"#.into()),
    });
    let (addr, _pool) = spawn_server(
        pool_with(&[("omk-surface001", 2), ("omk-surface002", 2)]),
        &up.url,
    );

    // Buffered non-stream response: header present.
    let body = r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":false}"#;
    let (_, _, h_ok) = post_with_headers(&addr, "/api/chat", body);
    assert!(
        header_value(&h_ok, "x-ollamux-affinity").is_some(),
        "{h_ok:?}"
    );
    assert!(header_value(&h_ok, "x-ollamux-key").is_some());

    // Streaming response: header present in the chunked head. Same
    // technique as streaming_passes_through_chunked: read only up to the
    // blank line — the keep-alive connection never EOFs.
    let stream_body = r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}"#;
    let mut req = std::net::TcpStream::connect(addr.trim_start_matches("http://")).unwrap();
    let http_body = format!(
        "POST /api/chat HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{stream_body}",
        stream_body.len()
    );
    std::io::Write::write_all(&mut req, http_body.as_bytes()).unwrap();
    let mut raw = String::new();
    // Read the head plus the first chunk; stop at the first blank line
    // (end of headers). Bound the read so a framing bug cannot hang CI.
    req.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut byte = [0u8; 1];
    loop {
        match std::io::Read::read(&mut req, &mut byte) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                raw.push(byte[0] as char);
                if raw.contains("\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let head_lower = raw.to_lowercase();
    assert!(
        head_lower.contains("x-ollamux-affinity:"),
        "streaming head must carry the affinity header: {raw}"
    );
    assert!(head_lower.contains("x-ollamux-key:"), "{raw}");
    drop(req);

    // Relayed 4xx: header present (a key served it).
    let (_, _, h_400) = post_with_headers(&addr, "/api/chat", body);
    assert!(header_value(&h_400, "x-ollamux-affinity").is_some());
    assert!(header_value(&h_400, "x-ollamux-key").is_some());

    // Exhausted failover (502): no key *served* — all attempts failed
    // before any response — so both headers are absent (documented:
    // "admission rejects, exhausted failover — omit the header").
    // Unreachable upstream → every attempt is retryable until the
    // budget runs out.
    let (addr_502, _pool_502) = spawn_server(
        pool_with(&[("omk-surface003", 2), ("omk-surface004", 2)]),
        "http://127.0.0.1:1", // nothing listens there
    );
    let (status_502, _, h_502) = post_with_headers(&addr_502, "/api/chat", body);
    assert_eq!(status_502, 502, "{h_502:?}");
    assert!(
        header_value(&h_502, "x-ollamux-affinity").is_none(),
        "exhausted-failover 502 must omit the affinity header: {h_502:?}"
    );
    assert!(
        header_value(&h_502, "x-ollamux-key").is_none(),
        "exhausted-failover 502 must omit the key header: {h_502:?}"
    );

    // No-auth: no key, no affinity header.
    let (_, _, h_tags) = get(&addr, "/api/tags");
    assert!(
        header_value(&h_tags, "x-ollamux-affinity").is_none(),
        "{h_tags:?}"
    );
    assert!(header_value(&h_tags, "x-ollamux-key").is_none());

    // Local endpoints: never.
    for path in ["/_keys", "/_health", "/nonsense"] {
        let (_, _, h) = get(&addr, path);
        assert!(
            header_value(&h, "x-ollamux-affinity").is_none(),
            "{path} must not carry the affinity header: {h:?}"
        );
    }
}

#[cfg(feature = "net")]
#[test]
fn net_failover_money_test() {
    // One-key pool; bogus key: upstream 401 marks the key dead, retry
    // exhausts, proxy answers the all-dead 403 in the surface's shape.
    let (addr, _pool) = spawn_server(pool_with(&[("omk-money0004", 1)]), "https://ollama.com");
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
