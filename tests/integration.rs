//! Integration tests through the real handler and a real tiny_http
//! server: /_keys, /_health, hintful 404, and the failover "money test"
//! at the pool level (the full proxy path hardcodes the upstream URL).

use omlx::proxy::Server;
use std::sync::Arc;
use std::time::Duration;

fn pool_with(keys: &[(&str, u32)]) -> Arc<omlx::Pool> {
    Arc::new(omlx::Pool::new(
        keys.iter().map(|(k, c)| (k.to_string(), *c)).collect(),
        32,
        false,
    ))
}

/// Spawn the real Server on a local tiny_http; returns base URL.
fn spawn_server(pool: Arc<omlx::Pool>) -> String {
    let server = Server::new(pool);
    let tiny = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let addr = match tiny.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("expected IP listener"),
    };
    std::thread::spawn(move || {
        while let Ok(req) = tiny.recv() {
            server.handle(req);
        }
    });
    format!("http://{addr}")
}

fn get(url: &str, path: &str) -> (u16, String, Vec<(String, String)>) {
    let resp = match ureq::get(&format!("{url}{path}")).call() {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => panic!("request failed: {e}"),
    };
    let status = resp.status();
    let headers: Vec<(String, String)> = resp
        .headers_names()
        .into_iter()
        .flat_map(|n| {
            resp.all(&n)
                .into_iter()
                .map(move |v| (n.clone(), v.to_string()))
        })
        .collect();
    (status, resp.into_string().unwrap(), headers)
}

fn post(url: &str, path: &str, body: &str) -> (u16, String) {
    match ureq::post(&format!("{url}{path}")).send_string(body) {
        Ok(r) => (r.status(), r.into_string().unwrap()),
        Err(ureq::Error::Status(code, r)) => (code, r.into_string().unwrap()),
        Err(e) => panic!("request failed: {e}"),
    }
}

#[test]
fn keys_health_and_404_endpoints() {
    let addr = spawn_server(pool_with(&[("omk-abcd1234", 1)]));

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

    let (status, body, _) = get(&addr, "/nonsense");
    assert_eq!(status, 404);
    assert!(body.contains("not a local Ollama"), "404 body: {body}");
}

#[test]
fn failover_money_test() {
    // One-key pool whose endpoint always 429s: the money path is that the
    // pool cools the key, the request budget exhausts, and the client gets
    // a clean 502 with the right error shape — not an upstream leak.
    let _addr = spawn_server(pool_with(&[("omk-money0001", 1)]));

    // /api/tags is no-auth and hits the network (ollama.com) — avoid it.
    // Use an unknown /api path upstream: our proxy forwards it; upstream
    // will 404. That still exercises routing + relay (needs network).
    // For a hermetic test, assert the admission path instead:
    let pool = pool_with(&[("omk-money0002", 1), ("omk-money0003", 1)]);
    let (permit, key0) = pool.admit(Duration::from_secs(1)).unwrap();
    pool.mark_cooldown(key0, Duration::from_millis(50), "429 rate limited");
    drop(permit);
    // Next admit must land on the OTHER key.
    let (_, key1) = pool.admit(Duration::from_secs(1)).unwrap();
    assert_ne!(key0, key1, "failover must rotate to the healthy key");

    // And the proxy surfaces its own errors in the surface's JSON shape.
    // NOTE: this genuinely calls ollama.com with a bogus key: upstream
    // 401 marks the key dead, the retry exhausts, and the proxy answers
    // with its all-dead 403 (surfaced as the (surface-correct) shape).
    let addr2 = spawn_server(pool_with(&[("omk-money0004", 1)]));
    let (status, body) = post(&addr2, "/api/chat", r#"{"model":"x"}"#);
    assert_eq!(status, 403, "body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["error"].is_string(), "ollama-style error: {body}");

    let (status2, body2) = post(&addr2, "/v1/chat/completions", r#"{"model":"x"}"#);
    // The key is already dead from the previous call — admission 403.
    assert_eq!(status2, 403, "body: {body2}");
    let v: serde_json::Value = serde_json::from_str(&body2).unwrap();
    assert!(
        v["error"]["message"].is_string(),
        "openai-style error: {body2}"
    );
}
