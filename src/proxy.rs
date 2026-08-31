//! Request handling: routing, upstream dispatch with key failover,
//! streaming passthrough, and proxy-generated error responses.
//!
//! Ownership: `tiny_http::Request` is consumed by `respond()` and
//! `into_writer()`. The request lives in an ` Option`-like slot and is
//! `std::mem::replace`d out exactly once, at the moment a response is
//! sent. Until then it can be reused across failover attempts.

use crate::pool::{Pool, Reject};
use crate::usage::UsageTracker;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Request body cap. Failover needs a replayable buffer; generous for
/// base64 images in /api/chat. 413 beyond this.
const MAX_BODY: usize = 16 * 1024 * 1024;
/// Upstream attempts per request before returning 502 (bounds the
/// per-request failover budget).
const MAX_ATTEMPTS: usize = 3;
/// How long one request may wait for a free slot before 429.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-op upstream timeouts — never a whole-request timeout (would kill
/// long streams). The generous read bound reaps half-dead connections.
const UP_READ_TIMEOUT: Duration = Duration::from_secs(300);
const UP_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const UP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on honoring an upstream Retry-After.
const RETRY_AFTER_CAP: Duration = Duration::from_secs(300);
/// Response relay chunk size.
const CHUNK: usize = 16 * 1024;
/// Bytes of an error body kept for the stderr snippet.
const SNIPPET: u64 = 256;

const UPSTREAM: &str = "https://ollama.com";

/// Self-identification for every response: lets an agent (or human with
/// curl) tell ollamux apart from a real Ollama server or a CDN error page.
fn identity_header() -> tiny_http::Header {
    hv("X-Ollamux", &format!("ollamux/{}", crate::VERSION))
}

/// No-auth upstream paths: single attempt; never rotate, never mark keys.
/// Stored WITH a leading slash; route() passes trimmed paths, so matching
/// normalizes both (an earlier build compared trimmed to untrimmed and the
/// fast path never fired).
const NO_AUTH_PATHS: &[&str] = &["/api/tags", "/api/version", "/v1/models"];

fn is_no_auth_path(path: &str) -> bool {
    NO_AUTH_PATHS
        .iter()
        .any(|p| p.trim_start_matches('/') == path)
}

static REQ_ID: AtomicUsize = AtomicUsize::new(1);

pub struct Server {
    pub pool: Arc<Pool>,
    /// Usage introspection (/_usage). Always present; fetches only when
    /// /_usage / ?refresh / --usage-aware actually run.
    pub usage: Arc<UsageTracker>,
    agent: ureq::Agent,
    /// Base URL of the upstream ("https://ollama.com"); injectable so tests
    /// can run hermetically against a local server.
    upstream: String,
}

impl Server {
    pub fn new(pool: Arc<Pool>) -> Server {
        Self::with_upstream(pool, UPSTREAM)
    }

    /// Spawn the quota-aware background poller (opt-in via --usage-aware):
    /// refresh the usage snapshot at the TTL cadence until `stop` is set.
    /// Fetch failures are non-fatal — the previous snapshot is kept and
    /// the failure surfaces per-key in /_usage.
    pub fn spawn_usage_poller(self: &Arc<Self>, stop: Arc<std::sync::atomic::AtomicBool>) {
        let server = Arc::clone(self);
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // tick() is a no-op while the snapshot is fresh, so an
                // on-demand /_usage refresh is never duplicated here.
                server.usage.tick();
                // Wake frequently enough to observe `stop` promptly.
                let deadline = Instant::now() + crate::usage::USAGE_TTL;
                while Instant::now() < deadline {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
            }
        });
    }

    /// Test seam: point the proxy at a different (e.g. local) upstream.
    /// HTTPS-only enforcement follows the scheme of `upstream`.
    #[doc(hidden)]
    pub fn with_upstream(pool: Arc<Pool>, upstream: &str) -> Server {
        let slots = pool.total_slots().max(4) as usize;
        let https_only = upstream.starts_with("https://");
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(UP_CONNECT_TIMEOUT)
            .timeout_read(UP_READ_TIMEOUT)
            .timeout_write(UP_WRITE_TIMEOUT)
            // Failover semantics require 3xx to be an error, not a followed
            // redirect: a hop to a CDN login page or another host must be
            // classified/relayed explicitly, and Authorization must never
            // ride an off-site redirect.
            .redirects(0)
            .max_idle_connections(slots.next_power_of_two() * 2)
            .max_idle_connections_per_host(slots)
            .https_only(https_only)
            .build();
        let usage = Arc::new(UsageTracker::new(pool.clone(), upstream));
        Server {
            pool,
            usage,
            agent,
            upstream: upstream.trim_end_matches('/').to_string(),
        }
    }

    /// Serve one request.
    pub fn handle(&self, req: tiny_http::Request) {
        let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let method = req.method().as_str().to_string();
        let full_url = req.url().to_string();
        let (path, query) = match full_url.split_once('?') {
            Some((p, q)) => (p.to_string(), Some(q.to_string())),
            None => (full_url, None),
        };

        let (status, retries, key) = self.route(req, &path, query.as_deref());

        if self.pool.verbose() {
            eprintln!(
                "ollamux: #{id} {method} {path} -> {status}{}{} dur={}ms",
                if retries > 0 {
                    format!(" retries={retries}")
                } else {
                    String::new()
                },
                key.map(|s| format!(" key={s}")).unwrap_or_default(),
                started.elapsed().as_millis(),
            );
        }
    }

    /// Returns (client-visible status, retries used, key suffix if proxied).
    fn route(
        &self,
        req: tiny_http::Request,
        path: &str,
        query: Option<&str>,
    ) -> (u16, usize, Option<String>) {
        let path = path.trim_start_matches('/');
        if path == "_keys" {
            // Pure in-memory read: /_keys must never trigger an upstream
            // usage fetch (incident readers rely on it being instant).
            // Usage columns appear only when a snapshot already exists.
            let body = match self.usage.peek() {
                Some(snap) => {
                    let rows = self.pool.info_with_usage(&snap);
                    let values: Vec<serde_json::Value> = rows
                        .into_iter()
                        .map(|(info, brief)| {
                            let mut v = serde_json::to_value(&info).unwrap_or_default();
                            if let Some(b) = brief {
                                v["usage"] = serde_json::to_value(b).unwrap_or_default();
                            }
                            v
                        })
                        .collect();
                    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())
                }
                None => serde_json::to_value(self.pool.info())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|_| "[]".to_string()),
            };
            json_response(req, 200, &body, None);
            return (200, 0, None);
        }
        if path == "_usage" {
            let force = query.is_some_and(|q| {
                q.split('&').any(|kv| {
                    let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                    k == "refresh" && v != "0" && v != "false"
                })
            });
            let snap = if force {
                self.usage.refresh()
            } else {
                self.usage.get()
            };
            let body = usage_json(&snap, self.pool.total_keys());
            json_response(req, 200, &body, None);
            return (200, 0, None);
        }
        if path == "_health" {
            let body = serde_json::json!({
                "service": "ollamux",
                "version": crate::VERSION,
                "ok": self.pool.healthy_any(),
                "keys": self.pool.len(),
                "total_slots": self.pool.total_slots(),
            });
            json_response(req, 200, &body.to_string(), None);
            return (200, 0, None);
        }
        if path.starts_with("api/") {
            return self.proxy(req, path, query, false);
        }
        if path.starts_with("v1/") {
            return self.proxy(req, path, query, true);
        }
        // Hintful 404: top confusion is expecting a local Ollama server.
        json_response(
            req,
            404,
            r#"{"error":"ollamux (key-rotating proxy for Ollama Cloud, https://ollama.com) serves only /api/*, /v1/*, /_keys, /_usage, /_health — this is not a local Ollama server and has no local models. If you meant a local Ollama, point your client at port 11434 instead; for Ollama Cloud, use /api/… or /v1/… here (models: GET /api/tags; per-key usage: GET /_usage)."}"#,
            None,
        );
        (404, 0, None)
    }

    // ----- proxy core -----

    fn proxy(
        &self,
        mut req: tiny_http::Request,
        sub_path: &str,
        query: Option<&str>,
        is_v1: bool,
    ) -> (u16, usize, Option<String>) {
        let no_auth = is_no_auth_path(sub_path);

        // 1. Buffer the body (needed for replay across failover attempts).
        let body = match read_body(&mut req) {
            Ok(b) => b,
            Err(too_large) => {
                let (status, msg) = if too_large {
                    (
                        413u16,
                        format!(
                            "request body exceeds ollamux's {} MiB limit (bodies are buffered in memory so they can be replayed across key failover); reduce attachments/images or send the model a URL instead",
                            MAX_BODY / (1024 * 1024)
                        ),
                    )
                } else {
                    (
                        400u16,
                        "ollamux failed to read the request body (client disconnected or sent a malformed/truncated request); retry the request".to_string(),
                    )
                };
                let json = error_json(is_v1, &msg, status);
                json_response(req, status, &json, None);
                return (status, 0, None);
            }
        };
        let streaming = wants_stream(&body, is_v1);

        // 2. Admission before dispatch. The permit is held across the whole
        // request and freed by RAII when this function returns.
        let mut retries = 0usize;

        // No-auth endpoints never consume a concurrency slot: they are valid
        // upstream without credentials, and blocking them on dead-key pool
        // state would wrongly take down /api/tags even when ollama.com works.
        if no_auth {
            let cx = AttemptCx {
                body: &body,
                sub_path,
                query,
                key: None,
                no_auth: true,
                streaming,
                retries: 0,
            };
            return match self.attempt(&cx, &mut req) {
                Attempt::Sent(status) => (status, 0, None),
                Attempt::Retryable(reason) => {
                    let json = error_json(
                        is_v1,
                        &format!(
                            "ollamux could not reach the upstream ({}): {reason}. Check network access and per-key state via GET /_keys; no automatic retry applies to this endpoint — repeat the request.",
                            self.upstream
                        ),
                        502,
                    );
                    json_response(req, 502, &json, None);
                    (502, 0, None)
                }
            };
        }

        let (mut permit, mut key) = match self.pool.admit(WAIT_TIMEOUT) {
            Ok(a) => a,
            Err(rej) => {
                send_reject(req, &rej, is_v1);
                return (rej.status, 0, None);
            }
        };

        let mut cx = AttemptCx {
            body: &body,
            sub_path,
            query,
            key: Some(key),
            no_auth,
            streaming,
            retries,
        };
        loop {
            match self.attempt(&cx, &mut req) {
                Attempt::Sent(status) => {
                    if let Some(k) = cx.key {
                        self.pool.settle(k, status < 400);
                    }
                    let sfx = cx.key.map(|k| self.pool.suffix_of(k));
                    return (status, retries, sfx);
                }
                Attempt::Retryable(reason) => {
                    if retries + 1 >= MAX_ATTEMPTS || no_auth {
                        let msg = format!(
                            "ollamux exhausted all {} failover attempt(s) against {}; last error: {reason}. Per-key state: GET /_keys. The keys each cool down and recover automatically, so retrying later may succeed.",
                            retries + 1,
                            self.upstream
                        );
                        let json = error_json(is_v1, &msg, 502);
                        json_response(req, 502, &json, None);
                        return (502, retries, Some(self.pool.suffix_of(key)));
                    }
                    retries += 1;
                    // Rotate: release this key's slot, admit on another.
                    drop(permit);
                    match self.pool.admit(WAIT_TIMEOUT) {
                        Ok((p, k)) => {
                            permit = p;
                            key = k;
                            cx.key = Some(k);
                            cx.retries = retries;
                        }
                        Err(rej) => {
                            send_reject(req, &rej, is_v1);
                            return (rej.status, retries, None);
                        }
                    }
                }
            }
        }
    }

    /// One upstream attempt. `req` is left intact on `Retryable` (so the
    /// next attempt can reuse it); it is consumed exactly when a response
    /// goes to the client.
    fn attempt(&self, cx: &AttemptCx, req: &mut tiny_http::Request) -> Attempt {
        let AttemptCx {
            body,
            sub_path,
            query,
            key,
            no_auth,
            streaming,
            retries,
        } = cx;
        let url = match query {
            Some(q) if !q.is_empty() => format!("{}/{sub_path}?{q}", self.upstream),
            _ => format!("{}/{sub_path}", self.upstream),
        };
        let mut call = self.agent.request(req.method().as_str(), &url);
        for (name, value) in curated_request_headers(req) {
            call = call.set(&name, &value);
        }
        if let Some(k) = key {
            if !*no_auth {
                call = call.set(
                    "Authorization",
                    &format!("Bearer {}", self.pool.secret_of(*k)),
                );
            }
        }
        // Force identity: with decompression enabled, ureq transparently
        // decompresses and silently rewrites framing.
        call = call.set("Accept-Encoding", "identity");

        let resp = match call.send_bytes(body) {
            Ok(resp) => resp,
            Err(ureq::Error::Status(code, resp)) => return self.classify(cx, code, resp, req),
            Err(e) => return Attempt::Retryable(format!("network: {e}")),
        };

        let status = resp.status();
        let headers = curated_response_headers(&resp);
        let reader = resp.into_reader();
        if *streaming {
            self.stream_chunked(take(req), status, &headers, reader, *key, *retries)
        } else {
            self.respond_buffered(take(req), status, &headers, reader, *key, *retries)
        }
    }

    /// Classify a 3xx/4xx/5xx response: mark key health, then either
    /// rotate (Retryable, nothing sent) or relay verbatim (Sent).
    fn classify(
        &self,
        cx: &AttemptCx,
        status: u16,
        resp: ureq::Response,
        req: &mut tiny_http::Request,
    ) -> Attempt {
        let headers = curated_response_headers(&resp);
        let retry_after = resp.header("retry-after").map(str::to_string);
        let mut reader = resp.into_reader();

        // Small snippet for logs / auth verdicts. The consumed bytes are
        // passed along so `relay_error` can re-prepend them: relaying the
        // remainder alone would hand clients a truncated error body.
        let mut snippet = Vec::new();
        let _ = (&mut reader).take(SNIPPET).read_to_end(&mut snippet);
        let snip = String::from_utf8_lossy(&snippet).into_owned();

        let AttemptCx {
            key,
            retries,
            no_auth,
            ..
        } = cx;
        let tries = *retries;
        if *no_auth {
            // Nothing to learn about key health; relay verbatim.
            return Self::relay_error(
                RelayCx {
                    pool: &self.pool,
                    req: take(req),
                    status,
                    headers: &headers,
                    prefix: snippet,
                    key: *key,
                    retries: tries,
                },
                reader,
            );
        }
        let key = key.expect("classify on non-auth attempt always has a key");

        let unauthorized = match status {
            401 => true,
            // CDN/WAF 403s exist; only the known API shape marks a dead key.
            403 => snip.contains("Unauthorized"),
            _ => false,
        };
        if unauthorized {
            self.pool.mark_dead(key, "upstream says unauthorized");
            return Attempt::Retryable(format!("unauthorized: {snip}"));
        }
        match status {
            429 => {
                let dur = retry_after
                    .as_deref()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(|s| Duration::from_secs(s.min(RETRY_AFTER_CAP.as_secs())))
                    .unwrap_or(crate::pool::COOLDOWN_429);
                self.pool.mark_cooldown(key, dur, &format!("429: {snip}"));
                Attempt::Retryable(format!("429: {snip}"))
            }
            500..=599 => {
                self.pool
                    .mark_strike(key, &format!("upstream {status}: {snip}"));
                Attempt::Retryable(format!("upstream {status}"))
            }
            _ => {
                // Other 4xx (client's fault) is relayed verbatim; it is not
                // a key-health signal. Nothing reaches here for 3xx: with
                // redirects disabled, ureq returns a 3xx as Ok (unit.rs
                // breaks the loop before the >=400 mapping), so redirects
                // relay via the success path in `attempt` instead.
                Self::relay_error(
                    RelayCx {
                        pool: &self.pool,
                        req: take(req),
                        status,
                        headers: &headers,
                        prefix: snippet,
                        key: Some(key),
                        retries: tries,
                    },
                    reader,
                )
            }
        }
    }

    /// Relay small bodies (error statuses, no-auth endpoints): buffer and
    /// answer via tiny_http with correct framing. `prefix` holds the bytes
    /// consumed for the log snippet so the body is relayed verbatim.
    fn relay_error(cx: RelayCx<'_>, mut reader: impl Read) -> Attempt {
        let RelayCx {
            pool,
            req,
            status,
            headers,
            prefix,
            key,
            retries,
        } = cx;
        let mut owned = prefix;
        owned.reserve(1024);
        let read_err = reader.read_to_end(&mut owned).is_err();
        let mut resp = tiny_http::Response::from_data(owned).with_status_code(status);
        for (n, v) in headers {
            if let Ok(h) = tiny_http::Header::from_bytes(n.as_bytes(), v.as_bytes()) {
                resp = resp.with_header(h);
            }
        }
        if let Some(k) = key {
            resp = resp.with_header(hv("X-Ollamux-Key", &pool.suffix_of(k)));
        }
        resp = resp.with_header(hv("X-Ollamux-Retries", &retries.to_string()));
        resp = resp.with_header(identity_header());
        let _ = req.respond(resp);
        Attempt::Sent(if read_err { 502 } else { status })
    }

    /// Non-streaming success relay via tiny_http: it sets framing
    /// (Content-Length/chunked) and handles HEAD and `Expect: 100-continue`
    /// correctly. The reader streams directly to the socket.
    fn respond_buffered(
        &self,
        req: tiny_http::Request,
        status: u16,
        headers: &[(String, String)],
        reader: impl Read + Send + 'static,
        key: Option<usize>,
        retries: usize,
    ) -> Attempt {
        let mut hs: Vec<tiny_http::Header> = headers
            .iter()
            .filter_map(|(n, v)| tiny_http::Header::from_bytes(n.as_bytes(), v.as_bytes()).ok())
            .collect();
        if let Some(k) = key {
            hs.push(hv("X-Ollamux-Key", &self.pool.suffix_of(k)));
        }
        hs.push(hv("X-Ollamux-Retries", &retries.to_string()));
        hs.push(identity_header());
        let resp = tiny_http::Response::new(
            tiny_http::StatusCode(status),
            hs,
            Box::new(reader),
            None,
            None,
        );
        match req.respond(resp) {
            Ok(()) => Attempt::Sent(status),
            Err(_) => Attempt::Sent(502),
        }
    }

    /// Streaming relay: raw writer, chunked framing, flush after every
    /// upstream read so SSE/NDJSON arrive token-incrementally. Write errors
    /// mean the client is gone: drop the upstream reader (permit frees via
    /// RAII; ureq discards its socket when dropped mid-body).
    fn stream_chunked(
        &self,
        req: tiny_http::Request,
        status: u16,
        headers: &[(String, String)],
        reader: impl Read,
        key: Option<usize>,
        retries: usize,
    ) -> Attempt {
        let http10 = req.http_version() < &tiny_http::HTTPVersion(1, 1);
        let mut w = req.into_writer();
        let mut head = format!("HTTP/1.1 {status} {}\r\n", reason_phrase(status));
        let ident = identity_header();
        head.push_str(&format!(
            "{}: {}\r\n",
            ident.field.as_str(),
            ident.value.as_str()
        ));
        if let Some(k) = key {
            head.push_str(&format!("X-Ollamux-Key: {}\r\n", self.pool.suffix_of(k)));
        }
        head.push_str(&format!("X-Ollamux-Retries: {retries}\r\n"));
        for (n, v) in headers {
            if !n.eq_ignore_ascii_case("content-length") {
                head.push_str(&format!("{n}: {v}\r\n"));
            }
        }
        if http10 {
            // HTTP/1.0 has no chunked framing: rely on connection close to
            // delimit the body (tiny_http keeps the writer open for us).
            head.push_str("Connection: close\r\n\r\n");
        } else {
            head.push_str("Transfer-Encoding: chunked\r\n\r\n");
        }
        if w.write_all(head.as_bytes()).is_err() || w.flush().is_err() {
            return Attempt::Sent(500);
        }
        let mut reader = reader.take(CHUNK as u64 * 1_000_000);
        let mut buf = vec![0u8; CHUNK];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let ok = if http10 {
                        w.write_all(&buf[..n]).is_ok() && w.flush().is_ok()
                    } else {
                        w.write_all(format!("{n:x}\r\n").as_bytes()).is_ok()
                            && w.write_all(&buf[..n]).is_ok()
                            && w.write_all(b"\r\n").is_ok()
                            && w.flush().is_ok()
                    };
                    if !ok {
                        // Client disconnected mid-stream.
                        return Attempt::Sent(200);
                    }
                }
                Err(e) => {
                    // Upstream died mid-stream: cannot failover after the
                    // first byte; terminate the chunked body so the client
                    // sees a (truncated) complete response.
                    if !http10 {
                        let _ = w.write_all(b"0\r\n\r\n");
                        let _ = w.flush();
                    }
                    if self.pool.verbose() {
                        eprintln!("ollamux: upstream died mid-stream: {e}");
                    }
                    return Attempt::Sent(502);
                }
            }
        }
        if !http10 {
            let _ = w.write_all(b"0\r\n\r\n");
            let _ = w.flush();
        }
        Attempt::Sent(status)
    }
}

/// Per-attempt context, threaded through dispatch → classify. `key` is
/// `None` for credential-less attempts (no-auth endpoints).
struct AttemptCx<'a> {
    body: &'a [u8],
    sub_path: &'a str,
    query: Option<&'a str>,
    key: Option<usize>,
    no_auth: bool,
    streaming: bool,
    retries: usize,
}

/// Bundled args for `relay_error_impl` (keeps the clippy arg count sane).
struct RelayCx<'a> {
    pool: &'a Pool,
    req: tiny_http::Request,
    status: u16,
    headers: &'a [(String, String)],
    prefix: Vec<u8>,
    key: Option<usize>,
    retries: usize,
}

/// One upstream attempt's outcome.
enum Attempt {
    /// The Request was consumed and something was sent (any status).
    Sent(u16),
    /// Nothing sent to the client; `req` is still usable; try the next key.
    Retryable(String),
}

/// Dummy request used when the real one has already been taken/consumed.
/// Only `respond()`able; never proxied.
fn dummy_request() -> tiny_http::Request {
    tiny_http::TestRequest::new().into()
}

/// Extract the request from its slot, leaving a harmless dummy behind so
/// the slot stays usable (never actually reused for a second response).
fn take(req: &mut tiny_http::Request) -> tiny_http::Request {
    std::mem::replace(req, dummy_request())
}

/// Read the request body into memory. Err(true) = over cap, Err(false) = I/O.
fn read_body(req: &mut tiny_http::Request) -> Result<Vec<u8>, bool> {
    let mut buf = Vec::new();
    {
        let mut reader = req.as_reader().take((MAX_BODY + 1) as u64);
        reader.read_to_end(&mut buf).map_err(|_| false)?;
    }
    if buf.len() > MAX_BODY {
        return Err(true);
    }
    Ok(buf)
}

/// Whether the client asked for a streaming response. Best-effort from the
/// buffered body: `"stream": true/false` if present, else the endpoint
/// default (Ollama streams by default; OpenAI-compat does not).
fn wants_stream(body: &[u8], is_v1: bool) -> bool {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return false;
    };
    match v.get("stream") {
        Some(serde_json::Value::Bool(b)) => *b,
        None => !is_v1,
        _ => false,
    }
}

fn error_json(is_v1: bool, msg: &str, status: u16) -> String {
    if is_v1 {
        serde_json::json!({
            "error": {
                "message": msg,
                "type": "ollamux_error",
                "param": null,
                "code": format!("ollamux_{status}"),
            }
        })
        .to_string()
    } else {
        serde_json::json!({ "error": msg }).to_string()
    }
}

fn json_response(req: tiny_http::Request, status: u16, body: &str, retry_after: Option<u64>) {
    let mut resp = tiny_http::Response::from_string(body.to_string())
        .with_status_code(status)
        .with_header(hv("Content-Type", "application/json"))
        .with_header(identity_header());
    if let Some(secs) = retry_after {
        resp = resp.with_header(hv("Retry-After", &secs.to_string()));
    }
    let _ = req.respond(resp);
}

fn send_reject(req: tiny_http::Request, rej: &Reject, is_v1: bool) {
    let json = error_json(is_v1, rej.reason, rej.status);
    let mut resp = tiny_http::Response::from_string(json)
        .with_status_code(rej.status)
        .with_header(hv("Content-Type", "application/json"))
        .with_header(identity_header());
    if let Some(secs) = rej.retry_after_s {
        resp = resp.with_header(hv("Retry-After", &secs.to_string()));
    }
    let _ = req.respond(resp);
}

/// Forward client headers minus hop-by-hop, framing, credential and
/// compression headers. Framing is re-derived by us; encoding is forced to
/// identity; credentials belong to the pool.
fn curated_request_headers(req: &tiny_http::Request) -> Vec<(String, String)> {
    const SKIP: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "expect",
        "host",
        "content-length",
        "authorization",
        "cookie",
        "accept-encoding",
    ];
    req.headers()
        .iter()
        .filter_map(|h| {
            let name = h.field.as_str().as_str().to_ascii_lowercase();
            if SKIP.contains(&name.as_str()) || name.starts_with("proxy-") {
                return None;
            }
            Some((
                h.field.as_str().as_str().to_string(),
                h.value.as_str().to_string(),
            ))
        })
        .collect()
}

/// Relay upstream response headers minus hop-by-hop, cookies, alt-svc, and
/// content-length (we re-derive framing). Date/Server kept: tiny_http only
/// adds its own when absent, so relaying upstream values is duplication-free.
fn curated_response_headers(resp: &ureq::Response) -> Vec<(String, String)> {
    const SKIP: &[&str] = &[
        "connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
        "set-cookie",
        "alt-svc",
        "content-length",
    ];
    let mut out = Vec::new();
    for name in resp.headers_names() {
        if SKIP.contains(&name.as_str()) {
            continue;
        }
        for value in resp.all(&name) {
            out.push((name.clone(), value.to_string()));
        }
    }
    out
}

/// Render a usage snapshot as the /_usage response body: envelope with
/// freshness, one row per pool key (ok:data or ok:false + error). Key
/// count comes from the pool so a snapshot older than a keys-file change
/// (impossible — keys are fixed at startup, but be defensive) rows as
/// failures rather than truncating silently.
fn usage_json(snap: &crate::usage::UsageSnapshot, expected: usize) -> String {
    let updated = snap.updated_unix();
    let age_s = snap.fetched_at.elapsed().as_secs();
    let keys: Vec<serde_json::Value> = snap
        .keys
        .iter()
        .map(|k| serde_json::to_value(k).unwrap_or_default())
        .chain(
            // Missing rows (pool grew? defensive only) render as failures.
            (snap.keys.len()..expected).map(|i| {
                serde_json::json!({"index": i, "suffix": "?", "ok": false, "error": "no usage data"})
            }),
        )
        .collect();
    // Re-fetch when the snapshot is older than the TTL but still served
    // (stale-while-revalidate): tell the client so it can decide.
    let stale = age_s >= crate::usage::USAGE_TTL.as_secs();
    serde_json::json!({
        "updated": updated,
        "age_s": age_s,
        "stale": stale,
        "session_window": "about 5 hours (rolling; no reset timestamps upstream)",
        "keys": keys,
    })
    .to_string()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

fn hv(name: &str, value: &str) -> tiny_http::Header {
    tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes())
        .expect("static header name/value")
}
