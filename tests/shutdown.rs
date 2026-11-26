//! Shutdown-signal integration: SIGTERM must drain and persist stats.

#[cfg(unix)]
fn spawn_ollamux(
    bin: &str,
    stats: &std::path::Path,
    extra_env: &[(&str, String)],
) -> (std::process::Child, String) {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    // A probe port can be taken by another process between `drop(probe)`
    // and the child's bind. Retry on a fresh port rather than failing the
    // test on that unrelated race.
    let mut last = String::from("no attempt");
    for _ in 0..10 {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let mut cmd = Command::new(bin);
        cmd.env_remove("OLLAMUX_NO_STATS")
            .env_remove("OLLAMUX_STATS_FILE")
            .env("OLLAMUX_KEYS", "abc:1@alice")
            .args([
                "--addr",
                &addr.to_string(),
                "--stats-file",
                stats.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (name, value) in extra_env {
            cmd.env(name, value);
        }
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                last = format!("spawn failed: {e}");
                continue;
            }
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if ureq::get(&format!("http://{addr}/_health")).call().is_ok() {
                return (child, addr.to_string());
            }
            if let Ok(Some(status)) = child.try_wait() {
                last = format!("child exited early: {status:?}");
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                last = "startup timed out".to_string();
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    panic!("ollamux did not start after retries: {last}");
}

#[cfg(unix)]
#[test]
fn sigterm_flushes_stats_before_exit() {
    use std::process::Command;
    use std::time::{Duration, Instant};

    let bin = env!("CARGO_BIN_EXE_ollamux");
    let dir = std::env::temp_dir().join(format!(
        "ollamux-sigterm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let stats = dir.join("stats.json");

    let mut child = spawn_ollamux(bin, &stats, &[]).0;

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("wait") {
            assert!(status.success(), "unexpected exit: {status:?}");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("ollamux did not exit after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let body = std::fs::read_to_string(&stats).expect("stats file written on shutdown");
    assert!(body.contains("\"marker\": \"ollamux-stats\""), "{body}");
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(all(unix, any(debug_assertions, feature = "test-upstream")))]
#[test]
fn sigterm_waits_for_an_inflight_request() {
    use std::io::{Read, Write};
    use std::process::Command;
    use std::time::{Duration, Instant};

    // Slow local upstream: notifies when the request is in flight, then
    // answers after 800 ms with native usage counters.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 8192];
        let _ = stream.read(&mut buf);
        started_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(800));
        let body =
            r#"{"message":{"content":"ok"},"done":true,"prompt_eval_count":8,"eval_count":3}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let bin = env!("CARGO_BIN_EXE_ollamux");
    let dir = std::env::temp_dir().join(format!(
        "ollamux-sigterm-inflight-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let stats = dir.join("stats.json");
    let (mut child, addr) = spawn_ollamux(
        bin,
        &stats,
        &[("OLLAMUX_UPSTREAM", format!("http://{upstream_addr}"))],
    );

    let client = std::thread::spawn(move || {
        ureq::post(&format!("http://{addr}/api/chat"))
            .send_string(r#"{"model":"m","stream":false}"#)
            .map(|r| r.status())
            .map_err(|e| e.to_string())
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("upstream never saw the request");

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let client_status = client.join().expect("client thread");
    assert_eq!(
        client_status.expect("response"),
        200,
        "in-flight request completed"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("wait") {
            assert!(status.success(), "unexpected exit: {status:?}");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("ollamux did not exit after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    let body = std::fs::read_to_string(&stats).expect("stats file");
    let v: serde_json::Value = serde_json::from_str(&body).expect("stats json");
    assert_eq!(v["totals"]["requests"], 1, "{body}");
    assert_eq!(v["totals"]["input_tokens"], 8, "{body}");
    assert_eq!(v["totals"]["output_tokens"], 3, "{body}");
    let _ = std::fs::remove_dir_all(dir);
}
