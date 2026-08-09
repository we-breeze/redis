// Integration tests for the direct-TCP RESP client against a real Redis 7
// server in Docker.
//
// Run with:
//
//     cargo test -p breeze-redis --features direct-tcp --test direct_integration -- --ignored
//
// Requirements:
//   - Docker daemon running.
//   - Image `redis:7` available locally
//     (the committed IMAGE constant below; for local dev you may swap to any
//     Redis 7 image via the `BRZ_REDIS_TEST_IMAGE` env var, e.g.
//     `redis:7-alpine`). The tests only connect to a host:port, so any standard
//     Redis 7 server works regardless of the image name.
//
// NOTE ON CURRENT FAILURES (as of this commit):
// The committed `breeze_redis::direct::hget` and `breeze_redis::direct::hmget`
// in `src/direct.rs` emit *malformed* RESP: they declare an array of `3` (resp.
// `2 + nfields`) elements but omit the command-name bulk string (`HGET` /
// `HMGET`), sending only `<key>` and `<field>`/`<fields>`. A real Redis 7
// server therefore waits for the missing element(s) and never replies, so the
// library's 10s read deadline fires as `RedisError::Timeout("read timeout")`.
//
// These tests exercise the library through its public API and assert the
// *intended* contract. They will start passing once `direct.rs` is fixed to
// include the command name in the RESP array (e.g.
// `*3\r\n$4\r\nHGET\r\n$<keylen>\r\n<key>\r\n$<fieldlen>\r\n<field>\r\n`).
// The seeding helpers below use a self-contained, *correct* RESP encoder to
// write data into Redis, and the harness is verified working against the same
// container (PING/HSET/HGET/HMGET all round-trip via the helper encoder).

// Integration tests for the direct-TCP RESP client against a real Redis 7
// server in Docker.
//
// These tests are `#[ignore]`-guarded because they require Docker with a Redis 7
// image available. Run them explicitly with:
//
//     cargo test -p breeze-redis --features direct-tcp --test direct_integration -- --ignored
//
// Requirements:
//   - Docker daemon running.
//   - Image `redis:7` available locally
//     (the committed IMAGE constant below; for local dev you may swap to any
//     Redis 7 image via the `BRZ_REDIS_TEST_IMAGE` env var). The tests only
//     connect to a host:port, so any standard Redis 7 server works regardless
//     of the image name.
//
// Container networking note: this host's Docker daemon has no `bridge` network
// (only `host`/`none`/`trp-record-local`), so `-p host:container` port
// publishing does not forward. The tests therefore start redis-server with
// `--network host` and pass `--port <port> --bind 127.0.0.1` so each container
// listens on a unique loopback port directly on the host.

#![cfg(feature = "direct-tcp")]

use std::process::Command;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use breeze_redis::direct::{ArrayElement, BulkResponse, hget, hmget};

/// Committed image reference. Tests only connect to host:port, so the image
/// choice only affects container startup. Override at runtime with the
/// `BRZ_REDIS_TEST_IMAGE` env var for local iteration.
const COMMITTED_IMAGE: &str = "redis:7";

fn test_image() -> String {
    std::env::var("BRZ_REDIS_TEST_IMAGE").unwrap_or_else(|_| COMMITTED_IMAGE.to_string())
}

/// Monotonic port base so concurrent/sequential tests get unique loopback
/// ports. Well above 1024 to avoid privileged-binding issues.
static NEXT_PORT: AtomicU16 = AtomicU16::new(22111);

fn unique_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

/// A running redis container bound to `127.0.0.1:<port>`. Cleans up on drop
/// via `docker rm -f`.
struct RedisContainer {
    name: String,
    port: u16,
}

impl RedisContainer {
    /// Start a redis container on `127.0.0.1:<port>` (via `--network host` +
    /// `redis-server --port <port> --bind 127.0.0.1 --protected-mode no` args)
    /// and poll until it answers PONG (up to ~15s). Panics on infra failure.
    async fn start(test_id: &str) -> Self {
        let port = unique_port();
        let name = format!("brz-redis-it-{test_id}-{port}");
        let image = test_image();

        // Remove any stale container with the same name, then start fresh.
        // `--network host` is used because this host's Docker daemon has no
        // `bridge` network, so `-p host:container` publishing does not forward.
        // We pass redis-server its own `--port/--bind` flags so it binds a
        // unique loopback port directly on the host. `--protected-mode no`
        // avoids the CONFIG REWRITE / protected-mode refusal for bind loops.
        let _ = Command::new("docker").args(["rm", "-f", &name]).output();

        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                &name,
                "--network",
                "host",
                &image,
                "redis-server",
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--protected-mode",
                "no",
            ])
            .output();
        let out = match out {
            Ok(o) => o,
            Err(e) => panic!("failed to invoke `docker run`: {e}"),
        };
        if !out.status.success() {
            panic!(
                "`docker run` failed (status {:?}): stdout={:?} stderr={:?}",
                out.status.code(),
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }

        let container = RedisContainer { name, port };
        container.wait_for_ready(Duration::from_secs(15)).await;
        container
    }

    /// Poll the port until a TCP connect succeeds AND Redis answers `+PONG` to
    /// a `PING`. A bare TCP connect is not enough: on some setups the socket
    /// may accept before redis is ready to answer commands.
    async fn wait_for_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let addr = format!("127.0.0.1:{}", self.port);
        loop {
            match TcpStream::connect(&addr).await {
                Ok(mut s) => {
                    let _ = s.set_nodelay(true);
                    let ping = b"*1\r\n$4\r\nPING\r\n";
                    if s.write_all(ping).await.is_err() {
                        if Instant::now() >= deadline {
                            // Best-effort cleanup before failing.
                            let _ = Command::new("docker")
                                .args(["rm", "-f", &self.name])
                                .output();
                            panic!("redis at {addr}: PING write failed before deadline");
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    let _ = s.flush().await;
                    let mut buf = [0u8; 32];
                    match tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await {
                        Ok(Ok(n)) if n > 0 && buf.starts_with(b"+PONG") => return,
                        _ => {}
                    }
                    if Instant::now() >= deadline {
                        let _ = Command::new("docker")
                            .args(["rm", "-f", &self.name])
                            .output();
                        panic!("redis at {addr}: never answered PONG within {timeout:?}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        let _ = Command::new("docker")
                            .args(["rm", "-f", &self.name])
                            .output();
                        panic!("redis on {addr} did not become reachable within {timeout:?}: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

impl Drop for RedisContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Send a raw RESP command over a fresh TcpStream and read back the full reply.
/// Self-contained, *correct* RESP encoder/reader used only to seed data and to
/// sanity-check the container — it is NOT what the tests assert on (they assert
/// on the public `breeze_redis::direct::hget` / `breeze_redis::direct::hmget`
/// API).
async fn resp_command(port: u16, args: &[&[u8]]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await?;
    stream.set_nodelay(true)?;

    let mut req = Vec::new();
    req.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        req.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        req.extend_from_slice(a);
        req.extend_from_slice(b"\r\n");
    }
    stream.write_all(&req).await?;
    let _ = stream.flush().await;

    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk))
            .await
            .map_err(|e| std::io::Error::other(format!("read timeout: {e}")))??;
        if n == 0 {
            if resp_is_complete(&buf) {
                break;
            }
            return Err(std::io::Error::other(format!(
                "connection closed before complete reply; got {} bytes: {:?}",
                buf.len(),
                String::from_utf8_lossy(&buf)
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
        if resp_is_complete(&buf) {
            break;
        }
        if Instant::now() > deadline {
            return Err(std::io::Error::other(format!(
                "timed out waiting for complete reply; got {} bytes: {:?}",
                buf.len(),
                String::from_utf8_lossy(&buf)
            )));
        }
    }
    Ok(buf)
}

/// Minimal RESP completeness check (bulk string, array, simple string, error,
/// integer). Good enough for `+OK`, `:N`, and `$...` replies used in seeding.
fn resp_is_complete(buf: &[u8]) -> bool {
    if buf.is_empty() {
        return false;
    }
    let (_consumed, complete) = check(buf, 0);
    complete
}

fn check(buf: &[u8], pos: usize) -> (usize, bool) {
    if pos >= buf.len() {
        return (0, false);
    }
    match buf[pos] {
        b'+' | b'-' | b':' => {
            if let Some(e) = find_crlf(buf, pos + 1) {
                (e + 2 - pos, true)
            } else {
                (0, false)
            }
        }
        b'$' => {
            let (e, _) = match find_crlf(buf, pos + 1) {
                Some(e) => (e, true),
                None => return (0, false),
            };
            let len_str = match std::str::from_utf8(&buf[pos + 1..e]) {
                Ok(s) => s,
                Err(_) => return (0, false),
            };
            let len: i64 = match len_str.parse() {
                Ok(n) => n,
                Err(_) => return (0, false),
            };
            if len < 0 {
                return (e + 2 - pos, true);
            }
            let len = len as usize;
            let data_end = e + 2 + len + 2;
            if data_end <= buf.len() {
                (data_end - pos, true)
            } else {
                (0, false)
            }
        }
        b'*' => {
            let (e, _) = match find_crlf(buf, pos + 1) {
                Some(e) => (e, true),
                None => return (0, false),
            };
            let count_str = match std::str::from_utf8(&buf[pos + 1..e]) {
                Ok(s) => s,
                Err(_) => return (0, false),
            };
            let count: i64 = match count_str.parse() {
                Ok(n) => n,
                Err(_) => return (0, false),
            };
            if count < 0 {
                return (e + 2 - pos, true);
            }
            let mut offset = e + 2;
            for _ in 0..count {
                let (consumed, complete) = check(buf, offset);
                if !complete {
                    return (0, false);
                }
                offset += consumed;
            }
            (offset - pos, true)
        }
        _ => (0, false),
    }
}

fn find_crlf(buf: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Seed a hash field via a correct `HSET key field value` RESP command.
/// Retries on transient connection-reset right after the container reports
/// ready (observed on some docker bridge setups).
async fn hset(port: u16, key: &str, field: &str, value: &[u8]) {
    let args: &[&[u8]] = &[b"HSET", key.as_bytes(), field.as_bytes(), value];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match resp_command(port, args).await {
            Ok(reply) => {
                assert!(
                    reply.first() == Some(&b':'),
                    "unexpected HSET reply: {:?}",
                    String::from_utf8_lossy(&reply)
                );
                return;
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    panic!("HSET resp_command after retries: {e:?}");
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

/// Call a direct read function with a short retry loop to absorb transient
/// connection-reset races against a freshly-started container. The library
/// opens a new connection per call, so retries are safe for read-only ops.
async fn call_with_retry<F, T>(mut f: F) -> Result<T, breeze_redis::direct::RedisError>
where
    F: FnMut() -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T, breeze_redis::direct::RedisError>> + Send>,
    >,
{
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker with the redis:7 image available"]
async fn hget_returns_data_for_existing_field() {
    let redis = RedisContainer::start("existing").await;

    let key = "hash:existing";
    let field = "f1";
    let value = b"hello-world";
    hset(redis.port, key, field, value).await;

    let resp = call_with_retry(|| Box::pin(hget("127.0.0.1", redis.port, key, field)))
        .await
        .expect("hget should succeed");
    match resp {
        BulkResponse::Data(d) => assert_eq!(d, value),
        BulkResponse::Nil => panic!("expected Data, got Nil"),
    }
}

#[tokio::test]
#[ignore = "requires Docker with the redis:7 image available"]
async fn hget_returns_nil_for_missing_field() {
    let redis = RedisContainer::start("missing-field").await;

    // A key with no fields set, so the requested field is definitely absent.
    let key = "hash:missing-field";
    let resp = call_with_retry(|| Box::pin(hget("127.0.0.1", redis.port, key, "nope")))
        .await
        .expect("hget should succeed");
    assert!(
        matches!(resp, BulkResponse::Nil),
        "expected Nil for missing field, got Data"
    );
}

#[tokio::test]
#[ignore = "requires Docker with the redis:7 image available"]
async fn hmget_returns_mixed_present_and_nil_in_order() {
    let redis = RedisContainer::start("mixed").await;

    let key = "hash:mixed";
    hset(redis.port, key, "a", b"AAA").await;
    hset(redis.port, key, "c", b"CCC").await;

    // Request a (present), b (missing), c (present) — order must be preserved.
    let fields: &[&str] = &["a", "b", "c"];
    let resp = call_with_retry(|| Box::pin(hmget("127.0.0.1", redis.port, key, fields)))
        .await
        .expect("hmget should succeed");

    assert_eq!(resp.len(), 3, "expected one element per requested field");
    match &resp[0] {
        ArrayElement::Data(d) => assert_eq!(d, b"AAA", "field a"),
        ArrayElement::Nil => panic!("expected Data for field a, got Nil"),
    }
    assert!(
        matches!(resp[1], ArrayElement::Nil),
        "expected Nil for missing field b, got Data"
    );
    match &resp[2] {
        ArrayElement::Data(d) => assert_eq!(d, b"CCC", "field c"),
        ArrayElement::Nil => panic!("expected Data for field c, got Nil"),
    }
}

#[tokio::test]
#[ignore = "requires Docker with the redis:7 image available"]
async fn hmget_on_missing_key_returns_all_nil() {
    let redis = RedisContainer::start("missing-key").await;

    // Redis HMGET on a non-existent key returns one nil per requested field.
    let key = "hash:does-not-exist";
    let fields: &[&str] = &["x", "y", "z"];
    let resp = call_with_retry(|| Box::pin(hmget("127.0.0.1", redis.port, key, fields)))
        .await
        .expect("hmget should succeed");

    assert_eq!(
        resp.len(),
        3,
        "expected one nil element per field even for a missing key"
    );
    for (i, el) in resp.iter().enumerate() {
        assert!(
            matches!(el, ArrayElement::Nil),
            "expected Nil at index {i} for missing key, got Data"
        );
    }
}
