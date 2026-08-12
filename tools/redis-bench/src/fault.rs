//! Fault injection via a local TCP proxy between the bench and the server.
//!
//! The proxy forwards request frames sequentially per connection and, on a
//! dice roll, delays a frame:
//!
//! - **slow** (`--slow-rate`/`--slow-ms`): the frame is delayed but still
//!   delivered — stretches the latency tail without tripping the client's
//!   `op_timeout`.
//! - **timeout** (`--timeout-rate`/`--timeout-ms`): the frame is delayed
//!   past the client's operation timeout, so the SDK's own timeout fires
//!   (connection poisoned, read retry, stats) — exactly the production
//!   behavior under a hanging server.
//!
//! Injecting *between* client and server (instead of wrapping the client)
//! is deliberate: only then do delays interact with the SDK's timeout,
//! circuit-breaker, and retry machinery. Frames on a delayed connection are
//! head-of-line blocked, which mirrors a slow single-threaded server.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Probabilistic delay injector, shared by all workers.
pub struct FaultInjector {
    slow_rate: f64,
    slow: Duration,
    timeout_rate: f64,
    timeout: Duration,
    counter: AtomicU64,
    slow_injected: AtomicU64,
    timeout_injected: AtomicU64,
}

impl FaultInjector {
    /// `None` when both rates are zero (no injection, zero overhead).
    pub fn new(slow_rate: f64, slow_ms: u64, timeout_rate: f64, timeout_ms: u64) -> Option<Self> {
        if slow_rate <= 0.0 && timeout_rate <= 0.0 {
            return None;
        }
        Some(FaultInjector {
            slow_rate,
            slow: Duration::from_millis(slow_ms),
            timeout_rate,
            timeout: Duration::from_millis(timeout_ms),
            counter: AtomicU64::new(0x243F6A8885A308D3),
            slow_injected: AtomicU64::new(0),
            timeout_injected: AtomicU64::new(0),
        })
    }

    /// A uniform roll in [0, 1). Counter-based murmur finalizer: cheap,
    /// thread-safe, no per-op allocation; statistical quality is plenty for
    /// fault injection.
    #[inline]
    fn roll(&self) -> f64 {
        let mut x = self
            .counter
            .fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^= x >> 33;
        (x >> 40) as f64 / (1u64 << 24) as f64
    }

    /// Sleep according to the dice. Timeout takes precedence over slow.
    pub async fn maybe_delay(&self) {
        let r = self.roll();
        if r < self.timeout_rate {
            self.timeout_injected.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.timeout).await;
        } else if r < self.timeout_rate + self.slow_rate {
            self.slow_injected.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(self.slow).await;
        }
    }

    /// (slow, timeout) injection counts so far.
    pub fn counts(&self) -> (u64, u64) {
        (
            self.slow_injected.load(Ordering::Relaxed),
            self.timeout_injected.load(Ordering::Relaxed),
        )
    }
}

/// Start a fault-injecting proxy forwarding to `target`; returns the local
/// address to connect to instead. Sequential per connection: a delayed frame
/// head-of-line-blocks later frames on the same connection, like a slow
/// single-threaded server would.
pub async fn start_proxy(
    target: std::net::SocketAddr,
    injector: Arc<FaultInjector>,
) -> Result<std::net::SocketAddr, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("fault proxy bind failed: {e}"))?;
    let local = listener.local_addr().map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((inbound, _)) => {
                    let injector = injector.clone();
                    tokio::spawn(async move {
                        forward(inbound, target, injector).await;
                    });
                }
                Err(err) => {
                    // Transient accept errors (e.g. fd pressure) must not
                    // kill the proxy permanently.
                    eprintln!("fault proxy: accept error: {err}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    });
    Ok(local)
}

/// One proxied connection: downstream (server→client) is a raw byte copy;
/// upstream (client→server) is forwarded one RESP frame at a time with
/// fault injection between frames.
async fn forward(inbound: TcpStream, target: std::net::SocketAddr, injector: Arc<FaultInjector>) {
    let Ok(outbound) = TcpStream::connect(target).await else {
        return;
    };
    outbound.set_nodelay(true).ok();
    let (mut ri, mut wi) = tokio::io::split(inbound);
    let (mut ro, mut wo) = tokio::io::split(outbound);
    let downstream = tokio::spawn(async move { tokio::io::copy(&mut ro, &mut wi).await });

    let mut buf = BytesMut::with_capacity(16 * 1024);
    loop {
        match ri.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        // Unexpected framing (not a multibulk): forward raw, never delay.
        if buf.first() != Some(&b'*') {
            if wo.write_all(&buf).await.is_err() {
                break;
            }
            buf.clear();
            continue;
        }
        while let Some(len) = parse_frame(&buf) {
            let frame = buf.split_to(len);
            injector.maybe_delay().await;
            if wo.write_all(&frame).await.is_err() {
                downstream.abort();
                return;
            }
        }
    }
    downstream.abort();
}

/// Length of one complete RESP multibulk command frame at the front of
/// `buf`, or `None` if incomplete (or empty). Expects `*N\r\n` followed by
/// N `$len\r\n<bytes>\r\n` bulks (what this SDK sends).
fn parse_frame(buf: &[u8]) -> Option<usize> {
    if buf.first() != Some(&b'*') {
        return None;
    }
    let mut pos = 1;
    let count = parse_line_i64(buf, &mut pos)?;
    for _ in 0..count {
        if buf.get(pos) != Some(&b'$') {
            return None;
        }
        pos += 1;
        let len = parse_line_i64(buf, &mut pos)?;
        if len < 0 {
            return None;
        }
        let end = pos.checked_add(len as usize)?.checked_add(2)?;
        if buf.len() < end {
            return None;
        }
        pos = end;
    }
    Some(pos)
}

/// Read one CRLF-terminated line as i64, advancing `pos` past it.
fn parse_line_i64(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let rest = &buf[*pos..];
    let idx = rest.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&rest[..idx]).ok()?;
    *pos += idx + 2;
    line.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end: SDK direct client -> proxy (no faults) -> real redis.
    /// Requires REDIS_BENCH_PROXY_TEST_ADDR=host:port of a live redis.
    #[tokio::test]
    async fn proxy_forwards_end_to_end() {
        let Ok(target) = std::env::var("REDIS_BENCH_PROXY_TEST_ADDR") else {
            eprintln!("skip: REDIS_BENCH_PROXY_TEST_ADDR not set");
            return;
        };
        let injector = Arc::new(FaultInjector::new(0.000001, 1, 0.0, 0).unwrap());
        let target_addr: std::net::SocketAddr = target.parse().unwrap();
        let proxy = start_proxy(target_addr, injector).await.unwrap();

        let cfg = redis::direct::ServerConfig::new(&proxy.to_string()).unwrap();
        let client = redis::direct::DirectClient::connect(cfg).await.unwrap();
        use redis::Commands;
        client.hset::<i64>("proxy:it", "f", "v").await.unwrap();
        let v: String = client.hget("proxy:it", "f").await.unwrap();
        assert_eq!(v, "v");

        // Concurrent burst: exercises multi-frame reads through the proxy.
        let mut handles = Vec::new();
        for w in 0..8 {
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..200 {
                    client
                        .hset::<i64>("proxy:it:burst", format!("f{w}"), i)
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    /// The listener must survive connection churn (open/close storms as
    /// pools evict and reconnect). Requires REDIS_BENCH_PROXY_TEST_ADDR.
    #[tokio::test]
    async fn proxy_survives_connection_churn() {
        let Ok(target) = std::env::var("REDIS_BENCH_PROXY_TEST_ADDR") else {
            eprintln!("skip: REDIS_BENCH_PROXY_TEST_ADDR not set");
            return;
        };
        let injector = Arc::new(FaultInjector::new(0.001, 5, 0.001, 50).unwrap());
        let proxy = start_proxy(target.parse().unwrap(), injector)
            .await
            .unwrap();

        // Churn: open, send one command, close — 500 times.
        for _ in 0..500 {
            let mut s = TcpStream::connect(proxy).await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut buf = [0u8; 64];
            let _ = s.read(&mut buf).await;
            drop(s);
        }
        // The proxy must still accept and forward.
        let mut s = TcpStream::connect(proxy).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 64];
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0, "proxy must still forward after churn");
    }

    #[test]
    fn parses_frames() {
        // *2\r\n$4\r\nHGET\r\n$1\r\nk\r\n
        let frame = b"*2\r\n$4\r\nHGET\r\n$1\r\nk\r\n";
        assert_eq!(parse_frame(frame), Some(frame.len()));
        // Two frames back to back: only the first is consumed.
        let mut two = frame.to_vec();
        two.extend_from_slice(frame);
        assert_eq!(parse_frame(&two), Some(frame.len()));
        // Truncated bulk body is incomplete.
        assert_eq!(parse_frame(&frame[..frame.len() - 3]), None);
        // A realistic 10-arg HSET produced by the SDK encoder.
        let mut cmd = redis::cmd("HSET");
        cmd.arg(&b"\x00\x00\x00\x00\x00\x00\x00\x0ckkkkkkkkkkkkkkkkkkkkkkkk"[..]);
        for f in ["f1", "f2", "f3", "f4"] {
            cmd.arg(f).arg(b"v");
        }
        let encoded = cmd.encoded();
        assert_eq!(parse_frame(&encoded), Some(encoded.len()));
        // Binary key bytes containing \r\n must not confuse the parser.
        let mut cmd = redis::cmd("HGET");
        cmd.arg(&b"k\r\nk"[..]).arg("f");
        let encoded = cmd.encoded();
        assert_eq!(parse_frame(&encoded), Some(encoded.len()));
    }
}
