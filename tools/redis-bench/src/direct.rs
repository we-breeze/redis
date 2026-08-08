//! A minimal pooled connection layer for direct (no-mesh) mode.
//!
//! In direct mode the harness talks to a raw redis-server (`--direct
//! host:port`) instead of going through the breeze mesh. The SDK's [`Client`]
//! is mesh-shaped (it discovers an endpoint via sock files and assumes the
//! mesh conveys identity/routing out-of-band), so it is not used here. Instead
//! we build a thin pool of the SDK's [`MultiplexedConnection`]s directly against
//! the resolved TCP/unix endpoint and expose it as a [`ConnectionLike`], so the
//! same [`Commands`](breeze_redis::Commands) workloads used against the mesh
//! client work here too.
//!
//! This still exercises the real RESP encoder, parser, multiplexing driver,
//! and in-flight budget. It deliberately does not replicate the SDK's circuit
//! breaker, retries, or slow-log — those are mesh-availability features and
//! are meaningless without a mesh. Each request is a single attempt on a live
//! connection, bounded by `op_timeout`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use breeze_redis::connection::{ConnectionLike, MultiplexedConnection, RedisFuture};
use breeze_redis::error::{ErrorKind, RedisError, RedisResult};
use breeze_redis::mesh::Endpoint;
use breeze_redis::types::Value;
use breeze_redis::{Cmd, Pipeline};

/// A small round-robin pool of [`MultiplexedConnection`]s to one direct
/// endpoint. Cheap to clone (shares one pool). Each request is timed out via
/// `op_timeout`; a timeout poisons the connection (it may be half-hung).
#[derive(Clone)]
pub struct DirectClient {
    inner: Arc<DirectInner>,
}

struct DirectInner {
    conns: Vec<MultiplexedConnection>,
    dispatch: AtomicUsize,
    op_timeout: Duration,
}

impl DirectClient {
    /// Open `pool_size` multiplexed connections to `addr` (`host:port` or a
    /// unix socket path prefixed with `unix:`).
    pub async fn connect(
        addr: &str,
        pool_size: usize,
        max_inflight: usize,
        op_timeout: Duration,
    ) -> RedisResult<Self> {
        let endpoint = parse_endpoint(addr).await?;
        let pool_size = pool_size.max(1);
        let mut conns = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            conns.push(MultiplexedConnection::connect(&endpoint, max_inflight).await?);
        }
        Ok(DirectClient {
            inner: Arc::new(DirectInner {
                conns,
                dispatch: AtomicUsize::new(0),
                op_timeout,
            }),
        })
    }

    /// Round-robin pick a live connection, skipping any marked dead.
    fn pick(&self) -> Option<&MultiplexedConnection> {
        let conns = &self.inner.conns;
        let n = conns.len();
        if n == 0 {
            return None;
        }
        let start = self.inner.dispatch.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let conn = &conns[(start + offset) % n];
            if conn.is_alive() {
                return Some(conn);
            }
        }
        None
    }
}

impl ConnectionLike for DirectClient {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let conn = self.pick().ok_or_else(|| {
                RedisError::new(ErrorKind::NoConnection, "no live direct connection")
            })?;
            let fut = conn.req_command(command);
            match tokio::time::timeout(self.inner.op_timeout, fut).await {
                Ok(result) => result,
                Err(_) => {
                    conn.poison();
                    Err(RedisError::new(
                        ErrorKind::Timeout,
                        "command timed out waiting for the redis reply",
                    ))
                }
            }
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let conn = self.pick().ok_or_else(|| {
                RedisError::new(ErrorKind::NoConnection, "no live direct connection")
            })?;
            let fut = conn.req_pipeline(pipeline, offset, count);
            match tokio::time::timeout(self.inner.op_timeout, fut).await {
                Ok(result) => result,
                Err(_) => {
                    conn.poison();
                    Err(RedisError::new(
                        ErrorKind::Timeout,
                        "pipeline timed out waiting for the redis reply",
                    ))
                }
            }
        })
    }
}

/// Parse a direct endpoint specifier.
///
/// - `host:port` → TCP on that address (resolved via tokio, DNS-aware).
/// - `unix:/path/to/sock` → a unix domain socket.
async fn parse_endpoint(addr: &str) -> RedisResult<Endpoint> {
    if let Some(path) = addr.strip_prefix("unix:") {
        return Ok(Endpoint::Unix(std::path::PathBuf::from(path)));
    }
    let addrs = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| RedisError::new(ErrorKind::Io, e.to_string()))?;
    // Prefer an IPv4 address, fall back to the first resolved.
    let mut tcp = None;
    for a in addrs {
        if a.is_ipv4() {
            tcp = Some(a);
            break;
        }
        if tcp.is_none() {
            tcp = Some(a);
        }
    }
    tcp.map(Endpoint::Tcp)
        .ok_or_else(|| RedisError::new(ErrorKind::Io, "no address resolved for direct endpoint"))
}
