//! A multiplexed async connection to the mesh.
//!
//! [`MultiplexedConnection`] is a cheap-`Clone` handle over an mpsc channel to a
//! single background driver task that owns the TCP socket. Many
//! callers issue commands concurrently; the driver writes them to the socket
//! and matches replies back to waiters in FIFO order. This gives automatic
//! pipelining and lets one socket serve high concurrency without a lock or a
//! connection-per-request pool.
//!
//! There is no Redis handshake: the mesh conveys identity/routing out-of-band
//! via the sock file, so the client opens the socket and immediately speaks
//! RESP.

use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::connection::{ConnectionLike, RedisFuture};
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::pipeline::Pipeline;
use crate::resp::parser::{ParseResult, parse_reply};
use crate::sidecar::discovery::Endpoint;
use crate::types::Value;

/// Optional per-connection handshake for direct backend access (no mesh):
/// `AUTH` then `SELECT`, sent immediately after the socket opens.
#[derive(Clone, Debug, Default)]
pub struct Handshake {
    /// Redis password, sent as `AUTH <password>`.
    pub auth: Option<String>,
    /// Logical database, sent as `SELECT <db>` when `Some`.
    pub db: Option<i64>,
}

/// One unit of work handed to the driver: an encoded payload and the number of
/// replies it expects, plus a channel to deliver them.
struct Request {
    payload: Vec<u8>,
    reply_count: usize,
    responder: oneshot::Sender<RedisResult<Vec<Value>>>,
}

/// A tracked in-flight request awaiting `reply_count` replies from the socket.
struct Pending {
    reply_count: usize,
    replies: Vec<Value>,
    responder: oneshot::Sender<RedisResult<Vec<Value>>>,
}

/// A handle to a multiplexed mesh connection. Clone freely; all clones share
/// one socket and one driver task.
///
/// The request channel is bounded to `max_inflight`: once that many requests
/// are waiting on the driver, new requests fail fast with
/// [`ErrorKind::Overloaded`] instead of queueing without bound.
#[derive(Clone)]
pub struct MultiplexedConnection {
    tx: mpsc::Sender<Request>,
    alive: Arc<AtomicBool>,
    /// The remote address this connection is bound to (TCP only). Used by
    /// the pool's per-IP balancing and DNS-change eviction.
    addr: Option<std::net::SocketAddr>,
    /// Requests handed to the driver but not yet fully answered. The pool
    /// reads this to grow the pool under load.
    inflight: Arc<AtomicUsize>,
    /// Wall-clock millis of the last reply received on this connection.
    /// Distinguishes "one request is slow" (replies still flowing) from
    /// "the connection is dead" (silent), so a single slow request doesn't
    /// get the whole connection poisoned.
    last_reply_ms: Arc<AtomicU64>,
    /// Set when this connection's death has been reported to the pool's
    /// breaker. A stalled connection fails its whole in-flight batch at the
    /// same timeout deadline; only the first failure should tick the
    /// consecutive-failure counter, or one stall looks like N failures.
    fail_noted: Arc<AtomicBool>,
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl MultiplexedConnection {
    /// Open a connection to the given mesh endpoint and spawn its driver.
    ///
    /// `max_inflight` bounds the request channel; see the struct docs.
    pub async fn connect(endpoint: &Endpoint, max_inflight: usize) -> RedisResult<Self> {
        Self::connect_with_handshake(endpoint, max_inflight, None).await
    }

    /// Open a connection and, when `handshake` is given, authenticate and/or
    /// select the logical database before the connection is handed out.
    pub async fn connect_with_handshake(
        endpoint: &Endpoint,
        max_inflight: usize,
        handshake: Option<&Handshake>,
    ) -> RedisResult<Self> {
        let (tx, rx) = mpsc::channel::<Request>(max_inflight.max(1));
        let alive = Arc::new(AtomicBool::new(true));
        let last_reply_ms = Arc::new(AtomicU64::new(0));
        let max_inflight = max_inflight.max(1);
        let stream =
            tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await?;
        stream.set_nodelay(true).ok();
        let addr = stream.peer_addr().ok();
        tokio::spawn(drive(
            stream,
            rx,
            alive.clone(),
            last_reply_ms.clone(),
            max_inflight,
        ));
        let conn = MultiplexedConnection {
            tx,
            alive,
            addr,
            inflight: Arc::new(AtomicUsize::new(0)),
            last_reply_ms,
            fail_noted: Arc::new(AtomicBool::new(false)),
        };
        if let Some(handshake) = handshake {
            conn.run_handshake(handshake).await?;
        }
        Ok(conn)
    }

    /// The remote address (TCP connections only).
    pub fn addr(&self) -> Option<std::net::SocketAddr> {
        self.addr
    }

    /// Requests currently queued or awaiting replies on this connection.
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Acquire)
    }

    /// Identity check (all clones of one connection share `alive`).
    pub fn same(&self, other: &MultiplexedConnection) -> bool {
        Arc::ptr_eq(&self.alive, &other.alive)
    }

    /// Marks this connection's failure as reported to the breaker; returns
    /// true for the first caller only.
    pub fn note_fail_once(&self) -> bool {
        !self.fail_noted.swap(true, Ordering::AcqRel)
    }

    /// Whether a reply arrived on this connection within `window`. Used on
    /// request timeout: a responsive connection means the timed-out request
    /// was an isolated slow one and the connection should be kept.
    pub fn responsive_within(&self, window: std::time::Duration) -> bool {
        let last = self.last_reply_ms.load(Ordering::Acquire);
        last > 0 && now_millis().saturating_sub(last) <= window.as_millis() as u64
    }

    /// `AUTH`/`SELECT` on a fresh connection; any failure rejects the
    /// connection (the caller drops it and the driver task exits).
    async fn run_handshake(&self, handshake: &Handshake) -> RedisResult<()> {
        use crate::connection::ConnectionLike;
        if let Some(password) = &handshake.auth {
            let mut auth = crate::cmd::cmd("AUTH");
            auth.arg(password.as_str());
            expect_ok(self.req_command(&auth).await?, "AUTH")?;
        }
        if let Some(db) = handshake.db {
            let mut select = crate::cmd::cmd("SELECT");
            select.arg(db);
            expect_ok(self.req_command(&select).await?, "SELECT")?;
        }
        Ok(())
    }

    /// Whether the driver task is still running (socket healthy).
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Mark the connection dead so the pool stops handing it out and evicts
    /// it. Used when a request times out: the socket may be half-hung (mesh
    /// alive but not answering), and the only safe recovery is a fresh
    /// connection. Once every handle is dropped the driver task exits and
    /// fails any remaining waiters.
    pub fn poison(&self) {
        self.alive.store(false, Ordering::Release);
    }

    /// Enqueue a raw payload expecting `reply_count` replies.
    async fn send(&self, payload: Vec<u8>, reply_count: usize) -> RedisResult<Vec<Value>> {
        if !self.is_alive() {
            return Err(dead_connection_error());
        }
        let (responder, rx) = oneshot::channel();
        let request = Request {
            payload,
            reply_count,
            responder,
        };
        // Count before handing to the driver so the counter never transiently
        // dips below the true in-flight number (the driver may answer
        // immediately after try_send).
        self.inflight.fetch_add(1, Ordering::AcqRel);
        if let Err(err) = self.tx.try_send(request) {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return Err(match err {
                mpsc::error::TrySendError::Full(_) => overloaded_error(),
                mpsc::error::TrySendError::Closed(_) => dead_connection_error(),
            });
        }
        let result = rx.await.map_err(|_| dead_connection_error());
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        result?
    }

    /// Send one command and await its reply. Inherent (unboxed) sibling of
    /// the [`ConnectionLike`] method — hot paths should prefer this to skip
    /// one future-boxing allocation per call.
    pub async fn request(&self, command: &crate::cmd::Cmd) -> RedisResult<Value> {
        let mut replies = self.send(command.encoded(), 1).await?;
        Ok(replies.pop().unwrap_or(Value::Nil))
    }

    /// Unboxed sibling of [`ConnectionLike::req_pipeline`].
    pub async fn request_pipeline(
        &self,
        pipeline: &Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let total = pipeline.command_count();
        let replies = self.send(pipeline.encoded(), total).await?;
        Ok(replies.into_iter().skip(offset).take(count).collect())
    }
}

impl ConnectionLike for MultiplexedConnection {
    fn req_command<'a>(&'a self, command: &'a crate::cmd::Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move { self.request(command).await })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move { self.request_pipeline(pipeline, offset, count).await })
    }
}

fn dead_connection_error() -> RedisError {
    RedisError::from_kind(ErrorKind::Io, "connection closed")
}

fn overloaded_error() -> RedisError {
    RedisError::from_kind(
        ErrorKind::Overloaded,
        "connection in-flight budget exhausted",
    )
}

fn expect_ok(value: Value, what: &'static str) -> RedisResult<()> {
    match value {
        Value::Okay => Ok(()),
        Value::ServerError(err) => Err(err.into()),
        other => Err(RedisError::with_detail(
            ErrorKind::ResponseError,
            "unexpected handshake reply",
            format!("{what}: {other:?}"),
        )),
    }
}

/// The driver loop: pumps requests to the socket and replies back to waiters.
///
/// `max_inflight` caps the number of requests that have been written to the
/// socket but not yet answered (`pending`); beyond it requests fail fast with
/// [`ErrorKind::Overloaded`] so a mesh that reads but never replies cannot
/// grow memory without bound.
async fn drive<S>(
    stream: S,
    mut rx: mpsc::Receiver<Request>,
    alive: Arc<AtomicBool>,
    last_reply_ms: Arc<AtomicU64>,
    max_inflight: usize,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut pending: VecDeque<Pending> = VecDeque::new();
    // Start small and grow on demand (a process can hold thousands of mostly
    // idle connections); shrunk again after oversized replies.
    let mut read_buf = BytesMut::with_capacity(4 * 1024);
    // Alternate which select branch is polled first: under continuous load a
    // fixed write bias can starve reads (replies pile up), a fixed read bias
    // hurts pipelining.
    let mut prefer_write = true;

    loop {
        if prefer_write {
            tokio::select! {
                biased;
                maybe_req = rx.recv() => {
                    if !on_request(&mut writer, &mut rx, maybe_req, &mut pending, max_inflight).await {
                        break;
                    }
                }
                read = reader.read_buf(&mut read_buf) => {
                    if !on_read(&mut reader, &mut read_buf, &mut pending, read, &last_reply_ms).await {
                        break;
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                read = reader.read_buf(&mut read_buf) => {
                    if !on_read(&mut reader, &mut read_buf, &mut pending, read, &last_reply_ms).await {
                        break;
                    }
                }
                maybe_req = rx.recv() => {
                    if !on_request(&mut writer, &mut rx, maybe_req, &mut pending, max_inflight).await {
                        break;
                    }
                }
            }
        }
        prefer_write = !prefer_write;
    }

    alive.store(false, Ordering::Release);
    for entry in pending {
        let _ = entry.responder.send(Err(dead_connection_error()));
    }
    while let Ok(req) = rx.try_recv() {
        let _ = req.responder.send(Err(dead_connection_error()));
    }
}

/// Handle one dequeued request (or channel close). Returns `false` if the
/// driver should exit.
async fn on_request<W>(
    writer: &mut W,
    rx: &mut mpsc::Receiver<Request>,
    maybe_req: Option<Request>,
    pending: &mut VecDeque<Pending>,
    max_inflight: usize,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    match maybe_req {
        Some(req) => {
            if pending.len() >= max_inflight {
                let _ = req.responder.send(Err(overloaded_error()));
                true
            } else {
                write_request(writer, rx, req, pending, max_inflight).await
            }
        }
        None => false, // all senders dropped.
    }
}

/// Handle a socket read: drain whatever else is already buffered by the OS
/// before parsing (so a large reply arriving in fragments is parsed once per
/// starvation point instead of once per TCP segment), then dispatch replies.
/// Returns `false` if the driver should exit.
async fn on_read<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    pending: &mut VecDeque<Pending>,
    read: std::io::Result<usize>,
    last_reply_ms: &AtomicU64,
) -> bool
where
    R: AsyncRead + Unpin,
{
    match read {
        Ok(0) => false, // EOF.
        Ok(_) => {
            drain_available(reader, read_buf).await;
            let (cont, delivered) = dispatch_replies(read_buf, pending);
            if delivered > 0 {
                last_reply_ms.store(now_millis(), Ordering::Release);
            }
            cont
        }
        Err(_) => false,
    }
}

/// Opportunistically pull any immediately-available bytes from the socket
/// without awaiting; stops when the socket would block (or after a bounded
/// number of chunks, to avoid starving the write path).
async fn drain_available<R>(reader: &mut R, buf: &mut BytesMut)
where
    R: AsyncRead + Unpin,
{
    use std::task::Poll;
    let mut chunk = [0u8; 16 * 1024];
    for _ in 0..8 {
        let filled = std::future::poll_fn(|cx| {
            let mut rb = tokio::io::ReadBuf::new(&mut chunk);
            match std::pin::Pin::new(&mut *reader).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => Poll::Ready(rb.filled().len()),
                // Pending (would-block) or error: stop draining.
                _ => Poll::Ready(0),
            }
        })
        .await;
        if filled == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..filled]);
    }
}

/// Write one request (coalescing any others already queued) and register the
/// pending waiters. Returns `false` if the socket write failed.
async fn write_request<W>(
    writer: &mut W,
    rx: &mut mpsc::Receiver<Request>,
    first: Request,
    pending: &mut VecDeque<Pending>,
    max_inflight: usize,
) -> bool
where
    W: AsyncWrite + Unpin,
{
    let mut payloads = vec![first.payload];
    let mut registered = vec![(first.reply_count, first.responder)];

    // Coalesce everything currently queued into a single write syscall,
    // staying within the in-flight budget.
    while pending.len() + registered.len() < max_inflight {
        let Ok(req) = rx.try_recv() else {
            break;
        };
        payloads.push(req.payload);
        registered.push((req.reply_count, req.responder));
    }

    let written = if writer.is_write_vectored() && payloads.len() > 1 {
        // Vectored write: one syscall, no coalescing copy.
        let mut slices: Vec<IoSlice<'_>> = payloads.iter().map(|p| IoSlice::new(p)).collect();
        write_all_vectored(writer, &mut slices).await
    } else {
        let mut iter = payloads.into_iter();
        let mut batch = iter.next().unwrap();
        for payload in iter {
            batch.extend_from_slice(&payload);
        }
        writer.write_all(&batch).await
    };
    if written.is_err() {
        for (_, responder) in registered {
            let _ = responder.send(Err(dead_connection_error()));
        }
        return false;
    }

    for (reply_count, responder) in registered {
        pending.push_back(Pending {
            reply_count,
            replies: Vec::with_capacity(reply_count),
            responder,
        });
    }
    true
}

/// `write_vectored` loop handling partial writes (tokio has no
/// `write_all_vectored`).
async fn write_all_vectored<W>(writer: &mut W, mut bufs: &mut [IoSlice<'_>]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while !bufs.is_empty() {
        let n = writer.write_vectored(bufs).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, n);
    }
    Ok(())
}

/// Parse as many complete replies as `read_buf` holds and route them to the
/// front pending entries. Returns `false` on an unrecoverable protocol error.
/// Returns `(keep_driving, delivered)` — `delivered` counts complete replies
/// handed to waiters in this batch.
fn dispatch_replies(read_buf: &mut BytesMut, pending: &mut VecDeque<Pending>) -> (bool, usize) {
    let mut delivered = 0usize;
    if read_buf.is_empty() {
        return (true, 0);
    }
    // Zero-copy: freeze the buffered bytes into a shared snapshot; reply bulk
    // strings are slices of it. Only the unparsed partial tail is copied
    // back into the read buffer.
    let snapshot = read_buf.split().freeze();
    let mut pos = 0usize;
    loop {
        match parse_reply(&snapshot.slice(pos..)) {
            Ok(ParseResult::Complete { value, consumed }) => {
                pos += consumed;
                deliver(value, pending);
                delivered += 1;
            }
            Ok(ParseResult::Incomplete) => {
                read_buf.extend_from_slice(&snapshot[pos..]);
                // Release memory after an oversized reply so a mostly-idle
                // connection doesn't pin a huge buffer (matters at ~1000
                // namespaces × pool connections per process).
                const SHRINK_THRESHOLD: usize = 64 * 1024;
                if read_buf.capacity() > SHRINK_THRESHOLD && read_buf.len() < SHRINK_THRESHOLD / 2 {
                    // No shrink API on BytesMut: swap in a fresh small buffer.
                    let rest = read_buf.split();
                    let mut fresh = BytesMut::with_capacity(4 * 1024);
                    fresh.extend_from_slice(&rest);
                    *read_buf = fresh;
                }
                return (true, delivered);
            }
            Err(err) => {
                if let Some(entry) = pending.pop_front() {
                    let _ = entry.responder.send(Err(err));
                }
                return (false, delivered);
            }
        }
    }
}

/// Append one parsed reply to the front pending entry, completing it when it
/// has collected all expected replies.
fn deliver(value: Value, pending: &mut VecDeque<Pending>) {
    let Some(front) = pending.front_mut() else {
        return; // stray reply with no waiter; drop it.
    };
    front.replies.push(value);
    if front.replies.len() >= front.reply_count {
        let entry = pending.pop_front().unwrap();
        let _ = entry.responder.send(Ok(entry.replies));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::cmd;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    /// Start a fake mesh that accepts connections and drains reads but never
    /// replies — the "hung mesh" case the in-flight budget guards against.
    async fn silent_mesh() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
                        .await
                        .unwrap_or(0)
                        > 0
                    {}
                });
            }
        });
        (addr, task)
    }

    fn endpoint(addr: SocketAddr) -> Endpoint {
        Endpoint {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }

    #[tokio::test]
    async fn fails_fast_when_inflight_budget_exhausted() {
        let (addr, _mesh) = silent_mesh().await;
        let conn = MultiplexedConnection::connect(&endpoint(addr), 2)
            .await
            .unwrap();

        // Two requests occupy the whole budget, waiting on replies that never
        // come.
        let c1 = conn.clone();
        let c2 = conn.clone();
        let pending1 = tokio::spawn(async move { c1.req_command(&cmd("GET")).await });
        let pending2 = tokio::spawn(async move { c2.req_command(&cmd("GET")).await });
        // Let the driver write both and park them in `pending`.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let err = conn.req_command(&cmd("GET")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Overloaded);
        // Backpressure errors must not be treated as connection failures.
        assert!(err.connection_still_valid());

        pending1.abort();
        pending2.abort();
    }

    #[tokio::test]
    async fn poisoned_connection_refuses_new_requests() {
        let (addr, _mesh) = silent_mesh().await;
        let conn = MultiplexedConnection::connect(&endpoint(addr), 16)
            .await
            .unwrap();
        assert!(conn.is_alive());
        conn.poison();
        assert!(!conn.is_alive());
        let err = conn.req_command(&cmd("GET")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
    }

    /// A server that answers everything except requests containing "SLOW",
    /// which it delays by 300ms.
    async fn slow_marker_mesh() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = tokio::io::AsyncReadExt::read(&mut socket, &mut buf)
                            .await
                            .unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        let slow = buf[..n].windows(4).any(|w| w == b"SLOW");
                        if slow {
                            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        }
                        if tokio::io::AsyncWriteExt::write_all(&mut socket, b"+OK\r\n")
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        (addr, task)
    }

    #[tokio::test]
    async fn responsive_within_distinguishes_slow_request_from_dead_connection() {
        use std::time::Duration;

        // Replies still flowing around the slow request: responsive.
        let (addr, _mesh) = slow_marker_mesh().await;
        let conn = MultiplexedConnection::connect(&endpoint(addr), 16)
            .await
            .unwrap();
        conn.req_command(&cmd("GET")).await.unwrap();
        let slow_cmd = cmd("SLOW");
        let slow = conn.req_command(&slow_cmd);
        let timed_out = tokio::time::timeout(Duration::from_millis(50), slow).await;
        assert!(timed_out.is_err(), "slow request should hit the deadline");
        // The previous reply arrived within the window: the timeout would be
        // treated as isolated and the connection kept.
        assert!(conn.responsive_within(Duration::from_millis(150)));
        assert!(conn.is_alive());

        // A fully silent connection: not responsive, would be poisoned.
        let (addr, _mesh) = silent_mesh().await;
        let conn = MultiplexedConnection::connect(&endpoint(addr), 16)
            .await
            .unwrap();
        assert!(!conn.responsive_within(Duration::from_millis(150)));
    }
}
