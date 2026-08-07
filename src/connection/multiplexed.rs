//! A multiplexed async connection to the mesh.
//!
//! [`MultiplexedConnection`] is a cheap-`Clone` handle over an mpsc channel to a
//! single background driver task that owns the socket (TCP or unix). Many
//! callers issue commands concurrently; the driver writes them to the socket
//! and matches replies back to waiters in FIFO order. This gives automatic
//! pipelining and lets one socket serve high concurrency without a lock or a
//! connection-per-request pool.
//!
//! There is no Redis handshake: the mesh conveys identity/routing out-of-band
//! via the sock file, so the client opens the socket and immediately speaks
//! RESP.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::connection::{ConnectionLike, RedisFuture};
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::mesh::Endpoint;
use crate::pipeline::Pipeline;
use crate::resp::parser::{ParseResult, parse_reply};
use crate::types::Value;

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
}

impl MultiplexedConnection {
    /// Open a connection to the given mesh endpoint and spawn its driver.
    ///
    /// `max_inflight` bounds the request channel; see the struct docs.
    pub async fn connect(endpoint: &Endpoint, max_inflight: usize) -> RedisResult<Self> {
        let (tx, rx) = mpsc::channel::<Request>(max_inflight.max(1));
        let alive = Arc::new(AtomicBool::new(true));
        let max_inflight = max_inflight.max(1);
        match endpoint {
            Endpoint::Tcp(addr) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                stream.set_nodelay(true).ok();
                tokio::spawn(drive(stream, rx, alive.clone(), max_inflight));
            }
            Endpoint::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(path).await?;
                tokio::spawn(drive(stream, rx, alive.clone(), max_inflight));
            }
        }
        Ok(MultiplexedConnection { tx, alive })
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
        self.tx.try_send(request).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => overloaded_error(),
            mpsc::error::TrySendError::Closed(_) => dead_connection_error(),
        })?;
        rx.await.map_err(|_| dead_connection_error())?
    }
}

impl ConnectionLike for MultiplexedConnection {
    fn req_command<'a>(&'a self, command: &'a crate::cmd::Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let mut replies = self.send(command.encoded(), 1).await?;
            Ok(replies.pop().unwrap_or(Value::Nil))
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let total = pipeline.command_count();
            let replies = self.send(pipeline.encoded(), total).await?;
            Ok(replies.into_iter().skip(offset).take(count).collect())
        })
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

/// The driver loop: pumps requests to the socket and replies back to waiters.
///
/// `max_inflight` caps the number of requests that have been written to the
/// socket but not yet answered (`pending`); beyond it requests fail fast with
/// [`ErrorKind::Overloaded`] so a mesh that reads but never replies cannot
/// grow memory without bound.
async fn drive<S>(stream: S, mut rx: mpsc::Receiver<Request>, alive: Arc<AtomicBool>, max_inflight: usize)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut read_buf = BytesMut::with_capacity(16 * 1024);

    loop {
        tokio::select! {
            // Biased so we always drain queued outgoing requests before reading,
            // maximizing pipelining under load.
            biased;

            maybe_req = rx.recv() => {
                match maybe_req {
                    Some(req) => {
                        if pending.len() >= max_inflight {
                            let _ = req.responder.send(Err(overloaded_error()));
                        } else if !write_request(&mut writer, &mut rx, req, &mut pending, max_inflight).await {
                            break;
                        }
                    }
                    None => break, // all senders dropped.
                }
            }

            read = reader.read_buf(&mut read_buf) => {
                match read {
                    Ok(0) => break, // EOF.
                    Ok(_) => {
                        if !dispatch_replies(&mut read_buf, &mut pending) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    alive.store(false, Ordering::Release);
    for entry in pending {
        let _ = entry.responder.send(Err(dead_connection_error()));
    }
    while let Ok(req) = rx.try_recv() {
        let _ = req.responder.send(Err(dead_connection_error()));
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
    let mut batch = first.payload;
    let mut registered = vec![(first.reply_count, first.responder)];

    // Coalesce everything currently queued into a single write syscall,
    // staying within the in-flight budget.
    while pending.len() + registered.len() < max_inflight {
        let Ok(req) = rx.try_recv() else {
            break;
        };
        batch.extend_from_slice(&req.payload);
        registered.push((req.reply_count, req.responder));
    }

    if writer.write_all(&batch).await.is_err() {
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

/// Parse as many complete replies as `read_buf` holds and route them to the
/// front pending entries. Returns `false` on an unrecoverable protocol error.
fn dispatch_replies(read_buf: &mut BytesMut, pending: &mut VecDeque<Pending>) -> bool {
    loop {
        match parse_reply(read_buf) {
            Ok(ParseResult::Complete { value, consumed }) => {
                let _ = read_buf.split_to(consumed);
                deliver(value, pending);
            }
            Ok(ParseResult::Incomplete) => return true,
            Err(err) => {
                if let Some(entry) = pending.pop_front() {
                    let _ = entry.responder.send(Err(err));
                }
                return false;
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

    #[tokio::test]
    async fn fails_fast_when_inflight_budget_exhausted() {
        let (addr, _mesh) = silent_mesh().await;
        let conn = MultiplexedConnection::connect(&Endpoint::Tcp(addr.into()), 2)
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
        let conn = MultiplexedConnection::connect(&Endpoint::Tcp(addr.into()), 16)
            .await
            .unwrap();
        assert!(conn.is_alive());
        conn.poison();
        assert!(!conn.is_alive());
        let err = conn.req_command(&cmd("GET")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);
    }
}
