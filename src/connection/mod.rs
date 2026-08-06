//! Async connections and the [`ConnectionLike`] abstraction that command
//! execution is built on.
//!
//! [`multiplexed`] provides [`MultiplexedConnection`], a cheap-`Clone` handle
//! that pipelines many concurrent requests over a single socket (TCP or unix)
//! to the mesh.

pub mod multiplexed;

use std::future::Future;
use std::pin::Pin;

use crate::cmd::Cmd;
use crate::error::RedisResult;
use crate::pipeline::Pipeline;
use crate::types::Value;

pub use multiplexed::MultiplexedConnection;

/// A boxed, `Send` future returned by connection operations. Boxing keeps the
/// [`ConnectionLike`] trait object-safe and its futures uniformly `Send`.
pub type RedisFuture<'a, T> = Pin<Box<dyn Future<Output = RedisResult<T>> + Send + 'a>>;

/// Anything that can execute a command or a pipeline.
///
/// Implemented by the raw [`MultiplexedConnection`] as well as higher-level
/// wrappers ([`crate::Client`]) that add pooling, retries, and stats. Because
/// implementors use interior mutability (an mpsc sender), methods take `&self`
/// and callers can share a connection across tasks by cloning.
pub trait ConnectionLike: Send + Sync {
    /// Send one command and await its single reply.
    fn req_command<'a>(&'a self, cmd: &'a Cmd) -> RedisFuture<'a, Value>;

    /// Send a pipeline and return the `count` replies starting at `offset`
    /// (used to skip `MULTI`/`QUEUED`/`EXEC` framing for atomic pipelines).
    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>>;
}

impl<T: ConnectionLike + ?Sized> ConnectionLike for &T {
    fn req_command<'a>(&'a self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        (**self).req_command(cmd)
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        (**self).req_pipeline(pipeline, offset, count)
    }
}
