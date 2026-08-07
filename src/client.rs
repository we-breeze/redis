//! The pooled, retrying [`Client`] — the primary entry point.
//!
//! Wraps a [`Pool`] of multiplexed connections to the mesh and adds bounded
//! retries on transient failures, connection invalidation vs. retention
//! depending on the error, slow-command logging, and per-command stats. Because
//! it implements [`ConnectionLike`], the whole
//! [`Commands`](crate::commands::Commands) surface is available on it directly,
//! and the mesh routing helpers (see [`crate::routing`]) layer on top.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cmd::Cmd;
use crate::config::MeshConfig;
use crate::connection::{ConnectionLike, RedisFuture};
use crate::error::{RedisError, RedisResult};
use crate::pipeline::Pipeline;
use crate::pool::Pool;
use crate::stats::{Stats, StatsSnapshot};
use crate::types::Value;

/// The mesh's "no backend available" server error (see the Java
/// `JedisMSServerProxy`); logged without a stack trace, like the original.
const REDIS_NO_AVAILABLE: &str = "ERR redis no available";

struct Inner {
    pool: Arc<Pool>,
    stats: Stats,
    namespace: String,
    max_try_time: u32,
    op_timeout: Duration,
    slow_threshold: Duration,
}

/// A high-availability, pooled mesh Redis client. Cheap to clone (shares one
/// pool).
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Connect to the mesh for `namespace` with default settings.
    pub async fn connect(namespace: impl Into<String>) -> RedisResult<Self> {
        Self::from_config(MeshConfig::new(namespace)).await
    }

    /// Connect using an explicit [`MeshConfig`].
    pub async fn from_config(config: MeshConfig) -> RedisResult<Self> {
        let max_try_time = config.max_try_time.max(1);
        let op_timeout = config.op_timeout;
        let slow_threshold = config.slow_time_threshold;
        let namespace = config.namespace.clone();
        let pool = Pool::connect(config).await?;
        Ok(Client {
            inner: Arc::new(Inner {
                pool,
                stats: Stats::new(),
                namespace,
                max_try_time,
                op_timeout,
                slow_threshold,
            }),
        })
    }

    /// A snapshot of this client's command statistics.
    pub fn stats(&self) -> StatsSnapshot {
        self.inner.stats.snapshot()
    }

    /// Whether the underlying pool is currently serving.
    pub fn is_available(&self) -> bool {
        self.inner.pool.can_serve()
    }

    /// Operator: drain and stop serving (maintenance).
    pub fn pause(&self) {
        self.inner.pool.pause();
    }

    /// Operator: resume serving.
    pub fn restart(&self) {
        self.inner.pool.restart();
    }

    /// Log a mesh request exception, mirroring the Java `JedisMSServerProxy`
    /// `callable` path: a `ERR redis no available` data error is logged as a
    /// plain message (no stack trace), every other error is logged together
    /// with the error itself.
    fn log_exception(&self, method: &str, key: &str, detail: &str, no_available: bool) {
        let ns = &self.inner.namespace;
        if no_available {
            tracing::error!(
                target: "breeze_redis::mesh",
                "redis mesh exception namespace:{ns} ,method:{method} ,key:{key} ,e:{detail}"
            );
        } else {
            tracing::error!(
                target: "breeze_redis::mesh",
                error = %detail,
                "redis mesh exception namespace:{ns} ,method:{method} ,key:{key} ,e:"
            );
        }
    }

    /// Execute one command with bounded retries and stats/slow-logging.
    ///
    /// Each attempt is bounded by `op_timeout`. A timeout poisons the
    /// connection (the mesh may be alive but not answering) so the pool
    /// replaces it, and surfaces as a retriable [`ErrorKind::Timeout`].
    async fn execute(&self, command: &Cmd) -> RedisResult<Value> {
        let inner = &self.inner;
        let name = command.name();
        let key = command.key();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let start = Instant::now();
            match inner.pool.get().await {
                Ok(conn) => match self.with_timeout(conn.req_command(command)).await {
                    Ok(value) => {
                        // A reply arrived — even an inline server error means the
                        // socket is healthy, so the pool is credited.
                        inner.pool.note_success();
                        let is_err = matches!(value, Value::ServerError(_));
                        if let Value::ServerError(server_err) = &value {
                            let no_available = server_err.message == REDIS_NO_AVAILABLE;
                            self.log_exception(&name, &key, &server_err.message, no_available);
                        }
                        inner
                            .stats
                            .record(&name, start.elapsed(), is_err, inner.slow_threshold);
                        return Ok(value);
                    }
                    Err(err) => {
                        if err.kind() == crate::ErrorKind::Timeout {
                            // The reply never came; the socket is suspect even
                            // though the driver task may still be running.
                            conn.poison();
                        }
                        inner
                            .stats
                            .record(&name, start.elapsed(), true, inner.slow_threshold);
                        self.log_exception(&name, &key, &err.to_string(), false);
                        // A response-level error keeps the connection; an I/O
                        // error invalidates it (Java "special data exception").
                        if err.connection_still_valid() {
                            inner.pool.note_success();
                        } else {
                            inner.pool.note_failure();
                        }
                        if attempt >= inner.max_try_time || !err.is_retriable() {
                            return Err(err);
                        }
                    }
                },
                Err(err) => {
                    inner.stats.record_unavailable();
                    self.log_exception(&name, &key, &err.to_string(), false);
                    inner.pool.note_failure();
                    if attempt >= inner.max_try_time {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Execute a pipeline once (pipelines are not blindly retried, since a
    /// partially applied batch cannot be safely replayed).
    async fn execute_pipeline(
        &self,
        pipeline: &Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let inner = &self.inner;
        let start = Instant::now();
        let conn = match inner.pool.get().await {
            Ok(conn) => conn,
            Err(err) => {
                inner.stats.record_unavailable();
                self.log_exception("pipeline", "", &err.to_string(), false);
                inner.pool.note_failure();
                return Err(err);
            }
        };
        match self.with_timeout(conn.req_pipeline(pipeline, offset, count)).await {
            Ok(values) => {
                inner.pool.note_success();
                inner
                    .stats
                    .record("pipeline", start.elapsed(), false, inner.slow_threshold);
                Ok(values)
            }
            Err(err) => {
                if err.kind() == crate::ErrorKind::Timeout {
                    conn.poison();
                }
                inner
                    .stats
                    .record("pipeline", start.elapsed(), true, inner.slow_threshold);
                self.log_exception("pipeline", "", &err.to_string(), false);
                if err.connection_still_valid() {
                    inner.pool.note_success();
                } else {
                    inner.pool.note_failure();
                }
                Err(err)
            }
        }
    }

    /// Bound an in-flight request by `op_timeout`, converting an elapsed
    /// deadline into a [`ErrorKind::Timeout`] error. The timed-out waiter is
    /// dropped; if a late reply eventually arrives the driver simply discards
    /// it.
    async fn with_timeout<T>(
        &self,
        fut: impl Future<Output = RedisResult<T>>,
    ) -> RedisResult<T> {
        match tokio::time::timeout(self.inner.op_timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(RedisError::from_kind(
                crate::ErrorKind::Timeout,
                "command timed out waiting for the mesh reply",
            )),
        }
    }
}

impl ConnectionLike for Client {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move { self.execute(command).await })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move { self.execute_pipeline(pipeline, offset, count).await })
    }
}
