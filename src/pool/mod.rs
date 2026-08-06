//! The connection [`Pool`] for a single mesh endpoint.
//!
//! There is exactly one local mesh server per namespace, so the pool holds a
//! fixed set of [`MultiplexedConnection`]s to that one endpoint and dispatches
//! requests across them round-robin. Each connection pipelines many concurrent
//! commands, so a small pool sustains high throughput. A [`health`] circuit
//! breaker plus a background maintenance task provide availability: dead
//! connections are replaced, and if the mesh becomes unreachable the breaker
//! trips and a probe restores service once it returns.

pub mod health;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use crate::cmd::cmd;
use crate::config::MeshConfig;
use crate::connection::MultiplexedConnection;
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::mesh::{self, Endpoint};

use health::HealthState;

/// A pool of multiplexed connections to one mesh endpoint.
pub struct Pool {
    endpoint: Endpoint,
    config: MeshConfig,
    health: HealthState,
    conns: RwLock<Vec<MultiplexedConnection>>,
    dispatch: AtomicUsize,
}

impl Pool {
    /// Discover the mesh endpoint, warm up the pool, and start maintenance.
    pub async fn connect(config: MeshConfig) -> RedisResult<Arc<Self>> {
        let endpoint = mesh::discover(&config).await?;
        let min = config.pool_size.max(1) as u32;
        let max = (config.pool_size as u32 * 4).max(4);
        let pool = Arc::new(Pool {
            endpoint,
            config,
            health: HealthState::new(min, max),
            conns: RwLock::new(Vec::new()),
            dispatch: AtomicUsize::new(0),
        });
        pool.warm_up().await?;
        spawn_maintenance(Arc::downgrade(&pool));
        Ok(pool)
    }

    /// The resolved endpoint, for logging/labels.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Whether the breaker/kill-switch currently allows serving.
    pub fn can_serve(&self) -> bool {
        self.health.can_serve()
    }

    /// Get a usable connection, creating one if none are live.
    pub async fn get(&self) -> RedisResult<MultiplexedConnection> {
        if !self.health.can_serve() {
            return Err(unavailable());
        }
        if let Some(conn) = self.pick_live() {
            return Ok(conn);
        }
        let conn = self.create_conn().await?;
        self.conns.write().unwrap().push(conn.clone());
        Ok(conn)
    }

    /// Record a successful command.
    pub fn note_success(&self) {
        self.health.on_success();
    }

    /// Record a failed command; may trip the breaker per configured thresholds.
    pub fn note_failure(&self) {
        let outcome = self.health.on_failure();
        if outcome.tripped {
            tracing::warn!(
                target: "breeze_redis::pool",
                endpoint = %self.endpoint,
                "circuit breaker tripped; mesh marked unhealthy"
            );
            self.evict_dead();
        }
    }

    /// Operator: drain and stop serving.
    pub fn pause(&self) {
        self.health.pause();
        self.conns.write().unwrap().clear();
    }

    /// Operator: resume serving.
    pub fn restart(&self) {
        self.health.restart();
    }

    fn pick_live(&self) -> Option<MultiplexedConnection> {
        let conns = self.conns.read().unwrap();
        let n = conns.len();
        if n == 0 {
            return None;
        }
        let start = self.dispatch.fetch_add(1, Ordering::Relaxed);
        for offset in 0..n {
            let conn = &conns[(start + offset) % n];
            if conn.is_alive() {
                return Some(conn.clone());
            }
        }
        None
    }

    async fn create_conn(&self) -> RedisResult<MultiplexedConnection> {
        tokio::time::timeout(
            self.config.op_timeout,
            MultiplexedConnection::connect(&self.endpoint),
        )
        .await
        .map_err(|_| RedisError::from_kind(ErrorKind::Timeout, "connection timed out"))?
    }

    fn evict_dead(&self) {
        self.conns
            .write()
            .unwrap()
            .retain(MultiplexedConnection::is_alive);
    }

    fn live_count(&self) -> usize {
        self.conns
            .read()
            .unwrap()
            .iter()
            .filter(|c| c.is_alive())
            .count()
    }

    async fn warm_up(&self) -> RedisResult<()> {
        for _ in 0..self.config.pool_size {
            match self.create_conn().await {
                Ok(conn) => self.conns.write().unwrap().push(conn),
                Err(err) => tracing::warn!(
                    target: "breeze_redis::pool",
                    endpoint = %self.endpoint,
                    error = %err,
                    "warm-up connection failed"
                ),
            }
        }
        if self.live_count() == 0 {
            return Err(unavailable());
        }
        Ok(())
    }

    /// The recovery probe: open one fresh connection and PING it.
    async fn probe(&self) {
        if let Ok(conn) = self.create_conn().await
            && cmd("PING").exec_async(&conn).await.is_ok()
        {
            self.conns.write().unwrap().push(conn);
            self.health.recover();
            tracing::info!(
                target: "breeze_redis::pool",
                endpoint = %self.endpoint,
                "recovery probe succeeded; mesh healthy again"
            );
        }
    }
}

fn unavailable() -> RedisError {
    RedisError::from_kind(
        ErrorKind::NoConnection,
        "no healthy mesh connection available",
    )
}

/// Background maintenance: drop dead connections, probe recovery when
/// unhealthy, and keep the pool at its configured size. Stops when the pool is
/// dropped (weak upgrade fails).
fn spawn_maintenance(weak: Weak<Pool>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let Some(pool) = weak.upgrade() else {
                return;
            };
            pool.evict_dead();
            if pool.health.is_kept() {
                if !pool.health.is_healthy() {
                    pool.probe().await;
                } else {
                    while pool.live_count() < pool.config.pool_size {
                        match pool.create_conn().await {
                            Ok(conn) => pool.conns.write().unwrap().push(conn),
                            Err(_) => break,
                        }
                    }
                }
            }
        }
    });
}
