//! The connection [`Pool`] for a single mesh endpoint or a direct backend.
//!
//! There is exactly one local mesh server per namespace, so the pool holds a
//! fixed set of [`MultiplexedConnection`]s to that one endpoint and dispatches
//! requests across them round-robin. Each connection pipelines many concurrent
//! commands, so a small pool sustains high throughput. A [`health`] circuit
//! breaker plus a background maintenance task provide availability: dead
//! connections are replaced, and if the mesh becomes unreachable the breaker
//! trips and a probe restores service once it returns.
//!
//! For direct backends configured by hostname, the pool additionally plays
//! the clientBalancer role: the hostname's DNS answer set is kept as
//! `direct_ips`, new connections are created round-robin across the set
//! (like `EndpointFactory.getNextIp`), a periodic sweeper evicts connections
//! from over-represented IPs until per-IP counts differ by at most one
//! (like `EndpointManagerImpl.watchPool`), and a DNS answer change evicts
//! connections to offline IPs immediately while fast-tracking connections
//! onto newly appeared IPs (like `refreshEndpointPool`).

pub mod health;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;

use crate::cmd::cmd;
use crate::connection::{Handshake, MultiplexedConnection};
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::sidecar::config::MeshConfig;
use crate::sidecar::discovery::{self, Endpoint};

use health::HealthState;

/// A pool of multiplexed connections to one mesh endpoint, or to the DNS
/// answer set of one direct backend hostname.
pub struct Pool {
    /// The mesh endpoint (mesh path / static direct path). When
    /// `direct_ips` is non-empty this is only a label for logging; actual
    /// connection targets come from `direct_ips`.
    endpoint: RwLock<Endpoint>,
    /// Direct-backend mode: the current DNS answer set (shuffled). New
    /// connections round-robin over it via `ip_cursor`.
    direct_ips: RwLock<Vec<SocketAddr>>,
    /// Round-robin cursor into `direct_ips` for connection creation.
    ip_cursor: AtomicUsize,
    config: MeshConfig,
    health: HealthState,
    conns: RwLock<Vec<MultiplexedConnection>>,
    dispatch: AtomicUsize,
    /// AUTH/SELECT for direct backend connections; `None` on the mesh path.
    handshake: Option<Handshake>,
    /// Whether the maintenance probe may re-resolve the endpoint from the
    /// sock directory (mesh path only; direct endpoints are static).
    rediscover: bool,
    /// `host:port` authority for direct backends configured by hostname.
    /// Re-resolved on breaker trips (the clientBalancer "immediate re-watch
    /// on min failures" role) and every `DNS_REFRESH_INTERVAL` while
    /// healthy; `None` for mesh pools and IP literals.
    resolver: Option<String>,
    /// Last successful DNS re-resolution (healthy-cadence refresh).
    last_dns_refresh: Mutex<Instant>,
    /// Single-flight connection creation: getters queue here when no live
    /// connection is available, so a burst creates at most one connection at
    /// a time and `max_connections` stays a hard cap.
    connect_lock: tokio::sync::Mutex<()>,
    /// Wakes the maintenance task immediately when the health state changes
    /// (breaker trip), so recovery probing starts at once instead of waiting
    /// out the long healthy patrol sleep. `Arc` so the maintenance loop can
    /// await it without holding the pool alive.
    state_changed: Arc<tokio::sync::Notify>,
}

/// How often a healthy direct pool re-resolves its hostname, so DNS changes
/// (backend migration/failover) are picked up without waiting for failures.
const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

impl Pool {
    /// Discover the mesh endpoint, warm up the pool, and start maintenance.
    pub async fn connect(config: MeshConfig) -> RedisResult<Arc<Self>> {
        let endpoint = discovery::discover(&config).await?;
        Self::start(endpoint, Vec::new(), None, true, None, config).await
    }

    /// Connect to a direct backend (no mesh), performing the optional
    /// `AUTH`/`SELECT` handshake on every connection.
    ///
    /// `addrs` is the current address set (DNS answer, or a single IP
    /// literal); connections are created round-robin across it. `resolver`
    /// is the `host:port` authority when the backend was configured by
    /// hostname: the pool then re-resolves it on breaker trips and every
    /// `DNS_REFRESH_INTERVAL` while healthy, rebalancing and evicting
    /// offline IPs when the answer changes. Pass `None` for IP literals.
    pub async fn connect_direct(
        addrs: Vec<SocketAddr>,
        handshake: Option<Handshake>,
        resolver: Option<String>,
        config: MeshConfig,
    ) -> RedisResult<Arc<Self>> {
        let mut addrs = addrs;
        if addrs.is_empty() {
            return Err(RedisError::from_kind(
                ErrorKind::NoConnection,
                "backend address set is empty",
            ));
        }
        // Shuffle like clientBalancer, so sibling processes don't all pick
        // the same first IP.
        addrs.shuffle(&mut rand::thread_rng());
        let endpoint = Endpoint::Tcp(addrs[0]);
        Self::start(endpoint, addrs, handshake, false, resolver, config).await
    }

    async fn start(
        endpoint: Endpoint,
        direct_ips: Vec<SocketAddr>,
        handshake: Option<Handshake>,
        rediscover: bool,
        resolver: Option<String>,
        config: MeshConfig,
    ) -> RedisResult<Arc<Self>> {
        // Breaker thresholds follow the pool bounds: min_connections
        // consecutive failures trigger endpoint re-discovery, max_connections
        // trip the breaker.
        let min = config.min_connections.max(1) as u32;
        let max = config.max_connections.max(1) as u32;
        let pool = Arc::new(Pool {
            endpoint: RwLock::new(endpoint),
            direct_ips: RwLock::new(direct_ips),
            ip_cursor: AtomicUsize::new(0),
            config,
            health: HealthState::new(min, max),
            conns: RwLock::new(Vec::new()),
            dispatch: AtomicUsize::new(0),
            handshake,
            rediscover,
            resolver,
            last_dns_refresh: Mutex::new(Instant::now()),
            connect_lock: tokio::sync::Mutex::new(()),
            state_changed: Arc::new(tokio::sync::Notify::new()),
        });
        pool.warm_up().await?;
        spawn_maintenance(Arc::downgrade(&pool));
        Ok(pool)
    }

    /// The currently resolved endpoint, for logging/labels. May change over
    /// the pool's lifetime when the mesh re-publishes the resource.
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.read().unwrap().clone()
    }

    /// Whether the breaker/kill-switch currently allows serving.
    pub fn can_serve(&self) -> bool {
        self.health.can_serve()
    }

    /// Get a usable connection.
    ///
    /// The pool starts at `min_connections` connections and grows on demand: when
    /// every live connection is above half its in-flight budget a new one is
    /// opened, up to the hard cap of `max_connections`. Creation is single-flight
    /// (`connect_lock`), so a concurrent burst cannot overshoot the cap.
    pub async fn get(&self) -> RedisResult<MultiplexedConnection> {
        if !self.health.can_serve() {
            return Err(unavailable());
        }
        if let Some(conn) = self.pick_usable() {
            return Ok(conn);
        }
        // Slow path, single-flight: re-check after taking the lock — the
        // previous creator may have published a connection meanwhile.
        let _guard = self.connect_lock.lock().await;
        if let Some(conn) = self.pick_usable() {
            return Ok(conn);
        }
        self.evict_dead();
        if self.live_count() >= self.max_conns() {
            // At the cap with everything loaded: fall back to round-robin on
            // what we have; backpressure comes from each connection's
            // in-flight budget.
            if let Some(conn) = self.pick_live() {
                return Ok(conn);
            }
            return Err(unavailable());
        }
        let conn = self.create_conn().await?;
        self.conns.write().unwrap().push(conn.clone());
        Ok(conn)
    }

    /// Get a usable connection other than `avoid` — used by the retry path
    /// so a retried request doesn't land back on the connection that just
    /// timed out. Falls back to any live connection (or creates one) when no
    /// alternative exists.
    pub async fn get_avoiding(
        &self,
        avoid: &MultiplexedConnection,
    ) -> RedisResult<MultiplexedConnection> {
        if !self.health.can_serve() {
            return Err(unavailable());
        }
        {
            let conns = self.conns.read().unwrap();
            let n = conns.len();
            if n > 0 {
                let start = self.dispatch.fetch_add(1, Ordering::Relaxed);
                for offset in 0..n {
                    let conn = &conns[(start + offset) % n];
                    if conn.is_alive() && !conn.same(avoid) {
                        return Ok(conn.clone());
                    }
                }
            }
        }
        // No alternative right now: grow if allowed, else reuse anything.
        if self.live_count() < self.max_conns() {
            let conn = self.create_conn().await?;
            self.conns.write().unwrap().push(conn.clone());
            return Ok(conn);
        }
        self.get().await
    }

    /// The hard cap on live connections.
    fn max_conns(&self) -> usize {
        self.config.max_connections.max(1)
    }

    /// Connections to keep warm (0 = fully lazy pool).
    fn min_connections(&self) -> usize {
        self.config.min_connections
    }

    /// A live connection with headroom, if one exists. "Headroom" means its
    /// in-flight load is below half its budget; when all live connections
    /// are loaded and the pool is below the cap, `None` makes `get` grow.
    fn pick_usable(&self) -> Option<MultiplexedConnection> {
        let conn = self.pick_live()?;
        let loaded = conn.inflight() >= self.config.max_inflight / 2;
        if loaded && self.live_count() < self.max_conns() {
            return None;
        }
        Some(conn)
    }

    /// Record a successful command.
    pub fn note_success(&self) {
        self.health.on_success();
    }

    /// Record a failed command; may trip the breaker per configured thresholds.
    pub fn note_failure(&self) {
        // Evict eagerly: poisoned/timed-out connections must leave the pool
        // immediately, not only when the breaker trips.
        self.evict_dead();
        let outcome = self.health.on_failure();
        if outcome.tripped {
            // Wake the maintenance task now; it may be in a long healthy
            // patrol sleep, and probing is the only recovery path.
            self.state_changed.notify_one();
            tracing::warn!(
                target: "redis::pool",
                endpoint = %self.endpoint(),
                "circuit breaker tripped; mesh marked unhealthy"
            );
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
        let endpoint = self.next_endpoint();
        let handshake = self.handshake.clone();
        tokio::time::timeout(
            self.config.op_timeout,
            MultiplexedConnection::connect_with_handshake(
                &endpoint,
                self.config.max_inflight,
                handshake.as_ref(),
            ),
        )
        .await
        .map_err(|_| RedisError::from_kind(ErrorKind::Timeout, "connection timed out"))?
    }

    /// Pick the target for a new connection: round-robin over the DNS answer
    /// set in direct mode (clientBalancer `getNextIp`), the fixed endpoint
    /// otherwise.
    fn next_endpoint(&self) -> Endpoint {
        let ips = self.direct_ips.read().unwrap();
        if ips.is_empty() {
            drop(ips);
            return self.endpoint();
        }
        let idx = self.ip_cursor.fetch_add(1, Ordering::Relaxed);
        Endpoint::Tcp(ips[idx % ips.len()])
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
        // Warm the configured minimum; the pool grows on demand via `get`.
        // min_connections == 0 means a fully lazy start: nothing is created
        // and an unreachable backend does not fail `connect`.
        for _ in 0..self.config.min_connections {
            match self.create_conn().await {
                Ok(conn) => self.conns.write().unwrap().push(conn),
                Err(err) => tracing::warn!(
                    target: "redis::pool",
                    endpoint = %self.endpoint(),
                    error = %err,
                    "warm-up connection failed"
                ),
            }
        }
        if self.config.min_connections == 0 {
            return Ok(());
        }
        if self.live_count() == 0 {
            return Err(unavailable());
        }
        Ok(())
    }

    /// The recovery probe: re-scan the sock directory (the mesh may have
    /// restarted on a new port while the breaker was open), then open one
    /// fresh connection and PING it with a timeout.
    async fn probe(&self) {
        tracing::debug!(
            target: "redis::pool",
            endpoint = %self.endpoint(),
            "recovery probe start"
        );
        self.refresh_endpoint().await;
        if let Ok(conn) = self.create_conn().await
            && tokio::time::timeout(self.config.op_timeout, cmd("PING").exec_async(&conn))
                .await
                .is_ok_and(|r| r.is_ok())
        {
            self.conns.write().unwrap().push(conn);
            self.health.recover();
            tracing::info!(
                target: "redis::pool",
                endpoint = %self.endpoint(),
                "recovery probe succeeded; mesh healthy again"
            );
            return;
        }
        tracing::debug!(
            target: "redis::pool",
            endpoint = %self.endpoint(),
            "recovery probe failed"
        );
    }

    /// Re-resolve the mesh endpoint from the sock directory. If the mesh has
    /// re-published the resource on a different endpoint, switch to it and
    /// drop the connections bound to the stale one.
    async fn refresh_endpoint(&self) {
        if self.rediscover {
            let Some(new) = discovery::scan_current(&self.config).await else {
                return;
            };
            let changed = {
                let mut current = self.endpoint.write().unwrap();
                if *current == new {
                    false
                } else {
                    *current = new.clone();
                    true
                }
            };
            if changed {
                self.conns.write().unwrap().clear();
                tracing::info!(
                    target: "redis::pool",
                    endpoint = %new,
                    "mesh endpoint re-published; switched to new endpoint"
                );
            }
        } else if let Some(authority) = self.resolver.clone() {
            let addrs: Vec<SocketAddr> = match tokio::net::lookup_host(&authority).await {
                Ok(iter) => iter.collect(),
                Err(_) => return, // keep the last good answer on DNS failure
            };
            self.apply_dns_answer(addrs);
        }
    }

    /// Apply a fresh DNS answer set (clientBalancer `refreshEndpointPool`):
    ///
    /// 1. Connections to IPs that left the answer are evicted immediately
    ///    (idle or not — a dead connection here is replaced lazily on the
    ///    next borrow, unlike the Java borrow/return pool).
    /// 2. If all current IPs are still valid but new IPs appeared, one
    ///    connection per new IP is evicted from the most-loaded old IPs so
    ///    the round-robin creation fast-tracks connections onto the new IPs.
    ///
    /// Returns without changes when the answer is empty (DNS failure) or
    /// identical to the current set.
    pub(crate) fn apply_dns_answer(&self, mut addrs: Vec<SocketAddr>) {
        if addrs.is_empty() {
            return;
        }
        addrs.sort();
        addrs.dedup();

        let (new_ips, offline_ips) = {
            let mut ips = self.direct_ips.write().unwrap();
            if *ips == addrs {
                return;
            }
            let old = std::mem::replace(&mut *ips, addrs.clone());
            let new_ips: Vec<SocketAddr> =
                addrs.iter().filter(|a| !old.contains(a)).copied().collect();
            let offline_ips: Vec<SocketAddr> =
                old.iter().filter(|a| !addrs.contains(a)).copied().collect();
            (new_ips, offline_ips)
        };

        if !offline_ips.is_empty() {
            let mut conns = self.conns.write().unwrap();
            let before = conns.len();
            conns.retain(|c| match c.addr() {
                Some(addr) => !offline_ips.contains(&addr),
                None => true,
            });
            tracing::info!(
                target: "redis::pool",
                offline = ?offline_ips,
                evicted = before - conns.len(),
                "dns answer changed; evicted connections to offline ips"
            );
        }

        // Fast-track new IPs into the pool.
        for _ in &new_ips {
            self.evict_one_from_most_loaded();
        }
        if !new_ips.is_empty() {
            tracing::info!(
                target: "redis::pool",
                new = ?new_ips,
                "dns answer changed; fast-tracking new ips"
            );
        }
    }

    /// Per-IP connection counts over live connections.
    fn ip_loads(&self) -> std::collections::HashMap<SocketAddr, usize> {
        let mut loads = std::collections::HashMap::new();
        for conn in self.conns.read().unwrap().iter() {
            if !conn.is_alive() {
                continue;
            }
            if let Some(addr) = conn.addr() {
                *loads.entry(addr).or_insert(0) += 1;
            }
        }
        loads
    }

    /// Evict one live connection from the most-loaded IP. Returns whether
    /// anything was evicted.
    fn evict_one_from_most_loaded(&self) -> bool {
        let mut conns = self.conns.write().unwrap();
        let mut loads: std::collections::HashMap<SocketAddr, usize> =
            std::collections::HashMap::new();
        for conn in conns.iter().filter(|c| c.is_alive()) {
            if let Some(addr) = conn.addr() {
                *loads.entry(addr).or_insert(0) += 1;
            }
        }
        let Some((&max_addr, _)) = loads.iter().max_by_key(|(_, n)| *n) else {
            return false;
        };
        if let Some(pos) = conns
            .iter()
            .position(|c| c.is_alive() && c.addr() == Some(max_addr))
        {
            conns.remove(pos);
            return true;
        }
        false
    }

    /// The clientBalancer `watchPool` balance sweep: while per-IP live
    /// connection counts differ by more than one, evict one connection from
    /// the most-loaded IP (round-robin creation lands its replacement on a
    /// less-loaded IP). Runs on healthy maintenance ticks.
    fn rebalance_ips(&self) {
        if self.direct_ips.read().unwrap().len() < 2 {
            return;
        }
        loop {
            let loads = self.ip_loads();
            if loads.len() < 2 {
                return;
            }
            let min = loads.values().min().copied().unwrap_or(0);
            let max = loads.values().max().copied().unwrap_or(0);
            if max - min <= 1 {
                return;
            }
            if !self.evict_one_from_most_loaded() {
                return;
            }
        }
    }

    /// The periodic healthy-state DNS refresh (clientBalancer
    /// `HostAddressWatcher` periodic re-resolution).
    async fn maybe_refresh_dns(&self) {
        if self.resolver.is_none() {
            return;
        }
        let due = {
            let mut last = self.last_dns_refresh.lock().unwrap();
            if last.elapsed() >= DNS_REFRESH_INTERVAL {
                *last = Instant::now();
                true
            } else {
                false
            }
        };
        if due {
            self.refresh_endpoint().await;
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
/// unhealthy, and keep `min_connections` live connections. Healthy pools patrol on
/// a slow cadence; unhealthy pools probe every second for fast recovery.
/// Stops when the pool is dropped (weak upgrade fails).
fn spawn_maintenance(weak: Weak<Pool>) {
    tokio::spawn(async move {
        loop {
            let Some(pool) = weak.upgrade() else {
                return;
            };
            // Adaptive cadence: slow patrol when healthy, fast probe while
            // the breaker is open (the only recovery path).
            let tick = if pool.health.is_healthy() {
                pool.config.healthy_patrol_interval
            } else {
                pool.config.unhealthy_probe_interval
            };
            let state_changed = pool.state_changed.clone();
            drop(pool);
            // Sleep until the next tick OR a health-state change (breaker
            // trip) wakes us to start probing immediately.
            tokio::select! {
                _ = tokio::time::sleep(tick) => {}
                _ = state_changed.notified() => {}
            }
            let Some(pool) = weak.upgrade() else {
                return;
            };
            tracing::debug!(
                target: "redis::pool",
                healthy = pool.health.is_healthy(),
                "maintenance tick"
            );
            pool.evict_dead();
            if pool.health.is_kept() {
                if !pool.health.is_healthy() {
                    pool.probe().await;
                } else {
                    pool.maybe_refresh_dns().await;
                    pool.rebalance_ips();
                    while pool.live_count() < pool.min_connections() {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A listener that accepts and holds connections, discarding input.
    async fn fake_listener() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket); // keep the connection open
            }
        });
        addr
    }

    /// A listener that answers every command with `+OK`.
    async fn ok_listener() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
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
        addr
    }

    fn test_config(max_connections: usize) -> MeshConfig {
        let mut config = MeshConfig::new("test");
        config.min_connections = 1;
        config.max_connections = max_connections;
        config
    }

    /// The pool now starts lazy (1 warm connection); grow it explicitly to
    /// `n` live connections for balancing tests.
    async fn grow_to(pool: &Pool, n: usize) {
        while pool.live_count() < n {
            let conn = pool.create_conn().await.unwrap();
            pool.conns.write().unwrap().push(conn);
        }
    }

    #[tokio::test]
    async fn min_connections_zero_starts_fully_lazy() {
        // Nothing listens on 127.0.0.1:1; with min_connections = 0 the pool
        // starts successfully anyway and holds no connections.
        let mut config = test_config(4);
        config.min_connections = 0;
        let pool = Pool::connect_direct(vec!["127.0.0.1:1".parse().unwrap()], None, None, config)
            .await
            .unwrap();
        assert_eq!(pool.live_count(), 0);
    }

    #[tokio::test]
    async fn min_connections_warms_at_startup() {
        let a = fake_listener().await;
        let mut config = test_config(4);
        config.min_connections = 3;
        let pool = Pool::connect_direct(vec![a], None, None, config)
            .await
            .unwrap();
        assert_eq!(pool.live_count(), 3);
    }

    #[tokio::test]
    async fn breaker_trip_wakes_maintenance_for_fast_recovery() {
        let addr = ok_listener().await;
        let mut config = test_config(4);
        // A long healthy patrol: without the trip wake-up, recovery would
        // wait this whole interval.
        config.healthy_patrol_interval = Duration::from_secs(3600);
        config.unhealthy_probe_interval = Duration::from_millis(50);
        let pool = Pool::connect_direct(vec![addr], None, None, config)
            .await
            .unwrap();
        assert!(pool.can_serve());
        // Trip the breaker (threshold = max_connections = 4).
        for _ in 0..4 {
            pool.note_failure();
        }
        assert!(!pool.can_serve());
        // The trip notifies the maintenance task, which probes (backend is
        // healthy) and closes the breaker well within a second.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.can_serve() {
            assert!(Instant::now() < deadline, "pool did not recover in time");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn connections_spread_round_robin_across_ips() {
        let a = fake_listener().await;
        let b = fake_listener().await;
        let pool = Pool::connect_direct(vec![a, b], None, None, test_config(4))
            .await
            .unwrap();
        grow_to(&pool, 4).await;
        let loads = pool.ip_loads();
        assert_eq!(loads.len(), 2, "expected connections on both ips");
        assert_eq!(
            loads.values().min(),
            loads.values().max(),
            "expected a 2/2 split, got {loads:?}"
        );
    }

    #[tokio::test]
    async fn dns_change_evicts_offline_ips_and_admits_new_ones() {
        let a = fake_listener().await;
        let b = fake_listener().await;
        let c = fake_listener().await;
        let pool = Pool::connect_direct(vec![a, b], None, None, test_config(4))
            .await
            .unwrap();
        grow_to(&pool, 4).await;
        assert_eq!(pool.ip_loads().len(), 2);

        // a goes offline, c appears.
        pool.apply_dns_answer(vec![b, c]);

        let loads = pool.ip_loads();
        assert!(
            !loads.contains_key(&a),
            "offline ip must be evicted: {loads:?}"
        );
        // New connections only target the fresh answer set.
        for _ in 0..4 {
            let conn = pool.create_conn().await.unwrap();
            assert!(matches!(conn.addr(), Some(addr) if addr == b || addr == c));
        }
    }

    #[tokio::test]
    async fn rebalance_converges_to_within_one() {
        let a = fake_listener().await;
        let pool = Pool::connect_direct(vec![a], None, None, test_config(4))
            .await
            .unwrap();
        grow_to(&pool, 4).await;
        assert_eq!(pool.ip_loads()[&a], 4);

        // b joins; one connection is evicted to fast-track it.
        let b = fake_listener().await;
        pool.apply_dns_answer(vec![a, b]);
        let on_a = pool.ip_loads()[&a];
        assert_eq!(on_a, 3, "one connection evicted for the new ip");

        // Recreate on the new set, then sweep until balanced.
        for _ in 0..2 {
            let conn = pool.create_conn().await.unwrap();
            assert!(matches!(conn.addr(), Some(addr) if addr == a || addr == b));
            pool.conns.write().unwrap().push(conn);
        }
        pool.rebalance_ips();
        let loads = pool.ip_loads();
        let min = *loads.values().min().unwrap();
        let max = *loads.values().max().unwrap();
        assert!(max - min <= 1, "unbalanced: {loads:?}");
    }
}
