//! Direct backend access (no mesh), ported from the Java `reference-library`
//! `JedisPort` / `JedisHAServer` / `JedisMSServer` access strategy on top of
//! the `clientBalancer` pooling model:
//!
//! - [`DirectClient`] (≈ `JedisPort`): one `host:port[:db]` server. A [`Pool`] with
//!   `AUTH`/`SELECT` handshake, per-command timeout, and read/write split
//!   retries (reads `max_try_time`, writes `write_retry`, matching the Java
//!   `callable`/`callUpdate` split). `read_only` backends reject writes.
//! - [`HaServer`] (≈ `JedisHAServer`): a `first` (read-write) plus an optional
//!   `second` read fallback, with optional double-write; `set_second` mode
//!   syncs counters by writing the first's result back with `SET`.
//! - [`MsServer`] (≈ `JedisMSServer`): writes only to `master`; reads prefer
//!   `slave` and fall back to `master`; [`MsServer::at_master`] pins reads to
//!   the master (read-your-writes / CAS sequences).
//! - [`Shards`]: client-side shard routing using the same hash/distribution
//!   algorithms as the breeze mesh ([`crate::direct::sharding`]).
//!
//! Availability comes from the same circuit breaker and maintenance probe as
//! the mesh path ([`crate::pool`]). Hostname-configured backends get the
//! clientBalancer `HostAddressWatcher` behavior: the hostname is re-resolved
//! on breaker trips and every 30s while healthy, and the pool switches
//! endpoints (dropping connections to offline IPs) when the answer changes.
//! Note one simplification vs. Java: when a hostname resolves to multiple
//! IPs, clientBalancer load-balances across all of them; this client pins
//! one and fails over on breaker trip.

pub mod sharding;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::cmd::Cmd;
use crate::sidecar::config::MeshConfig;
use crate::connection::{ConnectionLike, Handshake, RedisFuture};
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::pipeline::Pipeline;
use crate::pool::Pool;
use crate::direct::sharding::Sharding;
use crate::types::Value;
use crate::sidecar::SidecarClient as InnerClient;

/// Configuration for one direct backend server, mirroring the Java
/// `RedisConfig` (`host:port[:db]`, auth, timeout).
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Server host (IP or hostname).
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Logical database (`SELECT`), default 0.
    pub db: i64,
    /// Optional `AUTH` auth.
    pub auth: Option<String>,
    /// Reject writes (the Java `readOnly`; unlike Java, which silently
    /// returns default values, a write on a read-only backend fails with
    /// [`ErrorKind::ClientError`]).
    pub read_only: bool,
    /// Connections in the pool.
    pub pool_size: usize,
    /// Per-connection in-flight budget.
    pub max_inflight: usize,
    /// Per-command timeout.
    pub op_timeout: Duration,
    /// Commands slower than this are slow-logged.
    pub slow_time_threshold: Duration,
    /// Retry attempts for read commands (Java `max_try_time`).
    pub max_try_time: u32,
    /// Retry attempts for write commands (Java `DEFAULT_WRITE_RETRY`).
    pub write_retry: u32,
    /// Healthy-pool patrol cadence (see [`crate::sidecar::MeshConfig`]).
    pub healthy_patrol_interval: Duration,
    /// Breaker-open recovery probe cadence.
    pub unhealthy_probe_interval: Duration,
}

impl ServerConfig {
    /// Parse `host:port[:db]` (db defaults to 0), like the Java
    /// `RedisConfig.setServerPortDb`.
    pub fn new(server_port_db: &str) -> RedisResult<Self> {
        let parts: Vec<&str> = server_port_db.split(':').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                format!("invalid server address '{server_port_db}', expect host:port[:db]"),
            ));
        }
        let port = parts[1].parse::<u16>().map_err(|_| {
            RedisError::new(
                ErrorKind::ClientError,
                format!("invalid port in '{server_port_db}'"),
            )
        })?;
        let db = if parts.len() == 3 {
            parts[2].parse::<i64>().map_err(|_| {
                RedisError::new(
                    ErrorKind::ClientError,
                    format!("invalid db in '{server_port_db}'"),
                )
            })?
        } else {
            0
        };
        Ok(ServerConfig {
            host: parts[0].to_string(),
            port,
            db,
            auth: None,
            read_only: false,
            pool_size: 4,
            max_inflight: 4096,
            op_timeout: Duration::from_millis(500),
            slow_time_threshold: Duration::from_millis(50),
            max_try_time: 2,
            write_retry: 1,
            healthy_patrol_interval: Duration::from_secs(30),
            unhealthy_probe_interval: Duration::from_secs(1),
        })
    }

    /// Set the `AUTH` password.
    pub fn with_auth(mut self, auth: impl Into<String>) -> Self {
        self.auth = Some(auth.into());
        self
    }

    /// Mark the backend read-only.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set the pool size.
    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.pool_size = size.max(1);
        self
    }

    /// The `host:port:db` label used in logs and stats.
    pub fn label(&self) -> String {
        format!("{}:{}:{}", self.host, self.port, self.db)
    }
}

struct DirectClientInner {
    client: InnerClient,
    read_only: AtomicBool,
    label: String,
}

/// One direct backend server (≈ Java `JedisPort`). Cheap to clone.
#[derive(Clone)]
pub struct DirectClient {
    inner: Arc<DirectClientInner>,
}

impl DirectClient {
    /// Resolve the address, connect the pool (with `AUTH`/`SELECT` handshake
    /// on every connection), and start maintenance.
    pub async fn connect(config: ServerConfig) -> RedisResult<Self> {
        let label = config.label();
        let authority = format!("{}:{}", config.host, config.port);
        let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(&authority)
            .await?
            .collect();
        if addrs.is_empty() {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                format!("cannot resolve '{}:{}'", config.host, config.port),
            ));
        }
        let handshake = if config.auth.is_some() || config.db != 0 {
            Some(Handshake {
                auth: config.auth.clone(),
                db: Some(config.db),
            })
        } else {
            None
        };
        let mut pool_config = MeshConfig::new(label.clone());
        pool_config.pool_size = config.pool_size;
        pool_config.max_inflight = config.max_inflight;
        pool_config.op_timeout = config.op_timeout;
        pool_config.slow_time_threshold = config.slow_time_threshold;
        pool_config.max_try_time = config.max_try_time;
        pool_config.write_retry = config.write_retry;
        pool_config.healthy_patrol_interval = config.healthy_patrol_interval;
        pool_config.unhealthy_probe_interval = config.unhealthy_probe_interval;

        // Hostnames get a DNS watcher (re-resolve on breaker trip + periodic);
        // IP literals are static.
        let resolver = if config.host.parse::<std::net::IpAddr>().is_ok() {
            None
        } else {
            Some(authority)
        };
        let pool = Pool::connect_direct(addrs, handshake, resolver, pool_config).await?;
        let client = InnerClient::from_pool(
            pool,
            label.clone(),
            config.max_try_time.max(1),
            config.write_retry.max(1),
            config.op_timeout,
            config.slow_time_threshold,
        );
        Ok(DirectClient {
            inner: Arc::new(DirectClientInner {
                client,
                read_only: AtomicBool::new(config.read_only),
                label,
            }),
        })
    }

    /// The `host:port:db` label.
    pub fn label(&self) -> &str {
        &self.inner.label
    }

    /// Whether the pool's breaker currently allows serving.
    pub fn is_available(&self) -> bool {
        self.inner.client.is_available()
    }

    /// Whether writes are rejected.
    pub fn is_read_only(&self) -> bool {
        self.inner.read_only.load(Ordering::Acquire)
    }

    /// Toggle read-only at runtime (the Java `setReadOnly`).
    pub fn set_read_only(&self, read_only: bool) {
        self.inner.read_only.store(read_only, Ordering::Release);
    }

    /// The underlying pooled client (for stats and operator controls).
    pub fn pooled_client(&self) -> &InnerClient {
        &self.inner.client
    }

    fn check_writable(&self, readonly: bool) -> RedisResult<()> {
        if self.is_read_only() && !readonly {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                format!("backend {} is read-only", self.inner.label),
            ));
        }
        Ok(())
    }
}

impl ConnectionLike for DirectClient {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            self.check_writable(command.is_readonly())?;
            self.inner.client.req_command(command).await
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            self.check_writable(pipeline.is_readonly())?;
            self.inner.client.req_pipeline(pipeline, offset, count).await
        })
    }
}

/// Read-fallback helper: run `command` on `primary`, and if it is
/// unavailable or the attempt fails retriably, run it on `secondary`.
async fn command_with_fallback(
    primary: &DirectClient,
    secondary: Option<&DirectClient>,
    command: &Cmd,
) -> RedisResult<Value> {
    let mut last_err = None;
    if primary.is_available() {
        match primary.req_command(command).await {
            Err(err) if err.is_retriable() => last_err = Some(err),
            other => return other,
        }
    }
    if let Some(second) = secondary {
        return second.req_command(command).await;
    }
    match last_err {
        Some(err) => Err(err),
        // Primary was unavailable and there is no secondary; surface the
        // pool's unavailable error rather than a stale one.
        None => primary.req_command(command).await,
    }
}

/// A high-availability server pair (≈ Java `JedisHAServer`): `first` serves
/// reads and writes; when it is down, reads fall back to `second`. With
/// `double_write`, writes are additionally applied to `second` (the first's
/// result wins; second's errors are logged, never propagated).
pub struct HaServer {
    first: DirectClient,
    second: Option<DirectClient>,
    double_write: bool,
    /// Counter-sync mode (Java `setSecond`): for counter commands
    /// (INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT), instead of replaying the
    /// increment on `second`, write the **first's result** back with `SET`,
    /// keeping `second` calibrated to the authoritative value.
    set_second: bool,
    /// Optional `[min, max]` hash range for id-based routing (`contains`).
    hash_range: Option<(i64, i64)>,
}

/// Write commands whose result can be written back to the second backend
/// with a plain `SET` (the Java `setSecond` command set).
fn is_setsecond_command(name: &str) -> bool {
    matches!(name, "INCR" | "INCRBY" | "DECR" | "DECRBY" | "INCRBYFLOAT")
}

impl HaServer {
    /// Build from a first (read-write) backend and an optional fallback.
    pub fn new(first: DirectClient, second: Option<DirectClient>) -> Self {
        HaServer {
            first,
            second,
            double_write: false,
            set_second: false,
            hash_range: None,
        }
    }

    /// Enable double-write to the second backend.
    pub fn with_double_write(mut self, double_write: bool) -> Self {
        self.double_write = double_write;
        self
    }

    /// Enable counter-sync mode (Java `setSecond`); implies double-write.
    pub fn with_set_second(mut self, set_second: bool) -> Self {
        self.set_second = set_second;
        if set_second {
            self.double_write = true;
        }
        self
    }

    /// Set the `[min, max]` hash range this shard owns.
    pub fn with_hash_range(mut self, min: i64, max: i64) -> Self {
        self.hash_range = Some((min, max));
        self
    }

    /// Whether `id` falls into this shard's hash range (Java `contains`).
    pub fn contains(&self, id: i64) -> bool {
        match self.hash_range {
            Some((min, max)) => id >= min && id < max,
            None => false,
        }
    }

    /// The first (read-write) backend.
    pub fn first(&self) -> &DirectClient {
        &self.first
    }

    /// The read-fallback backend, if configured.
    pub fn second(&self) -> Option<&DirectClient> {
        self.second.as_ref()
    }

    /// A write, applying the Java `JedisHAServer` policy:
    ///
    /// 1. `first` alive → execute on it. With double-write, also sync
    ///    `second` (set-second: `SET key <first's result>` for counter
    ///    commands; otherwise replay the command). The first's result wins;
    ///    second's errors are only logged.
    /// 2. `first` down → with double-write and a live `second`, execute there
    ///    and return its result.
    /// 3. Neither → the first's error (or "no backend available").
    async fn write_command(&self, command: &Cmd) -> RedisResult<Value> {
        let mut first_result = None;
        if self.first.is_available() {
            first_result = Some(self.first.req_command(command).await);
        }

        let mut second_result = None;
        let second_live = self.double_write
            && self
                .second
                .as_ref()
                .is_some_and(DirectClient::is_available);
        if second_live {
            let second = self.second.as_ref().unwrap();
            let sync = match (&first_result, self.set_second) {
                (Some(Ok(value)), true) => self.setsecond_command(command, value),
                _ => None,
            };
            let second_call = match &sync {
                Some(sync_cmd) => second.req_command(sync_cmd).await,
                None => second.req_command(command).await,
            };
            if let Err(err) = &second_call {
                tracing::warn!(
                    target: "redis::direct",
                    backend = second.label(),
                    error = %err,
                    "double-write to second backend failed"
                );
            }
            second_result = Some(second_call);
        }

        match first_result {
            Some(Ok(value)) => Ok(value),
            Some(Err(err)) => match second_result {
                // first failed but the second applied the write.
                Some(Ok(value)) => Ok(value),
                _ => Err(err),
            },
            None => match second_result {
                Some(result) => result,
                None => Err(RedisError::new(
                    ErrorKind::NoConnection,
                    "redis server all dead",
                )),
            },
        }
    }

    /// Build the `SET key <value>` sync command for set-second mode. Returns
    /// `None` for non-counter commands or non-scalar replies (the caller then
    /// replays the original command instead).
    fn setsecond_command(&self, command: &Cmd, value: &Value) -> Option<Cmd> {
        if !is_setsecond_command(&command.name()) {
            return None;
        }
        let key = command.args().get(1)?;
        let mut sync = crate::cmd::cmd("SET");
        sync.arg_bytes(key);
        match value {
            Value::Int(i) => {
                sync.arg_bytes(i.to_string().as_bytes());
            }
            Value::Double(d) => {
                sync.arg_bytes(ryu::Buffer::new().format(*d).as_bytes());
            }
            Value::BulkString(bytes) => {
                sync.arg_bytes(bytes);
            }
            Value::SimpleString(s) => {
                sync.arg_bytes(s.as_bytes());
            }
            _ => return None,
        };
        Some(sync)
    }
}

impl ConnectionLike for HaServer {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            if command.is_readonly() {
                command_with_fallback(&self.first, self.second.as_ref(), command).await
            } else {
                self.write_command(command).await
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
            if pipeline.is_readonly() {
                if self.first.is_available() {
                    return self.first.req_pipeline(pipeline, offset, count).await;
                }
                if let Some(second) = &self.second {
                    return second.req_pipeline(pipeline, offset, count).await;
                }
            }
            self.first.req_pipeline(pipeline, offset, count).await
        })
    }
}

/// A master/slave server pair (≈ Java `JedisMSServer`): writes go only to
/// `master`; reads prefer `slave` and fall back to `master`. Use
/// [`MsServer::at_master`] for read-your-writes sequences (the Java
/// `*FromMaster` variants).
pub struct MsServer {
    master: DirectClient,
    slave: Option<DirectClient>,
    /// Optional `[min, max]` hash range for id-based routing (`contains`).
    hash_range: Option<(i64, i64)>,
}

impl MsServer {
    /// Build from a master (required, read-write) and an optional slave.
    ///
    /// The slave is forced read-only, like the Java `slave.setReadonly(true)`.
    pub fn new(master: DirectClient, slave: Option<DirectClient>) -> Self {
        if let Some(slave) = &slave {
            slave.set_read_only(true);
        }
        MsServer {
            master,
            slave,
            hash_range: None,
        }
    }

    /// Set the `[min, max]` hash range this shard owns.
    pub fn with_hash_range(mut self, min: i64, max: i64) -> Self {
        self.hash_range = Some((min, max));
        self
    }

    /// Whether `id` falls into this shard's hash range.
    pub fn contains(&self, id: i64) -> bool {
        match self.hash_range {
            Some((min, max)) => id >= min && id < max,
            None => false,
        }
    }

    /// The master backend (read-write).
    pub fn master(&self) -> &DirectClient {
        &self.master
    }

    /// The slave backend, if configured.
    pub fn slave(&self) -> Option<&DirectClient> {
        self.slave.as_ref()
    }

    /// Pin reads to the master (read-your-writes). The returned backend
    /// exposes the full [`crate::Commands`] surface.
    pub fn at_master(&self) -> &DirectClient {
        &self.master
    }
}

impl ConnectionLike for MsServer {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            if command.is_readonly() {
                match &self.slave {
                    // Prefer the slave; fall back to the master.
                    Some(slave) => command_with_fallback(slave, Some(&self.master), command).await,
                    None => self.master.req_command(command).await,
                }
            } else {
                self.master.req_command(command).await
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
            if pipeline.is_readonly()
                && let Some(slave) = &self.slave
                && slave.is_available()
            {
                return slave.req_pipeline(pipeline, offset, count).await;
            }
            self.master.req_pipeline(pipeline, offset, count).await
        })
    }
}

/// Client-side shard routing over a set of backends/HA-groups, using the
/// same hash and distribution algorithms as the breeze mesh.
///
/// ```no_run
/// # use redis::direct::*;
/// # async fn demo() -> redis::RedisResult<()> {
/// let shards = Shards::new(
///     "crc32", "modula",
///     vec!["10.0.0.1:6379:0".to_string(), "10.0.0.2:6379:0".to_string()],
///     vec![
///         DirectClient::connect(ServerConfig::new("10.0.0.1:6379")?).await?,
///         DirectClient::connect(ServerConfig::new("10.0.0.2:6379")?).await?,
///     ],
/// );
/// # Ok(()) }
/// ```
pub struct Shards<T: ConnectionLike> {
    sharding: Sharding,
    shards: Vec<T>,
}

impl<T: ConnectionLike> Shards<T> {
    /// Build the routing plan from the resource's `hash`/`distribution`
    /// configuration names and the per-shard connection list. `names` must
    /// match the configured backend names (ketama hashes them onto the ring).
    pub fn new(hash_alg: &str, distribution: &str, names: Vec<String>, shards: Vec<T>) -> Self {
        assert_eq!(
            names.len(),
            shards.len(),
            "names and shards must have the same length"
        );
        Shards {
            sharding: Sharding::new(hash_alg, distribution, &names),
            shards,
        }
    }

    /// The shard responsible for `key`.
    pub fn for_key(&self, key: &[u8]) -> &T {
        &self.shards[self.sharding.shard_idx(key)]
    }

    /// The shard responsible for a numeric id — the
    /// `shardingSupport.getClient(uid)` pattern from the Java services. The
    /// id is hashed in its decimal-string form, exactly like the mesh
    /// hashes a numeric key.
    pub fn get_client(&self, id: i64) -> &T {
        let mut buf = itoa::Buffer::new();
        self.for_key(buf.format(id).as_bytes())
    }

    /// The shard responsible for a string id
    /// (`shardingSupport.getClient(String)`).
    pub fn get_client_str(&self, id: &str) -> &T {
        self.for_key(id.as_bytes())
    }

    /// The raw hash of `key` (for `contains`-style range checks).
    pub fn hash(&self, key: &[u8]) -> i64 {
        self.sharding.hash(key)
    }

    /// All shards, in configuration order.
    pub fn all(&self) -> &[T] {
        &self.shards
    }
}

/// Commands route by their key (second argument); pipelines are rejected
/// unless the caller guarantees all keys land on one shard (use
/// [`Shards::for_key`] directly for that).
impl<T: ConnectionLike> ConnectionLike for Shards<T> {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            let key = command
                .args()
                .get(1)
                .ok_or_else(|| RedisError::new(ErrorKind::ClientError, "command has no key"))?;
            self.for_key(key).req_command(command).await
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        _pipeline: &'a Pipeline,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async {
            Err(RedisError::new(
                ErrorKind::ClientError,
                "pipelines must be issued per-shard via Shards::for_key",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Commands;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn parses_server_port_db() {
        let cfg = ServerConfig::new("10.0.0.1:6379").unwrap();
        assert_eq!(cfg.host, "10.0.0.1");
        assert_eq!(cfg.port, 6379);
        assert_eq!(cfg.db, 0);
        assert_eq!(cfg.label(), "10.0.0.1:6379:0");

        let cfg = ServerConfig::new("10.0.0.1:6379:3").unwrap();
        assert_eq!(cfg.db, 3);
        assert_eq!(cfg.label(), "10.0.0.1:6379:3");

        assert!(ServerConfig::new("no-port").is_err());
        assert!(ServerConfig::new("host:notaport").is_err());
        assert!(ServerConfig::new("host:6379:notadb").is_err());
        assert!(ServerConfig::new("a:1:2:3").is_err());
    }

    /// A minimal fake Redis: replies `+OK` to AUTH/SELECT/SET, a bulk string
    /// to GET. Records whether it saw the AUTH/SELECT handshake.
    async fn fake_redis() -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
        fake_redis_with(|_| "+OK".to_string(), |chunk| chunk.contains("GET"), "$1\r\nv\r\n").await
    }

    /// A fake Redis whose replies are computed per received chunk:
    /// `classify` picks the special reply, everything else gets `default`.
    async fn fake_redis_with(
        _default: impl Fn(&str) -> String + Send + 'static,
        classify: impl Fn(&str) -> bool + Send + Sync + 'static,
        special_reply: &'static str,
    ) -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let classify = Arc::new(classify);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let seen = seen_clone.clone();
                let classify = classify.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let mut pending = String::new();
                    loop {
                        let n = socket.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        pending.push_str(&String::from_utf8_lossy(&buf[..n]));
                        let chunk = std::mem::take(&mut pending);
                        let upper = chunk.to_uppercase();
                        seen.lock().unwrap().push(upper.clone());
                        if classify(&upper) {
                            let _ = socket.write_all(special_reply.as_bytes()).await;
                        } else {
                            let _ = socket.write_all(b"+OK\r\n").await;
                        }
                    }
                });
            }
        });
        (port, seen)
    }

    #[tokio::test]
    async fn handshake_and_read_only_enforcement() {
        let (port, seen) = fake_redis().await;
        let cfg = ServerConfig::new(&format!("127.0.0.1:{port}:2"))
            .unwrap()
            .with_auth("secret");
        let backend = DirectClient::connect(cfg).await.unwrap();

        let v: String = backend.hget("k", "f").await.unwrap();
        assert_eq!(v, "v");

        // The connection ran AUTH + SELECT before serving.
        let log = seen.lock().unwrap().join("|");
        assert!(log.contains("AUTH"), "expected AUTH in handshake: {log}");
        assert!(log.contains("SELECT"), "expected SELECT in handshake: {log}");

        // Flip to read-only: reads keep working, writes are rejected locally.
        backend.set_read_only(true);
        let mut set = crate::cmd::cmd("SET");
        set.arg("k").arg("v");
        let err = backend.req_command(&set).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ClientError);
        let v: String = backend.hget("k", "f").await.unwrap();
        assert_eq!(v, "v");
    }

    /// Raw INCR against an `HaServer` (counter commands are not in the
    /// trimmed Commands surface, so tests build the command directly).
    async fn raw_incr<C: ConnectionLike>(conn: &C, key: &str) -> crate::types::Value {
        let mut incr = crate::cmd::cmd("INCR");
        incr.arg(key);
        conn.req_command(&incr).await.unwrap()
    }

    /// INCR answers `:7`, everything else `+OK`.
    async fn counter_redis(
        value: i64,
    ) -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
        let reply = format!(":{value}\r\n");
        // Leak to get 'static; tests are short-lived.
        let reply: &'static str = Box::leak(reply.into_boxed_str());
        fake_redis_with(|_| "+OK".to_string(), |c| c.contains("INCR"), reply).await
    }

    #[tokio::test]
    async fn ha_setsecond_writes_firsts_result_to_second() {
        let (first_port, _) = counter_redis(7).await;
        let (second_port, second_seen) = counter_redis(3).await;

        let first = DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{first_port}")).unwrap())
            .await
            .unwrap();
        let second =
            DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{second_port}")).unwrap())
                .await
                .unwrap();

        // set-second: second is calibrated with SET key <first's result>.
        let ha = HaServer::new(first.clone(), Some(second.clone())).with_set_second(true);
        assert_eq!(raw_incr(&ha, "counter").await, crate::types::Value::Int(7));
        let log = second_seen.lock().unwrap().join("|");
        assert!(log.contains("SET"), "expected SET sync, got: {log}");
        assert!(log.contains("7"), "expected first's result in SET: {log}");
        assert!(!log.contains("INCR"), "counter must not be replayed: {log}");

        // plain double-write: the command itself is replayed on second.
        let (first2_port, _) = counter_redis(9).await;
        let (second2_port, second2_seen) = counter_redis(4).await;
        let first2 =
            DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{first2_port}")).unwrap())
                .await
                .unwrap();
        let second2 =
            DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{second2_port}")).unwrap())
                .await
                .unwrap();
        let ha2 = HaServer::new(first2, Some(second2)).with_double_write(true);
        assert_eq!(raw_incr(&ha2, "counter").await, crate::types::Value::Int(9));
        let log2 = second2_seen.lock().unwrap().join("|");
        assert!(log2.contains("INCR"), "expected replay on second: {log2}");
    }

    #[tokio::test]
    async fn ha_write_falls_back_to_second_when_first_down() {
        // first points at a closed port; second serves.
        let (second_port, _) = counter_redis(5).await;
        let first = DirectClient::connect(ServerConfig::new("127.0.0.1:1").unwrap())
            .await;
        // Connecting to a dead port must fail fast at warm-up.
        assert!(first.is_err());

        let first = DirectClient::connect(
            ServerConfig::new(&format!("127.0.0.1:{second_port}")).unwrap(),
        )
        .await
        .unwrap();
        first.pooled_client().pause(); // simulate a tripped breaker
        let second =
            DirectClient::connect(ServerConfig::new(&format!("127.0.0.1:{second_port}")).unwrap())
                .await
                .unwrap();
        let ha = HaServer::new(first, Some(second)).with_double_write(true);
        assert_eq!(raw_incr(&ha, "counter").await, crate::types::Value::Int(5));
    }
}
