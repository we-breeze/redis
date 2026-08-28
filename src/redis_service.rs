//! Configuration-source-independent direct sharded Redis service.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures_util::future::try_join_all;

use crate::direct::{ServerConfig, Shards};
use crate::{ErrorKind, MsRedis, Redis, RedisBytes, RedisError, RedisResult};

/// Hash and distribution names understood by Breeze's direct sharding layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRouting {
    hash_algorithm: String,
    distribution: String,
}

impl ShardRouting {
    /// Creates an explicit Breeze hash/distribution pair.
    pub fn new(hash_algorithm: impl Into<String>, distribution: impl Into<String>) -> Self {
        Self {
            hash_algorithm: hash_algorithm.into(),
            distribution: distribution.into(),
        }
    }

    /// Creates the Java `hashGene`-compatible range distribution.
    ///
    /// For example, `range("crc32", 256)` with 16 shards matches
    /// `ShardingSupportHash(hashAlg=crc32, hashGene=256, tablePerDb=16)`.
    /// This is the standard Java CRC32 mapping. The cache-client-specific
    /// convention that rewrites a configured `crc32` to `crc32-short` applies
    /// to memcached and must not be used for this Redis sharding component.
    pub fn range(hash_algorithm: impl Into<String>, slots: u64) -> Self {
        Self::new(hash_algorithm, format!("range-{slots}"))
    }

    pub fn hash_algorithm(&self) -> &str {
        &self.hash_algorithm
    }

    pub fn distribution(&self) -> &str {
        &self.distribution
    }
}

/// Connection-pool settings used while building every shard.
#[derive(Clone, Debug)]
pub struct RedisServiceOptions {
    min_connections: usize,
    max_connections: usize,
    max_inflight: usize,
    master_timeout: Duration,
    slave_timeout: Duration,
}

impl Default for RedisServiceOptions {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 16,
            max_inflight: 4096,
            master_timeout: Duration::from_millis(500),
            slave_timeout: Duration::from_millis(500),
        }
    }
}

impl RedisServiceOptions {
    #[must_use]
    pub fn with_min_connections(mut self, value: usize) -> Self {
        self.min_connections = value;
        self
    }

    #[must_use]
    pub fn with_max_connections(mut self, value: usize) -> Self {
        self.max_connections = value;
        self
    }

    #[must_use]
    pub fn with_max_inflight(mut self, value: usize) -> Self {
        self.max_inflight = value;
        self
    }

    #[must_use]
    pub fn with_master_timeout(mut self, value: Duration) -> Self {
        self.master_timeout = value;
        self
    }

    #[must_use]
    pub fn with_slave_timeout(mut self, value: Duration) -> Self {
        self.slave_timeout = value;
        self
    }
}

struct RedisServiceInner {
    topology: ArcSwap<Shards<MsRedis>>,
    routing: ShardRouting,
    options: RedisServiceOptions,
}

/// A pooled direct Redis implementation with client-side sharding.
///
/// This type does not know where configuration came from. Callers resolve
/// properties, Vintage, or another source into `(master, slaves)` values
/// before constructing it.
#[derive(Clone)]
pub struct RedisService {
    inner: Arc<RedisServiceInner>,
}

impl RedisService {
    /// Builds a direct sharded service using the SDK's pool defaults.
    pub async fn sharded(
        shards: Vec<(String, Vec<String>)>,
        routing: ShardRouting,
    ) -> RedisResult<Self> {
        Self::sharded_with_options(shards, routing, RedisServiceOptions::default()).await
    }

    /// Builds a direct sharded service with explicit pool settings.
    pub async fn sharded_with_options(
        shards: Vec<(String, Vec<String>)>,
        routing: ShardRouting,
        options: RedisServiceOptions,
    ) -> RedisResult<Self> {
        validate_options(&options)?;
        let topology = build_topology(shards, &routing, &options).await?;
        Ok(Self {
            inner: Arc::new(RedisServiceInner {
                topology: ArcSwap::from_pointee(topology),
                routing,
                options,
            }),
        })
    }

    /// Rebuilds all pools and atomically publishes the new shard list.
    ///
    /// A failed build leaves the previous topology serving traffic.
    pub async fn update_shards(&self, shards: Vec<(String, Vec<String>)>) -> RedisResult<()> {
        let topology = build_topology(shards, &self.inner.routing, &self.inner.options).await?;
        self.inner.topology.store(Arc::new(topology));
        Ok(())
    }

    fn shard_for(&self, routing_key: &[u8]) -> MsRedis {
        self.inner.topology.load().for_key(routing_key).clone()
    }
}

#[async_trait]
impl Redis for RedisService {
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::get(&self.shard_for(key.as_bytes()), key).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> RedisResult<()> {
        Redis::set(&self.shard_for(key.as_bytes()), key, value).await
    }

    async fn get_routed(&self, routing_key: &[u8], key: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::get(&self.shard_for(routing_key), key).await
    }

    async fn set_routed(&self, routing_key: &[u8], key: &str, value: &[u8]) -> RedisResult<()> {
        Redis::set(&self.shard_for(routing_key), key, value).await
    }

    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::hget(&self.shard_for(key.as_bytes()), key, field).await
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>> {
        Redis::hmget(&self.shard_for(key.as_bytes()), key, fields).await
    }
}

async fn build_topology(
    shards: Vec<(String, Vec<String>)>,
    routing: &ShardRouting,
    options: &RedisServiceOptions,
) -> RedisResult<Shards<MsRedis>> {
    validate_routing(routing, shards.len())?;
    let mut master_labels = HashSet::with_capacity(shards.len());
    let mut builds = Vec::with_capacity(shards.len());
    let mut names = Vec::with_capacity(shards.len());

    for (master_endpoint, slave_endpoints) in shards {
        let mut master = configured_server(&master_endpoint, options, options.master_timeout)?;
        let master_label = master.label();
        if !master_labels.insert(master_label.clone()) {
            return Err(client_error(format!(
                "duplicate shard master backend {master_label}"
            )));
        }
        master.read_only = false;
        let slaves = slave_endpoints
            .iter()
            .map(|endpoint| {
                let mut server = configured_server(endpoint, options, options.slave_timeout)?;
                server.read_only = true;
                Ok(server)
            })
            .collect::<RedisResult<Vec<_>>>()?;
        names.push(master_label);
        builds.push(MsRedis::from_server_configs(master, slaves));
    }

    let clients = try_join_all(builds).await?;
    Ok(Shards::new(
        routing.hash_algorithm(),
        routing.distribution(),
        names,
        clients,
    ))
}

fn configured_server(
    endpoint: &str,
    options: &RedisServiceOptions,
    timeout: Duration,
) -> RedisResult<ServerConfig> {
    let mut config = ServerConfig::new(endpoint.trim())?;
    config.min_connections = options.min_connections;
    config.max_connections = options.max_connections;
    config.max_inflight = options.max_inflight;
    config.op_timeout = timeout;
    Ok(config)
}

fn validate_options(options: &RedisServiceOptions) -> RedisResult<()> {
    if options.max_connections == 0 {
        return Err(client_error("max_connections must be greater than zero"));
    }
    if options.min_connections > options.max_connections {
        return Err(client_error(
            "min_connections must not exceed max_connections",
        ));
    }
    if options.max_inflight == 0 {
        return Err(client_error("max_inflight must be greater than zero"));
    }
    if options.master_timeout.is_zero() || options.slave_timeout.is_zero() {
        return Err(client_error("Redis operation timeouts must be non-zero"));
    }
    Ok(())
}

fn validate_routing(routing: &ShardRouting, shard_count: usize) -> RedisResult<()> {
    if shard_count == 0 {
        return Err(client_error("at least one Redis shard is required"));
    }
    match routing.hash_algorithm().to_ascii_lowercase().as_str() {
        "crc32"
        | "crc32-short"
        | "crc32-smartnum"
        | "crc32-mixnum"
        | "crc32-num"
        | "crc32local"
        | "crc32local-smartnum"
        | "raw"
        | "bkdr"
        | "crc64"
        | "fnv1_32"
        | "fnv1a_64" => {}
        algorithm => {
            return Err(client_error(format!(
                "unsupported Redis hash algorithm {algorithm:?}"
            )));
        }
    }

    let distribution = routing.distribution().to_ascii_lowercase();
    if let Some(slot) = distribution.strip_prefix("range-") {
        let slot = slot
            .parse::<u64>()
            .map_err(|_| client_error(format!("invalid range distribution {distribution:?}")))?;
        if slot < shard_count as u64 {
            return Err(client_error(format!(
                "range slot count {slot} is smaller than shard count {shard_count}"
            )));
        }
        if !shard_count.is_power_of_two() {
            return Err(client_error(
                "range distribution requires a power-of-two shard count",
            ));
        }
    } else if !matches!(
        distribution.as_str(),
        "range" | "modula" | "absmodula" | "ketama" | "ketama_origin" | "secmod"
    ) {
        return Err(client_error(format!(
            "unsupported Redis distribution {distribution:?}"
        )));
    }
    Ok(())
}

fn client_error(message: impl Into<String>) -> RedisError {
    RedisError::new(ErrorKind::ClientError, message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::{JoinHandle, JoinSet};

    use crate::direct::sharding::Sharding;
    use crate::direct::sharding::hash::{Hash, Hasher};

    use super::{RedisService, RedisServiceOptions, ShardRouting};

    struct FakeRedis {
        endpoint: String,
        accepted: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<String>>>,
        task: JoinHandle<()>,
    }

    impl FakeRedis {
        async fn start(read_value: &'static str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let accepted = Arc::new(AtomicUsize::new(0));
            let server_accepted = Arc::clone(&accepted);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let server_seen = Arc::clone(&seen);
            let task = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let Ok((mut socket, _)) = accepted else {
                                break;
                            };
                            server_accepted.fetch_add(1, Ordering::Relaxed);
                            let seen = Arc::clone(&server_seen);
                            connections.spawn(async move {
                                let mut buffer = [0_u8; 4096];
                                loop {
                                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                                    if read == 0 {
                                        break;
                                    }
                                    let command = String::from_utf8_lossy(&buffer[..read])
                                        .to_uppercase();
                                    seen.lock().unwrap().push(command.clone());
                                    let response = if command.contains("\r\nGET\r\n") {
                                        format!("${}\r\n{read_value}\r\n", read_value.len())
                                    } else if command.contains("\r\nSET\r\n") {
                                        "+OK\r\n".to_owned()
                                    } else if command.contains("PING") {
                                        "+PONG\r\n".to_owned()
                                    } else {
                                        ":1\r\n".to_owned()
                                    };
                                    if socket.write_all(response.as_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                        Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    }
                }
            });
            Self {
                endpoint,
                accepted,
                seen,
                task,
            }
        }

        fn accepted(&self) -> usize {
            self.accepted.load(Ordering::Relaxed)
        }

        fn saw(&self, command: &str) -> bool {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .any(|seen| seen.contains(command))
        }
    }

    impl Drop for FakeRedis {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[test]
    fn crc32_java_mapping_keeps_standard_and_short_distinct() {
        let key = b"123456789".as_slice();
        let standard = Hasher::from("crc32").hash(&key);
        let short = Hasher::from("crc32-short").hash(&key);

        // java.util.zip.CRC32("123456789") == 0xCBF43926.
        assert_eq!(standard, 0xcbf4_3926);
        assert_eq!(short, (standard >> 16) & 0x7fff);
        assert_eq!(short, 0x4bf4);
        assert_ne!(standard, short);

        // reference-library HashUtilTest.testGetHashCrc32 uses these exact expected
        // values for Java's `(crc32 / splitCount) % splitCount` mapping.
        let java_uid_crc = Hasher::from("crc32").hash(&b"1821155363".as_slice());
        assert_eq!((java_uid_crc / 32) % 32, 12);
        assert_eq!((java_uid_crc / 128) % 128, 51);
    }

    #[test]
    fn explicit_uid_route_matches_java_range_sharding() {
        let names = (0..16)
            .map(|index| format!("shard-{index}"))
            .collect::<Vec<_>>();
        let sharding = Sharding::new("crc32", "range-256", &names);
        let mut state = 0x4d59_5df4_d0f3_3173_u64;

        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let uid = 10_000 + state % 2_999_999_999_990_000;
            let uid = uid.to_string();
            let expected_crc = java_crc32(uid.as_bytes());
            let expected_slot = ((expected_crc / 256) % 256) / 16;

            assert_eq!(sharding.hash(uid.as_bytes()), expected_crc as i64);
            assert_eq!(sharding.shard_idx(uid.as_bytes()), expected_slot as usize);
        }
    }

    #[test]
    fn java_sharding_support_hash_test_vectors_match_range() {
        // Copied from reference-library ShardingSupportHashTest.testGetDbTableUid:
        // hashAlg=crc32, hashGene=1024, tablePerDb=64, noneHash=new.
        let vectors = [
            (1_750_715_731_u64, 15_usize),
            (1_821_155_363, 13),
            (1_779_195_673, 11),
            (1_734_528_095, 0),
            (10_503, 14),
        ];
        let names = (0..16)
            .map(|index| format!("shard-{index}"))
            .collect::<Vec<_>>();
        let sharding = Sharding::new("crc32", "range-1024", &names);

        for (uid, expected_db) in vectors {
            assert_eq!(sharding.shard_idx(uid.to_string().as_bytes()), expected_db);
        }
    }

    #[test]
    fn smartnum_is_only_an_equivalent_key_mapping_not_crc32_short() {
        let uid = b"4066469060".as_slice();
        let redis_key = b"u:4066469060".as_slice();
        let standard = Hasher::from("crc32").hash(&uid);

        assert_eq!(Hasher::from("crc32-smartnum").hash(&redis_key), standard);
        assert_ne!(Hasher::from("crc32-short").hash(&uid), standard);
    }

    #[tokio::test]
    async fn reuses_pooled_connections_and_splits_reads_from_writes() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::sharded_with_options(
            vec![(master.endpoint.clone(), vec![slave.endpoint.clone()])],
            ShardRouting::range("crc32", 256),
            RedisServiceOptions::default()
                .with_min_connections(1)
                .with_max_connections(1),
        )
        .await
        .unwrap();

        // Construction warms exactly one connection for each endpoint.
        while master.accepted() == 0 || slave.accepted() == 0 {
            tokio::task::yield_now().await;
        }
        let initial_master_connections = master.accepted();
        let initial_slave_connections = slave.accepted();

        for _ in 0..20 {
            let value = crate::Redis::get_routed(&redis, b"1821155363", "u:1821155363")
                .await
                .unwrap();
            assert_eq!(value.as_deref(), Some(b"slave".as_slice()));
        }
        crate::Redis::set_routed(&redis, b"1821155363", "u:1821155363", b"\x00\x7f\xff")
            .await
            .unwrap();

        assert!(slave.saw("\r\nGET\r\n"));
        assert!(!master.saw("\r\nGET\r\n"));
        assert!(master.saw("\r\nSET\r\n"));
        assert!(!slave.saw("\r\nSET\r\n"));
        assert_eq!(master.accepted(), initial_master_connections);
        assert_eq!(slave.accepted(), initial_slave_connections);
    }

    fn java_crc32(bytes: &[u8]) -> u32 {
        let mut crc = u32::MAX;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0xedb8_8320
                } else {
                    crc >> 1
                };
            }
        }
        crc ^ u32::MAX
    }
}
