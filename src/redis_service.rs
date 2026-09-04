//! Configuration-source-independent sharded Redis service on `brz-net`.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use brz_net::{
    DnsOptions, DnsSource, EndpointSet, EndpointSource, EphemeralBytesArena, MAX_IN_FLIGHT,
    NetError, Node, NodeOptions, QuotaBalancerOptions, ReplicaSet, ReplicaSetResponseFuture,
    ResponseFuture, SessionError, SessionReplica, ShardRouter, Sharded,
};
use futures_util::future::try_join_all;
use tokio::time::{Instant, MissedTickBehavior, sleep};

use crate::mesh::MeshConfig;
use crate::multi_key::{KeyListArgs, KeyRoute, classify_keys, group_keys};
use crate::net_transport::{
    RedisProtocol, RedisRequest, RedisResponse, RedisResponseKind, map_session_error,
};
#[cfg(feature = "metrics")]
use crate::profile_metrics::{EndpointProfileMetric, ProfiledResponseFuture};
use crate::sharding::Sharding;
use crate::{
    Cmd, EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, FromRedisValue, PipeResponse,
    Redis, RedisArgsSink, RedisError, RedisPipe, RedisResult, RedisValues,
};

const DNS_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

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

/// Transport settings used while building every shard.
#[derive(Clone, Debug)]
pub struct RedisServiceOptions {
    master_timeout: Duration,
    slave_timeout: Duration,
    connect_timeout: Duration,
    dns_refresh_interval: Duration,
    replica_balance: QuotaBalancerOptions,
}

impl Default for RedisServiceOptions {
    fn default() -> Self {
        Self {
            master_timeout: Duration::from_millis(200),
            slave_timeout: Duration::from_millis(200),
            connect_timeout: Duration::from_secs(2),
            dns_refresh_interval: Duration::from_secs(30),
            replica_balance: QuotaBalancerOptions::default(),
        }
    }
}

impl RedisServiceOptions {
    /// Set the request timeout for both master and slave sessions.
    #[must_use]
    pub fn with_timeout(mut self, value: Duration) -> Self {
        self.master_timeout = value;
        self.slave_timeout = value;
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

    #[must_use]
    pub fn with_connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    #[must_use]
    pub fn with_dns_refresh_interval(mut self, value: Duration) -> Self {
        self.dns_refresh_interval = value;
        self
    }

    #[must_use]
    pub fn with_replica_quota(mut self, value: Duration) -> Self {
        self.replica_balance.quota = value;
        self
    }

    #[must_use]
    pub fn with_failure_penalty(mut self, value: Duration) -> Self {
        self.replica_balance.failure_penalty = value;
        self
    }
}

type RedisNodeResult = Result<RedisResponse, SessionError<RedisError>>;

#[cfg(feature = "metrics")]
type RedisNodeResponseFuture = ProfiledResponseFuture<ResponseFuture<RedisNodeResult>>;
#[cfg(not(feature = "metrics"))]
type RedisNodeResponseFuture = ResponseFuture<RedisNodeResult>;

/// One physical Redis session plus the logical endpoint state shared by all
/// addresses resolved from the same configured `host:port`.
#[derive(Clone)]
pub(crate) struct RedisReplica {
    node: Node<RedisProtocol>,
    #[cfg(feature = "metrics")]
    profile_metric: Arc<EndpointProfileMetric>,
}

impl RedisReplica {
    #[cfg(feature = "metrics")]
    fn new(node: Node<RedisProtocol>, profile_metric: Arc<EndpointProfileMetric>) -> Self {
        Self {
            node,
            profile_metric,
        }
    }

    #[cfg(not(feature = "metrics"))]
    fn new(node: Node<RedisProtocol>) -> Self {
        Self { node }
    }

    fn is_connected(&self) -> bool {
        self.node.is_connected()
    }

    fn endpoint(&self) -> SocketAddr {
        self.node.endpoint()
    }
}

impl SessionReplica for RedisReplica {
    type Request = RedisRequest;
    type Response = RedisResponse;
    type Error = RedisError;
    type Future = RedisNodeResponseFuture;

    fn request(&self, request: Self::Request) -> Result<Self::Future, SessionError<Self::Error>> {
        #[cfg(feature = "metrics")]
        {
            let attempt = self.profile_metric.attempt();
            let response = self.node.request(request)?;
            Ok(attempt.wrap(response))
        }
        #[cfg(not(feature = "metrics"))]
        {
            self.node.request(request)
        }
    }

    fn request_with<F>(&self, build: F) -> Result<Self::Future, SessionError<Self::Error>>
    where
        F: FnOnce() -> Self::Request,
    {
        #[cfg(feature = "metrics")]
        {
            let attempt = self.profile_metric.attempt();
            let response = self.node.request_with(build)?;
            Ok(attempt.wrap(response))
        }
        #[cfg(not(feature = "metrics"))]
        {
            self.node.request_with(build)
        }
    }

    fn request_batch(
        &self,
        requests: Vec<Self::Request>,
    ) -> Result<Vec<Self::Future>, SessionError<Self::Error>> {
        #[cfg(feature = "metrics")]
        {
            let count = requests.len();
            let started = std::time::Instant::now();
            let responses = match self.node.request_batch(requests) {
                Ok(responses) => responses,
                Err(error) => {
                    self.profile_metric.record_batch_failure(count, started);
                    return Err(error);
                }
            };
            Ok(responses
                .into_iter()
                .map(|response| {
                    self.profile_metric
                        .attempt_started_at(started)
                        .wrap(response)
                })
                .collect())
        }
        #[cfg(not(feature = "metrics"))]
        {
            self.node.request_batch(requests)
        }
    }
}

pub(crate) type RedisReplicaResponseFuture = ReplicaSetResponseFuture<RedisReplica>;

struct RedisShard {
    // A hostname may resolve to more than one equivalent IPv4 address.
    master: ReplicaSet<RedisReplica>,
    slaves: ReplicaSet<RedisReplica>,
}

#[derive(Clone, Debug)]
struct RedisRouter {
    sharding: Sharding,
}

impl ShardRouter<[u8]> for RedisRouter {
    #[inline]
    fn route(&self, key: &[u8], shard_count: usize) -> usize {
        if shard_count == 1 {
            return 0;
        }
        let index = self.sharding.shard_idx(key);
        debug_assert!(index < shard_count);
        index
    }
}

pub(crate) struct RedisTopology {
    shards: Sharded<RedisShard, RedisRouter>,
    discovery: Arc<RedisDiscovery>,
    resolved: ResolvedTopology,
    nodes: HashMap<NodeKey, Node<RedisProtocol>>,
}

struct RedisServiceInner {
    topology: ArcSwap<RedisTopology>,
    options: RedisServiceOptions,
    request_arena: EphemeralBytesArena,
}

#[derive(Clone)]
struct RedisEndpointConfig {
    host: String,
    port: u16,
    db: i64,
    auth: Option<String>,
}

impl RedisEndpointConfig {
    fn label(&self) -> String {
        format!("{}:{}:{}", self.host, self.port, self.db)
    }

    #[cfg(feature = "metrics")]
    fn profile_name(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Clone)]
struct RedisEndpointSource {
    config: RedisEndpointConfig,
    endpoints: EndpointSet,
}

struct RedisShardSources {
    master: RedisEndpointSource,
    slaves: Vec<RedisEndpointSource>,
}

struct RedisDiscovery {
    shards: Vec<RedisShardSources>,
    router: RedisRouter,
    // Keeping the registrations alive keeps the shared DNS resolver watching
    // every logical endpoint. The data used below is the cloned EndpointSet.
    _dns_registrations: Vec<DnsSource>,
}

#[derive(Clone)]
struct ResolvedEndpoint {
    config: RedisEndpointConfig,
    addresses: Arc<[SocketAddr]>,
}

#[derive(Clone)]
struct ResolvedShard {
    master: ResolvedEndpoint,
    slaves: Vec<ResolvedEndpoint>,
}

#[derive(Clone)]
struct ResolvedTopology {
    shards: Vec<ResolvedShard>,
}

#[derive(Clone, Eq, PartialEq)]
struct NodeKey {
    endpoint: SocketAddr,
    db: i64,
    auth: Option<String>,
    request_timeout: Duration,
}

impl Hash for NodeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.db.hash(state);
        self.auth.hash(state);
        self.request_timeout.hash(state);
    }
}

impl RedisDiscovery {
    fn resolve(&self) -> ResolvedTopology {
        ResolvedTopology {
            shards: self
                .shards
                .iter()
                .map(|shard| ResolvedShard {
                    master: resolve_source(&shard.master),
                    slaves: shard.slaves.iter().map(resolve_source).collect(),
                })
                .collect(),
        }
    }
}

impl RedisTopology {
    fn dns_changed(&self) -> bool {
        self.discovery
            .shards
            .iter()
            .zip(&self.resolved.shards)
            .any(|(sources, resolved)| {
                !Arc::ptr_eq(
                    &sources.master.endpoints.snapshot(),
                    &resolved.master.addresses,
                ) || sources
                    .slaves
                    .iter()
                    .zip(&resolved.slaves)
                    .any(|(source, endpoint)| {
                        !Arc::ptr_eq(&source.endpoints.snapshot(), &endpoint.addresses)
                    })
            })
    }
}

fn resolve_source(source: &RedisEndpointSource) -> ResolvedEndpoint {
    ResolvedEndpoint {
        config: source.config.clone(),
        addresses: source.endpoints.snapshot(),
    }
}

/// Direct Redis access over persistent multiplexed sessions.
///
/// This type does not know where configuration came from. Callers resolve a
/// single endpoint or `(master, slaves)` groups from properties, Vintage, or
/// another source before constructing it.
#[derive(Clone)]
pub struct RedisService {
    inner: Arc<RedisServiceInner>,
}

impl RedisService {
    /// Discovers one local Breeze mesh TCP endpoint and builds a fixed service.
    ///
    /// Registry discovery happens once. The resolved endpoint then has exactly
    /// the same semantics as [`RedisService::single`].
    pub async fn mesh(group: impl Into<String>, namespace: impl Into<String>) -> RedisResult<Self> {
        Self::mesh_with_options(group, namespace, RedisServiceOptions::default()).await
    }

    /// Discovers one local Breeze mesh endpoint with explicit transport options.
    pub async fn mesh_with_options(
        group: impl Into<String>,
        namespace: impl Into<String>,
        options: RedisServiceOptions,
    ) -> RedisResult<Self> {
        Self::mesh_with_config(MeshConfig::new(group, namespace), options).await
    }

    /// Builds a fixed service from explicit mesh discovery coordinates.
    pub async fn mesh_with_config(
        config: MeshConfig,
        options: RedisServiceOptions,
    ) -> RedisResult<Self> {
        let endpoint = config.resolve()?;
        Self::single_with_options(format!("{}:{}", endpoint.host, endpoint.port), options).await
    }

    /// Builds one direct endpoint using the transport defaults.
    ///
    /// Internally the same address occupies the master and slave roles, so
    /// reads and writes use two independent physical connections.
    pub async fn single(endpoint: impl Into<String>) -> RedisResult<Self> {
        Self::single_with_options(endpoint, RedisServiceOptions::default()).await
    }

    /// Builds one direct endpoint with explicit transport settings.
    pub async fn single_with_options(
        endpoint: impl Into<String>,
        options: RedisServiceOptions,
    ) -> RedisResult<Self> {
        let endpoint = endpoint.into();
        Self::noshard_with_options(endpoint.clone(), [endpoint], options).await
    }

    /// Builds one unsharded master/slave service using the transport defaults.
    ///
    /// Reads use the slave replica set and writes use the master. Because the
    /// topology contains exactly one shard, requests skip key encoding and
    /// hashing during routing.
    pub async fn noshard<M, I, S>(master: M, slaves: I) -> RedisResult<Self>
    where
        M: Into<String>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::noshard_with_options(master, slaves, RedisServiceOptions::default()).await
    }

    /// Builds one unsharded master/slave service with explicit settings.
    pub async fn noshard_with_options<M, I, S>(
        master: M,
        slaves: I,
        options: RedisServiceOptions,
    ) -> RedisResult<Self>
    where
        M: Into<String>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let slaves = slaves.into_iter().map(Into::into).collect();
        Self::sharded_with_options(
            vec![(master.into(), slaves)],
            ShardRouting::new("raw", "modula"),
            options,
        )
        .await
    }

    /// Builds a direct sharded service using the SDK's transport defaults.
    pub async fn sharded(
        shards: Vec<(String, Vec<String>)>,
        routing: ShardRouting,
    ) -> RedisResult<Self> {
        Self::sharded_with_options(shards, routing, RedisServiceOptions::default()).await
    }

    /// Builds a direct sharded service with explicit transport settings.
    pub async fn sharded_with_options(
        shards: Vec<(String, Vec<String>)>,
        routing: ShardRouting,
        options: RedisServiceOptions,
    ) -> RedisResult<Self> {
        validate_options(&options)?;
        let request_arena = RedisRequest::shared_arena();
        let discovery = build_discovery(shards, &routing, &options).await?;
        let topology = build_topology(discovery, &options, None).await?;
        let service = Self {
            inner: Arc::new(RedisServiceInner {
                topology: ArcSwap::from_pointee(topology),
                options,
                request_arena,
            }),
        };
        spawn_dns_reconciler(&service.inner);
        Ok(service)
    }

    async fn execute<H, F>(&self, key: &H, readonly: bool, build: F) -> RedisResult<RedisResponse>
    where
        H: EncodeRedisArg + ?Sized,
        F: FnMut(&EphemeralBytesArena) -> RedisRequest,
    {
        // Keep the immutable topology alive until the admitted request
        // completes. A concurrent update can publish a new topology without
        // dropping this request's Node sender underneath it.
        let topology = self.inner.topology.load_full();
        let shard = if topology.shards.shard_count() == 1 {
            topology.shards.get(&[][..]).map_err(map_net_error)?
        } else {
            let key = crate::arg::encode_arg_contiguous(key)?;
            topology.shards.get(key.as_ref()).map_err(map_net_error)?
        };

        self.execute_on_shard(shard, readonly, build).await
    }

    async fn execute_for_keys<K, F>(
        &self,
        keys: &K,
        readonly: bool,
        build: F,
    ) -> RedisResult<RedisResponse>
    where
        K: EncodeRedisArgs + ?Sized,
        F: FnMut(&EphemeralBytesArena) -> RedisRequest,
    {
        let topology = self.inner.topology.load_full();
        let shard = match classify_keys(&topology.shards, keys)? {
            KeyRoute::Single(shard) => shard,
            KeyRoute::Multiple => {
                return Err(RedisError::new(
                    ErrorKind::ClientError,
                    "Redis command requires all keys to resolve to the same shard",
                ));
            }
        };
        self.execute_on_shard(shard, readonly, build).await
    }

    async fn execute_on_shard<F>(
        &self,
        shard: &RedisShard,
        readonly: bool,
        mut build: F,
    ) -> RedisResult<RedisResponse>
    where
        F: FnMut(&EphemeralBytesArena) -> RedisRequest,
    {
        let response = if readonly {
            let response = shard
                .slaves
                .request_with_failover(1, || build(&self.inner.request_arena))
                .await;
            if shard.slaves.len() == 1
                && response
                    .as_ref()
                    .is_err_and(|error| error.is_retryable_transport())
            {
                // Match reference-client: when there is only one slave, the one
                // bounded read retry falls back to the master replica group.
                match shard
                    .master
                    .request_with(|| build(&self.inner.request_arena))
                {
                    Ok(response) => response.await,
                    Err(error) => Err(error),
                }
            } else {
                response
            }
        } else {
            match shard
                .master
                .request_with(|| build(&self.inner.request_arena))
            {
                Ok(response) => response.await,
                Err(error) => Err(error),
            }
        };
        response.map_err(map_session_error)
    }

    async fn execute_script<D, K, A>(
        &self,
        command: &'static str,
        digest: &D,
        keys: &K,
        arguments: &A,
    ) -> RedisResult<crate::Value>
    where
        D: EncodeRedisArg + ?Sized,
        K: EncodeRedisArgs + ?Sized,
        A: EncodeRedisArgs + ?Sized,
    {
        let response = if keys.num_args() == 0 {
            self.execute(&"", false, |arena| {
                RedisRequest::encode(
                    arena,
                    &ScriptArgs {
                        command,
                        digest,
                        keys,
                        arguments,
                    },
                    RedisResponseKind::Value,
                )
            })
            .await?
        } else {
            self.execute_for_keys(keys, false, |arena| {
                RedisRequest::encode(
                    arena,
                    &ScriptArgs {
                        command,
                        digest,
                        keys,
                        arguments,
                    },
                    RedisResponseKind::Value,
                )
            })
            .await?
        };
        response.into_value()
    }

    fn validate_pipe(pipe: &RedisPipe) -> RedisResult<()> {
        if pipe.len() > MAX_IN_FLIGHT {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                format!(
                    "Redis pipeline contains {} commands, maximum is {MAX_IN_FLIGHT}",
                    pipe.len()
                ),
            ));
        }
        Ok(())
    }

    fn execute_pipe(&self, pipe: RedisPipe) -> RedisResult<PipeResponse> {
        Self::validate_pipe(&pipe)?;
        let topology = self.inner.topology.load_full();
        if topology.shards.shard_count() != 1 {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                "Redis pipelines are not supported on multi-shard services",
            ));
        }
        if pipe.is_empty() {
            return Ok(PipeResponse::ready(Vec::new()));
        }
        let readonly = pipe.is_readonly();
        let shard = topology.shards.get(&[][..]).map_err(map_net_error)?;
        let requests = pipe.into_requests(&self.inner.request_arena);
        let responses = if readonly {
            shard.slaves.request_batch(requests)
        } else {
            shard.master.request_batch(requests)
        }
        .map_err(map_session_error)?;
        Ok(PipeResponse::direct(responses, topology))
    }
}

struct HmgetArgs<'a, K: ?Sized, F: ?Sized> {
    key: &'a K,
    fields: &'a F,
}

struct ScriptArgs<'a, D: ?Sized, K: ?Sized, A: ?Sized> {
    command: &'static str,
    digest: &'a D,
    keys: &'a K,
    arguments: &'a A,
}

impl<D, K, A> EncodeRedisArgs for ScriptArgs<'_, D, K, A>
where
    D: EncodeRedisArg + ?Sized,
    K: EncodeRedisArgs + ?Sized,
    A: EncodeRedisArgs + ?Sized,
{
    fn num_args(&self) -> usize {
        3_usize
            .saturating_add(self.keys.num_args())
            .saturating_add(self.arguments.num_args())
    }

    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write_arg(self.command)?;
        sink.write_arg(self.digest)?;
        sink.write_arg(&self.keys.num_args())?;
        self.keys.encode_args(sink)?;
        self.arguments.encode_args(sink)
    }
}

impl<K, F> EncodeRedisArgs for HmgetArgs<'_, K, F>
where
    K: EncodeRedisArg + ?Sized,
    F: EncodeRedisArgs + ?Sized,
{
    fn num_args(&self) -> usize {
        2_usize.saturating_add(self.fields.num_args())
    }

    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write_arg("HMGET")?;
        sink.write_arg(self.key)?;
        self.fields.encode_args(sink)
    }
}

fn spawn_dns_reconciler(inner: &Arc<RedisServiceInner>) {
    let inner = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            Instant::now() + DNS_RECONCILE_INTERVAL,
            DNS_RECONCILE_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(inner) = inner.upgrade() else {
                return;
            };
            if let Err(error) = reconcile_dns_once(&inner).await {
                tracing::warn!(%error, "failed to apply changed Redis DNS endpoints");
            }
        }
    });
}

async fn reconcile_dns_once(inner: &RedisServiceInner) -> RedisResult<bool> {
    let previous = inner.topology.load_full();
    if !previous.dns_changed() {
        return Ok(false);
    }

    let topology = build_topology(
        Arc::clone(&previous.discovery),
        &inner.options,
        Some(&previous),
    )
    .await?;
    let replaced = inner
        .topology
        .compare_and_swap(&previous, Arc::new(topology));
    Ok(Arc::ptr_eq(&replaced, &previous))
}

impl Redis for RedisService {
    async fn command<R>(&self, command: Cmd) -> RedisResult<R>
    where
        R: FromRedisValue + Send,
    {
        if command.arg_count() == 0 {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                "Redis command must contain a command name",
            ));
        }
        let readonly = command.is_readonly();
        let key = command.arg_at(1).unwrap_or_default();
        let value = self
            .execute(key, readonly, |arena| {
                RedisRequest::encode(arena, &command, RedisResponseKind::Value)
            })
            .await?
            .into_value()?;
        R::from_redis_value(&value)
    }

    async fn pipe(&self, pipe: RedisPipe) -> RedisResult<PipeResponse> {
        self.execute_pipe(pipe)
    }

    async fn get<K, R>(&self, key: K) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        self.execute(&key, true, |arena| {
            RedisRequest::encode(arena, &("GET", &key), RedisResponseKind::Bulk)
        })
        .await?
        .into_bulk()?
        .map(R::from_redis_bulk)
        .transpose()
    }

    async fn set<K, V>(&self, key: K, value: V) -> RedisResult<()>
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        self.execute(&key, false, |arena| {
            RedisRequest::encode(arena, &("SET", &key, &value), RedisResponseKind::Unit)
        })
        .await?
        .into_unit()
    }

    async fn mget<K, R>(&self, keys: K) -> RedisResult<Vec<Option<R>>>
    where
        K: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send,
    {
        let expected = keys.num_args();
        let topology = self.inner.topology.load_full();
        match classify_keys(&topology.shards, &keys)? {
            KeyRoute::Single(shard) => {
                let values = self
                    .execute_on_shard(shard, true, |arena| {
                        RedisRequest::encode(
                            arena,
                            &KeyListArgs::new("MGET", &keys),
                            RedisResponseKind::MultiBulk { expected },
                        )
                    })
                    .await?
                    .into_multi_bulk::<R>()?;
                values.collect()
            }
            KeyRoute::Multiple => {
                let groups = group_keys(&topology.shards, "MGET", &keys)?;
                let responses = try_join_all(groups.iter().map(|group| {
                    self.execute_on_shard(group.target, true, |arena| {
                        RedisRequest::encode(
                            arena,
                            &group.command,
                            RedisResponseKind::MultiBulk {
                                expected: group.positions.len(),
                            },
                        )
                    })
                }))
                .await?;
                let mut output = Vec::with_capacity(expected);
                output.resize_with(expected, || None);
                for (group, response) in groups.iter().zip(responses) {
                    let values = response.into_multi_bulk::<R>()?;
                    for (&position, value) in group.positions.iter().zip(values) {
                        output[position] = value?;
                    }
                }
                Ok(output)
            }
        }
    }

    async fn del_many<K>(&self, keys: K) -> RedisResult<i64>
    where
        K: EncodeRedisArgs + Send,
    {
        let topology = self.inner.topology.load_full();
        match classify_keys(&topology.shards, &keys)? {
            KeyRoute::Single(shard) => self
                .execute_on_shard(shard, false, |arena| {
                    RedisRequest::encode(
                        arena,
                        &KeyListArgs::new("DEL", &keys),
                        RedisResponseKind::Integer,
                    )
                })
                .await?
                .into_integer(),
            KeyRoute::Multiple => {
                let groups = group_keys(&topology.shards, "DEL", &keys)?;
                let responses = try_join_all(groups.iter().map(|group| {
                    self.execute_on_shard(group.target, false, |arena| {
                        RedisRequest::encode(arena, &group.command, RedisResponseKind::Integer)
                    })
                }))
                .await?;
                responses.into_iter().try_fold(0_i64, |total, response| {
                    total.checked_add(response.into_integer()?).ok_or_else(|| {
                        RedisError::new(ErrorKind::TypeError, "Redis DEL result overflowed i64")
                    })
                })
            }
        }
    }

    async fn eval<S, K, A, R>(&self, script: S, keys: K, arguments: A) -> RedisResult<R>
    where
        S: EncodeRedisArg + Send,
        K: EncodeRedisArgs + Send,
        A: EncodeRedisArgs + Send,
        R: FromRedisValue + Send,
    {
        let value = self
            .execute_script("EVAL", &script, &keys, &arguments)
            .await?;
        R::from_redis_value(&value)
    }

    async fn evalsha<D, K, A, R>(&self, digest: D, keys: K, arguments: A) -> RedisResult<R>
    where
        D: EncodeRedisArg + Send,
        K: EncodeRedisArgs + Send,
        A: EncodeRedisArgs + Send,
        R: FromRedisValue + Send,
    {
        let value = self
            .execute_script("EVALSHA", &digest, &keys, &arguments)
            .await?;
        R::from_redis_value(&value)
    }

    async fn pfcount<K>(&self, keys: K) -> RedisResult<i64>
    where
        K: EncodeRedisArgs + Send,
    {
        self.execute_for_keys(&keys, true, |arena| {
            RedisRequest::encode(
                arena,
                &KeyListArgs::new("PFCOUNT", &keys),
                RedisResponseKind::Integer,
            )
        })
        .await?
        .into_integer()
    }

    async fn hget<K, F, R>(&self, key: K, field: F) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        self.execute(&key, true, |arena| {
            RedisRequest::encode(arena, &("HGET", &key, &field), RedisResponseKind::Bulk)
        })
        .await?
        .into_bulk()?
        .map(R::from_redis_bulk)
        .transpose()
    }

    async fn hset<K, F, V>(&self, key: K, field: F, value: V) -> RedisResult<i64>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        self.execute(&key, false, |arena| {
            RedisRequest::encode(
                arena,
                &("HSET", &key, &field, &value),
                RedisResponseKind::Integer,
            )
        })
        .await?
        .into_integer()
    }

    async fn hmget<K, F, R>(&self, key: K, fields: F) -> RedisResult<RedisValues<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send,
    {
        let expected = fields.num_args();
        self.execute(&key, true, |arena| {
            RedisRequest::encode(
                arena,
                &HmgetArgs {
                    key: &key,
                    fields: &fields,
                },
                RedisResponseKind::MultiBulk { expected },
            )
        })
        .await?
        .into_multi_bulk()
    }
}

async fn build_discovery(
    shards: Vec<(String, Vec<String>)>,
    routing: &ShardRouting,
    options: &RedisServiceOptions,
) -> RedisResult<Arc<RedisDiscovery>> {
    validate_routing(routing, shards.len())?;
    let mut master_labels = HashSet::with_capacity(shards.len());
    let mut builds = Vec::with_capacity(shards.len());
    let mut names = Vec::with_capacity(shards.len());

    for (master_endpoint, slave_endpoints) in shards {
        let master = configured_server(&master_endpoint)?;
        let master_label = master.label();
        if !master_labels.insert(master_label.clone()) {
            return Err(client_error(format!(
                "duplicate shard master backend {master_label}"
            )));
        }
        let slaves = slave_endpoints
            .iter()
            .map(|endpoint| configured_server(endpoint))
            .collect::<RedisResult<Vec<_>>>()?;
        validate_slave_configs(&slaves)?;
        names.push(master_label);
        builds.push(build_shard_sources(master, slaves, options));
    }

    let built = try_join_all(builds).await?;
    let mut registrations = Vec::new();
    let mut shards = Vec::with_capacity(built.len());
    for (shard, mut shard_registrations) in built {
        shards.push(shard);
        registrations.append(&mut shard_registrations);
    }
    let router = RedisRouter {
        sharding: Sharding::new(routing.hash_algorithm(), routing.distribution(), &names),
    };
    Ok(Arc::new(RedisDiscovery {
        shards,
        router,
        _dns_registrations: registrations,
    }))
}

async fn build_shard_sources(
    master: RedisEndpointConfig,
    slaves: Vec<RedisEndpointConfig>,
    options: &RedisServiceOptions,
) -> RedisResult<(RedisShardSources, Vec<DnsSource>)> {
    let (master, slaves) = tokio::try_join!(
        build_endpoint_source(master, options.dns_refresh_interval),
        try_join_all(
            slaves
                .into_iter()
                .map(|config| build_endpoint_source(config, options.dns_refresh_interval))
        ),
    )?;

    let (master, master_registration) = master;
    let mut registrations = Vec::with_capacity(slaves.len() + 1);
    registrations.push(master_registration);
    let mut slave_sources = Vec::with_capacity(slaves.len());
    for (source, registration) in slaves {
        slave_sources.push(source);
        registrations.push(registration);
    }
    Ok((
        RedisShardSources {
            master,
            slaves: slave_sources,
        },
        registrations,
    ))
}

async fn build_endpoint_source(
    config: RedisEndpointConfig,
    refresh_interval: Duration,
) -> RedisResult<(RedisEndpointSource, DnsSource)> {
    let authority = format!("{}:{}", config.host, config.port);
    let registration = DnsSource::new([authority], DnsOptions { refresh_interval })
        .await
        .map_err(map_net_error)?;
    let endpoints = registration.endpoint_set().clone();
    Ok((RedisEndpointSource { config, endpoints }, registration))
}

async fn build_topology(
    discovery: Arc<RedisDiscovery>,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<RedisTopology> {
    let resolved = discovery.resolve();
    let built = try_join_all(
        resolved
            .shards
            .iter()
            .map(|shard| build_resolved_shard(shard, options, previous)),
    )
    .await?;
    let mut nodes = HashMap::new();
    let mut shards = Vec::with_capacity(built.len());
    for (shard, shard_nodes) in built {
        shards.push(shard);
        nodes.extend(shard_nodes);
    }

    Ok(RedisTopology {
        shards: Sharded::new(discovery.router.clone(), shards).map_err(map_net_error)?,
        discovery,
        resolved,
        nodes,
    })
}

async fn build_resolved_shard(
    shard: &ResolvedShard,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<(RedisShard, HashMap<NodeKey, Node<RedisProtocol>>)> {
    let (master, slaves) = tokio::try_join!(
        build_replicas(
            std::slice::from_ref(&shard.master),
            options.master_timeout,
            options,
            previous,
        ),
        build_replicas(&shard.slaves, options.slave_timeout, options, previous),
    )?;
    let mut nodes = master.1;
    nodes.extend(slaves.1);
    Ok((
        RedisShard {
            master: master.0,
            slaves: slaves.0,
        },
        nodes,
    ))
}

async fn build_replicas(
    configs: &[ResolvedEndpoint],
    request_timeout: Duration,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<(
    ReplicaSet<RedisReplica>,
    HashMap<NodeKey, Node<RedisProtocol>>,
)> {
    let mut endpoints = HashSet::new();
    let mut node_cache = HashMap::new();
    let mut nodes = Vec::new();
    for resolved in configs {
        #[cfg(feature = "metrics")]
        let profile_metric = Arc::new(EndpointProfileMetric::new(resolved.config.profile_name()));
        for &address in resolved.addresses.iter() {
            let key = NodeKey {
                endpoint: address,
                db: resolved.config.db,
                auth: resolved.config.auth.clone(),
                request_timeout,
            };
            if !endpoints.insert(key.clone()) {
                continue;
            }
            let node = if let Some(node) = previous.and_then(|topology| topology.nodes.get(&key)) {
                node.clone()
            } else {
                Node::new(
                    address,
                    RedisProtocol::new(resolved.config.auth.clone(), resolved.config.db),
                    NodeOptions {
                        request_timeout,
                        connect_timeout: options.connect_timeout,
                        ..NodeOptions::default()
                    },
                )
                .map_err(map_net_error)?
            };
            node_cache.insert(key, node.clone());
            #[cfg(feature = "metrics")]
            nodes.push(RedisReplica::new(node, Arc::clone(&profile_metric)));
            #[cfg(not(feature = "metrics"))]
            nodes.push(RedisReplica::new(node));
        }
    }

    wait_until_connected(&nodes, options.connect_timeout).await?;
    let replicas =
        ReplicaSet::with_options(nodes, options.replica_balance).map_err(map_net_error)?;
    Ok((replicas, node_cache))
}

async fn wait_until_connected(
    nodes: &[RedisReplica],
    connect_timeout: Duration,
) -> RedisResult<()> {
    let deadline = Instant::now()
        + connect_timeout
            .saturating_mul(2)
            .saturating_add(Duration::from_millis(100));
    while nodes.iter().any(|node| !node.is_connected()) {
        if Instant::now() >= deadline {
            let endpoints = nodes
                .iter()
                .filter(|node| !node.is_connected())
                .map(|node| node.endpoint().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RedisError::new(
                ErrorKind::NoConnection,
                format!("Redis sessions did not become ready: {endpoints}"),
            ));
        }
        sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}

fn configured_server(endpoint: &str) -> RedisResult<RedisEndpointConfig> {
    let endpoint = endpoint.trim();
    let parts = endpoint.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) || parts[0].is_empty() {
        return Err(client_error(format!(
            "invalid Redis endpoint {endpoint:?}, expected host:port[:db]"
        )));
    }
    let port = parts[1]
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| client_error(format!("invalid port in Redis endpoint {endpoint:?}")))?;
    let db = parts.get(2).map_or(Ok(0), |db| {
        db.parse::<i64>()
            .map_err(|_| client_error(format!("invalid db in Redis endpoint {endpoint:?}")))
    })?;
    Ok(RedisEndpointConfig {
        host: parts[0].to_owned(),
        port,
        db,
        auth: None,
    })
}

fn validate_slave_configs(configs: &[RedisEndpointConfig]) -> RedisResult<()> {
    if configs.is_empty() {
        return Err(client_error("at least one slave backend is required"));
    }
    let mut labels = HashSet::with_capacity(configs.len());
    for config in configs {
        let label = config.label();
        if !labels.insert(label.clone()) {
            return Err(client_error(format!("duplicate slave backend {label}")));
        }
    }
    Ok(())
}

fn validate_options(options: &RedisServiceOptions) -> RedisResult<()> {
    if options.master_timeout.is_zero()
        || options.slave_timeout.is_zero()
        || options.connect_timeout.is_zero()
        || options.dns_refresh_interval.is_zero()
    {
        return Err(client_error(
            "Redis transport and DNS intervals must be non-zero",
        ));
    }
    if options.replica_balance.quota.is_zero() || options.replica_balance.failure_penalty.is_zero()
    {
        return Err(client_error(
            "Redis replica quota settings must be non-zero",
        ));
    }
    Ok(())
}

fn map_net_error(error: NetError) -> RedisError {
    RedisError::new(ErrorKind::ClientError, error.to_string())
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
    use std::time::Duration;

    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::{JoinHandle, JoinSet};
    use tokio::time::{Instant, sleep};

    use crate::resp::parser::{ParseResult, parse_reply};
    use crate::sharding::Sharding;
    use crate::sharding::hash::{Hash, Hasher};
    use crate::{
        ErrorKind, MeshConfig, Redis, RedisBytes, RedisPipe, RedisResult, RedisValues, Value,
    };

    use super::{RedisService, RedisServiceOptions, ShardRouting};

    struct FakeRedis {
        endpoint: String,
        accepted: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<String>>>,
        task: JoinHandle<()>,
    }

    impl FakeRedis {
        async fn start(read_value: &'static str) -> Self {
            Self::start_with_delay(read_value, Duration::ZERO).await
        }

        async fn start_with_delay(read_value: &'static str, delay: Duration) -> Self {
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
                                let mut read_buffer = BytesMut::with_capacity(4096);
                                loop {
                                    let read = socket.read_buf(&mut read_buffer).await.unwrap_or(0);
                                    if read == 0 {
                                        break;
                                    }
                                    let snapshot = read_buffer.split().freeze();
                                    let mut position = 0;
                                    let mut responses = BytesMut::new();
                                    loop {
                                        match parse_reply(&snapshot.slice(position..)).unwrap() {
                                            ParseResult::Complete { value, consumed } => {
                                                position += consumed;
                                                let command = command_name(&value);
                                                let argument_count = command_argument_count(&value);
                                                seen.lock().unwrap().push(command.clone());
                                                if !delay.is_zero() {
                                                    tokio::time::sleep(delay).await;
                                                }
                                                match command.as_str() {
                                                    "GET" | "HGET" => responses.extend_from_slice(
                                                        format!("${}\r\n{read_value}\r\n", read_value.len()).as_bytes()
                                                    ),
                                                    "MGET" | "HMGET" => {
                                                        let values = argument_count.saturating_sub(
                                                            if command == "MGET" { 1 } else { 2 }
                                                        );
                                                        responses.extend_from_slice(
                                                            format!("*{values}\r\n").as_bytes()
                                                        );
                                                        for _ in 0..values {
                                                            responses.extend_from_slice(
                                                                format!("${}\r\n{read_value}\r\n", read_value.len()).as_bytes()
                                                            );
                                                        }
                                                    }
                                                    "SET" | "AUTH" | "SELECT" => responses.extend_from_slice(b"+OK\r\n"),
                                                    "PING" => responses.extend_from_slice(b"+PONG\r\n"),
                                                    _ => responses.extend_from_slice(b":1\r\n"),
                                                }
                                            }
                                            ParseResult::Incomplete => {
                                                read_buffer.extend_from_slice(&snapshot[position..]);
                                                break;
                                            }
                                        }
                                    }
                                    if !responses.is_empty()
                                        && socket.write_all(&responses).await.is_err()
                                    {
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
            self.seen.lock().unwrap().iter().any(|seen| seen == command)
        }
    }

    impl Drop for FakeRedis {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct RecoveringRedis {
        endpoint: String,
        accepted: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl RecoveringRedis {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let accepted = Arc::new(AtomicUsize::new(0));
            let server_accepted = accepted.clone();
            let task = tokio::spawn(async move {
                let (mut stalled, _) = listener.accept().await.unwrap();
                server_accepted.fetch_add(1, Ordering::Relaxed);
                let mut buffer = [0_u8; 4096];
                let _ = stalled.read(&mut buffer).await;
                while stalled.read(&mut buffer).await.unwrap_or(0) != 0 {}

                let (mut recovered, _) = listener.accept().await.unwrap();
                server_accepted.fetch_add(1, Ordering::Relaxed);
                loop {
                    let length = recovered.read(&mut buffer).await.unwrap_or(0);
                    if length == 0 {
                        return;
                    }
                    if recovered.write_all(b"$9\r\nrecovered\r\n").await.is_err() {
                        return;
                    }
                }
            });
            Self {
                endpoint,
                accepted,
                task,
            }
        }

        fn accepted(&self) -> usize {
            self.accepted.load(Ordering::Relaxed)
        }
    }

    impl Drop for RecoveringRedis {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn command_name(value: &Value) -> String {
        let Value::Array(arguments) = value else {
            return String::new();
        };
        let Some(Value::BulkString(command)) = arguments.first() else {
            return String::new();
        };
        String::from_utf8_lossy(command).to_ascii_uppercase()
    }

    fn command_argument_count(value: &Value) -> usize {
        match value {
            Value::Array(arguments) => arguments.len(),
            _ => 0,
        }
    }

    #[test]
    fn redis_defaults_to_two_hundred_millisecond_request_timeout() {
        let options = RedisServiceOptions::default();

        assert_eq!(options.master_timeout, Duration::from_millis(200));
        assert_eq!(options.slave_timeout, Duration::from_millis(200));
    }

    #[test]
    fn unified_timeout_updates_both_roles() {
        let options = RedisServiceOptions::default().with_timeout(Duration::from_secs(3));

        assert_eq!(options.master_timeout, Duration::from_secs(3));
        assert_eq!(options.slave_timeout, Duration::from_secs(3));
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn metrics_feature_records_the_configured_authority_without_db_suffix() {
        let server = FakeRedis::start("metric-value").await;
        let redis = RedisService::single(format!("{}:3", server.endpoint))
            .await
            .unwrap();

        assert_eq!(
            Redis::get::<_, RedisBytes>(&redis, "key")
                .await
                .unwrap()
                .as_deref(),
            Some(b"metric-value".as_slice())
        );

        let mut snapshot = None;
        brz_metrics::visit(|name, metric_type, candidate| {
            if name == server.endpoint && metric_type == "REDIS" {
                snapshot = Some(candidate);
            }
        });
        let snapshot = snapshot.expect("the configured endpoint must be registered");
        assert_eq!(
            (snapshot.total, snapshot.success, snapshot.failure),
            (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn mesh_discovers_once_then_behaves_like_single() {
        let first = FakeRedis::start("first").await;
        let second = FakeRedis::start("second").await;
        let directory = tempfile::tempdir().unwrap();
        let first_port = first.endpoint.rsplit_once(':').unwrap().1;
        let second_port = second.endpoint.rsplit_once(':').unwrap().1;
        let first_record = directory.path().join(format!(
            "static.config.api.example.com+3+config+cloud+redis+feed+profiles@redis:{first_port}@rs"
        ));
        std::fs::write(&first_record, []).unwrap();

        let redis = RedisService::mesh_with_config(
            MeshConfig::new("feed", "profiles").with_socket_dir(directory.path()),
            RedisServiceOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            Redis::get::<_, RedisBytes>(&redis, "key")
                .await
                .unwrap()
                .as_deref(),
            Some(b"first".as_slice())
        );

        std::fs::remove_file(first_record).unwrap();
        std::fs::write(
            directory.path().join(format!(
                "static.config.api.example.com+3+config+cloud+redis+feed+profiles@redis:{second_port}@rs"
            )),
            [],
        )
        .unwrap();

        assert_eq!(
            Redis::get::<_, RedisBytes>(&redis, "key")
                .await
                .unwrap()
                .as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(second.accepted(), 0);
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
    async fn single_uses_independent_read_and_write_connections() {
        let server = FakeRedis::start("single").await;
        let redis = RedisService::single(server.endpoint.clone()).await.unwrap();

        assert_eq!(server.accepted(), 2);
        let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(b"single".as_slice()));
        crate::Redis::set(&redis, "key", "value").await.unwrap();
        assert!(server.saw("GET"));
        assert!(server.saw("SET"));

        assert_eq!(server.accepted(), 2);
    }

    #[tokio::test]
    async fn hset_uses_the_master_and_decodes_the_integer_response() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
            .await
            .unwrap();

        assert_eq!(
            Redis::hset(&redis, "hash", "field", "value").await.unwrap(),
            1
        );
        assert!(master.saw("HSET"));
        assert!(!slave.saw("HSET"));
    }

    #[tokio::test]
    async fn added_native_commands_use_declared_reader_and_writer_roles() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
            .await
            .unwrap();

        assert_eq!(Redis::incr(&redis, "counter").await.unwrap(), 1);
        assert_eq!(
            Redis::pfcount(&redis, ["metric:a", "metric:b"])
                .await
                .unwrap(),
            1
        );
        let values = Redis::mget::<_, RedisBytes>(&redis, ["metric:a", "metric:b"])
            .await
            .unwrap();
        assert_eq!(
            values
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>(),
            vec![Some(b"slave".as_slice()), Some(b"slave".as_slice())]
        );

        assert!(master.saw("INCR"));
        assert!(!slave.saw("INCR"));
        assert!(slave.saw("PFCOUNT"));
        assert!(!master.saw("PFCOUNT"));
        assert!(slave.saw("MGET"));
        assert!(!master.saw("MGET"));
    }

    #[tokio::test]
    async fn pipeline_submits_to_one_replica_and_takes_typed_responses_in_order() {
        let master = FakeRedis::start("master").await;
        let first_slave = FakeRedis::start("first").await;
        let second_slave = FakeRedis::start("second").await;
        let redis = RedisService::noshard(
            master.endpoint.clone(),
            [first_slave.endpoint.clone(), second_slave.endpoint.clone()],
        )
        .await
        .unwrap();

        let mut pipe = RedisPipe::with_capacity(3);
        pipe.hget("key", "version").unwrap();
        pipe.hmget("key", ["value", "hash"]).unwrap();
        pipe.hget("key", "version").unwrap();

        let mut responses = redis.pipe(pipe).await.unwrap();
        let first: Option<RedisBytes> = responses.take().await.unwrap();
        let values: RedisValues<RedisBytes> = responses.take().await.unwrap();
        let values = values.collect::<RedisResult<Vec<_>>>().unwrap();
        let last: Option<RedisBytes> = responses.take().await.unwrap();

        assert_eq!(first, last);
        assert_eq!(values, vec![first.clone(), first]);
        assert!(responses.is_empty());

        let first_commands = first_slave.seen.lock().unwrap().len();
        let second_commands = second_slave.seen.lock().unwrap().len();
        assert!(
            (first_commands == 3 && second_commands == 0)
                || (first_commands == 0 && second_commands == 3)
        );
    }

    #[tokio::test]
    async fn pipeline_rejects_multi_shard_service_before_sending() {
        let master_a = FakeRedis::start("master-a").await;
        let slave_a = FakeRedis::start("slave-a").await;
        let master_b = FakeRedis::start("master-b").await;
        let slave_b = FakeRedis::start("slave-b").await;
        let redis = RedisService::sharded(
            vec![
                (master_a.endpoint.clone(), vec![slave_a.endpoint.clone()]),
                (master_b.endpoint.clone(), vec![slave_b.endpoint.clone()]),
            ],
            ShardRouting::new("crc32", "modula"),
        )
        .await
        .unwrap();

        let mut pipe = RedisPipe::with_capacity(2);
        pipe.get("key-a").unwrap();
        pipe.get("key-b").unwrap();

        let error = redis.pipe(pipe).await.err().unwrap();

        assert_eq!(error.kind(), ErrorKind::ClientError);
        assert!(error.to_string().contains("multi-shard"));
        assert!(!slave_a.saw("GET"));
        assert!(!slave_b.saw("GET"));
        assert!(!master_a.saw("GET"));
        assert!(!master_b.saw("GET"));
    }

    #[tokio::test]
    async fn reuses_one_connection_and_splits_reads_from_writes() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::noshard_with_options(
            master.endpoint.clone(),
            [slave.endpoint.clone()],
            RedisServiceOptions::default(),
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
            let value: Option<crate::RedisBytes> =
                crate::Redis::get(&redis, "u:1821155363").await.unwrap();
            assert_eq!(value.as_deref(), Some(b"slave".as_slice()));
        }
        crate::Redis::set(&redis, "u:1821155363", b"\x00\x7f\xff")
            .await
            .unwrap();

        assert!(slave.saw("GET"));
        assert!(!master.saw("GET"));
        assert!(master.saw("SET"));
        assert!(!slave.saw("SET"));
        assert_eq!(master.accepted(), initial_master_connections);
        assert_eq!(slave.accepted(), initial_slave_connections);
    }

    #[tokio::test]
    async fn noshard_does_not_encode_the_key_for_routing() {
        struct CountingKey<'a>(&'a AtomicUsize);

        impl crate::EncodeRedisArg for CountingKey<'_> {
            fn encoded_len(&self) -> usize {
                3
            }

            fn encode<S: crate::RedisArgSink + ?Sized>(
                &self,
                sink: &mut S,
            ) -> crate::RedisResult<()> {
                self.0.fetch_add(1, Ordering::Relaxed);
                sink.write(b"key");
                Ok(())
            }
        }

        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
            .await
            .unwrap();
        let encodes = AtomicUsize::new(0);

        let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, CountingKey(&encodes))
            .await
            .unwrap();

        assert_eq!(value.as_deref(), Some(b"slave".as_slice()));
        assert_eq!(encodes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn noshard_rejects_an_empty_slave_set() {
        let error = RedisService::noshard("127.0.0.1:6379", Vec::<String>::new())
            .await
            .err()
            .unwrap();

        assert_eq!(error.kind(), ErrorKind::ClientError);
        assert!(error.to_string().contains("at least one slave"));
    }

    #[tokio::test]
    async fn sharded_routes_before_selecting_each_groups_slaves() {
        let master_a = FakeRedis::start("master-a").await;
        let slave_a = FakeRedis::start("slave-a").await;
        let master_b = FakeRedis::start("master-b").await;
        let slave_b = FakeRedis::start("slave-b").await;
        let redis = RedisService::sharded(
            vec![
                (master_a.endpoint.clone(), vec![slave_a.endpoint.clone()]),
                (master_b.endpoint.clone(), vec![slave_b.endpoint.clone()]),
            ],
            ShardRouting::new("crc32", "modula"),
        )
        .await
        .unwrap();
        let sharding = Sharding::new("crc32", "modula", &["a".to_owned(), "b".to_owned()]);
        let mut keys = [None, None];
        for index in 0_u64.. {
            let key = index.to_string();
            let shard = sharding.shard_idx(key.as_bytes());
            keys[shard].get_or_insert(key);
            if keys.iter().all(Option::is_some) {
                break;
            }
        }

        let keys = keys.map(Option::unwrap);
        for (key, expected) in keys
            .iter()
            .zip([b"slave-a".as_slice(), b"slave-b".as_slice()])
        {
            let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, key)
                .await
                .unwrap();
            assert_eq!(value.as_deref(), Some(expected));
        }

        let values = Redis::mget::<_, RedisBytes>(&redis, [&keys[1], &keys[0], &keys[1]])
            .await
            .unwrap();
        assert_eq!(
            values
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>(),
            [
                Some(b"slave-b".as_slice()),
                Some(b"slave-a".as_slice()),
                Some(b"slave-b".as_slice()),
            ]
        );
        assert!(slave_a.saw("MGET"));
        assert!(slave_b.saw("MGET"));

        assert_eq!(
            Redis::del_many(&redis, [&keys[0], &keys[1]]).await.unwrap(),
            2
        );
        assert!(master_a.saw("DEL"));
        assert!(master_b.saw("DEL"));

        let count_error = Redis::pfcount(&redis, [&keys[0], &keys[1]])
            .await
            .unwrap_err();
        assert_eq!(count_error.kind(), ErrorKind::ClientError);
        assert!(count_error.to_string().contains("same shard"));

        let script_result =
            Redis::eval::<_, _, _, i64>(&redis, "return 1", [&keys[0]], [] as [&str; 0])
                .await
                .unwrap();
        assert_eq!(script_result, 1);
        assert!(master_a.saw("EVAL"));

        let script_error =
            Redis::eval::<_, _, _, i64>(&redis, "return 1", [&keys[0], &keys[1]], [] as [&str; 0])
                .await
                .unwrap_err();
        assert_eq!(script_error.kind(), ErrorKind::ClientError);
        assert!(script_error.to_string().contains("same shard"));
    }

    #[tokio::test]
    async fn concurrent_reads_share_one_physical_slave_connection() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
            .await
            .unwrap();

        let mut requests = JoinSet::new();
        for index in 0..128 {
            let redis = redis.clone();
            requests.spawn(async move {
                crate::Redis::get::<_, crate::RedisBytes>(&redis, &format!("key-{index}"))
                    .await
                    .unwrap()
            });
        }
        while let Some(result) = requests.join_next().await {
            assert_eq!(result.unwrap().as_deref(), Some(b"slave".as_slice()));
        }

        assert_eq!(master.accepted(), 1);
        assert_eq!(slave.accepted(), 1);
    }

    #[tokio::test]
    async fn slave_selection_rotates_after_consuming_time_quota() {
        let master = FakeRedis::start("master").await;
        let slave_a = FakeRedis::start_with_delay("a", Duration::from_millis(3)).await;
        let slave_b = FakeRedis::start_with_delay("b", Duration::from_millis(3)).await;
        let redis = RedisService::noshard_with_options(
            master.endpoint.clone(),
            [slave_a.endpoint.clone(), slave_b.endpoint.clone()],
            RedisServiceOptions::default().with_replica_quota(Duration::from_millis(1)),
        )
        .await
        .unwrap();

        let first = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .unwrap();
        let second = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first, second);
        assert!(slave_a.saw("GET"));
        assert!(slave_b.saw("GET"));
    }

    #[tokio::test]
    async fn changed_dns_snapshot_is_applied_automatically_and_reuses_nodes() {
        let master = FakeRedis::start("master").await;
        let old_slave = FakeRedis::start("old-slave").await;
        let new_slave = FakeRedis::start("new-slave").await;
        let redis = RedisService::noshard(master.endpoint.clone(), [old_slave.endpoint.clone()])
            .await
            .unwrap();

        assert_eq!(
            crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
                .await
                .unwrap()
                .as_deref(),
            Some(b"old-slave".as_slice())
        );
        let topology = redis.inner.topology.load_full();
        assert!(
            topology.discovery.shards[0].slaves[0]
                .endpoints
                .replace([new_slave.endpoint.parse().unwrap()])
        );
        drop(topology);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
                .await
                .unwrap();
            if value.as_deref() == Some(b"new-slave".as_slice()) {
                break;
            }
            assert_eq!(value.as_deref(), Some(b"old-slave".as_slice()));
            assert!(Instant::now() < deadline);
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(master.accepted(), 1, "unchanged master Node must be reused");
        assert_eq!(old_slave.accepted(), 1);
        assert_eq!(new_slave.accepted(), 1);
    }

    #[tokio::test]
    async fn response_timeout_retries_master_then_reconnects_the_slave() {
        let master = FakeRedis::start("master").await;
        let slave = RecoveringRedis::start().await;
        let redis = RedisService::noshard_with_options(
            master.endpoint.clone(),
            [slave.endpoint.clone()],
            RedisServiceOptions::default().with_slave_timeout(Duration::from_millis(30)),
        )
        .await
        .unwrap();

        let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(b"master".as_slice()));

        #[cfg(feature = "metrics")]
        {
            let mut slave_metric = None;
            let mut master_metric = None;
            brz_metrics::visit(|name, metric_type, candidate| {
                if name == slave.endpoint && metric_type == "REDIS" {
                    slave_metric = Some(candidate);
                }
                if name == master.endpoint && metric_type == "REDIS" {
                    master_metric = Some(candidate);
                }
            });
            let slave_metric = slave_metric.expect("failed slave attempt must be recorded");
            let master_metric = master_metric.expect("master fallback must be recorded");
            assert_eq!(
                (
                    slave_metric.total,
                    slave_metric.success,
                    slave_metric.failure
                ),
                (1, 0, 1)
            );
            assert_eq!(
                (
                    master_metric.total,
                    master_metric.success,
                    master_metric.failure
                ),
                (1, 1, 0)
            );
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while slave.accepted() < 2 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        loop {
            match crate::Redis::get::<_, crate::RedisBytes>(&redis, "key").await {
                Ok(value) => {
                    assert_eq!(value.as_deref(), Some(b"recovered".as_slice()));
                    break;
                }
                Err(error) if error.kind() == ErrorKind::NoConnection => {
                    assert!(tokio::time::Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(error) => panic!("unexpected retry failure: {error}"),
            }
        }
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
