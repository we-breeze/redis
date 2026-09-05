//! Configuration-source-independent sharded Redis service on `brz-net`.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use brz_net::{
    DnsOptions, DnsSource, EndpointSet, EndpointSource, EphemeralBytesArena, MAX_IN_FLIGHT,
    NetError, Node, NodeOptions, ReplicaSet, ReplicaSetResponseFuture, ResponseFuture,
    SessionError, SessionReplica, ShardRouter, Sharded,
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

mod topology;
use topology::{build_discovery, build_topology, map_net_error, validate_options};

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

mod options;
pub use options::RedisServiceOptions;

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

#[cfg(test)]
mod tests;
