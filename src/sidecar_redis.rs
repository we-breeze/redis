//! Application-facing Redis access through the local Breeze sidecar.

use crate::sidecar::{MeshConfig, MeshRouting, SidecarClient};
use crate::{EncodeRedisArg, EncodeRedisArgs, FromRedisBulk, Redis, RedisResult, RedisValues};

/// A [`Redis`] implementation routed through the local Breeze sidecar.
///
/// [`SidecarRedis::new`] discovers the TCP endpoint for an exact
/// `(group, namespace)` pair. Backend sharding and failover remain owned by
/// the sidecar; pooling, bounded retries, deadlines, and health handling are
/// delegated to the existing [`SidecarClient`].
#[derive(Clone)]
pub struct SidecarRedis {
    client: SidecarClient,
}

impl SidecarRedis {
    /// Discover and connect to an exact sidecar `(group, namespace)`.
    pub async fn new(group: impl Into<String>, namespace: impl Into<String>) -> RedisResult<Self> {
        let config = MeshConfig::new(namespace).with_group(group);
        Self::from_mesh_config(config).await
    }

    /// Construct the application facade from explicit sidecar settings.
    ///
    /// Normal application code should use [`SidecarRedis::new`]. This
    /// constructor supports non-default registry directories and pool/timeout
    /// tuning in infrastructure composition code.
    pub async fn from_mesh_config(config: MeshConfig) -> RedisResult<Self> {
        Ok(Self {
            client: SidecarClient::from_config(config).await?,
        })
    }
}

impl Redis for SidecarRedis {
    async fn get<K, R>(&self, key: K) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        crate::api::get(&self.client, key).await
    }

    async fn set<K, V>(&self, key: K, value: V) -> RedisResult<()>
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        crate::api::set(&self.client, key, value).await
    }

    async fn get_routed<H, K, R>(&self, routing_key: H, key: K) -> RedisResult<Option<R>>
    where
        H: EncodeRedisArg + Send,
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        let routing_key = crate::arg::encode_arg_contiguous(&routing_key)?;
        let routed = self.client.with_hashkey(routing_key.as_ref());
        crate::api::get(&routed, key).await
    }

    async fn set_routed<H, K, V>(&self, routing_key: H, key: K, value: V) -> RedisResult<()>
    where
        H: EncodeRedisArg + Send,
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        let routing_key = crate::arg::encode_arg_contiguous(&routing_key)?;
        let routed = self.client.with_hashkey(routing_key.as_ref());
        crate::api::set(&routed, key, value).await
    }

    async fn hget<K, F, R>(&self, key: K, field: F) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        crate::api::hget(&self.client, key, field).await
    }

    async fn hmget<K, F, R>(&self, key: K, fields: F) -> RedisResult<RedisValues<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send,
    {
        crate::api::hmget(&self.client, key, fields).await
    }
}
