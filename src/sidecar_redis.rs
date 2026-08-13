//! Application-facing Redis access through the local Breeze sidecar.

use async_trait::async_trait;

use crate::sidecar::{MeshConfig, SidecarClient};
use crate::{Redis, RedisBytes, RedisResult};

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

#[async_trait]
impl Redis for SidecarRedis {
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::get(&self.client, key).await
    }

    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::hget(&self.client, key, field).await
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>> {
        crate::api::hmget(&self.client, key, fields).await
    }
}
