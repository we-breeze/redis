//! Application-facing Redis access to one explicit backend endpoint.

use async_trait::async_trait;

use crate::direct::{DirectClient, ServerConfig};
use crate::{Redis, RedisBytes, RedisResult};

/// A [`Redis`] implementation connected directly to one explicit endpoint.
///
/// This facade is intended for tests, validation tools, and other cases where
/// service discovery is deliberately bypassed. Production application code
/// normally uses [`crate::SidecarRedis`].
#[derive(Clone)]
pub struct DirectRedis {
    client: DirectClient,
}

impl DirectRedis {
    /// Connects to `host:port[:db]` using direct-mode defaults.
    pub async fn new(endpoint: &str) -> RedisResult<Self> {
        Self::from_server_config(ServerConfig::new(endpoint)?).await
    }

    /// Connects using an explicit direct-backend configuration.
    pub async fn from_server_config(config: ServerConfig) -> RedisResult<Self> {
        Ok(Self {
            client: DirectClient::connect(config).await?,
        })
    }
}

#[async_trait]
impl Redis for DirectRedis {
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::get(&self.client, key).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> RedisResult<()> {
        crate::api::set(&self.client, key, value).await
    }

    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::hget(&self.client, key, field).await
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>> {
        crate::api::hmget(&self.client, key, fields).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implements_application_contract() {
        fn assert_redis<T: Redis>() {}
        assert_redis::<DirectRedis>();
    }

    #[tokio::test]
    async fn rejects_an_invalid_endpoint_before_connecting() {
        assert!(DirectRedis::new("missing-port").await.is_err());
    }
}
