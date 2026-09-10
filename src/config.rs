//! Configuration supplied by the application, independent of its source.

use std::future::Future;
use std::pin::Pin;

use crate::{RedisResult, RedisServiceOptions, ShardRouting};

/// One logical shard. Writes use `master`; reads use `slaves`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisShardConfig {
    pub master: String,
    pub slaves: Vec<String>,
}

/// A fixed logical topology and its transport settings.
///
/// Addresses use `host:port[:db]`. Hostnames retain automatic DNS refresh.
/// Shard order is significant for routing. Validation happens when constructing
/// [`crate::RedisService`]; no directory or configuration-source rules apply here.
#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub shards: Vec<RedisShardConfig>,
    pub routing: ShardRouting,
    pub options: RedisServiceOptions,
}

impl RedisConfig {
    /// Use one endpoint for both read and write roles, as in
    /// [`crate::RedisService::single`].
    pub fn single(endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self::noshard(endpoint.clone(), [endpoint])
    }

    /// Configure one master/slave group without key hashing.
    pub fn noshard<M, I, S>(master: M, slaves: I) -> Self
    where
        M: Into<String>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::sharded(
            vec![RedisShardConfig {
                master: master.into(),
                slaves: slaves.into_iter().map(Into::into).collect(),
            }],
            ShardRouting::new("raw", "modula"),
        )
    }

    /// Configure ordered logical shards and explicit key routing.
    pub fn sharded(shards: Vec<RedisShardConfig>, routing: ShardRouting) -> Self {
        Self {
            shards,
            routing,
            options: RedisServiceOptions::default(),
        }
    }

    #[must_use]
    pub fn with_options(mut self, options: RedisServiceOptions) -> Self {
        self.options = options;
        self
    }
}

/// An application-owned asynchronous configuration load operation.
pub type RedisConfigFuture<'a> =
    Pin<Box<dyn Future<Output = RedisResult<RedisConfig>> + Send + 'a>>;

/// Supplies a configuration snapshot from an application-defined source.
///
/// [`crate::RedisService::from_provider`] calls `load` exactly once and does
/// not retain the provider. Updating the provider does not change an existing
/// service's logical topology. Errors propagate without constructing a service.
/// This trait also supports `&dyn RedisConfigProvider`.
///
/// ```
/// use brz_redis::{RedisConfig, RedisConfigFuture, RedisConfigProvider};
///
/// struct AppConfig { endpoint: String }
/// impl RedisConfigProvider for AppConfig {
///     fn load(&self) -> RedisConfigFuture<'_> {
///         Box::pin(async move { Ok(RedisConfig::single(&self.endpoint)) })
///     }
/// }
/// ```
pub trait RedisConfigProvider: Send + Sync {
    fn load(&self) -> RedisConfigFuture<'_>;
}
