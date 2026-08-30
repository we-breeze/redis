//! Application-facing typed Redis contract.

use crate::{
    EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, PipeResponse, RedisError, RedisPipe,
    RedisResult, RedisValues,
};

/// The typed Redis contract consumed by application code.
///
/// Keys, fields, values, and bulk responses remain application-defined types;
/// the SDK only requires their encoding or decoding traits.
#[allow(async_fn_in_trait)]
pub trait Redis: Send + Sync {
    /// Submit a finite ordered pipeline.
    ///
    /// The returned response handle consumes replies lazily in command order
    /// through [`PipeResponse::take`]. Implementations that cannot guarantee
    /// correct cross-shard admission and failure semantics must reject a
    /// pipeline on a multi-shard topology before sending any command.
    async fn pipe(&self, pipe: RedisPipe) -> RedisResult<PipeResponse> {
        let _ = pipe;
        Err(RedisError::new(
            ErrorKind::ClientError,
            "this Redis implementation does not support pipelines",
        ))
    }

    /// `GET key`; a missing key is `Ok(None)`.
    async fn get<K, R>(&self, key: K) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send;

    /// `SET key value`; both arguments are encoded directly into the command.
    async fn set<K, V>(&self, key: K, value: V) -> RedisResult<()>
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send;

    /// `GET key` while selecting a shard from an explicit routing key.
    async fn get_routed<H, K, R>(&self, routing_key: H, key: K) -> RedisResult<Option<R>>
    where
        H: EncodeRedisArg + Send,
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        let _ = routing_key;
        self.get(key).await
    }

    /// `SET key value` while selecting a shard from an explicit routing key.
    async fn set_routed<H, K, V>(&self, routing_key: H, key: K, value: V) -> RedisResult<()>
    where
        H: EncodeRedisArg + Send,
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        let _ = routing_key;
        self.set(key, value).await
    }

    /// `HGET key field`; a missing key or field is `Ok(None)`.
    async fn hget<K, F, R>(&self, key: K, field: F) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        R: FromRedisBulk + Send;

    /// `HSET key field value`; returns the number of newly added fields.
    async fn hset<K, F, V>(&self, key: K, field: F, value: V) -> RedisResult<i64>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        let _ = (key, field, value);
        Err(RedisError::new(
            ErrorKind::ClientError,
            "this Redis implementation does not support HSET",
        ))
    }

    /// `HMGET key field [field ...]` in input order.
    ///
    /// Missing fields remain `None`, so the returned iterator has the same
    /// length and ordering as `fields` when Redis returns a valid response.
    async fn hmget<K, F, R>(&self, key: K, fields: F) -> RedisResult<RedisValues<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send;
}
