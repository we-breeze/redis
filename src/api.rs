//! Application-facing Redis contract and its command adapters.

use bytes::Bytes;

use crate::cmd::cmd;
use crate::connection::ConnectionLike;
use crate::{
    EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, FromRedisValue, RedisBytes,
    RedisError, RedisResult, RedisValues, Value,
};

/// The typed Redis contract consumed by application code.
///
/// Keys, fields, values, and bulk responses remain application-defined types;
/// the SDK only requires their encoding or decoding traits.
#[allow(async_fn_in_trait)]
pub trait Redis: Send + Sync {
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
    ///
    /// Unsharded implementations may ignore `routing_key`. Sharded
    /// implementations must route with it rather than with `key`, matching
    /// Java's `getClient(routingKey).get(key)` call shape.
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

pub(crate) async fn get<K, R>(
    connection: &(impl ConnectionLike + ?Sized),
    key: K,
) -> RedisResult<Option<R>>
where
    K: EncodeRedisArg,
    R: FromRedisBulk,
{
    let mut command = cmd("GET");
    command.mark_readonly();
    command.arg_encoded(key)?;
    query_bulk(connection, &command).await
}

pub(crate) async fn set<K, V>(
    connection: &(impl ConnectionLike + ?Sized),
    key: K,
    value: V,
) -> RedisResult<()>
where
    K: EncodeRedisArg,
    V: EncodeRedisArg,
{
    let mut command = cmd("SET");
    command.arg_encoded(key)?.arg_encoded(value)?;
    query_unit(connection, &command).await
}

pub(crate) async fn hget<K, F, R>(
    connection: &(impl ConnectionLike + ?Sized),
    key: K,
    field: F,
) -> RedisResult<Option<R>>
where
    K: EncodeRedisArg,
    F: EncodeRedisArg,
    R: FromRedisBulk,
{
    let mut command = cmd("HGET");
    command.mark_readonly();
    command.arg_encoded(key)?.arg_encoded(field)?;
    query_bulk(connection, &command).await
}

pub(crate) async fn hmget<K, F, R>(
    connection: &(impl ConnectionLike + ?Sized),
    key: K,
    fields: F,
) -> RedisResult<RedisValues<R>>
where
    K: EncodeRedisArg,
    F: EncodeRedisArgs,
    R: FromRedisBulk,
{
    let mut command = cmd("HMGET");
    command.mark_readonly();
    command.arg_encoded(key)?;
    fields.encode_args(&mut command)?;
    query_multi_bulk(connection, &command).await
}

async fn query_unit(
    connection: &(impl ConnectionLike + ?Sized),
    command: &crate::Cmd,
) -> RedisResult<()> {
    let value = connection.req_command(command).await?.into_result()?;
    <()>::from_redis_value(&value)
}

async fn query_bulk<R: FromRedisBulk>(
    connection: &(impl ConnectionLike + ?Sized),
    command: &crate::Cmd,
) -> RedisResult<Option<R>> {
    let value = connection.req_command(command).await?.into_result()?;
    value_into_bulk(value)?.map(R::from_redis_bulk).transpose()
}

async fn query_multi_bulk<R: FromRedisBulk>(
    connection: &(impl ConnectionLike + ?Sized),
    command: &crate::Cmd,
) -> RedisResult<RedisValues<R>> {
    let value = connection.req_command(command).await?.into_result()?;
    let Value::Array(values) = value else {
        return Err(type_error("expected an array reply"));
    };
    let values = values
        .into_iter()
        .map(value_into_bulk)
        .collect::<RedisResult<Vec<_>>>()?;
    Ok(RedisValues::materialized(values))
}

fn value_into_bulk(value: Value) -> RedisResult<Option<RedisBytes>> {
    match value {
        Value::Nil => Ok(None),
        Value::BulkString(value) => Ok(Some(value)),
        Value::SimpleString(value) => Ok(Some(Bytes::from(value))),
        Value::VerbatimString { text, .. } => Ok(Some(Bytes::from(text))),
        _ => Err(type_error("expected a bulk string reply")),
    }
}

fn type_error(message: &'static str) -> RedisError {
    RedisError::new(ErrorKind::TypeError, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;

    use crate::connection::RedisFuture;
    use crate::{Cmd, Pipeline, Value};

    use super::*;

    struct Stub {
        response: Value,
        command: Mutex<Vec<Vec<u8>>>,
    }

    impl Stub {
        fn new(response: Value) -> Self {
            Self {
                response,
                command: Mutex::new(Vec::new()),
            }
        }

        fn command(&self) -> Vec<Vec<u8>> {
            self.command.lock().unwrap().clone()
        }
    }

    impl ConnectionLike for Stub {
        fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
            let args = (0..command.arg_count())
                .map(|index| command.arg_at(index).unwrap().to_vec())
                .collect();
            *self.command.lock().unwrap() = args;
            Box::pin(async { Ok(self.response.clone()) })
        }

        fn req_pipeline<'a>(
            &'a self,
            _pipeline: &'a Pipeline,
            _offset: usize,
            _count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            unreachable!("the application API does not expose pipelines")
        }
    }

    #[tokio::test]
    async fn get_returns_binary_safe_bytes() {
        let bytes = Bytes::from_static(b"\x00profile\xff");
        let connection = Stub::new(Value::BulkString(bytes.clone()));

        let result: Option<Bytes> = get(&connection, "u:42").await.unwrap();

        assert_eq!(result.as_ref().unwrap().as_ptr(), bytes.as_ptr());
        assert_eq!(result, Some(bytes));
        assert_eq!(
            connection.command(),
            [b"GET".as_slice(), b"u:42".as_slice()].map(<[u8]>::to_vec)
        );
    }

    #[tokio::test]
    async fn get_encodes_a_redis_key_as_one_composite_key() {
        let connection = Stub::new(Value::Nil);

        let result: Option<Bytes> = get(&connection, crate::RedisKey3("u:", 12345_u64, ".suffix"))
            .await
            .unwrap();

        assert_eq!(result, None);
        assert_eq!(
            connection.command(),
            [b"GET".as_slice(), b"u:12345.suffix".as_slice()].map(<[u8]>::to_vec)
        );
    }

    #[tokio::test]
    async fn set_preserves_binary_value_bytes() {
        let connection = Stub::new(Value::Okay);

        set(&connection, "u:42", b"\x00\x7f\x80\xff").await.unwrap();

        assert_eq!(
            connection.command(),
            [
                b"SET".as_slice(),
                b"u:42".as_slice(),
                b"\x00\x7f\x80\xff".as_slice(),
            ]
            .map(<[u8]>::to_vec)
        );
    }

    #[tokio::test]
    async fn hget_preserves_nil_and_builds_a_read_command() {
        let connection = Stub::new(Value::Nil);

        let result: Option<Bytes> = hget(&connection, "document:42", "version").await.unwrap();

        assert_eq!(result, None);
        assert_eq!(
            connection.command(),
            [
                b"HGET".as_slice(),
                b"document:42".as_slice(),
                b"version".as_slice()
            ]
            .map(<[u8]>::to_vec)
        );
    }

    #[tokio::test]
    async fn hmget_preserves_field_order_and_shares_bulk_bytes() {
        let first = Bytes::from_static(b"payload");
        let connection = Stub::new(Value::Array(vec![
            Value::BulkString(first.clone()),
            Value::Nil,
            Value::BulkString(Bytes::from_static(b"digest")),
        ]));

        let values =
            hmget::<_, _, Bytes>(&connection, "document:42", &["value", "compress", "hash"])
                .await
                .unwrap()
                .collect::<RedisResult<Vec<_>>>()
                .unwrap();

        assert_eq!(values[0].as_ref().unwrap().as_ptr(), first.as_ptr());
        assert_eq!(values[0], Some(first));
        assert_eq!(values[1], None);
        assert_eq!(values[2].as_deref(), Some(b"digest".as_slice()));
        assert_eq!(
            connection.command(),
            [
                b"HMGET".as_slice(),
                b"document:42".as_slice(),
                b"value".as_slice(),
                b"compress".as_slice(),
                b"hash".as_slice(),
            ]
            .map(<[u8]>::to_vec)
        );
    }
}
