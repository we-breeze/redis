//! Application-facing Redis contract and its command adapters.

use async_trait::async_trait;

use crate::cmd::cmd;
use crate::connection::ConnectionLike;
use crate::{FromRedisValue, RedisResult};

/// Binary-safe bytes returned by the application-facing [`Redis`] API.
///
/// Bulk-string replies share the RESP read buffer instead of copying their
/// payload. Callers that need UTF-8 or a structured format decode it at their
/// own application boundary.
pub type RedisBytes = bytes::Bytes;

/// The small Redis contract consumed by application code.
///
/// The initial surface follows the commands observed in abtest. Add commands
/// only when a real consumer needs them rather than exposing the SDK's entire
/// low-level command builder through this boundary.
#[async_trait]
pub trait Redis: Send + Sync {
    /// `GET key`; a missing key is `Ok(None)`.
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>>;

    /// `HGET key field`; a missing key or field is `Ok(None)`.
    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>>;

    /// `HMGET key field [field ...]` in input order.
    ///
    /// Missing fields remain `None`, so the returned vector has the same
    /// length and ordering as `fields` when Redis returns a valid response.
    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>>;
}

pub(crate) async fn get(
    connection: &(impl ConnectionLike + ?Sized),
    key: &str,
) -> RedisResult<Option<RedisBytes>> {
    let mut command = cmd("GET");
    command.mark_readonly();
    command.arg(key);
    query(connection, &command).await
}

pub(crate) async fn hget(
    connection: &(impl ConnectionLike + ?Sized),
    key: &str,
    field: &str,
) -> RedisResult<Option<RedisBytes>> {
    let mut command = cmd("HGET");
    command.mark_readonly();
    command.arg(key).arg(field);
    query(connection, &command).await
}

pub(crate) async fn hmget(
    connection: &(impl ConnectionLike + ?Sized),
    key: &str,
    fields: &[&str],
) -> RedisResult<Vec<Option<RedisBytes>>> {
    let mut command = cmd("HMGET");
    command.mark_readonly();
    command.arg(key);
    for field in fields {
        command.arg(*field);
    }
    query(connection, &command).await
}

async fn query<T: FromRedisValue>(
    connection: &(impl ConnectionLike + ?Sized),
    command: &crate::Cmd,
) -> RedisResult<T> {
    let value = connection.req_command(command).await?.into_result()?;
    T::from_redis_value(&value)
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

    #[test]
    fn redis_trait_is_object_safe() {
        fn accepts_trait_object(_: &dyn Redis) {}
        let _ = accepts_trait_object;
    }

    #[tokio::test]
    async fn get_returns_binary_safe_bytes() {
        let bytes = Bytes::from_static(b"\x00profile\xff");
        let connection = Stub::new(Value::BulkString(bytes.clone()));

        let result = get(&connection, "u:42").await.unwrap();

        assert_eq!(result.as_ref().unwrap().as_ptr(), bytes.as_ptr());
        assert_eq!(result, Some(bytes));
        assert_eq!(
            connection.command(),
            [b"GET".as_slice(), b"u:42".as_slice()].map(<[u8]>::to_vec)
        );
    }

    #[tokio::test]
    async fn hget_preserves_nil_and_builds_a_read_command() {
        let connection = Stub::new(Value::Nil);

        let result = hget(&connection, "document:42", "version").await.unwrap();

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

        let values = hmget(&connection, "document:42", &["value", "compress", "hash"])
            .await
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
