//! Typed finite pipelines for the application-facing [`crate::Redis`] API.

use std::{collections::VecDeque, sync::Arc};

use brz_net::EphemeralBytesArena;

use crate::net_transport::{RedisRequest, RedisResponse, RedisResponseKind, map_session_error};
use crate::redis_service::{RedisReplicaResponseFuture, RedisTopology};
use crate::{
    Cmd, EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, RedisError, RedisResult,
    RedisValues, cmd,
};

/// Maximum number of commands admitted by one finite Redis pipeline.
pub const MAX_PIPELINE_COMMANDS: usize = brz_net::MAX_IN_FLIGHT;

/// Starts a finite typed Redis pipeline.
pub fn pipe() -> RedisPipe {
    RedisPipe::new()
}

struct PipeCommand {
    command: Cmd,
    response: RedisResponseKind,
}

/// A finite ordered group of Redis commands submitted to one logical shard.
///
/// Direct multi-shard services reject pipelines until cross-shard admission
/// and failure semantics are explicitly supported. A pipeline may contain at
/// most [`brz_net::MAX_IN_FLIGHT`] commands.
#[derive(Default)]
pub struct RedisPipe {
    commands: Vec<PipeCommand>,
}

impl RedisPipe {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            commands: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn get<K>(&mut self, key: K) -> RedisResult<&mut Self>
    where
        K: EncodeRedisArg,
    {
        let mut command = readonly_command("GET");
        command.arg_encoded(key)?;
        Ok(self.push(command, RedisResponseKind::Bulk))
    }

    pub fn set<K, V>(&mut self, key: K, value: V) -> RedisResult<&mut Self>
    where
        K: EncodeRedisArg,
        V: EncodeRedisArg,
    {
        let mut command = cmd("SET");
        command.arg_encoded(key)?.arg_encoded(value)?;
        Ok(self.push(command, RedisResponseKind::Unit))
    }

    pub fn hget<K, F>(&mut self, key: K, field: F) -> RedisResult<&mut Self>
    where
        K: EncodeRedisArg,
        F: EncodeRedisArg,
    {
        let mut command = readonly_command("HGET");
        command.arg_encoded(key)?.arg_encoded(field)?;
        Ok(self.push(command, RedisResponseKind::Bulk))
    }

    pub fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> RedisResult<&mut Self>
    where
        K: EncodeRedisArg,
        F: EncodeRedisArg,
        V: EncodeRedisArg,
    {
        let mut command = cmd("HSET");
        command
            .arg_encoded(key)?
            .arg_encoded(field)?
            .arg_encoded(value)?;
        Ok(self.push(command, RedisResponseKind::Integer))
    }

    pub fn hmget<K, F>(&mut self, key: K, fields: F) -> RedisResult<&mut Self>
    where
        K: EncodeRedisArg,
        F: EncodeRedisArgs,
    {
        let expected = fields.num_args();
        let mut command = readonly_command("HMGET");
        command.arg_encoded(key)?;
        fields.encode_args(&mut command)?;
        Ok(self.push(command, RedisResponseKind::MultiBulk { expected }))
    }

    fn push(&mut self, command: Cmd, response: RedisResponseKind) -> &mut Self {
        self.commands.push(PipeCommand { command, response });
        self
    }

    pub(crate) fn is_readonly(&self) -> bool {
        self.commands
            .iter()
            .all(|entry| entry.command.is_readonly())
    }

    pub(crate) fn into_requests(self, arena: &EphemeralBytesArena) -> Vec<RedisRequest> {
        self.commands
            .into_iter()
            .map(|entry| RedisRequest::encode(arena, &entry.command, entry.response))
            .collect()
    }
}

fn readonly_command(name: &str) -> Cmd {
    let mut command = cmd(name);
    command.mark_readonly();
    command
}

enum PendingResponses {
    Direct(VecDeque<RedisReplicaResponseFuture>),
    Ready(VecDeque<RedisResponse>),
}

/// Responses from one admitted Redis pipeline.
///
/// [`take`](Self::take) consumes exactly one response in command order and
/// waits only for that response.
pub struct PipeResponse {
    pending: PendingResponses,
    // A DNS topology update must not tear down nodes that still own admitted
    // requests from the previous immutable snapshot.
    _topology: Option<Arc<RedisTopology>>,
}

impl PipeResponse {
    pub(crate) fn direct(
        responses: Vec<RedisReplicaResponseFuture>,
        topology: Arc<RedisTopology>,
    ) -> Self {
        Self {
            pending: PendingResponses::Direct(responses.into()),
            _topology: Some(topology),
        }
    }

    pub(crate) fn ready(responses: Vec<RedisResponse>) -> Self {
        Self {
            pending: PendingResponses::Ready(responses.into()),
            _topology: None,
        }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        match &self.pending {
            PendingResponses::Direct(responses) => responses.len(),
            PendingResponses::Ready(responses) => responses.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub async fn take<T>(&mut self) -> RedisResult<T>
    where
        T: FromPipeResponse,
    {
        let response = match &mut self.pending {
            PendingResponses::Direct(responses) => responses
                .pop_front()
                .ok_or_else(no_pipe_response)?
                .await
                .map_err(map_session_error)?,
            PendingResponses::Ready(responses) => {
                responses.pop_front().ok_or_else(no_pipe_response)?
            }
        };
        <T as private::FromResponse>::from_response(response)
    }
}

fn no_pipe_response() -> RedisError {
    RedisError::new(
        ErrorKind::ResponseError,
        "Redis pipeline has no remaining response",
    )
}

mod private {
    use super::*;

    pub(crate) trait FromResponse: Sized {
        fn from_response(response: RedisResponse) -> RedisResult<Self>;
    }

    impl FromResponse for () {
        fn from_response(response: RedisResponse) -> RedisResult<Self> {
            response.into_unit()
        }
    }

    impl FromResponse for i64 {
        fn from_response(response: RedisResponse) -> RedisResult<Self> {
            response.into_integer()
        }
    }

    impl<R: FromRedisBulk> FromResponse for Option<R> {
        fn from_response(response: RedisResponse) -> RedisResult<Self> {
            response.into_bulk()?.map(R::from_redis_bulk).transpose()
        }
    }

    impl<R: FromRedisBulk> FromResponse for RedisValues<R> {
        fn from_response(response: RedisResponse) -> RedisResult<Self> {
            response.into_multi_bulk()
        }
    }
}

/// Types accepted by [`PipeResponse::take`].
#[allow(private_bounds)]
pub trait FromPipeResponse: private::FromResponse {}

impl<T: private::FromResponse> FromPipeResponse for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::bulk::RedisValuesSource;

    #[test]
    fn builder_keeps_command_order_and_response_shapes() {
        let mut pipe = RedisPipe::with_capacity(2);
        pipe.hget("key", "version").unwrap();
        pipe.hmget("key", ["value", "hash"]).unwrap();

        assert_eq!(pipe.len(), 2);
        assert!(pipe.is_readonly());
        assert_eq!(pipe.commands[0].command.arg_at(1), Some(b"key".as_slice()));
        assert!(matches!(pipe.commands[0].response, RedisResponseKind::Bulk));
        assert!(matches!(
            pipe.commands[1].response,
            RedisResponseKind::MultiBulk { expected: 2 }
        ));
    }

    #[tokio::test]
    async fn ready_responses_are_taken_in_order_and_lazily_converted() {
        let responses = vec![
            RedisResponse::Bulk(Some(Bytes::from_static(b"42"))),
            RedisResponse::MultiBulk(RedisValuesSource::Materialized(
                vec![Some(Bytes::from_static(b"v")), None].into_iter(),
            )),
        ];
        let mut responses = PipeResponse::ready(responses);

        let version: Option<i64> = responses.take().await.unwrap();
        let values: RedisValues<Bytes> = responses.take().await.unwrap();
        let values = values.collect::<RedisResult<Vec<_>>>().unwrap();

        assert_eq!(version, Some(42));
        assert_eq!(values, vec![Some(Bytes::from_static(b"v")), None]);
        assert!(responses.is_empty());
    }
}
