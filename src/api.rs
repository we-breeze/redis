//! Application-facing typed Redis contract.

use std::future::Future;

use crate::{
    Cmd, EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, FromRedisValue, PipeResponse,
    RedisError, RedisPipe, RedisResult, RedisValues, Value, cmd,
};

/// Expiration modifier encoded by Redis' `SET` command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetExpiration {
    Seconds(u64),
    Milliseconds(u64),
}

/// Conditional modifier encoded by Redis' `SET` command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetCondition {
    IfAbsent,
    IfPresent,
}

/// Redis-native options for [`Redis::set_with`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetOptions {
    pub expiration: Option<SetExpiration>,
    pub condition: Option<SetCondition>,
}

impl SetOptions {
    #[must_use]
    pub fn with_expiration(mut self, expiration: SetExpiration) -> Self {
        self.expiration = Some(expiration);
        self
    }

    #[must_use]
    pub fn if_absent(mut self) -> Self {
        self.condition = Some(SetCondition::IfAbsent);
        self
    }

    #[must_use]
    pub fn if_present(mut self) -> Self {
        self.condition = Some(SetCondition::IfPresent);
        self
    }

    pub(crate) fn encode(&self, command: &mut Cmd) -> RedisResult<()> {
        if let Some(expiration) = self.expiration {
            match expiration {
                SetExpiration::Seconds(value) => {
                    command.arg_encoded("EX")?.arg_encoded(value)?;
                }
                SetExpiration::Milliseconds(value) => {
                    command.arg_encoded("PX")?.arg_encoded(value)?;
                }
            }
        }
        if let Some(condition) = self.condition {
            command.arg_encoded(match condition {
                SetCondition::IfAbsent => "NX",
                SetCondition::IfPresent => "XX",
            })?;
        }
        Ok(())
    }
}

/// The typed Redis contract consumed by application code.
///
/// Keys, fields, values, and bulk responses remain application-defined types;
/// the SDK only requires their encoding or decoding traits. All command futures
/// are `Send`, including when called through a generic `R: Redis`.
pub trait Redis: Send + Sync {
    /// Executes one RESP command, routing by its first argument after the
    /// command name when present.
    ///
    /// [`Cmd::is_readonly`] decides whether the service uses its reader or
    /// writer side. This is the protocol escape hatch used by the typed command
    /// methods; it does not add retry, locking, caching, or queue semantics.
    fn command<R>(&self, command: Cmd) -> impl Future<Output = RedisResult<R>> + Send
    where
        R: FromRedisValue + Send,
    {
        async move {
            let _ = command;
            Err(unsupported("arbitrary commands"))
        }
    }

    /// Submit a finite ordered pipeline.
    ///
    /// The returned response handle consumes replies lazily in command order
    /// through [`PipeResponse::take`]. Implementations that cannot guarantee
    /// correct cross-shard admission and failure semantics must reject a
    /// pipeline on a multi-shard topology before sending any command.
    fn pipe(&self, pipe: RedisPipe) -> impl Future<Output = RedisResult<PipeResponse>> + Send {
        async move {
            let _ = pipe;
            Err(RedisError::new(
                ErrorKind::ClientError,
                "this Redis implementation does not support pipelines",
            ))
        }
    }

    /// `GET key`; a missing key is `Ok(None)`.
    fn get<K, R>(&self, key: K) -> impl Future<Output = RedisResult<Option<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send;

    /// `SET key value`; both arguments are encoded directly into the command.
    fn set<K, V>(&self, key: K, value: V) -> impl Future<Output = RedisResult<()>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send;

    /// `SETEX key seconds value`; preserves the SETEX wire command.
    fn set_ex<K, V>(
        &self,
        key: K,
        seconds: u64,
        value: V,
    ) -> impl Future<Output = RedisResult<()>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("SETEX");
            command
                .arg_encoded(&key)?
                .arg_encoded(seconds)?
                .arg_encoded(value)?;
            let response: String = self.command(command).await?;
            status_ok("SETEX", response)
        }
    }

    /// `PING`; validates that Redis returns `PONG`.
    fn ping(&self) -> impl Future<Output = RedisResult<()>> + Send {
        async move {
            let response: String = self.command(readonly_command("PING")).await?;
            if response == "PONG" {
                Ok(())
            } else {
                Err(RedisError::new(
                    ErrorKind::TypeError,
                    format!("expected Redis PONG response, received {response:?}"),
                ))
            }
        }
    }

    /// `MGET key [key ...]`.
    ///
    /// The service derives routing from every key. Cross-shard inputs are
    /// fanned out internally and restored to input order.
    fn mget<K, R>(&self, keys: K) -> impl Future<Output = RedisResult<Vec<Option<R>>>> + Send
    where
        K: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send;

    /// `SET key value [EX seconds|PX milliseconds] [NX|XX]`.
    ///
    /// Returns `false` when a conditional write is not performed.
    fn set_with<K, V>(
        &self,
        key: K,
        value: V,
        options: SetOptions,
    ) -> impl Future<Output = RedisResult<bool>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("SET");
            command.arg_encoded(&key)?.arg_encoded(value)?;
            options.encode(&mut command)?;
            self.command(command).await
        }
    }

    /// `DEL key`.
    fn del<K>(&self, key: K) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
    {
        async move { integer_key_command(self, "DEL", key).await }
    }

    /// `DEL key [key ...]`, fanned out transparently across shards.
    ///
    /// Cross-shard deletion is not atomic. If one shard fails, deletions that
    /// already succeeded on other shards are not rolled back.
    fn del_many<K>(&self, keys: K) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArgs + Send;

    /// `EXISTS key`.
    fn exists<K>(&self, key: K) -> impl Future<Output = RedisResult<bool>> + Send
    where
        K: EncodeRedisArg + Send,
    {
        async move {
            let mut command = readonly_command("EXISTS");
            command.arg_encoded(&key)?;
            self.command(command).await
        }
    }

    /// `EXPIRE key seconds`.
    fn expire<K>(&self, key: K, seconds: u64) -> impl Future<Output = RedisResult<bool>> + Send
    where
        K: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("EXPIRE");
            command.arg_encoded(&key)?.arg_encoded(seconds)?;
            self.command(command).await
        }
    }

    /// `INCR key`.
    fn incr<K>(&self, key: K) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
    {
        async move { integer_key_command(self, "INCR", key).await }
    }

    /// `APPEND key value`.
    fn append<K, V>(&self, key: K, value: V) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("APPEND");
            command.arg_encoded(&key)?.arg_encoded(value)?;
            self.command(command).await
        }
    }

    /// `EVAL script numkeys key [key ...] arg [arg ...]`.
    fn eval<S, K, A, R>(
        &self,
        script: S,
        keys: K,
        arguments: A,
    ) -> impl Future<Output = RedisResult<R>> + Send
    where
        S: EncodeRedisArg + Send,
        K: EncodeRedisArgs + Send,
        A: EncodeRedisArgs + Send,
        R: FromRedisValue + Send,
    {
        async move {
            let _ = (script, keys, arguments);
            Err(unsupported("EVAL"))
        }
    }

    /// `EVALSHA digest numkeys key [key ...] arg [arg ...]`.
    fn evalsha<D, K, A, R>(
        &self,
        digest: D,
        keys: K,
        arguments: A,
    ) -> impl Future<Output = RedisResult<R>> + Send
    where
        D: EncodeRedisArg + Send,
        K: EncodeRedisArgs + Send,
        A: EncodeRedisArgs + Send,
        R: FromRedisValue + Send,
    {
        async move {
            let _ = (digest, keys, arguments);
            Err(unsupported("EVALSHA"))
        }
    }

    /// `RPUSH key value [value ...]`.
    fn rpush<K, V>(&self, key: K, values: V) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArgs + Send,
    {
        async move {
            let mut command = cmd("RPUSH");
            command.arg_encoded(&key)?;
            values.encode_args(&mut command)?;
            self.command(command).await
        }
    }

    /// `LPOP key`; a missing key is `Ok(None)`.
    fn lpop<K, R>(&self, key: K) -> impl Future<Output = RedisResult<Option<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move {
            let mut command = cmd("LPOP");
            command.arg_encoded(&key)?;
            self.command(command).await
        }
    }

    /// `LRANGE key start stop`.
    fn lrange<K, R>(
        &self,
        key: K,
        start: i64,
        stop: i64,
    ) -> impl Future<Output = RedisResult<Vec<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move {
            let mut command = readonly_command("LRANGE");
            command
                .arg_encoded(&key)?
                .arg_encoded(start)?
                .arg_encoded(stop)?;
            self.command(command).await
        }
    }

    /// `LSET key index value`.
    fn lset<K, V>(
        &self,
        key: K,
        index: i64,
        value: V,
    ) -> impl Future<Output = RedisResult<()>> + Send
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("LSET");
            command
                .arg_encoded(&key)?
                .arg_encoded(index)?
                .arg_encoded(value)?;
            let response: String = self.command(command).await?;
            status_ok("LSET", response)
        }
    }

    /// `SADD key member [member ...]`.
    fn sadd<K, M>(&self, key: K, members: M) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        M: EncodeRedisArgs + Send,
    {
        async move { integer_key_args_command(self, "SADD", key, members).await }
    }

    /// `SREM key member [member ...]`.
    fn srem<K, M>(&self, key: K, members: M) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        M: EncodeRedisArgs + Send,
    {
        async move { integer_key_args_command(self, "SREM", key, members).await }
    }

    /// `SISMEMBER key member`; returns whether the member belongs to the set.
    fn sismember<K, M>(&self, key: K, member: M) -> impl Future<Output = RedisResult<bool>> + Send
    where
        K: EncodeRedisArg + Send,
        M: EncodeRedisArg + Send,
    {
        async move {
            let mut command = readonly_command("SISMEMBER");
            command.arg_encoded(&key)?.arg_encoded(member)?;
            self.command(command).await
        }
    }

    /// `SMEMBERS key`.
    fn smembers<K, R>(&self, key: K) -> impl Future<Output = RedisResult<Vec<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move { array_key_command(self, "SMEMBERS", key).await }
    }

    /// `HKEYS key`.
    fn hkeys<K, R>(&self, key: K) -> impl Future<Output = RedisResult<Vec<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move { array_key_command(self, "HKEYS", key).await }
    }

    /// `HGETALL key`, decoded through [`FromRedisValue`].
    fn hgetall<K, R>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move { value_key_command(self, "HGETALL", key).await }
    }

    /// `ZADD key score member`.
    fn zadd<K, S, M>(
        &self,
        key: K,
        score: S,
        member: M,
    ) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        S: EncodeRedisArg + Send,
        M: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("ZADD");
            command
                .arg_encoded(&key)?
                .arg_encoded(score)?
                .arg_encoded(member)?;
            self.command(command).await
        }
    }

    /// `ZREM key member [member ...]`.
    fn zrem<K, M>(&self, key: K, members: M) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        M: EncodeRedisArgs + Send,
    {
        async move { integer_key_args_command(self, "ZREM", key, members).await }
    }

    /// `ZREMRANGEBYSCORE key min max`.
    fn zremrangebyscore<K, Min, Max>(
        &self,
        key: K,
        min: Min,
        max: Max,
    ) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        Min: EncodeRedisArg + Send,
        Max: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("ZREMRANGEBYSCORE");
            command
                .arg_encoded(&key)?
                .arg_encoded(min)?
                .arg_encoded(max)?;
            self.command(command).await
        }
    }

    /// `ZREVRANGE key start stop`.
    fn zrevrange<K, R>(
        &self,
        key: K,
        start: i64,
        stop: i64,
    ) -> impl Future<Output = RedisResult<Vec<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        R: FromRedisValue + Send,
    {
        async move {
            let mut command = readonly_command("ZREVRANGE");
            command
                .arg_encoded(&key)?
                .arg_encoded(start)?
                .arg_encoded(stop)?;
            self.command(command).await
        }
    }

    /// `ZREVRANGE key start stop WITHSCORES` as `(member, score)` pairs.
    fn zrevrange_with_scores<K, M, S>(
        &self,
        key: K,
        start: i64,
        stop: i64,
    ) -> impl Future<Output = RedisResult<Vec<(M, S)>>> + Send
    where
        K: EncodeRedisArg + Send,
        M: FromRedisValue + Send,
        S: FromRedisValue + Send,
    {
        async move {
            let mut command = readonly_command("ZREVRANGE");
            command
                .arg_encoded(&key)?
                .arg_encoded(start)?
                .arg_encoded(stop)?
                .arg_encoded("WITHSCORES")?;
            let response: Value = self.command(command).await?;
            alternating_pairs(&response)
        }
    }

    /// `PFADD key element [element ...]`.
    fn pfadd<K, E>(&self, key: K, elements: E) -> impl Future<Output = RedisResult<bool>> + Send
    where
        K: EncodeRedisArg + Send,
        E: EncodeRedisArgs + Send,
    {
        async move {
            let mut command = cmd("PFADD");
            command.arg_encoded(&key)?;
            elements.encode_args(&mut command)?;
            self.command(command).await
        }
    }

    /// `PFCOUNT key [key ...]`.
    ///
    /// Multiple keys must resolve to one shard because Redis computes the
    /// cardinality of their union; per-shard counts cannot be added safely.
    fn pfcount<K>(&self, keys: K) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArgs + Send;

    /// `PUBLISH channel message`.
    fn publish<C, M>(&self, channel: C, message: M) -> impl Future<Output = RedisResult<i64>> + Send
    where
        C: EncodeRedisArg + Send,
        M: EncodeRedisArg + Send,
    {
        async move {
            let mut command = cmd("PUBLISH");
            command.arg_encoded(&channel)?.arg_encoded(message)?;
            self.command(command).await
        }
    }

    /// `HGET key field`; a missing key or field is `Ok(None)`.
    fn hget<K, F, R>(
        &self,
        key: K,
        field: F,
    ) -> impl Future<Output = RedisResult<Option<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        R: FromRedisBulk + Send;

    /// `HSET key field value`; returns the number of newly added fields.
    fn hset<K, F, V>(
        &self,
        key: K,
        field: F,
        value: V,
    ) -> impl Future<Output = RedisResult<i64>> + Send
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        async move {
            let _ = (key, field, value);
            Err(RedisError::new(
                ErrorKind::ClientError,
                "this Redis implementation does not support HSET",
            ))
        }
    }

    /// `HMGET key field [field ...]` in input order.
    ///
    /// Missing fields remain `None`, so the returned iterator has the same
    /// length and ordering as `fields` when Redis returns a valid response.
    fn hmget<K, F, R>(
        &self,
        key: K,
        fields: F,
    ) -> impl Future<Output = RedisResult<RedisValues<R>>> + Send
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send;
}

fn unsupported(capability: &'static str) -> RedisError {
    RedisError::new(
        ErrorKind::ClientError,
        format!("this Redis implementation does not support {capability}"),
    )
}

fn readonly_command(name: &str) -> Cmd {
    let mut command = cmd(name);
    command.mark_readonly();
    command
}

async fn value_key_command<S, K, R>(service: &S, name: &str, key: K) -> RedisResult<R>
where
    S: Redis + ?Sized,
    K: EncodeRedisArg + Send,
    R: FromRedisValue + Send,
{
    let mut command = readonly_command(name);
    command.arg_encoded(&key)?;
    service.command(command).await
}

async fn array_key_command<S, K, R>(service: &S, name: &str, key: K) -> RedisResult<Vec<R>>
where
    S: Redis + ?Sized,
    K: EncodeRedisArg + Send,
    R: FromRedisValue + Send,
{
    value_key_command(service, name, key).await
}

async fn integer_key_command<S, K>(service: &S, name: &str, key: K) -> RedisResult<i64>
where
    S: Redis + ?Sized,
    K: EncodeRedisArg + Send,
{
    let mut command = cmd(name);
    command.arg_encoded(&key)?;
    service.command(command).await
}

fn alternating_pairs<M, S>(value: &Value) -> RedisResult<Vec<(M, S)>>
where
    M: FromRedisValue,
    S: FromRedisValue,
{
    let values = match value {
        Value::Array(values) => values,
        Value::Nil => return Ok(Vec::new()),
        _ => {
            return Err(RedisError::new(
                ErrorKind::TypeError,
                "expected a Redis array containing member/score pairs",
            ));
        }
    };
    if values.len() % 2 != 0 {
        return Err(RedisError::new(
            ErrorKind::TypeError,
            "Redis member/score response contains an odd number of elements",
        ));
    }
    values
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            Ok((
                M::from_redis_value(&pair[0])?,
                S::from_redis_value(&pair[1])?,
            ))
        })
        .collect()
}

fn status_ok(command: &'static str, response: String) -> RedisResult<()> {
    if response == "OK" {
        Ok(())
    } else {
        Err(RedisError::new(
            ErrorKind::TypeError,
            format!("expected Redis {command} to return OK, received {response:?}"),
        ))
    }
}

async fn integer_key_args_command<S, K, A>(
    service: &S,
    name: &str,
    key: K,
    arguments: A,
) -> RedisResult<i64>
where
    S: Redis + ?Sized,
    K: EncodeRedisArg + Send,
    A: EncodeRedisArgs + Send,
{
    let mut command = cmd(name);
    command.arg_encoded(&key)?;
    arguments.encode_args(&mut command)?;
    service.command(command).await
}

#[cfg(test)]
mod tests;
