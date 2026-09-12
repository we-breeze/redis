use super::*;
use std::sync::Mutex;

struct Capture {
    response: Value,
    commands: Mutex<Vec<Cmd>>,
}

impl Redis for Capture {
    async fn command<R: FromRedisValue + Send>(&self, command: Cmd) -> RedisResult<R> {
        self.commands.lock().unwrap().push(command);
        R::from_redis_value(&self.response)
    }
    async fn get<K, R>(&self, _key: K) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn set<K, V>(&self, _key: K, _value: V) -> RedisResult<()>
    where
        K: EncodeRedisArg + Send,
        V: EncodeRedisArg + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn mget<K, R>(&self, _keys: K) -> RedisResult<Vec<Option<R>>>
    where
        K: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn del_many<K>(&self, _keys: K) -> RedisResult<i64>
    where
        K: EncodeRedisArgs + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn pfcount<K>(&self, _keys: K) -> RedisResult<i64>
    where
        K: EncodeRedisArgs + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn hget<K, F, R>(&self, _key: K, _field: F) -> RedisResult<Option<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArg + Send,
        R: FromRedisBulk + Send,
    {
        unimplemented!("unused by command tests")
    }
    async fn hmget<K, F, R>(&self, _key: K, _fields: F) -> RedisResult<RedisValues<R>>
    where
        K: EncodeRedisArg + Send,
        F: EncodeRedisArgs + Send,
        R: FromRedisBulk + Send,
    {
        unimplemented!("unused by command tests")
    }
}

fn capture(response: Value) -> Capture {
    Capture {
        response,
        commands: Mutex::new(Vec::new()),
    }
}

#[tokio::test]
async fn setex_preserves_wire_order_binary_payload_and_writer_routing() {
    let redis = capture(Value::Okay);
    redis
        .set_ex("cache:key", 300, &b"a\0b\xff"[..])
        .await
        .unwrap();
    let commands = redis.commands.lock().unwrap();
    let command = &commands[0];
    assert_eq!(
        command.encoded(),
        b"*4\r\n$5\r\nSETEX\r\n$9\r\ncache:key\r\n$3\r\n300\r\n$4\r\na\0b\xff\r\n"
    );
    assert!(!command.is_readonly());
}

#[tokio::test]
async fn setex_rejects_non_ok_replies() {
    let redis = capture(Value::SimpleString("QUEUED".into()));
    assert_eq!(
        redis.set_ex("key", 1, "value").await.unwrap_err().kind(),
        ErrorKind::TypeError
    );
}

#[tokio::test]
async fn sismember_decodes_membership_and_marks_reader_routing() {
    for (reply, expected) in [(0, false), (1, true)] {
        let redis = capture(Value::Int(reply));
        assert_eq!(redis.sismember("grey:uids", 42).await.unwrap(), expected);
        let commands = redis.commands.lock().unwrap();
        assert_eq!(
            commands[0].encoded(),
            b"*3\r\n$9\r\nSISMEMBER\r\n$9\r\ngrey:uids\r\n$2\r\n42\r\n"
        );
        assert!(commands[0].is_readonly());
    }
}

// This function must compile using only R: Redis, independent of RedisService.
fn assert_generic_futures_are_send<R: Redis>(redis: &R) {
    fn is_send(_: impl Send) {}
    is_send(redis.get::<_, String>("key"));
    is_send(redis.set_with("key", "value", SetOptions::default()));
    is_send(redis.set_ex("key", 300, "value"));
    is_send(redis.sismember("set", "member"));
    is_send(redis.hget::<_, _, String>("key", "field"));
    is_send(redis.command::<Value>(cmd("PING")));
    is_send(redis.pipe(RedisPipe::new()));
}

#[test]
fn generic_consumers_can_require_send() {
    assert_generic_futures_are_send(&capture(Value::Okay));
}
