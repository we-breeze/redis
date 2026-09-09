#![cfg(feature = "integration-tests")]

use std::collections::HashMap;

use redis::{Redis, RedisPipe, RedisService, SetExpiration, SetOptions};

fn endpoint() -> Option<String> {
    std::env::var("BREEZE_REDIS_TEST_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())
}

#[tokio::test]
async fn native_commands_roundtrip_against_redis_7() {
    let Some(endpoint) = endpoint() else {
        eprintln!("skipping: BREEZE_REDIS_TEST_ENDPOINT not set");
        return;
    };
    let redis = RedisService::single(endpoint).await.unwrap();
    let prefix = format!("brz:redis:it:{}", std::process::id());
    let scalar = format!("{prefix}:scalar");
    let counter = format!("{prefix}:counter");
    let list = format!("{prefix}:list");
    let set = format!("{prefix}:set");
    let hash = format!("{prefix}:hash");
    let zset = format!("{prefix}:zset");
    let hll = format!("{prefix}:hll");
    let keys = vec![
        scalar.clone(),
        counter.clone(),
        list.clone(),
        set.clone(),
        hash.clone(),
        zset.clone(),
        hll.clone(),
    ];
    Redis::del_many(&redis, &keys).await.unwrap();

    let set_options = SetOptions::default()
        .with_expiration(SetExpiration::Seconds(60))
        .if_absent();
    assert!(
        Redis::set_with(&redis, &scalar, "first", set_options)
            .await
            .unwrap()
    );
    assert!(
        !Redis::set_with(&redis, &scalar, "second", set_options)
            .await
            .unwrap()
    );
    assert_eq!(
        Redis::get::<_, String>(&redis, &scalar).await.unwrap(),
        Some("first".to_string())
    );
    assert_eq!(
        Redis::mget::<_, String>(&redis, [&scalar, &counter])
            .await
            .unwrap(),
        [Some("first".to_string()), None]
    );

    assert_eq!(Redis::incr(&redis, &counter).await.unwrap(), 1);
    assert!(Redis::expire(&redis, &counter, 60).await.unwrap());
    assert_eq!(Redis::append(&redis, &scalar, "-tail").await.unwrap(), 10);
    assert_eq!(
        Redis::eval::<_, _, _, String>(
            &redis,
            "return redis.call('GET', KEYS[1])",
            [&scalar],
            [] as [&str; 0],
        )
        .await
        .unwrap(),
        "first-tail"
    );

    assert_eq!(Redis::rpush(&redis, &list, ["a", "b"]).await.unwrap(), 2);
    Redis::lset(&redis, &list, 1, "c").await.unwrap();
    assert_eq!(
        Redis::lrange::<_, String>(&redis, &list, 0, -1)
            .await
            .unwrap(),
        ["a", "c"]
    );
    assert_eq!(
        Redis::lpop::<_, String>(&redis, &list).await.unwrap(),
        Some("a".to_string())
    );

    assert_eq!(Redis::sadd(&redis, &set, ["a", "b"]).await.unwrap(), 2);
    assert_eq!(Redis::srem(&redis, &set, ["a"]).await.unwrap(), 1);
    assert_eq!(
        Redis::smembers::<_, String>(&redis, &set).await.unwrap(),
        ["b"]
    );

    assert_eq!(
        Redis::hset(&redis, &hash, "field", "value").await.unwrap(),
        1
    );
    assert_eq!(
        Redis::hget::<_, _, String>(&redis, &hash, "field")
            .await
            .unwrap(),
        Some("value".to_string())
    );
    let values: HashMap<String, String> = Redis::hgetall(&redis, &hash).await.unwrap();
    assert_eq!(values.get("field").map(String::as_str), Some("value"));

    assert_eq!(Redis::zadd(&redis, &zset, 1.5, "member").await.unwrap(), 1);
    assert_eq!(
        Redis::zrevrange_with_scores::<_, String, f64>(&redis, &zset, 0, -1)
            .await
            .unwrap(),
        [("member".to_string(), 1.5)]
    );

    assert!(Redis::pfadd(&redis, &hll, ["u1", "u2"]).await.unwrap());
    assert_eq!(Redis::pfcount(&redis, [&hll]).await.unwrap(), 2);
    assert_eq!(
        Redis::publish(&redis, "brz:redis:it:channel", "message")
            .await
            .unwrap(),
        0
    );

    let mut pipe = RedisPipe::with_capacity(5);
    pipe.incr(&counter).unwrap();
    pipe.expire(&counter, 60).unwrap();
    pipe.rpush(&list, ["x"]).unwrap();
    pipe.lset(&list, 0, "y").unwrap();
    pipe.append(&scalar, "!").unwrap();
    let mut responses = Redis::pipe(&redis, pipe).await.unwrap();
    assert_eq!(responses.take::<i64>().await.unwrap(), 2);
    assert_eq!(responses.take::<i64>().await.unwrap(), 1);
    assert_eq!(responses.take::<i64>().await.unwrap(), 2);
    responses.take::<()>().await.unwrap();
    assert_eq!(responses.take::<i64>().await.unwrap(), 11);
    assert!(responses.is_empty());

    Redis::del_many(&redis, &keys).await.unwrap();
}
