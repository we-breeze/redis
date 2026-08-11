//! End-to-end tests against a live breeze mesh.
//!
//! These are gated behind the `integration-tests` feature AND require the
//! `BREEZE_REDIS_NS` env var naming a reachable mesh namespace, so they no-op
//! in environments without a mesh. Run with:
//!
//! ```bash
//! BREEZE_REDIS_NS=my_ns cargo test --features integration-tests
//! ```

#![cfg(feature = "integration-tests")]

use redis::Commands;
use redis::sidecar::{MeshConfig, MeshRouting, SidecarClient};

fn namespace() -> Option<String> {
    std::env::var("BREEZE_REDIS_NS")
        .ok()
        .filter(|s| !s.is_empty())
}

async fn connect() -> SidecarClient {
    let ns = namespace().expect("set BREEZE_REDIS_NS to run integration tests");
    let cfg = MeshConfig::new(ns);
    SidecarClient::from_config(cfg)
        .await
        .expect("connect to mesh")
}

#[tokio::test]
async fn hset_hget_roundtrip() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    client
        .hset::<i64>("breeze_redis:it:k", "f", "v")
        .await
        .unwrap();
    let v: String = client.hget("breeze_redis:it:k", "f").await.unwrap();
    assert_eq!(v, "v");
}

#[tokio::test]
async fn hmget_roundtrip() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    client.del::<i64>("breeze_redis:it:h").await.unwrap();
    client
        .hset::<i64>("breeze_redis:it:h", "f", "1")
        .await
        .unwrap();
    let vals: Vec<Option<String>> = client
        .hmget("breeze_redis:it:h", vec!["f", "missing"])
        .await
        .unwrap();
    assert_eq!(vals, vec![Some("1".to_string()), None]);
}

#[tokio::test]
async fn pipeline_batches() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    let mut pipe = redis::pipe();
    pipe.hset("breeze_redis:it:p1", "f", "a")
        .hset("breeze_redis:it:p2", "f", "b");
    let _: Vec<redis::Value> = pipe.query_async(&client).await.unwrap();
    let a: String = client.hget("breeze_redis:it:p1", "f").await.unwrap();
    let b: String = client.hget("breeze_redis:it:p2", "f").await.unwrap();
    assert_eq!((a, b), ("a".to_string(), "b".to_string()));
}

#[tokio::test]
async fn hashkey_routing() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    // Route by an explicit hashkey; the mesh applies it to the next command.
    client
        .with_hashkey("uid:42")
        .hset::<i64>("breeze_redis:it:uid:42", "f", "x")
        .await
        .unwrap();
    let v: String = client
        .with_hashkey("uid:42")
        .hget("breeze_redis:it:uid:42", "f")
        .await
        .unwrap();
    assert_eq!(v, "x");
}
