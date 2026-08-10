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

use redis::sidecar::{SidecarClient, MeshConfig, MeshRouting};
use redis::Commands;

fn namespace() -> Option<String> {
    std::env::var("BREEZE_REDIS_NS")
        .ok()
        .filter(|s| !s.is_empty())
}

async fn connect() -> SidecarClient {
    let ns = namespace().expect("set BREEZE_REDIS_NS to run integration tests");
    let cfg = MeshConfig::new(ns);
    SidecarClient::from_config(cfg).await.expect("connect to mesh")
}

#[tokio::test]
async fn set_get_roundtrip() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    client.set::<()>("breeze_redis:it:k", "v").await.unwrap();
    let v: String = client.get("breeze_redis:it:k").await.unwrap();
    assert_eq!(v, "v");
}

#[tokio::test]
async fn hash_and_incr() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    client.del::<i64>("breeze_redis:it:n").await.unwrap();
    let n: i64 = client.incr("breeze_redis:it:n").await.unwrap();
    assert_eq!(n, 1);

    client
        .hset::<i64>("breeze_redis:it:h", "f", "1")
        .await
        .unwrap();
    let f: String = client.hget("breeze_redis:it:h", "f").await.unwrap();
    assert_eq!(f, "1");
}

#[tokio::test]
async fn pipeline_batches() {
    let Some(_) = namespace() else {
        eprintln!("skipping: BREEZE_REDIS_NS not set");
        return;
    };
    let client = connect().await;
    let mut pipe = redis::pipe();
    pipe.set("breeze_redis:it:p1", "a")
        .set("breeze_redis:it:p2", "b");
    let _: Vec<redis::Value> = pipe.query_async(&client).await.unwrap();
    let vals: Vec<String> = client
        .mget(vec!["breeze_redis:it:p1", "breeze_redis:it:p2"])
        .await
        .unwrap();
    assert_eq!(vals, vec!["a".to_string(), "b".to_string()]);
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
        .set::<()>("breeze_redis:it:uid:42", "x")
        .await
        .unwrap();
    let v: String = client
        .with_hashkey("uid:42")
        .get("breeze_redis:it:uid:42")
        .await
        .unwrap();
    assert_eq!(v, "x");
}
