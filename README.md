# redis

A high-performance, high-availability **async Redis client** (Rust / tokio) that
reaches Redis **through the breeze mesh**.

Instead of connecting to Redis backends directly, the client talks to the
single local breeze mesh agent for a resource **namespace** — over a unix
domain socket or a `127.0.0.1` port — and speaks plain RESP. The mesh proxies
to the real Redis backends and handles sharding and backend failover. This
mirrors the Java `breeze-sdk-core` `datamesh/redis` client (the `byMesh` path),
reimplemented in Rust.

## Design

- **Mesh-only.** No direct-TCP-to-Redis fallback, no client-side backend
  balancing — the mesh owns backend topology and HA.
- **Single mesh, pooled.** One local mesh endpoint per namespace, fronted by a
  pool of **multiplexed** connections: each connection pipelines many
  concurrent commands over one socket, so a small pool sustains high throughput.
- **High availability.** A circuit breaker plus a background maintenance task
  replace dead connections and probe the mesh back to health after an outage.
- **PaaS-mode discovery.** The mesh publishes its endpoint as a sock file under
  `/tmp/breeze/socks/`; the client discovers it (unix path `U_<namespace>.sock`
  or the TCP port parsed from the sock-file name) and waits until it is
  connectable. The client does not write sock files or register backends.
- **No handshake.** Identity/routing is conveyed out-of-band by the sock file,
  so the client opens the socket and immediately sends RESP (no AUTH/SELECT).

## Usage

```rust
use redis::sidecar::{SidecarClient, MeshRouting};
use redis::Commands;

# async fn demo() -> redis::RedisResult<()> {
// Connect to the mesh for a resource namespace (defaults to TCP transport).
let client = SidecarClient::connect("my_redis_namespace").await?;

client.hset::<i64>("key", "f", "value").await?;
let v: String = client.hget("key", "f").await?;

// Mesh routing: pin the next command to a shard via the hashkey side-channel,
// broadcast to all shards, or force the master (read-your-writes).
let n: i64 = client.with_hashkey("uid:42").hset("counter:uid:42", "f", 1).await?;
let latest: String = client.at_master().hget("key", "f").await?;
# let _ = (v, n, latest);
# Ok(())
# }
```

Custom configuration (group, transport, pool size, socket dir):

```rust
use redis::sidecar::{SidecarClient, MeshConfig, Transport};

# async fn demo() -> redis::RedisResult<()> {
let cfg = MeshConfig::new("my_ns")
    .with_group("prod")
    .with_transport(Transport::Unix)
    .with_max_connections(16);
let client = SidecarClient::from_config(cfg).await?;
# let _ = client;
# Ok(())
# }
```

## Command coverage

Keys/TTL, string, hash, list, set, sorted-set, scan, scripting (`EVAL`/`EVALSHA`
with auto `SCRIPT LOAD`), pipelines (incl. `MULTI`/`EXEC`), plus mesh routing
(`with_hashkey`, `broadcast`, `at_master`, and `keyshard`). Every typed command
comes from a single `implement_commands!` macro and is available on both the
`Client` and inside a `Pipeline`.

Deferred: bloom-filter (`bf*`) and long-set (`ls*`) module commands,
HyperLogLog/bitmap, RESP3 pub/sub.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test

# Integration tests need a reachable mesh; point them at a namespace:
BREEZE_REDIS_NS=my_ns cargo test --features integration-tests
```

Dependencies resolve through the `rsproxy.cn` sparse registry configured in
`.cargo/config.toml`.
