# redis

A high-performance, high-availability **async Redis client** (Rust / tokio) that
reaches Redis **through the breeze mesh**.

Instead of connecting to Redis backends directly, the client talks to the
single local breeze mesh agent for a resource **namespace** over a local TCP
port and speaks plain RESP. The mesh proxies
to the real Redis backends and handles sharding and backend failover. This
mirrors the Java `breeze-sdk-core` `datamesh/redis` client (the `byMesh` path),
reimplemented in Rust.

## Application API

Application code should depend on the root `Redis` trait. Its first version
contains only the `GET`, `HGET`, and `HMGET` commands currently used by abtest:

```rust
use redis::{Redis, SidecarRedis};

# async fn demo() -> redis::RedisResult<()> {
let redis = SidecarRedis::new("feed", "auto_translate_llm").await?;
let profile = redis.get("u:42").await?;
let version = redis.hget("document:42", "version").await?;
let fields = redis
    .hmget("document:42", &["value", "compress", "hash"])
    .await?;
# let _ = (profile, version, fields);
# Ok(())
# }
```

Both methods return binary-safe `RedisBytes`; missing values are represented
as `None`. The concrete facade keeps pools and the low-level command API out of
the application boundary.

For direct master/slave access, construct `MsRedis` with one writable master
and at least one slave. Endpoints use `host:port[:db]`. The application
`Redis` reads are spread across healthy slaves, while the lower-level
`Commands` write operations always use the master:

```rust
use redis::{Commands, MsRedis, Redis};

# async fn demo() -> redis::RedisResult<()> {
let redis = MsRedis::new(
    "redis-master.example:6379",
    ["redis-slave-a.example:6379", "redis-slave-b.example:6379"],
)
.await?;
let value = Redis::hget(&redis, "key", "field").await?;
let _: i64 = redis.hset("key", "field", "value").await?;
# let _ = value;
# Ok(())
# }
```

For the `brz-net` direct transport, use `RedisService`. Each physical IPv4
node owns one persistent multiplexed TCP connection; replicas are selected by
consumed-time quota. DNS lookup is IPv4-only and process-shared, and changed
address snapshots are applied outside the request path with copy-on-write
topology publication. There are no pool min/max connection settings:

```rust
use redis::{Redis, RedisService, ShardRouting};

# async fn demo() -> redis::RedisResult<()> {
let redis = RedisService::sharded(
    vec![(
        "redis-master.example:6379".to_owned(),
        vec![
            "redis-slave-a.example:6379".to_owned(),
            "redis-slave-b.example:6379".to_owned(),
        ],
    )],
    ShardRouting::range("crc32", 256),
)
.await?;
let value = redis.get_routed(b"1821155363", "u:1821155363").await?;
# let _ = value;
# Ok(())
# }
```

With the default feature set, direct-backend implementation types remain
internal. Enable `direct-mock` only for tests, validation tools, or benchmarks
that need to construct direct clients. That feature exposes:

- `direct::DirectClient`: one direct Redis backend;
- `direct::HaServer`: primary/fallback with optional double write;
- `direct::MsServer`: master/slave read splitting;
- `direct::Shards<T>`: client-side sharding over another connection form;
- the `Client::Direct` variant and `DirectRedis` facade.

`sidecar::SidecarClient`, `MsRedis`, `ShardedMsRedis`, and the sidecar `Client`
variant remain available with default features.

For several master/slave groups, `ShardedMsRedis` hashes each Redis key first
and then delegates the operation to the selected `MsRedis`. The distribution
is `modula`, so group order is part of the routing contract:

```rust
use redis::{Redis, ShardedMsRedis};

# async fn demo() -> redis::RedisResult<()> {
let redis = ShardedMsRedis::new(
    "crc32",
    vec![
        (
            "redis-a-master.example:6379",
            vec!["redis-a-slave-1.example:6379", "redis-a-slave-2.example:6379"],
        ),
        (
            "redis-b-master.example:6379",
            vec!["redis-b-slave-1.example:6379", "redis-b-slave-2.example:6379"],
        ),
    ],
)
.await?;
let value = Redis::hget(&redis, "user:42", "name").await?;
# let _ = value;
# Ok(())
# }
```

`MsRedis` also exposes the low-level `Commands`/`ConnectionLike` surface.
`ShardedMsRedis` deliberately exposes only the application `Redis` contract,
so routing always uses the explicit key supplied to `get`, `hget`, or `hmget`.

## Design

- **Sidecar-first application API.** `SidecarRedis` is the first application
  implementation; direct access remains an explicit lower-level mode.
- **Single mesh, pooled.** One local mesh endpoint per namespace, fronted by a
  pool of **multiplexed** connections: each connection pipelines many
  concurrent commands over one socket, so a small pool sustains high throughput.
- **High availability.** A circuit breaker plus a background maintenance task
  replace dead connections and probe the mesh back to health after an outage.
- **PaaS-mode discovery.** The mesh publishes its endpoint as a sock file under
  `/data1/breeze/socks/`; the client discovers the TCP port parsed from the
  sock-file name and waits until it is connectable. TCP uses a non-blank
  `MESH_CONNECT_HOST`, falling back to `127.0.0.1`. Unix sockets and
  non-numeric slots are unsupported. The client does not write sock files or
  register backends.
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

Custom configuration (group, pool size, socket dir):

```rust
use redis::sidecar::{SidecarClient, MeshConfig};

# async fn demo() -> redis::RedisResult<()> {
let cfg = MeshConfig::new("my_ns")
    .with_group("prod")
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
