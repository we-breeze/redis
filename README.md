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

Application code should depend on the generic root `Redis` trait. Keys,
fields, values, and bulk responses are converted through small encoding and
decoding traits rather than fixed application types:

```rust
use redis::{Redis, SidecarRedis};

# async fn demo() -> redis::RedisResult<()> {
let redis = SidecarRedis::new("feed", "auto_translate_llm").await?;
let profile: Option<redis::RedisBytes> = redis.get(("u:", 42_u64)).await?;
let version: Option<i64> = redis.hget("document:42", "version").await?;
let fields: redis::RedisValues<redis::RedisBytes> = redis
    .hmget("document:42", &["value", "compress", "hash"])
    .await?;
# let _ = (profile, version, fields);
# Ok(())
# }
```

`EncodeRedisArg` supports strings, bytes, integers encoded as decimal text, and
two- or three-element tuples concatenated into one argument. For example,
`("u:", 12345_u64, ".suffix")` is encoded directly as `u:12345.suffix` without
first building a temporary `String`. `FromRedisBulk` selects the response type;
missing values are represented as `None`. The concrete facade keeps transport
and the low-level command API out of the application boundary.

For one direct endpoint, use `RedisService::single(addr)`. Reads and writes use
two independent physical connections to the same address, keeping their queues
and timeout state isolated.

For direct master/slave access, construct an unsharded `RedisService` with one
writable master and at least one slave. Endpoints use `host:port[:db]`. Reads
use the slave replica set and writes use the master:

```rust
use redis::{Redis, RedisService};

# async fn demo() -> redis::RedisResult<()> {
let redis = RedisService::noshard(
    "redis-master.example:6379",
    ["redis-slave-a.example:6379", "redis-slave-b.example:6379"],
)
.await?;
let value: Option<redis::RedisBytes> = redis.hget("key", "field").await?;
redis.set("key", "value").await?;
# let _ = value;
# Ok(())
# }
```

Each physical IPv4 node owns one persistent multiplexed TCP connection;
replicas are selected by consumed-time quota. DNS lookup is IPv4-only and
process-shared, and changed address snapshots are applied outside the request
path with copy-on-write topology publication. There are no pool min/max
connection settings. An unsharded service directly selects its only shard and
does not encode or hash the key for routing.

`RedisService` encodes borrowed command arguments directly into the `brz-net`
process-wide dual-chunk ephemeral arena after the selected node has admitted the request.
The resulting owned RESP frame is written by `brz-net` without another
userspace buffer copy and is released as soon as the socket consumes it. The
default arena is 64 MiB per chunk (128 MiB total); applications may call
`redis::init_global_request_arena(chunk_size)` once before constructing clients.

Responses use `brz-net`'s per-connection dynamically resized ring buffer. The
driver drains a readable socket to `WouldBlock`, lets RESP parsing consume every
complete response, and reserves the remaining bulk length before reading again.
GET/HGET/HMGET replies are decoded directly into their application result shape
instead of first constructing the generic `Value` tree. A contiguous response
backs returned `Bytes` without a payload copy; only a response crossing the ring
boundary is copied. RESP tail bytes and subsequent pipelined responses remain in
the same logical ring cursor.

```rust
use redis::{Redis, RedisService, ShardRouting};

# async fn demo() -> redis::RedisResult<()> {
let redis = RedisService::sharded(
    vec![
        (
            "redis-a-master.example:6379".to_owned(),
            vec!["redis-a-slave.example:6379".to_owned()],
        ),
        (
            "redis-b-master.example:6379".to_owned(),
            vec!["redis-b-slave.example:6379".to_owned()],
        ),
    ],
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
- the low-level `Client::Direct` variant.

`sidecar::SidecarClient` and the sidecar `Client` variant remain available with
default features. Direct master/slave application access is uniformly exposed
through `RedisService::single`, `RedisService::noshard`, and
`RedisService::sharded`.

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
