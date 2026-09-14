# brz-redis

An asynchronous Redis client built on `brz-net`, with multiplexed connections,
master/replica routing, client-side sharding, and typed command results.

The application supplies connection configuration. The library does not read
service-registry directories or depend on a particular configuration service.

## Configuration

```rust,no_run
use brz_redis::{Redis, RedisBytes, RedisConfig, RedisService, RedisServiceOptions};

# async fn example() -> brz_redis::RedisResult<()> {
let config = RedisConfig::single("127.0.0.1:6379")
    .with_options(RedisServiceOptions::default());
let service = RedisService::from_config(config).await?;
let value: Option<RedisBytes> = service.get("example:key").await?;
# Ok(())
# }
```

`RedisConfig` contains:

- `shards: Vec<RedisShardConfig>`: ordered master/replica groups.
- `routing: ShardRouting`: hash and distribution algorithms.
- `options: RedisServiceOptions`: authentication, deadlines, DNS refresh, and
  replica balancing settings.

Each `RedisShardConfig` has a `master` address and a `slaves` list. Addresses use
`host:port[:db]`. Writes go to the master and reads use the replicas. A single
endpoint fills both roles using independent read and write connections.

Use `RedisConfig::noshard(master, replicas)` for one group, or
`RedisConfig::sharded(shards, routing)` for multiple groups. Shard order affects
key placement. The existing `RedisService::single`, `noshard`, `sharded`, and
`*_with_options` constructors remain available.

Pass passwords separately with `RedisServiceOptions::with_password`. The Debug
representation redacts the password. Authentication and database selection run
before commands on initial connection and reconnection.

## Configuration providers

Applications can implement `RedisConfigProvider` for asynchronous configuration
loading:

```rust,no_run
use brz_redis::{RedisConfig, RedisConfigFuture, RedisConfigProvider, RedisService};

struct AppConfig {
    endpoint: String,
}

impl RedisConfigProvider for AppConfig {
    fn load(&self) -> RedisConfigFuture<'_> {
        Box::pin(async move { Ok(RedisConfig::single(&self.endpoint)) })
    }
}

# async fn example() -> brz_redis::RedisResult<()> {
let provider = AppConfig { endpoint: "127.0.0.1:6379".into() };
let service = RedisService::from_provider(&provider).await?;
# Ok(())
# }
```

Providers can also be passed as `&dyn RedisConfigProvider`. The service loads one
snapshot, validates it, and does not retain or poll the provider. Load failures
propagate to the caller. Logical topology stays fixed; hostname IPv4 addresses
continue to refresh through DNS.

## Commands and routing

The `Redis` trait provides string, hash, list, set, sorted-set, HyperLogLog,
expiration, scripting, and publish commands. Use `Cmd` and `Redis::command` for
additional commands. Typed `set_ex` sends `SETEX key seconds value`, and
`sismember` sends a read-only `SISMEMBER key member`. Command futures are `Send`
for generic `R: Redis` consumers. `RedisPipe` supports typed results for a single-shard
pipeline; multi-shard pipelines are rejected before sending.

One physical node owns one persistent multiplexed session. Admission fails fast
when the in-flight limit is reached. Request timeouts default to 200 ms and can
be configured separately for master and replica requests.

Hash and distribution variants retain their existing compatibility behavior.
Changing the configured algorithm, shard order, or shard count can change key
placement. The library does not provide distributed locks, cache policies, or
other application-level abstractions.

## Validation

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test --workspace --all-targets --features metrics
cargo test --doc
```

Tests normally use local protocol fixtures. Tests against an actual Redis server
are opt-in:

```sh
BREEZE_REDIS_TEST_ENDPOINT=127.0.0.1:6379 \
  cargo test --features integration-tests --test integration
```

## Benchmarks

`tools/redis-bench` is a standalone, non-publishable workspace. It accepts either
`--direct host:port[:db]` or `--shards address,address,...`, constructs a
`RedisConfig`, and reports throughput, error rate, and latency percentiles.
Fault injection supports delays, resets, and outages. The tool uses the sibling
`../memory` crate for allocator statistics; it has no service-discovery dependency.

```sh
cargo run --release --manifest-path tools/redis-bench/Cargo.toml -- \
  --direct 127.0.0.1:6379 --concurrency 32 --ops 10000 hget

cargo run --release --manifest-path tools/redis-bench/Cargo.toml -- \
  --shards 127.0.0.1:6379,127.0.0.1:6380 --ops 10000 hmget
```

`bench.sh` starts disposable Docker containers using `redis:7` by default;
`bench_local.sh` uses local `redis-server` processes. Both support `MODE=direct`
and `MODE=shards`, with `MATRIX=1` selecting the fault-injection matrix. Run these
against dedicated benchmark instances because workloads write data.

## Releases

The GitHub workflows provide CI and manual publishing. Run **Actions → Publish**
on `main`, leaving `retry_tag` empty to allocate the next `v0.0.x` version. After
validation, the workflow pushes the version commit and tag atomically and
publishes using `CARGO_REGISTRY_TOKEN`. Normal pushes do not publish.

If uploading fails after tagging, rerun with the existing tag in `retry_tag`.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.

## Crate naming

The package name is `brz-redis`; the Rust library name is `brz_redis`.
Use `brz_redis::...` in Rust code. This replaces the previous `redis`
library name. Existing explicit dependency aliases remain supported.

```toml
[dependencies]
brz-redis = "0.0.8"
```
