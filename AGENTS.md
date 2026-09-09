# Repository Guidelines

## Architecture

This crate exposes one production facade: `RedisService` on `brz-net`.

- `src/redis_service.rs`: fixed logical topologies, DNS reconciliation,
  replica selection, typed commands, and pipelines.
- `src/net_transport.rs`: RESP request/response adapter for one `brz-net`
  multiplexed session.
- `src/config.rs`: application-supplied configuration and one-shot providers.
  The SDK must not depend on directory discovery or deployment naming rules.
  Deployment adapters belong in the caller, including the standalone benchmark.
- `src/sharding/`: vendored Breeze hash/distribution algorithms. Preserve
  compatibility vectors when changing it.
- `src/arg.rs`, `src/bulk.rs`, `src/service_pipe.rs`: allocation-conscious
  typed request and response APIs.

Do not reintroduce the removed sidecar client, direct client, generic
`ConnectionLike`, or connection pool. Config, provider, single, noshard, and sharded are
construction modes of the same `RedisService`.

## Required checks

Run `cargo fmt --all -- --check`, `cargo test --workspace --all-targets`, and
`cargo clippy --workspace --all-targets -- -D warnings` for behavior changes.
Live Redis integration checks must remain opt-in.
