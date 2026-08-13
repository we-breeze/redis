# Repository Map

## Overview

A high-performance, high-availability async Redis SDK for the breeze
platform, designed to carry ~1000 business namespaces per process. It offers
two explicitly separated access modes:

- **Sidecar mode** (`src/sidecar/`): reach Redis through the local breeze
  mesh agent. The endpoint is discovered by parsing the mesh-published sock
  config file names (`Quadruple` naming); sharding and backend failover are
  the mesh's job.
- **Direct mode** (`src/direct/`): talk to Redis backends directly. Includes
  client-side sharding (`Shards`, bit-compatible hash/distribution with the
  mesh), HA layouts (`HaServer` read fallback / double-write / set-second,
  `MsServer` master/slave read splitting), and DNS watching with per-IP load
  balancing (aligned with the Java clientBalancer).

Also included: `src/replay.rs` (single-connection client for
replay/comparison topologies, feature `direct-tcp`) and the
`tools/redis-bench` load-test harness (three client modes, fault-injection
proxy, correctness verification; scripts `bench_local.sh` / `bench.sh`,
`MATRIX=1` runs the full scenario suite).

Shared layers: `src/connection/` (single-socket multiplexing + driver task),
`src/pool/` (circuit breaker, evidence-based poisoning, lazy/load-grown
connection pool), `src/resp/` (RESP2/3 codec, zero-copy), `src/commands/`
(command surface; only hget/hmget + hset/del/ping are currently enabled,
the rest live in a block comment).

## Essential Commands

```bash
cargo fmt
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features   # zero warnings required
```

Benchmarks (native local redis; trustworthy numbers):

```bash
cd tools/redis-bench
./bench_local.sh --ops 1000000 hget                # direct baseline
MATRIX=1 ./bench_local.sh                          # full scenario matrix
```

## Non-Negotiable Rules

- **Before every code commit, run `cargo fmt` and pass the full test suite**
  (`cargo test --workspace --all-features`) with zero clippy warnings.
  Changes that don't meet this bar must not be committed.
- **Commit messages are always written in Chinese.**
- `src/direct/sharding/` is a **verbatim vendored copy** of the breeze
  sharding crate: no style adjustments (module-level clippy allow); any
  behavior change must stay bit-compatible with the mesh and pass the
  vector/randomized cross-checks in `tests.rs`.
- Keep the two access modes in separate packages: `sidecar/` and `direct/`;
  shared logic lives in root modules; the crate root only re-exports
  mode-agnostic API.
- `Value::BulkString` is zero-copy `Bytes` (not `Vec<u8>`); preserve this
  when touching the protocol layer.
- The trimmed command surface is deliberate: when enabling a new command,
  move its entry out of the block comment in `src/commands/mod.rs` and mark
  read-only commands with `@ro`.

## Git Hygiene

Keep commits focused on a single topic and avoid unrelated local edits.
Before pushing, confirm:

```bash
git status --short
git diff --check
```
