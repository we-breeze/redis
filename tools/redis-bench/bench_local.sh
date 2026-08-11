#!/usr/bin/env bash
#
# Local (no docker) load test: start a native redis-server, run redis-bench
# against it, then stop the server. This is the fast path — no container
# port-forwarding or platform emulation, so the numbers reflect the SDK.
#
# Modes (MODE env):
#   direct   (default) SDK direct::DirectClient -> redis
#   sidecar  emulates the mesh with a sock config file -> SDK sidecar stack
#   replay   redis::replay single-connection client (HGET)
#   shards   N native redis instances as shards, SDK direct::Shards router
#
# Examples:
#
# Basic:
#   ./bench_local.sh --ops 1000000 hget                     # direct mode (default)
#   ./bench_local.sh -c 64 -d 60 hmget                      # 60s timed run, 4-field HMGET
#   MODE=sidecar ./bench_local.sh --ops 1000000 hget        # via a fake mesh sock file
#   MODE=replay  ./bench_local.sh --ops 100000              # replay client (HGET)
#   MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 hget  # N local redis as shards
#
# Fault injection (a local proxy delays a fraction of request frames):
#   ./bench_local.sh --ops 1000000 hget --slow-rate 0.0001 --slow-ms 200
#       # 0.01% of requests slowed by 200ms (tail latency)
#   ./bench_local.sh --ops 1000000 hget --timeout-rate 0.0001 --timeout-ms 5000
#       # 0.01% hang past the SDK op_timeout (1s) -> timeout + retry path
#   MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 hget \
#       --slow-rate 0.001 --slow-ms 100 --fault-shard 0
#       # only shard 0 is slowed — "one slow shard among many backends"
#
# Optional env vars:
#   PORT      redis port            (default: 16399)
#   SHARDS    shard count in shards mode (default: 4); shards listen on
#             PORT, PORT+1, ... — the "base" single instance is shard 0
#   REDIS     redis-server binary   (default: redis-server from PATH)
#   CARGO     cargo binary          (default: cargo)
#   KEEP_REDIS=1  leave the server running after the bench
#
# The redis server is started like this (expand REDIS/PORT/LOG):
#   redis-server --port 16399 --daemonize yes  --save '' --appendonly no --logfile /tmp/bench-local-redis-16399.log
# Or bring your own: start redis however you like, then KEEP_REDIS=1
# ./bench_local.sh ...  (the script reuses any instance already on PORT).
#
# Inspecting bench keys afterwards (KEEP_REDIS=1):
#   redis-cli -p 16399 --no-raw --scan | head        # direct/sidecar keys
#   redis-cli -p 16399 hget h:bench:0 f              # replay keys

set -euo pipefail

# redis-server 8.x refuses to start under some locale names (e.g. C.UTF-8 on
# macOS); force a known-good one.
export LC_ALL=en_US.UTF-8

MODE="${MODE:-direct}"
PORT="${PORT:-16399}"
SHARDS="${SHARDS:-4}"
REDIS="${REDIS:-redis-server}"
CARGO="${CARGO:-cargo}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
# Private per-run sock dir: stale sock files from other runs would win the
# discovery score tie and point at dead ports.
SOCK_DIR="$(mktemp -d /tmp/bench-socks.XXXXXX)"

started_by_us=0
started_ports=()
cleanup() {
  if [[ "${KEEP_REDIS:-0}" != "1" ]]; then
    for p in ${started_ports[@]+"${started_ports[@]}"}; do
      redis-cli -p "$p" shutdown nosave 2>/dev/null || true
    done
  fi
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# Start (or reuse) a redis on the given port.
ensure_redis() {
  local port="$1"
  if redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; then
    echo "redis already running on 127.0.0.1:$port (leaving it as-is)"
    return
  fi
  echo "starting redis-server on port $port..."
  "$REDIS" --port "$port" --daemonize yes --save '' --appendonly no \
    --logfile "/tmp/bench-local-redis-$port.log"
  started_ports+=("$port")
  for _ in {1..50}; do
    if redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; then
      break
    fi
    sleep 0.1
  done
  redis-cli -p "$port" ping >/dev/null
}

case "$MODE" in
  direct)
    ensure_redis "$PORT"
    "$CARGO" run --release -p redis-bench -- \
      --direct "127.0.0.1:$PORT" "$@"
    ;;
  sidecar)
    ensure_redis "$PORT"
    # Emulate the mesh: publish a sock config file pointing at our redis.
    sock="$SOCK_DIR/static.config.api.example.com+3+config+cloud+redis+${GROUP}+${NAMESPACE}@redis:${PORT}@rs"
    touch "$sock"
    echo "sidecar mode: published $(basename "$sock")"
    "$CARGO" run --release -p redis-bench -- \
      --namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR" "$@"
    ;;
  replay)
    ensure_redis "$PORT"
    "$CARGO" run --release -p redis-bench -- \
      --replay "127.0.0.1:$PORT" "$@"
    ;;
  shards)
    addrs=""
    for i in $(seq 0 $((SHARDS - 1))); do
      port=$((PORT + i))
      ensure_redis "$port"
      addrs="${addrs:+$addrs,}127.0.0.1:$port"
    done
    echo "shards mode: $SHARDS shards ($addrs)"
    "$CARGO" run --release -p redis-bench -- \
      --shards "$addrs" "$@"
    ;;
  *)
    echo "error: unknown MODE '$MODE' (direct | sidecar | replay | shards)" >&2
    exit 2
    ;;
esac
