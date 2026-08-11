#!/usr/bin/env bash
#
# One-shot load test: start a throwaway redis-server container, run redis-bench
# against it, then stop the container — on success, failure, or Ctrl-C.
# Works on Linux and macOS (Docker Desktop): explicit port publishing, no
# GNU-only tools.
#
# Forward any redis-bench flags; they pass through verbatim, e.g.:
#
# Basic:
#   ./bench.sh --concurrency 64 --ops 1000000 hget          # direct mode (default)
#   ./bench.sh -c 128 -d 60 hmget                           # 60s timed run, 4-field HMGET
#   MODE=sidecar ./bench.sh --ops 1000000 hget              # via a fake mesh sock file
#   MODE=shards SHARDS=4 ./bench.sh --ops 1000000 hget      # N containers as shards
#   ./bench.sh --replay 127.0.0.1:16379 --ops 100000        # replay client (HGET)
#
# Fault injection (a local proxy delays a fraction of request frames):
#   ./bench.sh --ops 1000000 hget --slow-rate 0.0001 --slow-ms 200
#       # 0.01% of requests slowed by 200ms (tail latency)
#   ./bench.sh --ops 1000000 hget --timeout-rate 0.0001 --timeout-ms 5000
#       # 0.01% hang past the SDK op_timeout (1s) -> timeout + retry path
#   MODE=shards SHARDS=4 ./bench.sh --ops 1000000 hget \
#       --slow-rate 0.001 --slow-ms 100 --fault-shard 0
#       # only shard 0 is slowed — "one slow shard among many backends"
#
# Optional env vars:
#   MODE    direct | sidecar | shards       (default: direct)
#   IMAGE   redis image to run            (default: the example redis:7 image)
#   PORT    host port the server binds to (default: 16379; shards mode uses
#           PORT..PORT+SHARDS-1)
#   SHARDS  shard count in shards mode    (default: 4)
#   CARGO   cargo binary                  (default: cargo)
#
# MODE=sidecar emulates the mesh by publishing a sock config file pointing at
# the container, so the SDK takes the full sidecar path (discovery, pool,
# breaker). Namespace/group are fixed bench values; NAMESPACE/GROUP overridable.
#
# Uses explicit port mapping (works on both Linux and Docker Desktop for
# macOS, where --network host is a no-op). First run compiles redis-bench in
# release mode (a few seconds); later runs reuse the cached build.

set -euo pipefail

MODE="${MODE:-direct}"
IMAGE="${IMAGE:-redis:7}"
PORT="${PORT:-16379}"
SHARDS="${SHARDS:-4}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
CARGO="${CARGO:-cargo}"
NAME="redis-bench-$$"
# Plain `mktemp -d` is GNU-only; an explicit template works everywhere.
SOCK_DIR="$(mktemp -d /tmp/bench-socks.XXXXXX)"

cleanup() {
  docker stop $(docker ps -q --filter "name=^${NAME}") >/dev/null 2>&1 || true
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# Start a container publishing $2=$1 on the host, and wait for redis.
start_redis() {
  local port="$1" name="$2"
  echo "starting $IMAGE on port $port (container $name)..."
  docker run -d --rm --name "$name" -p "$port:$port" "$IMAGE" \
    redis-server --save "" --appendonly no --port "$port" >/dev/null

  echo -n "waiting for redis on 127.0.0.1:$port ..."
  local ready=0
  for _ in {1..50}; do
    # `timeout(1)` is GNU coreutils, absent on macOS; a refused /dev/tcp
    # connect returns immediately, and the loop bounds the total wait.
    if bash -c ": > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      ready=1
      break
    fi
    echo -n "."
    sleep 0.1
  done
  if (( ready != 1 )); then
    echo " not ready"
    echo "error: redis did not start; container logs:" >&2
    docker logs "$name" >&2 || true
    exit 1
  fi
  echo " ready"
  echo
}

case "$MODE" in
  direct)
    start_redis "$PORT" "$NAME"
    "$CARGO" run --release -p redis-bench -- \
      --direct "127.0.0.1:$PORT" "$@"
    ;;
  sidecar)
    start_redis "$PORT" "$NAME"
    # Publish a sock config file exactly like the mesh agent would:
    #   <service path with + separators>@redis:<port>@rs
    # ending in +<group>+<namespace>.
    sock="$SOCK_DIR/static.config.api.example.com+3+config+cloud+redis+${GROUP}+${NAMESPACE}@redis:${PORT}@rs"
    touch "$sock"
    echo "sidecar mode: published sock file $(basename "$sock")"
    echo
    "$CARGO" run --release -p redis-bench -- \
      --namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR" "$@"
    ;;
  shards)
    addrs=""
    for i in $(seq 0 $((SHARDS - 1))); do
      port=$((PORT + i))
      start_redis "$port" "$NAME-$i"
      addrs="${addrs:+$addrs,}127.0.0.1:$port"
    done
    echo "shards mode: $SHARDS containers ($addrs)"
    "$CARGO" run --release -p redis-bench -- \
      --shards "$addrs" "$@"
    ;;
  *)
    echo "error: unknown MODE '$MODE' (direct | sidecar | shards)" >&2
    exit 2
    ;;
esac
