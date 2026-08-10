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
#
# Examples:
#   ./bench_local.sh --ops 1000000 get
#   MODE=sidecar ./bench_local.sh --concurrency 64 --ops 1000000 set
#   MODE=replay  ./bench_local.sh --ops 100000
#
# Optional env vars:
#   PORT      redis port            (default: 16399)
#   REDIS     redis-server binary   (default: redis-server from PATH)
#   CARGO     cargo binary          (default: cargo)
#   KEEP_REDIS=1  leave the server running after the bench
#
# The redis server is started like this (expand REDIS/PORT/LOG):
#   redis-server --port 16399 --daemonize yes  --save '' --appendonly no --logfile /tmp/bench-local-redis.log
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
REDIS="${REDIS:-redis-server}"
CARGO="${CARGO:-cargo}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
# Private per-run sock dir: stale sock files from other runs would win the
# discovery score tie and point at dead ports.
SOCK_DIR="$(mktemp -d /tmp/bench-socks.XXXXXX)"
LOG=/tmp/bench-local-redis.log

started_by_us=0
cleanup() {
  if (( started_by_us == 1 )) && [[ "${KEEP_REDIS:-0}" != "1" ]]; then
    redis-cli -p "$PORT" shutdown nosave 2>/dev/null || true
  fi
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# Start redis unless one is already answering on PORT.
if redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG; then
  echo "redis already running on 127.0.0.1:$PORT (leaving it as-is)"
else
  echo "starting redis-server on port $PORT..."
  "$REDIS" --port "$PORT" --daemonize yes --save '' --appendonly no --logfile "$LOG"
  started_by_us=1
  for _ in {1..50}; do
    if redis-cli -p "$PORT" ping 2>/dev/null | grep -q PONG; then
      break
    fi
    sleep 0.1
  done
fi
redis-cli -p "$PORT" ping >/dev/null

case "$MODE" in
  direct)
    "$CARGO" run --release -p redis-bench -- \
      --direct "127.0.0.1:$PORT" "$@"
    ;;
  sidecar)
    # Emulate the mesh: publish a sock config file pointing at our redis.
    sock="$SOCK_DIR/static.config.api.example.com+3+config+cloud+redis+${GROUP}+${NAMESPACE}@redis:${PORT}@rs"
    touch "$sock"
    echo "sidecar mode: published $(basename "$sock")"
    "$CARGO" run --release -p redis-bench -- \
      --namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR" "$@"
    ;;
  replay)
    "$CARGO" run --release -p redis-bench -- \
      --replay "127.0.0.1:$PORT" "$@"
    ;;
  *)
    echo "error: unknown MODE '$MODE' (direct | sidecar | replay)" >&2
    exit 2
    ;;
esac
