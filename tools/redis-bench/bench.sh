#!/usr/bin/env bash
#
# One-shot load test: start a throwaway redis-server container, run redis-bench
# against it, then stop the container — on success, failure, or Ctrl-C.
#
# Forward any redis-bench flags; they pass through verbatim, e.g.:
#   ./bench.sh --concurrency 64 --ops 100000
#   ./bench.sh -c 128 -d 60
#   MODE=sidecar ./bench.sh --ops 100000 get           # through a fake mesh sock file
#   ./bench.sh --replay 127.0.0.1:16379 --ops 100000   # replay client (HGET)
#
# Optional env vars:
#   MODE    direct | sidecar                (default: direct)
#   IMAGE   redis image to run            (default: the example redis:7 image)
#   PORT    host port the server binds to (default: 16379, host networking)
#   CARGO   cargo binary                  (default: cargo)
#
# MODE=sidecar emulates the mesh by publishing a sock config file pointing at
# the container, so the SDK takes the full sidecar path (discovery, pool,
# breaker). Namespace/group are fixed bench values; NAMESPACE/GROUP overridable.
#
# Uses host networking (Linux). First run compiles redis-bench in release mode
# (a few seconds); later runs reuse the cached build.

set -euo pipefail

MODE="${MODE:-direct}"
IMAGE="${IMAGE:-redis:7}"
PORT="${PORT:-16379}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
CARGO="${CARGO:-cargo}"
NAME="redis-bench-$$"
SOCK_DIR="$(mktemp -d)"

cleanup() {
  docker stop "$NAME" >/dev/null 2>&1 || true
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

echo "starting $IMAGE on port $PORT..."
docker run -d --rm --name "$NAME" --network host "$IMAGE" \
  redis-server --save "" --appendonly no --port "$PORT" >/dev/null

echo -n "waiting for redis on 127.0.0.1:$PORT ..."
ready=0
for _ in {1..50}; do
  if timeout 1 bash -c ": > /dev/tcp/127.0.0.1/$PORT" 2>/dev/null; then
    ready=1
    break
  fi
  echo -n "."
  sleep 0.1
done
if (( ready != 1 )); then
  echo " not ready"
  echo "error: redis did not start; container logs:" >&2
  docker logs "$NAME" >&2 || true
  exit 1
fi
echo " ready"
echo

case "$MODE" in
  direct)
    "$CARGO" run --release -p redis-bench -- \
      --direct "127.0.0.1:$PORT" "$@"
    ;;
  sidecar)
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
  *)
    echo "error: unknown MODE '$MODE' (direct | sidecar)" >&2
    exit 2
    ;;
esac
