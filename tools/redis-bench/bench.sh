#!/usr/bin/env bash
#
# One-shot load test: start a throwaway redis-server container, run redis-bench
# against it in direct mode, then stop the container — on success, failure, or
# Ctrl-C.
#
# Forward any redis-bench flags; they pass through verbatim, e.g.:
#   ./bench.sh --concurrency 64 --ops 100000
#   ./bench.sh -c 128 -d 60
#
# Optional env vars:
#   IMAGE   redis image to run            (default: the example redis:7 image)
#   PORT    host port the server binds to (default: 16379, host networking)
#   CARGO   cargo binary                  (default: cargo)
#
# Uses host networking (Linux). First run compiles redis-bench in release mode
# (a few seconds); later runs reuse the cached build.

set -euo pipefail

IMAGE="${IMAGE:-redis:7}"
PORT="${PORT:-16379}"
CARGO="${CARGO:-cargo}"
NAME="redis-bench-$$"

cleanup() {
  docker stop "$NAME" >/dev/null 2>&1 || true
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

"$CARGO" run --release -p redis-bench -- \
  --direct "127.0.0.1:$PORT" "$@"
