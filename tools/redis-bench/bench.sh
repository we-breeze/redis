#!/usr/bin/env bash
#
# 容器版一键压测：启动一次性 redis 容器，跑 redis-bench，结束后清理。
# 同时兼容 Linux 和 macOS（Docker Desktop）：显式端口映射，无 GNU 专有工具。
# 注意：Mac 上 amd64 镜像走模拟 + 端口转发，吞吐只有本机直跑的零头，
# 只适合功能验证；要真实性能数字请用 bench_local.sh（原生 redis）。
#
# 模式（MODE 环境变量）：
#   direct   （默认）SDK direct::DirectClient 直连 redis 容器
#   sidecar  用假 sock 文件模拟 mesh 发布，走完整 sidecar 链路
#   shards   N 个容器当分片，走 direct::Shards 客户端路由
#
# 用法示例（更多故障注入示例见 bench_local.sh 头部注释，参数完全一致）：
#
#   基本：
#     ./bench.sh --concurrency 64 --ops 1000000 hget
#     ./bench.sh -c 128 -d 60 hmget
#     MODE=sidecar ./bench.sh --ops 1000000 hget
#     MODE=shards SHARDS=4 ./bench.sh --ops 1000000 hget
#     ./bench.sh --replay 127.0.0.1:16379 --ops 100000     # replay 客户端（HGET）
#
#   故障注入：
#     ./bench.sh --ops 1000000 hget --slow-rate 0.0001 --slow-ms 200      # 慢请求
#     ./bench.sh --ops 1000000 hget --timeout-rate 0.0001 --timeout-ms 5000  # 超时
#     ./bench.sh --ops 1000000 hget --reset-rate 0.0005                   # 断连
#     ./bench.sh -d 40 hget --outage-ms 3000 --outage-interval-ms 20000   # 周期不可用
#     ./bench.sh --ops 1000000 hget --cpu-stall-rate 0.002 --cpu-stall-ms 20  # CPU 过载
#     MODE=shards SHARDS=4 ./bench.sh --ops 1000000 hget \
#         --slow-rate 0.001 --slow-ms 100 --fault-shard 0                 # 单分片慢
#
#   大 value 与正确性：
#     ./bench.sh --ops 1000000 hget --big-value-rate 0.1 --big-value-size 1024
#     ./bench.sh --ops 1000000 hmget --verify
#
#   压测矩阵（一键全场景）：
#     MATRIX=1 ./bench.sh
#
# 环境变量：
#   MODE    direct | sidecar | shards（默认 direct）
#   MATRIX  1 = 跑完整压测矩阵（默认关）
#   IMAGE   redis 镜像（默认 redis:7；
#           Apple Silicon 建议 IMAGE=redis:7 用 arm64 原生镜像）
#   PORT    容器映射到宿主机的端口（默认 16379；shards 模式占用 PORT..PORT+N-1）
#   SHARDS  分片数（默认 4）
#   OPS     矩阵模式每场的操作数（默认 500000）
#   CARGO   cargo 路径（默认 cargo）

set -euo pipefail

MODE="${MODE:-direct}"
MATRIX="${MATRIX:-0}"
IMAGE="${IMAGE:-redis:7}"
PORT="${PORT:-16379}"
SHARDS="${SHARDS:-4}"
OPS="${OPS:-500000}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
CARGO="${CARGO:-cargo}"
NAME="redis-bench-$$"
# 裸 mktemp -d 是 GNU 写法；显式模板在 GNU/macOS 下都可用。
SOCK_DIR="$(mktemp -d /tmp/bench-socks.XXXXXX)"

cleanup() {
  docker stop $(docker ps -q --filter "name=^${NAME}") >/dev/null 2>&1 || true
  rm -rf "$SOCK_DIR"
}
trap cleanup EXIT INT TERM

# 启动一个容器并把端口映射到宿主机，等待 redis 就绪。
start_redis() {
  local port="$1" name="$2"
  echo "starting $IMAGE on port $port (container $name)..."
  docker run -d --rm --name "$name" -p "$port:$port" "$IMAGE" \
    redis-server --save "" --appendonly no --port "$port" >/dev/null

  echo -n "waiting for redis on 127.0.0.1:$port ..."
  local ready=0
  for _ in {1..50}; do
    # timeout(1) 是 GNU coreutils，macOS 没有；/dev/tcp 连接被拒会立即返回，
    # 循环次数兜底总等待。
    if bash -c ": > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      ready=1
      break
    fi
    echo -n "."
    sleep 0.1
  done
  if (( ready != 1 )); then
    echo " not ready"
    echo "error: redis 未就绪，容器日志：" >&2
    docker logs "$name" >&2 || true
    exit 1
  fi
  echo " ready"
  echo
}

# 按模式准备环境并生成目标参数（写入全局 TARGET_ARGS）。
TARGET_ARGS=()
prepare() {
  case "$MODE" in
    direct)
      start_redis "$PORT" "$NAME"
      TARGET_ARGS=(--direct "127.0.0.1:$PORT")
      ;;
    sidecar)
      start_redis "$PORT" "$NAME"
      # 模拟 mesh 发布 sock 配置文件：
      #   <以 + 分隔的服务路径>@redis:<port>@rs，尾部是 +<group>+<namespace>
      local sock="$SOCK_DIR/static.config.api.example.com+3+config+cloud+redis+${GROUP}+${NAMESPACE}@redis:${PORT}@rs"
      touch "$sock"
      echo "sidecar 模式: 已发布 sock 文件 $(basename "$sock")"
      echo
      TARGET_ARGS=(--namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR")
      ;;
    shards)
      local addrs=""
      for i in $(seq 0 $((SHARDS - 1))); do
        local port=$((PORT + i))
        start_redis "$port" "$NAME-$i"
        addrs="${addrs:+$addrs,}127.0.0.1:$port"
      done
      echo "shards 模式: $SHARDS 个容器 ($addrs)"
      TARGET_ARGS=(--shards "$addrs")
      ;;
    *)
      echo "error: 未知 MODE '$MODE'（direct | sidecar | shards）" >&2
      exit 2
      ;;
  esac
}

# 跑一场：run_one <说明> <redis-bench 参数...>
run_one() {
  local title="$1"; shift
  echo
  echo "########## $title ##########"
  "$CARGO" run --release -p redis-bench -- "${TARGET_ARGS[@]}" "$@"
}

run_matrix() {
  echo "== 压测矩阵（MODE=$MODE, 每场 $OPS ops / outage 场为时长模式）=="
  run_one "基线 HGET"        --ops "$OPS" hget
  run_one "基线 HMGET"       --ops "$OPS" hmget
  run_one "大 value 10%x10k" --ops "$OPS" --big-value-rate 0.1 --big-value-size 10240 hget
  run_one "偶发慢请求 0.1%x200ms" --ops "$OPS" --slow-rate 0.001 --slow-ms 200 hget
  run_one "偶发超时 0.01%x5s"   --ops "$OPS" --timeout-rate 0.0001 --timeout-ms 5000 hget
  run_one "偶发断连 0.05%"      --ops "$OPS" --reset-rate 0.0005 hget
  run_one "SDK 侧 CPU 过载 0.2%x20ms" --ops "$OPS" --cpu-stall-rate 0.002 --cpu-stall-ms 20 hget
  run_one "周期性不可用 3s/10s" -d 25 --outage-ms 3000 --outage-interval-ms 10000 hget
  run_one "正确性校验 HMGET"    --ops "$OPS" --verify hmget
}

prepare

if [[ "$MATRIX" == "1" ]]; then
  run_matrix
else
  "$CARGO" run --release -p redis-bench -- "${TARGET_ARGS[@]}" "$@"
fi
