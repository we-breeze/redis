#!/usr/bin/env bash
#
# 本机压测脚本（不依赖 docker）：启动原生 redis-server，跑 redis-bench，
# 结束后自动清理。没有容器端口转发和平台模拟开销，数字反映 SDK 真实水平。
#
# 模式（MODE 环境变量）：
#   direct   （默认）SDK direct::DirectClient 直连 redis
#   sidecar  用假 sock 文件模拟 mesh 发布，走完整 sidecar 链路
#   shards   N 个原生 redis 当分片，走 direct::Shards 客户端路由
#
# 用法示例：
#
#   基本：
#     ./bench_local.sh --ops 1000000 hget                     # direct 模式
#     ./bench_local.sh -c 64 -d 60 hmget                      # 60 秒时长模式，4 字段 HMGET
#     MODE=sidecar ./bench_local.sh --ops 1000000 hget        # sidecar（假 sock 文件）
#     MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 hget  # N 个本机 redis 分片
#
#   故障注入（本地代理按帧注入）：
#     ./bench_local.sh --ops 1000000 hget --slow-rate 0.0001 --slow-ms 200
#         # 万分之一请求慢 200ms（尾延迟）
#     ./bench_local.sh --ops 1000000 hget --timeout-rate 0.0001 --timeout-ms 5000
#         # 万分之一请求挂起 5s，超过 SDK op_timeout(1s)，走超时重试路径
#     ./bench_local.sh --ops 1000000 hget --reset-rate 0.0005
#         # 万分之五连接被重置（断连重连路径）
#     ./bench_local.sh -d 40 hget --outage-ms 3000 --outage-interval-ms 20000
#         # 每 20 秒一次 3 秒完全不可用（熔断 + 1s 探测自愈）
#     ./bench_local.sh --ops 1000000 hget --cpu-stall-rate 0.002 --cpu-stall-ms 20
#         # 千分之二请求前 SDK 侧自旋 20ms（模拟 CPU 过载/GC 停顿）
#     MODE=shards SHARDS=4 ./bench_local.sh --ops 1000000 hget \
#         --slow-rate 0.001 --slow-ms 100 --fault-shard 0
#         # 只慢 0 号分片（多分片下单分片故障）
#
#   大 value 与正确性：
#     ./bench_local.sh --ops 1000000 hget --big-value-rate 0.1 --big-value-size 1024
#         # 10% 的 key 是 1k 大 value
#     ./bench_local.sh --ops 1000000 hget --verify
#         # 逐条校验回复（种子始终写 field||key 自描述内容，--verify 只是校验开关）
#
#   压测矩阵（一键全场景）：
#     MATRIX=1 ./bench_local.sh                 # direct 模式跑全套场景
#     MATRIX=1 MODE=shards SHARDS=4 ./bench_local.sh
#
# 环境变量：
#   MODE      direct | sidecar | shards（默认 direct）
#   MATRIX    1 = 跑完整压测矩阵（默认关）
#   PORT      redis 端口（默认 16399；shards 模式占用 PORT..PORT+SHARDS-1）
#   SHARDS    分片数（默认 4）
#   OPS       矩阵模式每场的操作数（默认 500000）
#   REDIS     redis-server 路径（默认 PATH 里的 redis-server）
#   CARGO     cargo 路径（默认 cargo）
#   KEEP_REDIS=1  压测结束后保留 redis（方便手动查 key）
#
# redis 启动命令（展开 REDIS/PORT 后）：
#   redis-server --port 16399 --daemonize yes \
#       --save '' --appendonly no --logfile /tmp/bench-local-redis-16399.log
# 也可以自己起 redis，脚本会复用 PORT 上已在监听的实例。
#
# 压测后查看 key（需 KEEP_REDIS=1）：
#   redis-cli -p 16399 --no-raw --scan | head        # direct/sidecar 的 key

set -euo pipefail

# redis-server 8.x 在某些 locale 下拒绝启动（如 macOS 的 C.UTF-8），强制可用值。
export LC_ALL=en_US.UTF-8

MODE="${MODE:-direct}"
MATRIX="${MATRIX:-0}"
PORT="${PORT:-16399}"
SHARDS="${SHARDS:-4}"
OPS="${OPS:-500000}"
REDIS="${REDIS:-redis-server}"
CARGO="${CARGO:-cargo}"
NAMESPACE="${NAMESPACE:-bench_ns}"
GROUP="${GROUP:-bench}"
# 每次运行独立的 sock 目录：旧目录里的残留 sock 文件会赢得发现评分、指向死端口。
SOCK_DIR="$(mktemp -d /tmp/bench-socks.XXXXXX)"

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

# 启动（或复用）指定端口的 redis。
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

# 按模式准备环境并生成目标参数（写入全局 TARGET_ARGS）。
TARGET_ARGS=()
prepare() {
  case "$MODE" in
    direct)
      ensure_redis "$PORT"
      TARGET_ARGS=(--direct "127.0.0.1:$PORT")
      ;;
    sidecar)
      ensure_redis "$PORT"
      # 模拟 mesh 发布 sock 配置文件：
      #   <以 + 分隔的服务路径>@redis:<port>@rs，尾部是 +<group>+<namespace>
      local sock="$SOCK_DIR/static.config.api.example.com+3+config+cloud+redis+${GROUP}+${NAMESPACE}@redis:${PORT}@rs"
      touch "$sock"
      echo "sidecar 模式: 已发布 sock 文件 $(basename "$sock")"
      TARGET_ARGS=(--namespace "$NAMESPACE" --group "$GROUP" --socket-dir "$SOCK_DIR")
      ;;
    shards)
      local addrs=""
      for i in $(seq 0 $((SHARDS - 1))); do
        local port=$((PORT + i))
        ensure_redis "$port"
        addrs="${addrs:+$addrs,}127.0.0.1:$port"
      done
      echo "shards 模式: $SHARDS 个分片 ($addrs)"
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
