# redis

基于 `brz-net` 单连接多路复用会话的高性能异步 Redis SDK。对外统一使用
`RedisService`，不再区分旧的 sidecar client、direct client、connection pool。

## 构造方式

```rust,no_run
use redis::{Redis, RedisBytes, RedisService};

# async fn example() -> redis::RedisResult<()> {
// 一次性发现 Breeze mesh 发布的本地 TCP 端口，之后等价于 single。
let mesh = RedisService::mesh("feed", "profiles").await?;

// 一个地址；读、写角色各自持有一个物理连接。
let single = RedisService::single("127.0.0.1:6379").await?;

// 一个 master/slave replica group。
let service = RedisService::noshard(
    "redis-master.example:6379",
    ["redis-slave-a.example:6379", "redis-slave-b.example:6379"],
)
.await?;

let value: Option<RedisBytes> = service.get("key").await?;
# let _ = (mesh, single, value);
# Ok(())
# }
```

多分片场景使用 `RedisService::sharded`：先按 key 选择 shard，再在该 shard
的等价 slave replicas 之间使用 quota 负载均衡。pipeline 当前只支持单 shard
拓扑；多 shard 会在发送前快速失败。

## 命令能力

`Redis` trait 提供 application 当前需要的 Redis 原生命令，包括带 `EX/PX/NX/XX`
选项的 `SET`、`MGET/DEL/EXPIRE/INCR/APPEND/EVAL/EVALSHA`、list/set/hash/zset、
`PFADD/PFCOUNT` 和 `PUBLISH`。有限 pipeline 同样支持 application 使用的写命令和
动态 RESP 返回值；额外命令可通过 `Cmd` 和 `Redis::command` 编码执行。

这里仅封装 Redis 协议、读写角色和 shard 路由，不包含分布式锁、缓存、限流、
队列或其他业务语义。

真实 Redis 集成测试保持 opt-in：

```bash
BREEZE_REDIS_TEST_ENDPOINT=127.0.0.1:6379 \
  cargo test --workspace --all-targets --features integration-tests
```

## Mesh 语义

`RedisService::mesh(group, namespace)` 复用共享 `discovery` crate，以 Redis 的
`GroupNamespace` 坐标规则扫描 `/data1/breeze/socks`。发现只发生一次，不等待
endpoint 就绪，也不监听注册文件变化；未发现精确坐标时构造直接失败。发现到
TCP endpoint 后使用 `RedisService::single` 的实现。

域名的 IPv4 解析变化由 `RedisService`/`brz-net` 独立持续刷新，这与 mesh 注册
文件的固定语义互不混淆。

## Transport

- 每个物理节点一个持久、多路复用的 TCP session；
- 默认请求超时 200ms，可通过 `RedisServiceOptions` 调整；
- in-flight 容量耗尽时快速失败，不排队等待；
- 连接错误或超时会关闭 session，由 session 自行重连；
- 弹性接收 ring buffer 和全局 request arena 由 `brz-net` 统一提供；
- replica 使用 quota balancer，逻辑 shard 配置构造后保持不变。

`tools/redis-bench` 同样只构造 `RedisService`。其 allocator 统计依赖共享的
`../memory`，Redis 仓库不再内嵌 memory submodule。
