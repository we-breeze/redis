# Repository Map

## Overview

breeze 平台的 Rust 异步 Redis SDK,高性能、高可用,单进程可承载近千个
业务 namespace。提供两种显式分离的访问模式:

- **sidecar 模式**(`src/sidecar/`):通过本地 breeze mesh 访问 Redis。
  解析 mesh 发布的 sock 配置文件名(`Quadruple` 命名)发现本地
  endpoint,分片与后端 failover 由 mesh 负责。
- **direct 模式**(`src/direct/`):直连后端 Redis。含客户端分片
  (`Shards`,hash/distribution 与 mesh 逐位一致)、HA 布局
  (`HaServer` 读兜底/双写/setSecond、`MsServer` 主从读写分离)、
  DNS watcher 与多 IP 负载均衡(对齐 Java clientBalancer)。

另有 `src/replay.rs`(回放比对专用单连接客户端,feature `direct-tcp`)
与压测工具 `tools/redis-bench`(三模式压测、故障注入代理、正确性校验,
脚本 `bench_local.sh` / `bench.sh`,`MATRIX=1` 一键全场景)。

共享层:`src/connection/`(单 socket 多路复用 + 驱动 task)、
`src/pool/`(断路器、证据式 poison、lazy/按需增长连接池)、
`src/resp/`(RESP2/3 编解码,零拷贝)、`src/commands/`(命令面,
当前仅启用 hget/hmget + hset/del/ping,其余在块注释中)。

## Essential Commands

```bash
cargo fmt
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features   # 要求零告警
```

压测(本机原生 redis,数字可信):

```bash
cd tools/redis-bench
./bench_local.sh --ops 1000000 hget                # direct 基线
MATRIX=1 ./bench_local.sh                          # 一键全场景矩阵
```

## Non-Negotiable Rules

- **每次代码提交之前,必须先 `cargo fmt` 并通过全部测试**
  (`cargo test --workspace --all-features`),clippy 零告警。未满足的
  改动不得提交。
- **commit message 永远使用中文。**
- `src/direct/sharding/` 是 breeze sharding 的**原样复制(vendored)**:
  不做风格调整(模块级 clippy 豁免);任何行为改动必须与 mesh 逐位
  一致,并通过 `tests.rs` 中的向量/随机交叉校验。
- 两种访问模式的代码必须分包:`sidecar/` 与 `direct/`,共享逻辑放
  根部模块;crate 根只导出模式无关 API。
- `Value::BulkString` 是零拷贝的 `Bytes`(非 `Vec<u8>`),改动协议层
  时保持这一性质。
- 命令面裁剪是刻意的:启用新命令时,从 `src/commands/mod.rs` 的块
  注释中挪回条目并标注 `@ro`(只读)属性。

## Git Hygiene

提交聚焦单一主题,不夹带无关改动;推送前确认:

```bash
git status --short
git diff --check
```
