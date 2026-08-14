//! # redis
//!
//! A high-performance, high-availability async Redis client for the breeze
//! platform, with **two explicitly separated access modes**:
//!
//! ## Application API — [`Redis`], [`SidecarRedis`], [`MsRedis`], and
//! [`ShardedMsRedis`]
//!
//! Application code should depend on the small [`Redis`] contract. Its first
//! version contains only the `GET`, `HGET`, and `HMGET` operations used by
//! abtest. [`SidecarRedis`] discovers an exact group/namespace through the
//! local breeze sidecar, while [`MsRedis`] connects to one master and one or
//! more slave endpoints with read/write splitting. [`ShardedMsRedis`] first
//! routes by key across multiple master/slave groups, then applies the same
//! read splitting within the selected group. These facades keep pools and
//! command machinery out of the application boundary. `DirectRedis` is
//! available only with the `direct-mock` feature.
//!
//! ```no_run
//! use redis::{Redis, SidecarRedis};
//!
//! # async fn demo() -> redis::RedisResult<()> {
//! let redis = SidecarRedis::new("feed", "auto_translate_llm").await?;
//! let profile = redis.get("u:42").await?;
//! let version = redis.hget("document:42", "version").await?;
//! let values = redis.hmget("document:42", &["value", "compress", "hash"]).await?;
//! # let _ = (profile, version, values);
//! # Ok(())
//! # }
//! ```
//!
//! ## Mesh mode — [`sidecar`]
//!
//! The SDK talks to the **local breeze mesh agent**, discovered by parsing
//! the sock config files the mesh publishes (see [`sidecar::discovery`]);
//! the mesh proxies to the real backends and owns sharding and failover.
//! Use [`sidecar::SidecarClient`], configured by [`sidecar::MeshConfig`], with
//! [`sidecar::MeshRouting`] for hashkey/broadcast/master routing.
//!
//! ```no_run
//! use redis::sidecar::{SidecarClient, MeshRouting};
//! use redis::Commands;
//!
//! # async fn demo() -> redis::RedisResult<()> {
//! let client = SidecarClient::connect("my_redis_namespace").await?;
//! client.hset::<i64>("key", "f", "value").await?;
//! let v: String = client.hget("key", "f").await?;
//! assert_eq!(v, "value");
//! let n: i64 = client.with_hashkey("uid:42").hset("counter:uid:42", "f", 1).await?;
//! # let _ = n;
//! # Ok(())
//! # }
//! ```
//!
//! ## Direct backend mode (`direct-mock` feature)
//!
//! Direct-backend types are public only when `direct-mock` is enabled. The
//! internal implementation remains available to higher-level clients such as
//! [`MsRedis`]. The feature exposes `direct::DirectClient`, `direct::Shards`,
//! `direct::HaServer`, and `direct::MsServer` for tests, validation tools, and
//! benchmarks.
//!
//! ```ignore
//! use redis::direct::{DirectClient, ServerConfig, Shards};
//! use redis::Commands;
//!
//! # async fn demo() -> redis::RedisResult<()> {
//! let shards = Shards::new(
//!     "crc32", "modula",
//!     vec!["10.0.0.1:6379:0".to_string(), "10.0.0.2:6379:0".to_string()],
//!     vec![
//!         DirectClient::connect(ServerConfig::new("10.0.0.1:6379")?).await?,
//!         DirectClient::connect(ServerConfig::new("10.0.0.2:6379")?).await?,
//!     ],
//! );
//! // shardingSupport.getClient(uid) style:
//! let client = shards.get_client(12345678);
//! client.hset::<i64>("u:12345678", "f", "data").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Unified proxy — [`Client`]
//!
//! [`Client`] wraps the sidecar client by default and additionally exposes its
//! direct variant when `direct-mock` is enabled. It implements
//! [`ConnectionLike`] so the whole [`Commands`] surface remains uniform.
//!
//! ## Shared layers (both modes)
//!
//! - **Protocol core** ([`types`], [`error`], [`resp`], [`to_args`],
//!   [`from_value`]) — the RESP wire format, the [`Value`] model, and the
//!   [`ToRedisArgs`]/[`FromRedisValue`] conversion traits.
//! - **Commands** ([`cmd`][mod@crate::cmd], [`commands`], [`pipeline`],
//!   [`script`]) — the
//!   [`Cmd`] builder, the macro-generated [`Commands`] trait, pipelines and
//!   Lua scripts.
//! - **Connection & pool** ([`connection`], [`pool`]) —
//!   [`connection::MultiplexedConnection`] pipelines
//!   many concurrent requests over one socket; [`Pool`] adds the circuit
//!   breaker, maintenance probe, and (for hostname backends) DNS watching
//!   with per-IP load balancing.

mod api;
pub mod client;
pub mod cmd;
pub mod commands;
pub mod connection;
#[cfg(any(feature = "direct-mock", doctest))]
pub mod direct;
#[cfg(all(not(feature = "direct-mock"), not(doctest)))]
#[allow(dead_code, unused_imports)]
#[doc(hidden)]
mod direct;
#[cfg(feature = "direct-mock")]
mod direct_redis;
pub mod error;
pub mod from_value;
mod ms_redis;
pub mod pipeline;
pub mod pool;
pub mod resp;
pub mod script;
mod sharded_ms_redis;
pub mod sidecar;
mod sidecar_redis;
pub mod stats;
pub mod to_args;
pub mod types;

// Shared, mode-agnostic API.
pub use api::{Redis, RedisBytes};
pub use client::Client;
pub use cmd::{Cmd, cmd, pipe};
pub use commands::Commands;
pub use connection::{ConnectionLike, Handshake, MultiplexedConnection};
#[cfg(feature = "direct-mock")]
pub use direct_redis::DirectRedis;
pub use error::{ErrorKind, RedisError, RedisResult};
pub use from_value::FromRedisValue;
pub use ms_redis::MsRedis;
pub use pipeline::Pipeline;
pub use pool::Pool;
pub use script::Script;
pub use sharded_ms_redis::ShardedMsRedis;
pub use sidecar_redis::SidecarRedis;
pub use to_args::{Bytes, RedisWrite, ToRedisArgs, ToSingleRedisArg};
pub use types::Value;
