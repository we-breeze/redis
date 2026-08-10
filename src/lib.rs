//! # redis
//!
//! A high-performance, high-availability async Redis client for the breeze
//! platform, with **two explicitly separated access modes**:
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
//! client.set::<()>("key", "value").await?;
//! let v: String = client.get("key").await?;
//! assert_eq!(v, "value");
//! let n: i64 = client.with_hashkey("uid:42").incr("counter:uid:42").await?;
//! # let _ = n;
//! # Ok(())
//! # }
//! ```
//!
//! ## Direct backend mode — [`direct`]
//!
//! The SDK connects to the Redis backends directly (no mesh), with
//! client-side sharding ([`direct::Shards`], the `shardingSupport`
//! pattern) using the same hash/distribution algorithms as the mesh
//! ([`direct::sharding`]), and HA layouts ported from the Java client:
//! [`direct::DirectClient`] (one `host:port[:db]` server),
//! [`direct::HaServer`] (read fallback + double-write/set-second), and
//! [`direct::MsServer`] (master/slave read splitting).
//!
//! ```no_run
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
//! client.set::<()>("u:12345678", "data").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Unified proxy — [`Client`]
//!
//! [`Client`] is a mode-agnostic enum over both clients, implementing
//! [`ConnectionLike`] so the whole [`Commands`] surface works regardless of
//! the resource's access mode:
//!
//! ```no_run
//! use redis::{Client, Commands};
//! use redis::sidecar::SidecarClient;
//!
//! # async fn demo(direct: redis::direct::DirectClient) -> redis::RedisResult<()> {
//! let sidecar = SidecarClient::connect("my_ns").await?;
//! let clients: Vec<Client> = vec![sidecar.into(), direct.into()];
//! for client in &clients {
//!     let _: Option<String> = client.get("key").await?;
//! }
//! # Ok(())
//! # }
//! ```
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

pub mod client;
pub mod cmd;
pub mod commands;
pub mod connection;
pub mod direct;
pub mod error;
pub mod from_value;
pub mod pipeline;
pub mod pool;
pub mod resp;
pub mod script;
pub mod sidecar;
pub mod stats;
pub mod to_args;
pub mod types;

#[cfg(feature = "direct-tcp")]
pub mod replay;

// Shared, mode-agnostic API.
pub use client::Client;
pub use cmd::{Cmd, cmd, pipe};
pub use commands::Commands;
pub use connection::{ConnectionLike, Handshake, MultiplexedConnection};
pub use error::{ErrorKind, RedisError, RedisResult};
pub use from_value::FromRedisValue;
pub use pipeline::Pipeline;
pub use pool::Pool;
pub use script::Script;
pub use to_args::{Bytes, RedisWrite, ToRedisArgs, ToSingleRedisArg};
pub use types::Value;
