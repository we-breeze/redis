//! # breeze-redis
//!
//! A high-performance, high-availability async Redis client that reaches Redis
//! **through the breeze mesh**. It connects to the single local mesh agent for
//! a resource namespace (over a unix socket or `127.0.0.1` port) and speaks
//! RESP; the mesh proxies to the real Redis backends and handles sharding and
//! backend failover.
//!
//! The crate is layered bottom-up:
//!
//! - **Protocol core** ([`types`], [`error`], [`resp`], [`to_args`],
//!   [`from_value`]) — the RESP wire format, the [`Value`] model, and the
//!   [`ToRedisArgs`]/[`FromRedisValue`] conversion traits.
//! - **Commands** ([`cmd`], [`commands`], [`pipeline`]) — a [`Cmd`] builder and
//!   a macro-generated [`Commands`] trait exposing typed command methods.
//! - **Connection & discovery** ([`mesh`], [`connection`]) — mesh endpoint
//!   discovery plus [`MultiplexedConnection`], which pipelines many concurrent
//!   requests over one socket.
//! - **Pool & client** ([`pool`], [`client`]) — a pool of multiplexed
//!   connections to the mesh with a circuit breaker, and a retrying,
//!   slow-logging [`Client`].
//! - **Mesh routing** ([`routing`]) — [`MeshRouting::with_hashkey`],
//!   [`MeshRouting::broadcast`], and [`MeshRouting::at_master`].
//!
//! ```no_run
//! use breeze_redis::{Client, Commands, MeshRouting};
//!
//! # async fn demo() -> breeze_redis::RedisResult<()> {
//! // Connect to the mesh for a resource namespace.
//! let client = Client::connect("my_redis_namespace").await?;
//! client.set::<()>("key", "value").await?;
//! let v: String = client.get("key").await?;
//! assert_eq!(v, "value");
//!
//! // Route a command to a specific shard via the mesh hashkey.
//! let n: i64 = client.with_hashkey("uid:42").incr("counter:uid:42").await?;
//! # let _ = n;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod cmd;
pub mod commands;
pub mod config;
pub mod connection;
pub mod error;
pub mod from_value;
pub mod mesh;
pub mod pipeline;
pub mod pool;
pub mod resp;
pub mod routing;
pub mod script;
pub mod stats;
pub mod to_args;
pub mod types;

pub use client::Client;
pub use cmd::{Cmd, cmd, pipe};
pub use commands::Commands;
pub use config::{MeshConfig, Transport};
pub use connection::MultiplexedConnection;
pub use error::{ErrorKind, RedisError, RedisResult};
pub use from_value::FromRedisValue;
pub use mesh::Endpoint;
pub use pipeline::Pipeline;
pub use pool::Pool;
pub use routing::{MeshRouting, Prefixed};
pub use script::Script;
pub use to_args::{Bytes, RedisWrite, ToRedisArgs, ToSingleRedisArg};
pub use types::Value;
