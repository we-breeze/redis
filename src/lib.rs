//! High-performance asynchronous Redis access through one unified
//! [`RedisService`] facade.
//!
//! - [`RedisService::from_config`] accepts application-supplied configuration.
//! - [`RedisService::from_provider`] loads a configuration snapshot once.
//! - [`RedisService::noshard`] builds one master/slave replica group.
//! - [`RedisService::sharded`] routes a key to one master/slave group before
//!   selecting a replica.
//! - Hostname endpoints retain automatic IPv4 DNS refresh. Logical shard
//!   layouts remain fixed after construction.
//!
//! Each physical node owns one multiplexed `brz-net` session with fail-fast
//! admission, request deadlines, elastic receive buffering, and quota-based
//! replica balancing.

mod api;
mod arg;
mod bulk;
pub mod cmd;
mod config;
pub mod error;
pub mod from_value;
mod multi_key;
mod net_transport;
#[cfg(feature = "metrics")]
mod profile_metrics;
mod redis_service;
pub mod resp;
mod service_pipe;
mod sharding;
pub mod to_args;
pub mod types;

pub use api::{Redis, SetCondition, SetExpiration, SetOptions};
pub use arg::{EncodeRedisArg, EncodeRedisArgs, RedisArgSink, RedisArgsSink, RedisKey2, RedisKey3};
pub use brz_net::{
    DEFAULT_REQUEST_ARENA_CHUNK_SIZE, global_request_arena, init_global_request_arena,
};
pub use bulk::{FromRedisBulk, RedisBytes, RedisValues};
pub use cmd::{Cmd, cmd};
pub use config::{RedisConfig, RedisConfigFuture, RedisConfigProvider, RedisShardConfig};
pub use error::{ErrorKind, RedisError, RedisResult};
pub use from_value::FromRedisValue;
pub use redis_service::{RedisService, RedisServiceOptions, ShardRouting};
pub use service_pipe::{FromPipeResponse, MAX_PIPELINE_COMMANDS, PipeResponse, RedisPipe, pipe};
pub use to_args::{Bytes, RedisWrite, ToRedisArgs, ToSingleRedisArg};
pub use types::Value;
