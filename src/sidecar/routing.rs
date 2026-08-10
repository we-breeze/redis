//! Mesh routing helpers.
//!
//! The mesh applies certain "preamble" commands to the *next* command sent on
//! the same connection:
//!
//! - `hashkeyq <hashKey>` — route the next command to the shard owning
//!   `hashKey` (this is how the Java `*WithHashKey` variants work).
//! - `sendtoallq` — broadcast the next command to all shards.
//! - `master` — send the next command to the master (for read-your-writes).
//!
//! [`Prefixed`] wraps any [`ConnectionLike`] and emits the preamble immediately
//! before each command, sent as one atomic unit so no other command can
//! interleave between them on a multiplexed socket. Because it is itself a
//! [`ConnectionLike`], the entire [`Commands`](crate::commands::Commands)
//! surface is available on it — e.g. `client.with_hashkey("uid:42").get(key)`.

use crate::cmd::{Cmd, cmd};
use crate::connection::{ConnectionLike, RedisFuture};
use crate::error::{ErrorKind, RedisError};
use crate::pipeline::Pipeline;
use crate::to_args::ToRedisArgs;
use crate::types::Value;

/// Preamble command names as understood by the mesh.
const HASHKEYQ: &str = "hashkeyq";
const SENDTOALLQ: &str = "sendtoallq";
const MASTER: &str = "master";

/// A connection view that prefixes every command with a mesh routing preamble.
pub struct Prefixed<'a, C: ConnectionLike + ?Sized> {
    inner: &'a C,
    preamble: Cmd,
}

impl<C: ConnectionLike + ?Sized> Prefixed<'_, C> {
    fn pair(&self, command: &Cmd) -> Pipeline {
        let mut pipe = Pipeline::with_capacity(2);
        pipe.add_command(self.preamble.clone());
        pipe.add_command(command.clone());
        pipe
    }
}

impl<C: ConnectionLike + ?Sized> ConnectionLike for Prefixed<'_, C> {
    fn req_command<'b>(&'b self, command: &'b Cmd) -> RedisFuture<'b, Value> {
        Box::pin(async move {
            let pipe = self.pair(command);
            // Reply layout: [preamble-reply, command-reply]; take the command's.
            let mut replies = self.inner.req_pipeline(&pipe, 1, 1).await?;
            Ok(replies.pop().unwrap_or(Value::Nil))
        })
    }

    fn req_pipeline<'b>(
        &'b self,
        pipeline: &'b Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'b, Vec<Value>> {
        Box::pin(async move {
            if pipeline.is_atomic() {
                return Err(RedisError::from_kind(
                    ErrorKind::ClientError,
                    "hashkey/broadcast routing is not supported on atomic pipelines",
                ));
            }
            // Interleave the preamble before each user command so routing
            // applies to every command in the batch.
            let mut interleaved = Pipeline::with_capacity(pipeline.len() * 2);
            for command in pipeline.commands() {
                interleaved.add_command(self.preamble.clone());
                interleaved.add_command(command.clone());
            }
            let n = pipeline.len();
            let raw = self
                .inner
                .req_pipeline(&interleaved, 0, interleaved.command_count())
                .await?;
            // The real replies are at odd indices (1, 3, 5, ...).
            let selected: Vec<Value> = (0..n)
                .filter_map(|i| raw.get(2 * i + 1).cloned())
                .skip(offset)
                .take(count)
                .collect();
            Ok(selected)
        })
    }
}

/// Mesh routing extension methods, available on any [`ConnectionLike`].
pub trait MeshRouting: ConnectionLike {
    /// Route subsequent commands to the shard owning `hash_key`.
    fn with_hashkey(&self, hash_key: impl ToRedisArgs) -> Prefixed<'_, Self> {
        let mut preamble = cmd(HASHKEYQ);
        preamble.arg(hash_key);
        Prefixed {
            inner: self,
            preamble,
        }
    }

    /// Broadcast subsequent commands to all shards.
    fn broadcast(&self) -> Prefixed<'_, Self> {
        Prefixed {
            inner: self,
            preamble: cmd(SENDTOALLQ),
        }
    }

    /// Route subsequent commands to the master (read-your-writes).
    fn at_master(&self) -> Prefixed<'_, Self> {
        Prefixed {
            inner: self,
            preamble: cmd(MASTER),
        }
    }
}

impl<T: ConnectionLike + ?Sized> MeshRouting for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashkey_preamble_is_built() {
        // A minimal ConnectionLike stub to inspect what Prefixed sends.
        struct Stub;
        impl ConnectionLike for Stub {
            fn req_command<'a>(&'a self, _c: &'a Cmd) -> RedisFuture<'a, Value> {
                Box::pin(async { Ok(Value::Nil) })
            }
            fn req_pipeline<'a>(
                &'a self,
                pipeline: &'a Pipeline,
                _offset: usize,
                _count: usize,
            ) -> RedisFuture<'a, Vec<Value>> {
                // Assert the preamble+command pairing on the wire.
                let cmds = pipeline.commands();
                assert_eq!(cmds[0].name(), HASHKEYQ);
                assert_eq!(cmds[0].args()[1], b"uid:1");
                assert_eq!(cmds[1].name(), "GET");
                Box::pin(async { Ok(vec![Value::Okay, Value::BulkString(b"v".to_vec())]) })
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let stub = Stub;
        let mut get = cmd("GET");
        get.arg("k");
        let routed = stub.with_hashkey("uid:1");
        let value = rt.block_on(routed.req_command(&get)).unwrap();
        assert_eq!(value, Value::BulkString(b"v".to_vec()));
    }
}
