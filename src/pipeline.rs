//! Command pipelining, including atomic `MULTI`/`EXEC` transactions.

use crate::cmd::Cmd;
use crate::connection::ConnectionLike;
use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::from_value::FromRedisValue;
use crate::resp::encoder::encode_command;
use crate::to_args::ToRedisArgs;
use crate::types::Value;

/// A batch of commands sent together to amortize round-trips.
///
/// Build with [`crate::pipe`], chaining `.cmd(...).arg(...)`. Call
/// [`Pipeline::atomic`] to wrap the batch in `MULTI`/`EXEC`.
#[derive(Clone, Debug, Default)]
pub struct Pipeline {
    commands: Vec<Cmd>,
    transaction_mode: bool,
}

impl Pipeline {
    /// An empty pipeline.
    pub fn new() -> Self {
        Pipeline {
            commands: Vec::new(),
            transaction_mode: false,
        }
    }

    /// An empty pipeline with capacity for `n` commands.
    pub fn with_capacity(n: usize) -> Self {
        Pipeline {
            commands: Vec::with_capacity(n),
            transaction_mode: false,
        }
    }

    /// Wrap the batch in `MULTI`/`EXEC` so it executes atomically.
    pub fn atomic(&mut self) -> &mut Self {
        self.transaction_mode = true;
        self
    }

    /// Begin a new command in the pipeline.
    pub fn cmd(&mut self, name: &str) -> &mut Self {
        self.commands.push(crate::cmd::cmd(name));
        self
    }

    /// Append an argument to the most recently added command.
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
        if let Some(last) = self.commands.last_mut() {
            last.arg(arg);
        }
        self
    }

    /// Add a fully-built command.
    pub fn add_command(&mut self, cmd: Cmd) -> &mut Self {
        self.commands.push(cmd);
        self
    }

    /// Number of user commands queued (excluding `MULTI`/`EXEC`).
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Whether no commands are queued.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// The queued commands (excluding `MULTI`/`EXEC`).
    pub fn commands(&self) -> &[Cmd] {
        &self.commands
    }

    /// Whether this pipeline runs as a `MULTI`/`EXEC` transaction.
    pub fn is_atomic(&self) -> bool {
        self.transaction_mode
    }

    /// Total number of commands actually written to the wire, including the
    /// `MULTI` and `EXEC` framing commands when atomic. Used by the connection
    /// to know how many replies to await.
    pub fn command_count(&self) -> usize {
        if self.transaction_mode {
            self.commands.len() + 2
        } else {
            self.commands.len()
        }
    }

    /// Encode the whole pipeline into a single RESP buffer.
    pub fn encoded(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.transaction_mode {
            encode_command(&[b"MULTI".to_vec()], &mut out);
        }
        for cmd in &self.commands {
            out.extend_from_slice(&cmd.encoded());
        }
        if self.transaction_mode {
            encode_command(&[b"EXEC".to_vec()], &mut out);
        }
        out
    }

    /// Execute the pipeline and convert the aggregated replies into `RV`.
    ///
    /// For a plain pipeline `RV` is built from an array of the per-command
    /// replies. For an atomic pipeline `RV` is built from the array returned by
    /// `EXEC`.
    pub async fn query_async<RV, C>(&self, con: &C) -> RedisResult<RV>
    where
        RV: FromRedisValue,
        C: ConnectionLike + ?Sized,
    {
        let n = self.commands.len();
        if self.transaction_mode {
            // Wire layout: MULTI(0), cmds(1..=n), EXEC(n+1). We only need EXEC.
            let mut replies = con.req_pipeline(self, n + 1, 1).await?;
            let exec = replies
                .pop()
                .ok_or_else(|| {
                    RedisError::from_kind(ErrorKind::ResponseError, "missing EXEC reply")
                })?
                .into_result()?;
            RV::from_redis_value(&exec)
        } else {
            let replies = con.req_pipeline(self, 0, n).await?;
            for reply in &replies {
                reply.check_error()?;
            }
            RV::from_redis_value(&Value::Array(replies))
        }
    }

    /// Execute the pipeline, discarding replies but surfacing errors.
    pub async fn exec_async<C>(&self, con: &C) -> RedisResult<()>
    where
        C: ConnectionLike + ?Sized,
    {
        self.query_async::<Vec<Value>, C>(con).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_pipeline_encoding() {
        let mut p = Pipeline::new();
        p.cmd("SET").arg("k").arg("v").cmd("GET").arg("k");
        assert_eq!(p.len(), 2);
        assert_eq!(p.command_count(), 2);
        let encoded = p.encoded();
        assert!(encoded.starts_with(b"*3\r\n$3\r\nSET"));
    }

    #[test]
    fn atomic_wraps_multi_exec() {
        let mut p = Pipeline::new();
        p.atomic().cmd("INCR").arg("n");
        assert_eq!(p.command_count(), 3);
        let encoded = p.encoded();
        assert!(encoded.starts_with(b"*1\r\n$5\r\nMULTI\r\n"));
        assert!(encoded.ends_with(b"*1\r\n$4\r\nEXEC\r\n"));
    }
}
