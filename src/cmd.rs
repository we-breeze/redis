//! The [`Cmd`] command builder.

use crate::connection::ConnectionLike;
use crate::error::RedisResult;
use crate::from_value::FromRedisValue;
use crate::pipeline::Pipeline;
use crate::resp::encoder::encode_command;
use crate::to_args::ToRedisArgs;

/// A single Redis command: a command name plus its already-serialized
/// arguments. Cheap to build and clone.
#[derive(Clone, Debug, Default)]
pub struct Cmd {
    args: Vec<Vec<u8>>,
    /// Whether this command is read-only. Read-only commands use the read
    /// retry budget and are permitted on read-only backends.
    readonly: bool,
}

/// Start building a command, e.g. `cmd("GET").arg("key")`.
pub fn cmd(name: &str) -> Cmd {
    let mut c = Cmd::new();
    c.arg(name);
    c
}

/// Start building a [`Pipeline`].
pub fn pipe() -> Pipeline {
    Pipeline::new()
}

impl Cmd {
    /// An empty command with no name yet.
    pub fn new() -> Self {
        Cmd {
            args: Vec::new(),
            readonly: false,
        }
    }

    /// Mark this command as read-only (set by the `Commands` macro from the
    /// command list's `@ro` annotation).
    pub fn mark_readonly(&mut self) {
        self.readonly = true;
    }

    /// Whether this command is read-only.
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    /// Append one logical argument (which may expand to several RESP args).
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
        arg.write_redis_args(&mut self.args);
        self
    }

    /// Append one raw pre-serialized argument as a single bulk string. Used
    /// internally when the bytes are already known (e.g. script keys).
    pub fn arg_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.args.push(bytes.to_vec());
        self
    }

    /// The RESP argument slices, for inspection/testing.
    pub fn args(&self) -> &[Vec<u8>] {
        &self.args
    }

    /// The command verb (first argument) as a UTF-8 string, for logging and
    /// stats. Empty if the command has no arguments yet.
    pub fn name(&self) -> String {
        self.args
            .first()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .unwrap_or_default()
    }

    /// The command's key (second argument, by Redis convention) as a UTF-8
    /// string, for logging. Empty if the command has no key argument.
    pub fn key(&self) -> String {
        self.args
            .get(1)
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .unwrap_or_default()
    }

    /// Encode this command into a RESP multibulk frame.
    pub fn encoded(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_command(&self.args, &mut out);
        out
    }

    /// Send the command and convert its reply into `RV`.
    pub async fn query_async<RV, C>(&self, con: &C) -> RedisResult<RV>
    where
        RV: FromRedisValue,
        C: ConnectionLike + ?Sized,
    {
        let value = con.req_command(self).await?.into_result()?;
        RV::from_redis_value(&value)
    }

    /// Send the command and discard its reply, surfacing only errors.
    pub async fn exec_async<C>(&self, con: &C) -> RedisResult<()>
    where
        C: ConnectionLike + ?Sized,
    {
        con.req_command(self).await?.into_result()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_and_encodes() {
        let mut c = cmd("SET");
        c.arg("k").arg(42i64);
        assert_eq!(c.args(), &[b"SET".to_vec(), b"k".to_vec(), b"42".to_vec()]);
        assert_eq!(c.encoded(), b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n42\r\n");
    }

    #[test]
    fn variadic_arg_expands() {
        let mut c = cmd("DEL");
        c.arg(vec!["a", "b", "c"]);
        assert_eq!(c.args().len(), 4);
    }
}
