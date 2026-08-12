//! The [`Cmd`] command builder.

use crate::connection::ConnectionLike;
use crate::error::RedisResult;
use crate::from_value::FromRedisValue;
use crate::pipeline::Pipeline;
use crate::to_args::ToRedisArgs;

/// A single Redis command: a command name plus its already-serialized
/// arguments, stored as one flat byte buffer plus span indices — two
/// allocations total regardless of argument count (hot-path friendly).
#[derive(Clone, Debug, Default)]
pub struct Cmd {
    /// All argument bytes, concatenated.
    buf: Vec<u8>,
    /// (start, len) of each argument in `buf`.
    spans: Vec<(u32, u32)>,
    /// Whether this command is read-only. Read-only commands use the read
    /// retry budget and are permitted on read-only backends.
    readonly: bool,
}

/// The [`RedisWrite`] sink feeding a [`Cmd`]'s flat buffer.
struct ArgSink<'a> {
    buf: &'a mut Vec<u8>,
    spans: &'a mut Vec<(u32, u32)>,
}

impl crate::to_args::RedisWrite for ArgSink<'_> {
    fn write_arg(&mut self, arg: &[u8]) {
        let start = self.buf.len() as u32;
        self.buf.extend_from_slice(arg);
        self.spans.push((start, arg.len() as u32));
    }
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
            buf: Vec::new(),
            spans: Vec::new(),
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
        arg.write_redis_args(&mut ArgSink {
            buf: &mut self.buf,
            spans: &mut self.spans,
        });
        self
    }

    /// Append one raw pre-serialized argument as a single bulk string. Used
    /// internally when the bytes are already known (e.g. script keys).
    pub fn arg_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        let start = self.buf.len() as u32;
        self.buf.extend_from_slice(bytes);
        self.spans.push((start, bytes.len() as u32));
        self
    }

    /// Number of RESP arguments.
    pub fn arg_count(&self) -> usize {
        self.spans.len()
    }

    /// The `i`th RESP argument, if present.
    pub fn arg_at(&self, index: usize) -> Option<&[u8]> {
        self.spans
            .get(index)
            .map(|&(start, len)| &self.buf[start as usize..start as usize + len as usize])
    }

    /// The command verb (first argument) as a UTF-8 string, for logging and
    /// stats. Empty if the command has no arguments yet. Borrows when the
    /// verb is valid UTF-8 (the common case) — no allocation on the hot path.
    pub fn name(&self) -> std::borrow::Cow<'_, str> {
        self.arg_at(0)
            .map(String::from_utf8_lossy)
            .unwrap_or(std::borrow::Cow::Borrowed(""))
    }

    /// The command's key (second argument, by Redis convention) as a UTF-8
    /// string, for logging. Empty if the command has no key argument.
    /// Borrows when possible.
    pub fn key(&self) -> std::borrow::Cow<'_, str> {
        self.arg_at(1)
            .map(String::from_utf8_lossy)
            .unwrap_or(std::borrow::Cow::Borrowed(""))
    }

    /// Encode this command into a RESP multibulk frame.
    pub fn encoded(&self) -> Vec<u8> {
        // Pre-size to avoid growth reallocs.
        let cap = self.buf.len() + self.spans.len() * 21 + 23;
        let mut out = Vec::with_capacity(cap);
        crate::resp::encoder::encode_command_slices(
            self.spans
                .iter()
                .map(|&(start, len)| &self.buf[start as usize..start as usize + len as usize]),
            self.spans.len(),
            &mut out,
        );
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
        assert_eq!(c.arg_at(0).unwrap(), b"SET");
        assert_eq!(c.arg_at(1).unwrap(), b"k");
        assert_eq!(c.arg_at(2).unwrap(), b"42");
        assert_eq!(c.encoded(), b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n42\r\n");
    }

    #[test]
    fn variadic_arg_expands() {
        let mut c = cmd("DEL");
        c.arg(vec!["a", "b", "c"]);
        assert_eq!(c.arg_count(), 4);
    }
}
