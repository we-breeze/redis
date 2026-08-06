//! The [`Value`] model — an in-memory representation of any RESP2/RESP3 reply.

use crate::error::{RedisResult, ServerError};

/// A parsed reply from the Redis server.
///
/// Covers both RESP2 and RESP3 reply types. `#[non_exhaustive]` so new RESP3
/// types can be added without a breaking change.
#[derive(PartialEq, Clone)]
#[non_exhaustive]
pub enum Value {
    /// A nil reply (`$-1`, `*-1`, or RESP3 `_`).
    Nil,
    /// An integer reply (`:`).
    Int(i64),
    /// A binary-safe bulk string (`$`).
    BulkString(Vec<u8>),
    /// An array reply (`*`).
    Array(Vec<Value>),
    /// A simple string reply (`+`), e.g. a status other than `OK`.
    SimpleString(String),
    /// The `+OK` status reply.
    Okay,
    /// A RESP3 map (`%`) as key/value pairs.
    Map(Vec<(Value, Value)>),
    /// A RESP3 set (`~`).
    Set(Vec<Value>),
    /// A RESP3 double (`,`).
    Double(f64),
    /// A RESP3 boolean (`#`).
    Boolean(bool),
    /// A RESP3 big number (`(`), preserved as text.
    BigNumber(String),
    /// A RESP3 verbatim string (`=`) with its 3-char format tag.
    VerbatimString {
        /// The format tag, e.g. `txt` or `mkd`.
        format: String,
        /// The string body.
        text: String,
    },
    /// A RESP3 out-of-band push message (`>`).
    Push {
        /// The push kind, e.g. `message` or `pmessage`.
        kind: String,
        /// The push payload.
        data: Vec<Value>,
    },
    /// A server error reply (`-`), carried inline so one failing command does
    /// not tear down a multiplexed connection.
    ServerError(ServerError),
}

impl Value {
    /// Whether this value is [`Value::Nil`].
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    /// Convert an inline [`Value::ServerError`] into an `Err`, leaving all other
    /// values as `Ok`. Called at the query boundary so a single failed command
    /// surfaces as a proper [`crate::RedisError`].
    pub fn into_result(self) -> RedisResult<Value> {
        match self {
            Value::ServerError(err) => Err(err.into()),
            other => Ok(other),
        }
    }

    /// Borrowing form of [`Value::into_result`].
    pub fn check_error(&self) -> RedisResult<()> {
        match self {
            Value::ServerError(err) => Err(err.clone().into()),
            _ => Ok(()),
        }
    }

    /// If this is a server-error reply carried inline, return it. Server errors
    /// are surfaced as [`crate::RedisError`] during parsing, so this is always
    /// `None` for successfully parsed values; kept for symmetry with callers
    /// that pattern-match replies.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::BulkString(bytes) => Some(bytes),
            Value::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Nil => write!(f, "nil"),
            Value::Int(v) => write!(f, "int({v})"),
            Value::BulkString(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => write!(f, "bulk-string({s:?})"),
                Err(_) => write!(f, "bulk-string({} bytes)", bytes.len()),
            },
            Value::Array(items) => write!(f, "array({items:?})"),
            Value::SimpleString(s) => write!(f, "status({s:?})"),
            Value::Okay => write!(f, "okay"),
            Value::Map(pairs) => write!(f, "map({pairs:?})"),
            Value::Set(items) => write!(f, "set({items:?})"),
            Value::Double(v) => write!(f, "double({v})"),
            Value::Boolean(v) => write!(f, "boolean({v})"),
            Value::BigNumber(v) => write!(f, "big-number({v})"),
            Value::VerbatimString { format, text } => {
                write!(f, "verbatim-string({format}:{text:?})")
            }
            Value::Push { kind, data } => write!(f, "push({kind}, {data:?})"),
            Value::ServerError(err) => write!(f, "server-error({})", err.message),
        }
    }
}
