//! Error and result types.
//!
//! Modeled after `redis-rs`: a single [`RedisError`] wraps a private
//! representation so we can evolve internals without breaking the public API,
//! and [`ErrorKind`] is `#[non_exhaustive]` for forward compatibility.

use std::fmt;
use std::io;

/// The result type returned by all fallible operations in this crate.
pub type RedisResult<T = ()> = Result<T, RedisError>;

/// A server-sent error reply (a `-` line), carried inline as a [`Value`] so a
/// single failing command does not tear down a multiplexed connection.
///
/// [`Value`]: crate::types::Value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerError {
    /// The leading error code, e.g. `WRONGTYPE`, `ERR`, `NOSCRIPT`.
    pub code: String,
    /// The full error line as sent by the server.
    pub message: String,
}

impl ServerError {
    /// Split a raw `-` line into its code and full message.
    pub fn from_line(line: String) -> Self {
        let code = line.split(' ').next().unwrap_or("").to_string();
        ServerError {
            code,
            message: line,
        }
    }

    /// Classify this server error into an [`ErrorKind`].
    pub fn kind(&self) -> ErrorKind {
        match self.code.as_str() {
            "NOSCRIPT" => ErrorKind::NoScript,
            "MOVED" => ErrorKind::Moved,
            "ASK" => ErrorKind::Ask,
            "BUSY" => ErrorKind::Busy,
            "NOAUTH" | "WRONGPASS" => ErrorKind::AuthenticationFailed,
            _ => ErrorKind::ResponseError,
        }
    }
}

impl From<ServerError> for RedisError {
    fn from(err: ServerError) -> Self {
        RedisError::with_detail(err.kind(), "server returned an error", err.message)
    }
}

/// A coarse classification of an error, used for programmatic handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A response could not be parsed as the requested type.
    TypeError,
    /// The server returned an error reply (e.g. `WRONGTYPE`, `ERR ...`).
    ResponseError,
    /// Authentication failed.
    AuthenticationFailed,
    /// The script referenced by `EVALSHA` is not cached (`NOSCRIPT`).
    NoScript,
    /// The key moved to another node (cluster `MOVED`).
    Moved,
    /// The key is being migrated (cluster `ASK`).
    Ask,
    /// The server is busy running a script (`BUSY`).
    Busy,
    /// A read/write to the underlying socket failed.
    Io,
    /// The client was misconfigured (bad URL, missing host, ...).
    ClientError,
    /// The pool has no healthy endpoint available.
    NoConnection,
    /// An operation exceeded its timeout budget.
    Timeout,
    /// An extension/module error not otherwise classified.
    ExtensionError,
}

impl ErrorKind {
    fn description(self) -> &'static str {
        match self {
            ErrorKind::TypeError => "type conversion error",
            ErrorKind::ResponseError => "response error",
            ErrorKind::AuthenticationFailed => "authentication failed",
            ErrorKind::NoScript => "no matching script",
            ErrorKind::Moved => "key moved",
            ErrorKind::Ask => "key migrating",
            ErrorKind::Busy => "server busy",
            ErrorKind::Io => "I/O error",
            ErrorKind::ClientError => "client error",
            ErrorKind::NoConnection => "no connection available",
            ErrorKind::Timeout => "operation timed out",
            ErrorKind::ExtensionError => "extension error",
        }
    }
}

#[derive(Debug)]
enum ErrorRepr {
    /// A structured error with a kind and a human-readable message. The
    /// optional `detail` carries the server's raw error line when present.
    WithDescription(ErrorKind, &'static str, Option<String>),
    Io(io::Error),
}

/// The error type for all Redis operations.
pub struct RedisError {
    repr: ErrorRepr,
}

impl RedisError {
    /// Build an error from a kind and a static description.
    pub(crate) fn from_kind(kind: ErrorKind, desc: &'static str) -> Self {
        RedisError {
            repr: ErrorRepr::WithDescription(kind, desc, None),
        }
    }

    /// Build an error from a kind, a static description, and a runtime detail.
    pub(crate) fn with_detail(kind: ErrorKind, desc: &'static str, detail: String) -> Self {
        RedisError {
            repr: ErrorRepr::WithDescription(kind, desc, Some(detail)),
        }
    }

    /// The coarse classification of this error.
    pub fn kind(&self) -> ErrorKind {
        match &self.repr {
            ErrorRepr::WithDescription(kind, _, _) => *kind,
            ErrorRepr::Io(_) => ErrorKind::Io,
        }
    }

    /// The server-provided detail line, if this originated from a server error.
    pub fn detail(&self) -> Option<&str> {
        match &self.repr {
            ErrorRepr::WithDescription(_, _, detail) => detail.as_deref(),
            ErrorRepr::Io(_) => None,
        }
    }

    /// Whether retrying the operation on a fresh connection may succeed.
    ///
    /// I/O errors and "no connection" are transient; type and response errors
    /// are deterministic and will not change on retry.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Io | ErrorKind::NoConnection | ErrorKind::Timeout
        )
    }

    /// Whether the connection is still usable after this error.
    ///
    /// Ported from `JedisPort`'s "special data exception" concept: a server
    /// *response* error (WRONGTYPE, etc.) leaves the socket healthy, whereas an
    /// I/O error means the connection must be discarded.
    pub fn connection_still_valid(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::ResponseError | ErrorKind::TypeError | ErrorKind::NoScript
        )
    }
}

impl fmt::Debug for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            ErrorRepr::WithDescription(kind, desc, None) => {
                write!(f, "{}: {}", kind.description(), desc)
            }
            ErrorRepr::WithDescription(kind, desc, Some(detail)) => {
                write!(f, "{}: {} ({detail})", kind.description(), desc)
            }
            ErrorRepr::Io(err) => write!(f, "I/O error: {err}"),
        }
    }
}

impl std::error::Error for RedisError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.repr {
            ErrorRepr::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for RedisError {
    fn from(err: io::Error) -> Self {
        RedisError {
            repr: ErrorRepr::Io(err),
        }
    }
}
