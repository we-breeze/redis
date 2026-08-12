//! Incremental RESP2/RESP3 reply parser.
//!
//! Hand-rolled so it can be driven directly off a connection's read buffer:
//! [`parse_reply`] returns [`ParseResult::Incomplete`] when more bytes are
//! needed, and otherwise reports exactly how many bytes the reply consumed
//! so the caller can advance its buffer. Bulk strings are zero-copy
//! [`Bytes`] slices of the input.

use bytes::Bytes;

use crate::error::{ErrorKind, RedisError, ServerError};
use crate::types::Value;

/// The outcome of attempting to parse one reply from a byte buffer.
#[derive(Debug)]
pub enum ParseResult {
    /// A complete reply was parsed, consuming `consumed` bytes from the front.
    Complete {
        /// The parsed value.
        value: Value,
        /// Number of bytes consumed from the input.
        consumed: usize,
    },
    /// The buffer does not yet contain a full reply; read more and retry.
    Incomplete,
}

/// Guard against pathological nesting from a hostile or corrupt server.
const MAX_DEPTH: usize = 128;

/// Try to parse a single reply from the front of `buf`.
///
/// Returns [`ParseResult::Incomplete`] if the buffer is a prefix of a valid
/// reply. Returns `Err` for a protocol violation or a server error reply.
pub fn parse_reply(buf: &Bytes) -> Result<ParseResult, RedisError> {
    let mut parser = Parser { buf, pos: 0 };
    match parser.parse_value(0)? {
        Some(value) => Ok(ParseResult::Complete {
            value,
            consumed: parser.pos,
        }),
        None => Ok(ParseResult::Incomplete),
    }
}

struct Parser<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl Parser<'_> {
    /// Parse one value starting at `self.pos`. Returns `Ok(None)` if the buffer
    /// is incomplete (in which case `self.pos` is meaningless to the caller).
    fn parse_value(&mut self, depth: usize) -> Result<Option<Value>, RedisError> {
        if depth > MAX_DEPTH {
            return Err(RedisError::from_kind(
                ErrorKind::ResponseError,
                "reply nested too deeply",
            ));
        }
        let Some(&marker) = self.buf.get(self.pos) else {
            return Ok(None);
        };
        self.pos += 1;
        match marker {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => Ok(self.parse_int()?.map(Value::Int)),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(depth, Value::Array),
            b'_' => self.parse_null(),
            b'#' => self.parse_boolean(),
            b',' => self.parse_double(),
            b'(' => self.parse_big_number(),
            b'=' => self.parse_verbatim_string(),
            b'%' => self.parse_map(depth),
            b'~' => self.parse_array(depth, Value::Set),
            b'>' => self.parse_push(depth),
            other => Err(RedisError::with_detail(
                ErrorKind::ResponseError,
                "unknown reply marker",
                (other as char).to_string(),
            )),
        }
    }

    /// Read one CRLF-terminated line (excluding the CRLF). Returns `None` if the
    /// terminator has not arrived yet.
    fn read_line(&mut self) -> Option<&[u8]> {
        let rest = &self.buf[self.pos..];
        let idx = find_crlf(rest)?;
        let line = &rest[..idx];
        self.pos += idx + 2;
        Some(line)
    }

    fn parse_simple_string(&mut self) -> Result<Option<Value>, RedisError> {
        Ok(self.read_line().map(|line| {
            if line == b"OK" {
                Value::Okay
            } else {
                Value::SimpleString(String::from_utf8_lossy(line).into_owned())
            }
        }))
    }

    fn parse_error(&mut self) -> Result<Option<Value>, RedisError> {
        Ok(self.read_line().map(|line| {
            Value::ServerError(ServerError::from_line(
                String::from_utf8_lossy(line).into_owned(),
            ))
        }))
    }

    fn parse_int(&mut self) -> Result<Option<i64>, RedisError> {
        match self.read_line() {
            Some(line) => parse_i64(line).map(Some),
            None => Ok(None),
        }
    }

    fn parse_len(&mut self) -> Result<Option<isize>, RedisError> {
        match self.read_line() {
            Some(line) => parse_isize(line).map(Some),
            None => Ok(None),
        }
    }

    fn parse_bulk_string(&mut self) -> Result<Option<Value>, RedisError> {
        let Some(len) = self.parse_len()? else {
            return Ok(None);
        };
        if len < 0 {
            return Ok(Some(Value::Nil));
        }
        let len = len as usize;
        // body + trailing CRLF must both be present.
        if self.buf.len() < self.pos + len + 2 {
            return Ok(None);
        }
        let body = self.buf.slice(self.pos..self.pos + len);
        self.pos += len + 2;
        Ok(Some(Value::BulkString(body)))
    }

    fn parse_array<F>(&mut self, depth: usize, wrap: F) -> Result<Option<Value>, RedisError>
    where
        F: FnOnce(Vec<Value>) -> Value,
    {
        let Some(len) = self.parse_len()? else {
            return Ok(None);
        };
        if len < 0 {
            return Ok(Some(Value::Nil));
        }
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            match self.parse_value(depth + 1)? {
                Some(item) => items.push(item),
                None => return Ok(None),
            }
        }
        Ok(Some(wrap(items)))
    }

    fn parse_map(&mut self, depth: usize) -> Result<Option<Value>, RedisError> {
        let Some(len) = self.parse_len()? else {
            return Ok(None);
        };
        if len < 0 {
            return Ok(Some(Value::Nil));
        }
        let mut pairs = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let Some(key) = self.parse_value(depth + 1)? else {
                return Ok(None);
            };
            let Some(val) = self.parse_value(depth + 1)? else {
                return Ok(None);
            };
            pairs.push((key, val));
        }
        Ok(Some(Value::Map(pairs)))
    }

    fn parse_push(&mut self, depth: usize) -> Result<Option<Value>, RedisError> {
        let Some(len) = self.parse_len()? else {
            return Ok(None);
        };
        if len <= 0 {
            return Ok(Some(Value::Push {
                kind: String::new(),
                data: Vec::new(),
            }));
        }
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            match self.parse_value(depth + 1)? {
                Some(item) => items.push(item),
                None => return Ok(None),
            }
        }
        let mut iter = items.into_iter();
        let kind = match iter.next() {
            Some(Value::BulkString(b)) => String::from_utf8_lossy(&b).into_owned(),
            Some(Value::SimpleString(s)) => s,
            _ => String::new(),
        };
        Ok(Some(Value::Push {
            kind,
            data: iter.collect(),
        }))
    }

    fn parse_null(&mut self) -> Result<Option<Value>, RedisError> {
        Ok(self.read_line().map(|_| Value::Nil))
    }

    fn parse_boolean(&mut self) -> Result<Option<Value>, RedisError> {
        match self.read_line() {
            Some(line) => Ok(Some(Value::Boolean(line == b"t"))),
            None => Ok(None),
        }
    }

    fn parse_double(&mut self) -> Result<Option<Value>, RedisError> {
        match self.read_line() {
            Some(line) => {
                let text = std::str::from_utf8(line).map_err(|_| bad_number())?;
                let value = match text {
                    "inf" => f64::INFINITY,
                    "-inf" => f64::NEG_INFINITY,
                    "nan" => f64::NAN,
                    other => other.parse::<f64>().map_err(|_| bad_number())?,
                };
                Ok(Some(Value::Double(value)))
            }
            None => Ok(None),
        }
    }

    fn parse_big_number(&mut self) -> Result<Option<Value>, RedisError> {
        Ok(self
            .read_line()
            .map(|line| Value::BigNumber(String::from_utf8_lossy(line).into_owned())))
    }

    fn parse_verbatim_string(&mut self) -> Result<Option<Value>, RedisError> {
        let Some(len) = self.parse_len()? else {
            return Ok(None);
        };
        if len < 0 {
            return Ok(Some(Value::Nil));
        }
        let len = len as usize;
        if self.buf.len() < self.pos + len + 2 {
            return Ok(None);
        }
        let raw = &self.buf[self.pos..self.pos + len];
        self.pos += len + 2;
        // Format is `xxx:body` where `xxx` is a 3-char tag.
        let (format, text) = if raw.len() >= 4 && raw[3] == b':' {
            (
                String::from_utf8_lossy(&raw[..3]).into_owned(),
                String::from_utf8_lossy(&raw[4..]).into_owned(),
            )
        } else {
            (String::new(), String::from_utf8_lossy(raw).into_owned())
        };
        Ok(Some(Value::VerbatimString { format, text }))
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    // SIMD-accelerated scan for '\r', then confirm the '\n'.
    let mut offset = 0;
    while let Some(i) = memchr::memchr(b'\r', &buf[offset..]) {
        let pos = offset + i;
        if buf.get(pos + 1) == Some(&b'\n') {
            return Some(pos);
        }
        offset = pos + 1;
    }
    None
}

fn parse_i64(line: &[u8]) -> Result<i64, RedisError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(bad_number)
}

fn parse_isize(line: &[u8]) -> Result<isize, RedisError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<isize>().ok())
        .ok_or_else(bad_number)
}

fn bad_number() -> RedisError {
    RedisError::from_kind(ErrorKind::ResponseError, "invalid number in reply")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn parse(buf: &[u8]) -> (Value, usize) {
        match parse_reply(&Bytes::copy_from_slice(buf)).unwrap() {
            ParseResult::Complete { value, consumed } => (value, consumed),
            ParseResult::Incomplete => panic!("unexpected incomplete parse"),
        }
    }

    #[test]
    fn parses_simple_and_okay() {
        assert_eq!(parse(b"+OK\r\n").0, Value::Okay);
        assert_eq!(parse(b"+PONG\r\n").0, Value::SimpleString("PONG".into()));
    }

    #[test]
    fn parses_int_and_bulk() {
        assert_eq!(parse(b":42\r\n").0, Value::Int(42));
        assert_eq!(
            parse(b"$3\r\nabc\r\n").0,
            Value::BulkString(Bytes::from_static(b"abc"))
        );
        assert_eq!(parse(b"$-1\r\n").0, Value::Nil);
        assert_eq!(parse(b"$0\r\n\r\n").0, Value::BulkString(Bytes::new()));
    }

    #[test]
    fn parses_nested_array() {
        let (v, consumed) = parse(b"*2\r\n:1\r\n$2\r\nhi\r\n");
        assert_eq!(
            v,
            Value::Array(vec![
                Value::Int(1),
                Value::BulkString(Bytes::from_static(b"hi"))
            ])
        );
        assert_eq!(consumed, 16);
        assert_eq!(parse(b"*-1\r\n").0, Value::Nil);
    }

    #[test]
    fn server_error_is_inline_value() {
        let (v, _) = parse(b"-WRONGTYPE nope\r\n");
        match v {
            Value::ServerError(err) => {
                assert_eq!(err.code, "WRONGTYPE");
                assert_eq!(err.kind(), ErrorKind::ResponseError);
            }
            other => panic!("expected server error, got {other:?}"),
        }

        let (v, _) = parse(b"-NOSCRIPT missing\r\n");
        match v {
            Value::ServerError(err) => assert_eq!(err.kind(), ErrorKind::NoScript),
            other => panic!("expected server error, got {other:?}"),
        }
    }

    #[test]
    fn incomplete_when_truncated() {
        assert!(matches!(
            parse_reply(&Bytes::from_static(b"$3\r\nab")).unwrap(),
            ParseResult::Incomplete
        ));
        assert!(matches!(
            parse_reply(&Bytes::from_static(b"*2\r\n:1\r\n")).unwrap(),
            ParseResult::Incomplete
        ));
        assert!(matches!(
            parse_reply(&Bytes::from_static(b"")).unwrap(),
            ParseResult::Incomplete
        ));
    }

    #[test]
    fn parses_resp3_scalars() {
        assert_eq!(parse(b"#t\r\n").0, Value::Boolean(true));
        assert_eq!(parse(b"#f\r\n").0, Value::Boolean(false));
        assert_eq!(parse(b",2.5\r\n").0, Value::Double(2.5));
        assert_eq!(parse(b"_\r\n").0, Value::Nil);
        assert_eq!(
            parse(b"%1\r\n$1\r\na\r\n:1\r\n").0,
            Value::Map(vec![(
                Value::BulkString(Bytes::from_static(b"a")),
                Value::Int(1)
            )])
        );
    }
}
