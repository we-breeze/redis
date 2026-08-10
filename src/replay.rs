//! Replay/comparison direct-TCP RESP client (HGET/HMGET only) for replay/comparison topologies
//! that need a single persistent TCP connection to a recorded Redis endpoint
//! (e.g. `rs50600:50600`). This is NOT the mesh path; it bypasses the breeze
//! mesh and speaks RESP directly over a raw `TcpStream`.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const READ_BUFFER_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024; // 64 MB (some values are large)

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("nil response")]
    Nil,
    #[error("server error: {0}")]
    Server(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("response too large: {0} bytes")]
    TooLarge(usize),
}

/// A Redis bulk string response (may be nil).
pub enum BulkResponse {
    Nil,
    Data(Vec<u8>),
}

/// A Redis array element (may be bulk string or nil).
pub enum ArrayElement {
    Nil,
    Data(Vec<u8>),
}

/// A persistent Redis connection that reuses one `TcpStream` for multiple
/// commands.
///
/// The source Java service uses a Jedis connection pool (`JedisMSServerProxy` /
/// mesh-socket balancer) that sends many HGET/HMGET commands sequentially over
/// the same connection. The replay proxy lanes a target TCP connection to a
/// recorded connection and only advances the lane when the target's next
/// command matches the next recorded command ON THAT LANE. If the target opens
/// a fresh connection per command, the proxy cannot lane-match the second
/// command onward (each new target connection is bound to the start of a
/// recorded lane, expecting the first recorded command, not the second), so
/// every command after the first is reported `lane_blocked`.
///
/// `RedisConnection` mirrors the source topology by holding one `TcpStream`
/// for a sequence of commands. The startup warm-up uses a single
/// `RedisConnection` for all its HGET/HMGET calls in source order, so the
/// command stream on that one connection is a sequential
/// HGET,HMGET,HGET,HMGET,... pattern that the proxy can lane-match against a
/// single recorded connection's sequence.
///
/// The connection is bounded: each command has a 10s deadline, response reads
/// are capped at `MAX_RESPONSE_BYTES`, and the connection is dropped on any
/// error (no retry, no reconnection) — the caller treats the failure as a
/// cache-miss and degrades gracefully.
pub struct RedisConnection {
    stream: TcpStream,
}

impl RedisConnection {
    /// Connect to the Redis endpoint and return a reusable connection.
    pub async fn connect(host: &str, port: u16) -> Result<Self, RedisError> {
        let deadline = Duration::from_secs(10);
        let addr = format!("{host}:{port}");
        let stream = tokio::time::timeout(deadline, TcpStream::connect(&addr))
            .await
            .map_err(|_| RedisError::Timeout(format!("connect timeout to {addr}")))??;
        let _ = stream.set_nodelay(true);
        Ok(Self { stream })
    }

    /// Execute HGET on this connection, reusing the underlying stream.
    ///
    /// Returns the bulk string value, or Nil if the field does not exist.
    pub async fn hget(&mut self, key: &str, field: &str) -> Result<BulkResponse, RedisError> {
        let deadline = Duration::from_secs(10);
        let req = format!(
            "*3\r\n$4\r\nHGET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            field.len(),
            field
        );
        tokio::time::timeout(deadline, self.stream.write_all(req.as_bytes()))
            .await
            .map_err(|_| RedisError::Timeout("write timeout".into()))??;
        let resp = read_response(&mut self.stream).await?;
        parse_bulk(resp)
    }

    /// Execute HMGET on this connection, reusing the underlying stream.
    ///
    /// Returns an array of bulk strings (one per requested field).
    pub async fn hmget(
        &mut self,
        key: &str,
        fields: &[&str],
    ) -> Result<Vec<ArrayElement>, RedisError> {
        let deadline = Duration::from_secs(10);
        let mut req = format!(
            "*{}\r\n$5\r\nHMGET\r\n${}\r\n{}\r\n",
            2 + fields.len(),
            key.len(),
            key
        );
        for f in fields {
            req.push_str(&format!("${}\r\n{}\r\n", f.len(), f));
        }
        tokio::time::timeout(deadline, self.stream.write_all(req.as_bytes()))
            .await
            .map_err(|_| RedisError::Timeout("write timeout".into()))??;
        let resp = read_response(&mut self.stream).await?;
        parse_array(resp)
    }
}

/// Connect to the Redis mesh socket and execute HGET.
///
/// Returns the bulk string value, or Nil if the field does not exist.
pub async fn hget(
    host: &str,
    port: u16,
    key: &str,
    field: &str,
) -> Result<BulkResponse, RedisError> {
    let deadline = Duration::from_secs(10);
    let addr = format!("{host}:{port}");
    let mut stream = tokio::time::timeout(deadline, TcpStream::connect(&addr))
        .await
        .map_err(|_| RedisError::Timeout(format!("connect timeout to {addr}")))??;
    let _ = stream.set_nodelay(true);

    // RESP: *3\r\n$4\r\nHGET\r\n$<keylen>\r\n<key>\r\n$<fieldlen>\r\n<field>\r\n
    let req = format!(
        "*3\r\n$4\r\nHGET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(),
        key,
        field.len(),
        field
    );
    tokio::time::timeout(deadline, stream.write_all(req.as_bytes()))
        .await
        .map_err(|_| RedisError::Timeout("write timeout".into()))??;

    let resp = read_response(&mut stream).await?;
    parse_bulk(resp)
}

/// Connect to the Redis mesh socket and execute HMGET.
///
/// Returns an array of bulk strings (one per requested field).
pub async fn hmget(
    host: &str,
    port: u16,
    key: &str,
    fields: &[&str],
) -> Result<Vec<ArrayElement>, RedisError> {
    let deadline = Duration::from_secs(10);
    let addr = format!("{host}:{port}");
    let mut stream = tokio::time::timeout(deadline, TcpStream::connect(&addr))
        .await
        .map_err(|_| RedisError::Timeout(format!("connect timeout to {addr}")))??;
    let _ = stream.set_nodelay(true);

    // RESP: *(2+nfields)\r\n$5\r\nHMGET\r\n$<keylen>\r\n<key>\r\n$<flen>\r\n<f>\r\n ...
    let mut req = format!(
        "*{}\r\n$5\r\nHMGET\r\n${}\r\n{}\r\n",
        2 + fields.len(),
        key.len(),
        key
    );
    for f in fields {
        req.push_str(&format!("${}\r\n{}\r\n", f.len(), f));
    }
    tokio::time::timeout(deadline, stream.write_all(req.as_bytes()))
        .await
        .map_err(|_| RedisError::Timeout("write timeout".into()))??;

    let resp = read_response(&mut stream).await?;
    parse_array(resp)
}

/// Read the complete RESP response from the stream.
async fn read_response(stream: &mut TcpStream) -> Result<Vec<u8>, RedisError> {
    let mut buf = Vec::with_capacity(READ_BUFFER_BYTES);
    loop {
        let mut chunk = [0u8; READ_BUFFER_BYTES];
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
            .await
            .map_err(|_| RedisError::Timeout("read timeout".into()))??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_RESPONSE_BYTES {
            return Err(RedisError::TooLarge(buf.len()));
        }
        // Check if we have a complete RESP response by parsing the header.
        if is_complete_resp(&buf) {
            break;
        }
    }
    Ok(buf)
}

/// Check if the RESP buffer contains a complete response.
/// Parses the first byte to determine the type and recursively checks.
fn is_complete_resp(buf: &[u8]) -> bool {
    if buf.is_empty() {
        return false;
    }
    let (_consumed, complete) = check_resp_complete(buf, 0);
    complete
}

/// Recursively check if a RESP message starting at `pos` is complete.
/// Returns (bytes_consumed, is_complete).
fn check_resp_complete(buf: &[u8], pos: usize) -> (usize, bool) {
    if pos >= buf.len() {
        return (0, false);
    }
    match buf[pos] {
        b'+' | b'-' | b':' => {
            // Simple string, error, or integer: terminated by \r\n
            if let Some(line_end) = find_crlf(buf, pos + 1) {
                (line_end + 2 - pos, true)
            } else {
                (0, false)
            }
        }
        b'$' => {
            // Bulk string: $<len>\r\n<data>\r\n
            let (line_end, _) = match find_crlf(buf, pos + 1) {
                Some(e) => (e, true),
                None => return (0, false),
            };
            let len_str = match std::str::from_utf8(&buf[pos + 1..line_end]) {
                Ok(s) => s,
                Err(_) => return (0, false),
            };
            let len: i64 = match len_str.parse() {
                Ok(n) => n,
                Err(_) => return (0, false),
            };
            if len < 0 {
                // Nil bulk string
                return (line_end + 2 - pos, true);
            }
            let len = len as usize;
            let data_start = line_end + 2;
            let data_end = data_start + len + 2; // data + \r\n
            if data_end <= buf.len() {
                (data_end - pos, true)
            } else {
                (0, false)
            }
        }
        b'*' => {
            // Array: *<count>\r\n<elements>
            let (line_end, _) = match find_crlf(buf, pos + 1) {
                Some(e) => (e, true),
                None => return (0, false),
            };
            let count_str = match std::str::from_utf8(&buf[pos + 1..line_end]) {
                Ok(s) => s,
                Err(_) => return (0, false),
            };
            let count: i64 = match count_str.parse() {
                Ok(n) => n,
                Err(_) => return (0, false),
            };
            if count < 0 {
                return (line_end + 2 - pos, true);
            }
            let mut offset = line_end + 2;
            for _ in 0..count {
                let (consumed, complete) = check_resp_complete(buf, offset);
                if !complete {
                    return (0, false);
                }
                offset += consumed;
            }
            (offset - pos, true)
        }
        _ => (0, false),
    }
}

/// Find the position of the first \r\n starting from `pos`.
fn find_crlf(buf: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a RESP bulk string response.
fn parse_bulk(buf: Vec<u8>) -> Result<BulkResponse, RedisError> {
    if buf.is_empty() {
        return Err(RedisError::Malformed("empty response".into()));
    }
    match buf[0] {
        b'$' => {
            let line_end = find_crlf(&buf, 1)
                .ok_or_else(|| RedisError::Malformed("missing CRLF in bulk header".into()))?;
            let len_str = std::str::from_utf8(&buf[1..line_end])
                .map_err(|_| RedisError::Malformed("non-utf8 bulk header".into()))?;
            let len: i64 = len_str
                .parse()
                .map_err(|_| RedisError::Malformed(format!("invalid bulk length: {len_str}")))?;
            if len < 0 {
                return Ok(BulkResponse::Nil);
            }
            let len = len as usize;
            let data_start = line_end + 2;
            if buf.len() < data_start + len {
                return Err(RedisError::Malformed("truncated bulk data".into()));
            }
            Ok(BulkResponse::Data(
                buf[data_start..data_start + len].to_vec(),
            ))
        }
        b'-' => {
            let line_end = find_crlf(&buf, 1)
                .ok_or_else(|| RedisError::Malformed("missing CRLF in error".into()))?;
            let msg = String::from_utf8_lossy(&buf[1..line_end]).to_string();
            Err(RedisError::Server(msg))
        }
        _ => Err(RedisError::Malformed(format!(
            "unexpected response type: {}",
            buf[0] as char
        ))),
    }
}

/// Parse a RESP array response into individual elements.
fn parse_array(buf: Vec<u8>) -> Result<Vec<ArrayElement>, RedisError> {
    if buf.is_empty() {
        return Err(RedisError::Malformed("empty response".into()));
    }
    match buf[0] {
        b'*' => {
            let line_end = find_crlf(&buf, 1)
                .ok_or_else(|| RedisError::Malformed("missing CRLF in array header".into()))?;
            let count_str = std::str::from_utf8(&buf[1..line_end])
                .map_err(|_| RedisError::Malformed("non-utf8 array header".into()))?;
            let count: i64 = count_str
                .parse()
                .map_err(|_| RedisError::Malformed(format!("invalid array count: {count_str}")))?;
            if count < 0 {
                return Ok(vec![]);
            }
            let count = count as usize;
            let mut elements = Vec::with_capacity(count);
            let mut offset = line_end + 2;
            for _ in 0..count {
                if offset >= buf.len() {
                    return Err(RedisError::Malformed("truncated array".into()));
                }
                match buf[offset] {
                    b'$' => {
                        let el_end = find_crlf(&buf, offset + 1).ok_or_else(|| {
                            RedisError::Malformed("missing CRLF in array element".into())
                        })?;
                        let len_str = std::str::from_utf8(&buf[offset + 1..el_end])
                            .map_err(|_| RedisError::Malformed("non-utf8 element header".into()))?;
                        let len: i64 = len_str.parse().map_err(|_| {
                            RedisError::Malformed(format!("invalid element length: {len_str}"))
                        })?;
                        if len < 0 {
                            elements.push(ArrayElement::Nil);
                            offset = el_end + 2;
                        } else {
                            let len = len as usize;
                            let data_start = el_end + 2;
                            if buf.len() < data_start + len {
                                return Err(RedisError::Malformed(
                                    "truncated array element data".into(),
                                ));
                            }
                            elements.push(ArrayElement::Data(
                                buf[data_start..data_start + len].to_vec(),
                            ));
                            offset = data_start + len + 2; // skip data + \r\n
                        }
                    }
                    _ => {
                        return Err(RedisError::Malformed(format!(
                            "unexpected array element type: {}",
                            buf[offset] as char
                        )));
                    }
                }
            }
            Ok(elements)
        }
        b'-' => {
            let line_end = find_crlf(&buf, 1)
                .ok_or_else(|| RedisError::Malformed("missing CRLF in error".into()))?;
            let msg = String::from_utf8_lossy(&buf[1..line_end]).to_string();
            Err(RedisError::Server(msg))
        }
        _ => Err(RedisError::Malformed(format!(
            "unexpected response type: {}",
            buf[0] as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bulk_string_response() {
        // Response: $5\r\nhello\r\n
        let resp = b"$5\r\nhello\r\n".to_vec();
        let result = parse_bulk(resp).unwrap();
        match result {
            BulkResponse::Data(d) => assert_eq!(d, b"hello"),
            BulkResponse::Nil => panic!("expected data"),
        }
    }

    #[test]
    fn parses_nil_bulk_response() {
        // Response: $-1\r\n
        let resp = b"$-1\r\n".to_vec();
        let result = parse_bulk(resp).unwrap();
        assert!(matches!(result, BulkResponse::Nil));
    }

    #[test]
    fn parses_array_response() {
        // Response: *3\r\n$5\r\nhello\r\n$0\r\n\r\n$3\r\nfoo\r\n
        let resp = b"*3\r\n$5\r\nhello\r\n$0\r\n\r\n$3\r\nfoo\r\n".to_vec();
        let result = parse_array(resp).unwrap();
        assert_eq!(result.len(), 3);
        match &result[0] {
            ArrayElement::Data(d) => assert_eq!(d, b"hello"),
            ArrayElement::Nil => panic!("expected data"),
        }
        match &result[1] {
            ArrayElement::Data(d) => assert_eq!(d, b""),
            ArrayElement::Nil => panic!("expected data"),
        }
        match &result[2] {
            ArrayElement::Data(d) => assert_eq!(d, b"foo"),
            ArrayElement::Nil => panic!("expected data"),
        }
    }

    #[test]
    fn parses_array_with_nil_element() {
        // Response: *2\r\n$-1\r\n$3\r\nfoo\r\n
        let resp = b"*2\r\n$-1\r\n$3\r\nfoo\r\n".to_vec();
        let result = parse_array(resp).unwrap();
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], ArrayElement::Nil));
        match &result[1] {
            ArrayElement::Data(d) => assert_eq!(d, b"foo"),
            ArrayElement::Nil => panic!("expected data"),
        }
    }

    #[test]
    fn is_complete_resp_detects_complete_bulk() {
        let resp = b"$5\r\nhello\r\n";
        assert!(is_complete_resp(resp));
    }

    #[test]
    fn is_complete_resp_detects_incomplete_bulk() {
        let resp = b"$5\r\nhel";
        assert!(!is_complete_resp(resp));
    }

    #[test]
    fn is_complete_resp_detects_complete_array() {
        let resp = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert!(is_complete_resp(resp));
    }
}
