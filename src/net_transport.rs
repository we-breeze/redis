//! Redis wire adapter for Breeze's shared single-session transport.

use std::{collections::VecDeque, ops::Range};

use brz_net::{
    DecodedResponse, EphemeralBytes, EphemeralBytesArena, EphemeralBytesMut, HandshakeStatus,
    RequestToken, RxBuffer, RxFrame, SessionError, SessionProtocol, global_request_arena,
};
use bytes::{Bytes, BytesMut};

use crate::bulk::RedisValuesSource;
use crate::error::ServerError;
use crate::{
    EncodeRedisArg, EncodeRedisArgs, ErrorKind, FromRedisBulk, RedisArgSink, RedisArgsSink,
    RedisError, RedisResult, RedisValues, Value, cmd,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RedisResponseKind {
    Unit,
    Integer,
    Bulk,
    MultiBulk { expected: usize },
    Value,
}

#[derive(Debug)]
pub(crate) enum RedisResponse {
    Unit,
    Integer(i64),
    Bulk(Option<Bytes>),
    MultiBulk(RedisValuesSource),
    Value(Value),
    ServerError(ServerError),
}

impl RedisResponse {
    pub(crate) fn into_unit(self) -> RedisResult<()> {
        match self {
            Self::Unit => Ok(()),
            Self::ServerError(error) => Err(error.into()),
            other => Err(unexpected_reply("status", &other)),
        }
    }

    pub(crate) fn into_integer(self) -> RedisResult<i64> {
        match self {
            Self::Integer(value) => Ok(value),
            Self::ServerError(error) => Err(error.into()),
            other => Err(unexpected_reply("integer", &other)),
        }
    }

    pub(crate) fn into_bulk(self) -> RedisResult<Option<Bytes>> {
        match self {
            Self::Bulk(value) => Ok(value),
            Self::ServerError(error) => Err(error.into()),
            other => Err(unexpected_reply("bulk string", &other)),
        }
    }

    pub(crate) fn into_multi_bulk<R: FromRedisBulk>(self) -> RedisResult<RedisValues<R>> {
        match self {
            Self::MultiBulk(values) => Ok(RedisValues::from_source(values)),
            Self::ServerError(error) => Err(error.into()),
            other => Err(unexpected_reply("bulk-string array", &other)),
        }
    }

    pub(crate) fn into_value(self) -> RedisResult<Value> {
        match self {
            Self::Value(value) => value.into_result(),
            Self::ServerError(error) => Err(error.into()),
            other => Err(unexpected_reply("RESP value", &other)),
        }
    }
}

fn unexpected_reply(expected: &str, actual: &RedisResponse) -> RedisError {
    RedisError::new(
        ErrorKind::TypeError,
        format!("expected Redis {expected} response, received {actual:?}"),
    )
}

/// One complete RESP request in its final socket-write storage.
pub(crate) struct RedisRequest {
    frame: RedisResult<EphemeralBytes>,
    response: RedisResponseKind,
}

impl RedisRequest {
    pub(crate) fn encode<A>(
        arena: &EphemeralBytesArena,
        arguments: &A,
        response: RedisResponseKind,
    ) -> Self
    where
        A: EncodeRedisArgs + ?Sized,
    {
        Self {
            frame: encode_frame(arena, arguments),
            response,
        }
    }

    pub(crate) fn shared_arena() -> EphemeralBytesArena {
        global_request_arena().clone()
    }
}

struct MeasureArgs {
    body_len: usize,
}

impl RedisArgsSink for MeasureArgs {
    fn write_arg<A: EncodeRedisArg + ?Sized>(&mut self, arg: &A) -> RedisResult<()> {
        let length = arg.encoded_len();
        self.body_len = self
            .body_len
            .checked_add(1 + decimal_len(length) + 2)
            .and_then(|value| value.checked_add(length + 2))
            .ok_or_else(|| protocol_error("Redis request length overflow"))?;
        Ok(())
    }
}

struct FramePayload<'a> {
    frame: &'a mut EphemeralBytesMut,
}

impl RedisArgSink for FramePayload<'_> {
    fn write(&mut self, bytes: &[u8]) {
        self.frame.extend_from_slice(bytes);
    }
}

struct FrameArgs<'a> {
    frame: &'a mut EphemeralBytesMut,
}

impl RedisArgsSink for FrameArgs<'_> {
    fn write_arg<A: EncodeRedisArg + ?Sized>(&mut self, arg: &A) -> RedisResult<()> {
        self.frame.extend_from_slice(b"$");
        write_usize(arg.encoded_len(), self.frame);
        self.frame.extend_from_slice(b"\r\n");
        let start = self.frame.len();
        arg.encode(&mut FramePayload { frame: self.frame })?;
        if self.frame.len() - start != arg.encoded_len() {
            return Err(protocol_error(
                "Redis argument encoder wrote a length different from encoded_len",
            ));
        }
        self.frame.extend_from_slice(b"\r\n");
        Ok(())
    }
}

fn encode_frame<A: EncodeRedisArgs + ?Sized>(
    arena: &EphemeralBytesArena,
    arguments: &A,
) -> RedisResult<EphemeralBytes> {
    let count = arguments.num_args();
    let mut measure = MeasureArgs { body_len: 0 };
    arguments.encode_args(&mut measure)?;
    let frame_len = 1_usize
        .checked_add(decimal_len(count) + 2)
        .and_then(|length| length.checked_add(measure.body_len))
        .ok_or_else(|| protocol_error("Redis request length overflow"))?;

    let mut frame = arena.alloc(frame_len);
    frame.extend_from_slice(b"*");
    write_usize(count, &mut frame);
    frame.extend_from_slice(b"\r\n");
    arguments.encode_args(&mut FrameArgs { frame: &mut frame })?;
    debug_assert_eq!(frame.len(), frame_len);
    Ok(frame.freeze())
}

#[inline]
fn decimal_len(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

#[inline]
fn write_usize(value: usize, destination: &mut EphemeralBytesMut) {
    let mut buffer = itoa::Buffer::new();
    destination.extend_from_slice(buffer.format(value).as_bytes());
}

pub(crate) struct RedisProtocol {
    auth: Option<String>,
    db: i64,
    handshake_replies: usize,
    responses: VecDeque<RedisResponseKind>,
}

impl RedisProtocol {
    pub(crate) fn new(auth: Option<String>, db: i64) -> Self {
        Self {
            auth,
            db,
            handshake_replies: 0,
            responses: VecDeque::new(),
        }
    }

    fn decode_response(&mut self, source: &mut RxBuffer) -> RedisResult<Option<RedisResponse>> {
        let Some(expected) = self.responses.front().copied() else {
            if source.is_empty() {
                return Ok(None);
            }
            return Err(protocol_error("Redis response has no encoded request"));
        };

        match scan_response(source, expected)? {
            Scanned::Incomplete { reserve } => {
                if reserve > 0 {
                    source
                        .reserve(reserve)
                        .map_err(|error| protocol_detail("Redis response is too large", error))?;
                }
                Ok(None)
            }
            Scanned::Complete { layout, consumed } => {
                self.responses.pop_front();
                Ok(Some(materialize(source.take(consumed), layout)?))
            }
        }
    }
}

impl SessionProtocol for RedisProtocol {
    type Request = RedisRequest;
    type Frame = EphemeralBytes;
    type Response = RedisResponse;
    type Error = RedisError;

    fn reset(&mut self) {
        self.handshake_replies = 0;
        self.responses.clear();
    }

    fn begin_handshake(&mut self, destination: &mut BytesMut) -> RedisResult<HandshakeStatus> {
        if let Some(auth) = self.auth.as_deref() {
            let mut command = cmd("AUTH");
            command.arg(auth);
            command.encode_into(destination);
            self.handshake_replies += 1;
        }
        if self.db != 0 {
            let mut command = cmd("SELECT");
            command.arg(self.db);
            command.encode_into(destination);
            self.handshake_replies += 1;
        }
        Ok(if self.handshake_replies == 0 {
            HandshakeStatus::Ready
        } else {
            HandshakeStatus::Pending
        })
    }

    fn decode_handshake(
        &mut self,
        source: &mut BytesMut,
        _destination: &mut BytesMut,
    ) -> RedisResult<HandshakeStatus> {
        while self.handshake_replies > 0 {
            let Some(end) = source.windows(2).position(|window| window == b"\r\n") else {
                return Ok(HandshakeStatus::Pending);
            };
            let line = source.split_to(end + 2).freeze();
            match line.first() {
                Some(b'+') if &line[1..end] == b"OK" => self.handshake_replies -= 1,
                Some(b'-') => {
                    let message = String::from_utf8_lossy(&line[1..end]).into_owned();
                    return Err(ServerError::from_line(message).into());
                }
                _ => return Err(protocol_error("unexpected Redis handshake response")),
            }
        }
        Ok(HandshakeStatus::Ready)
    }

    fn encode(
        &mut self,
        request: RedisRequest,
        _request_id: RequestToken,
    ) -> RedisResult<EphemeralBytes> {
        let frame = request.frame?;
        self.responses.push_back(request.response);
        Ok(frame)
    }

    fn decode(
        &mut self,
        source: &mut RxBuffer,
    ) -> RedisResult<Option<DecodedResponse<RedisResponse>>> {
        Ok(self.decode_response(source)?.map(DecodedResponse::fifo))
    }
}

#[derive(Debug)]
enum ResponseLayout {
    Unit,
    Integer(i64),
    Bulk(Option<Range<usize>>),
    MultiBulk { first: usize, count: usize },
    Value,
    ServerError(Range<usize>),
}

enum Scanned {
    Complete {
        layout: ResponseLayout,
        consumed: usize,
    },
    Incomplete {
        reserve: usize,
    },
}

#[derive(Clone)]
struct BulkLayout {
    body: Option<Range<usize>>,
    consumed: usize,
}

fn scan_response(source: &RxBuffer, expected: RedisResponseKind) -> RedisResult<Scanned> {
    let Some(marker) = source.byte(0) else {
        return Ok(Scanned::Incomplete { reserve: 1 });
    };
    if marker == b'-' {
        return match line_range(source, 1) {
            Some((line, consumed)) => Ok(Scanned::Complete {
                layout: ResponseLayout::ServerError(line),
                consumed,
            }),
            None => Ok(Scanned::Incomplete { reserve: 512 }),
        };
    }

    match expected {
        RedisResponseKind::Unit => scan_unit(source),
        RedisResponseKind::Integer => scan_integer(source),
        RedisResponseKind::Bulk => match scan_bulk(source, 0)? {
            Some(layout) => Ok(Scanned::Complete {
                consumed: layout.consumed,
                layout: ResponseLayout::Bulk(layout.body),
            }),
            None => Ok(Scanned::Incomplete {
                reserve: bulk_reserve_hint(source, 0)?,
            }),
        },
        RedisResponseKind::MultiBulk { expected } => scan_multi_bulk(source, expected),
        RedisResponseKind::Value => scan_value(source, 0, 0),
    }
}

fn scan_value(source: &RxBuffer, start: usize, depth: usize) -> RedisResult<Scanned> {
    if depth > 128 {
        return Err(protocol_error("Redis response nested too deeply"));
    }
    let Some(marker) = source.byte(start) else {
        return Ok(Scanned::Incomplete { reserve: 1 });
    };
    match marker {
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => {
            let Some((_, consumed)) = line_range(source, start + 1) else {
                return Ok(Scanned::Incomplete { reserve: 512 });
            };
            Ok(Scanned::Complete {
                layout: ResponseLayout::Value,
                consumed,
            })
        }
        b'$' | b'=' => {
            let Some((length_range, body_start)) = line_range(source, start + 1) else {
                return Ok(Scanned::Incomplete { reserve: 512 });
            };
            let length = parse_length(source, length_range)?;
            if length < 0 {
                return Ok(Scanned::Complete {
                    layout: ResponseLayout::Value,
                    consumed: body_start,
                });
            }
            let length = usize::try_from(length)
                .map_err(|_| protocol_error("Redis response is too large"))?;
            let consumed = body_start
                .checked_add(length)
                .and_then(|end| end.checked_add(2))
                .ok_or_else(|| protocol_error("Redis response length overflow"))?;
            if source.len() < consumed {
                return Ok(Scanned::Incomplete {
                    reserve: consumed.saturating_sub(source.len()).max(1),
                });
            }
            if source.byte(consumed - 2) != Some(b'\r') || source.byte(consumed - 1) != Some(b'\n')
            {
                return Err(protocol_error("Redis response is missing trailing CRLF"));
            }
            Ok(Scanned::Complete {
                layout: ResponseLayout::Value,
                consumed,
            })
        }
        b'*' | b'~' | b'>' | b'%' => {
            let Some((length_range, mut position)) = line_range(source, start + 1) else {
                return Ok(Scanned::Incomplete { reserve: 512 });
            };
            let length = parse_length(source, length_range)?;
            if length < 0 {
                return Ok(Scanned::Complete {
                    layout: ResponseLayout::Value,
                    consumed: position,
                });
            }
            let mut elements = usize::try_from(length)
                .map_err(|_| protocol_error("Redis response is too large"))?;
            if marker == b'%' {
                elements = elements
                    .checked_mul(2)
                    .ok_or_else(|| protocol_error("Redis map length overflow"))?;
            }
            for _ in 0..elements {
                match scan_value(source, position, depth + 1)? {
                    Scanned::Complete { consumed, .. } => position = consumed,
                    Scanned::Incomplete { reserve } => {
                        return Ok(Scanned::Incomplete { reserve });
                    }
                }
            }
            Ok(Scanned::Complete {
                layout: ResponseLayout::Value,
                consumed: position,
            })
        }
        _ => Err(protocol_error("unknown Redis response marker")),
    }
}

fn scan_integer(source: &RxBuffer) -> RedisResult<Scanned> {
    if source.byte(0) != Some(b':') {
        return Err(protocol_error("expected Redis integer response"));
    }
    let Some((range, consumed)) = line_range(source, 1) else {
        return Ok(Scanned::Incomplete { reserve: 32 });
    };
    let value = parse_length(source, range)?;
    Ok(Scanned::Complete {
        layout: ResponseLayout::Integer(value),
        consumed,
    })
}

fn scan_unit(source: &RxBuffer) -> RedisResult<Scanned> {
    if source.byte(0) != Some(b'+') {
        return Err(protocol_error("expected Redis status response"));
    }
    let Some((line, consumed)) = line_range(source, 1) else {
        return Ok(Scanned::Incomplete { reserve: 512 });
    };
    if !source.range_eq(line, b"OK") {
        return Err(protocol_error("expected +OK Redis response"));
    }
    Ok(Scanned::Complete {
        layout: ResponseLayout::Unit,
        consumed,
    })
}

fn scan_multi_bulk(source: &RxBuffer, expected: usize) -> RedisResult<Scanned> {
    if source.byte(0) != Some(b'*') {
        return Err(protocol_error("expected Redis array response"));
    }
    let Some((count_range, mut position)) = line_range(source, 1) else {
        return Ok(Scanned::Incomplete { reserve: 512 });
    };
    let count = parse_length(source, count_range)?;
    if count < 0 || count as usize != expected {
        return Err(protocol_error("unexpected Redis array length"));
    }

    let first = position;
    for _ in 0..expected {
        let Some(layout) = scan_bulk(source, position)? else {
            return Ok(Scanned::Incomplete {
                reserve: bulk_reserve_hint(source, position)?,
            });
        };
        position = layout.consumed;
    }

    Ok(Scanned::Complete {
        layout: ResponseLayout::MultiBulk {
            first,
            count: expected,
        },
        consumed: position,
    })
}

fn scan_bulk(source: &RxBuffer, start: usize) -> RedisResult<Option<BulkLayout>> {
    let Some(marker) = source.byte(start) else {
        return Ok(None);
    };
    if marker != b'$' {
        return Err(protocol_error("expected Redis bulk-string element"));
    }
    let Some((length_range, body_start)) = line_range(source, start + 1) else {
        return Ok(None);
    };
    let length = parse_length(source, length_range)?;
    if length == -1 {
        return Ok(Some(BulkLayout {
            body: None,
            consumed: body_start,
        }));
    }
    if length < 0 {
        return Err(protocol_error("invalid negative Redis bulk length"));
    }
    let length = usize::try_from(length).map_err(|_| protocol_error("Redis bulk is too large"))?;
    let body_end = body_start
        .checked_add(length)
        .ok_or_else(|| protocol_error("Redis bulk length overflow"))?;
    let consumed = body_end
        .checked_add(2)
        .ok_or_else(|| protocol_error("Redis bulk length overflow"))?;
    if source.len() < consumed {
        return Ok(None);
    }
    if source.byte(body_end) != Some(b'\r') || source.byte(body_end + 1) != Some(b'\n') {
        return Err(protocol_error("Redis bulk is missing trailing CRLF"));
    }
    Ok(Some(BulkLayout {
        body: Some(body_start..body_end),
        consumed,
    }))
}

fn bulk_reserve_hint(source: &RxBuffer, start: usize) -> RedisResult<usize> {
    let Some((length_range, body_start)) = line_range(source, start + 1) else {
        return Ok(512);
    };
    let length = parse_length(source, length_range)?;
    if length < 0 {
        return Ok(1);
    }
    let length = usize::try_from(length).map_err(|_| protocol_error("Redis bulk is too large"))?;
    let total = body_start
        .checked_add(length)
        .and_then(|end| end.checked_add(2))
        .ok_or_else(|| protocol_error("Redis bulk length overflow"))?;
    Ok(total.saturating_sub(source.len()).max(1))
}

fn line_range(source: &RxBuffer, start: usize) -> Option<(Range<usize>, usize)> {
    let end = source.find_crlf(start)?;
    Some((start..end, end + 2))
}

fn parse_length(source: &RxBuffer, range: Range<usize>) -> RedisResult<i64> {
    if range.is_empty() {
        return Err(protocol_error("empty Redis length"));
    }
    let negative = source.byte(range.start) == Some(b'-');
    let digits = if negative {
        range.start + 1..range.end
    } else {
        range
    };
    if digits.is_empty() {
        return Err(protocol_error("invalid Redis length"));
    }
    let mut value = 0_i64;
    for index in digits {
        let digit = source
            .byte(index)
            .filter(u8::is_ascii_digit)
            .ok_or_else(|| protocol_error("invalid Redis length"))?;
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i64::from(digit - b'0')))
            .ok_or_else(|| protocol_error("Redis length overflow"))?;
    }
    Ok(if negative { -value } else { value })
}

fn materialize(frame: RxFrame, layout: ResponseLayout) -> RedisResult<RedisResponse> {
    Ok(match layout {
        ResponseLayout::Unit => RedisResponse::Unit,
        ResponseLayout::Integer(value) => RedisResponse::Integer(value),
        ResponseLayout::ServerError(range) => {
            let line = frame.copy_range(range);
            RedisResponse::ServerError(ServerError::from_line(
                String::from_utf8_lossy(&line).into_owned(),
            ))
        }
        ResponseLayout::Bulk(range) => match frame.into_contiguous() {
            Ok(frame) => {
                let response = Bytes::from_owner(frame);
                RedisResponse::Bulk(range.map(|range| response.slice(range)))
            }
            Err(frame) => RedisResponse::Bulk(range.map(|range| frame.copy_range(range))),
        },
        ResponseLayout::MultiBulk { first, count } => match frame.into_contiguous() {
            Ok(frame) => RedisResponse::MultiBulk(RedisValuesSource::Contiguous {
                frame: Bytes::from_owner(frame),
                cursor: first,
                remaining: count,
            }),
            Err(frame) => RedisResponse::MultiBulk(RedisValuesSource::Wrapped {
                frame,
                cursor: first,
                remaining: count,
            }),
        },
        ResponseLayout::Value => {
            let bytes = match frame.into_contiguous() {
                Ok(frame) => Bytes::from_owner(frame),
                Err(frame) => frame.copy_range(0..frame.len()),
            };
            match crate::resp::parse_reply(&bytes)? {
                crate::resp::ParseResult::Complete { value, consumed }
                    if consumed == bytes.len() =>
                {
                    RedisResponse::Value(value)
                }
                crate::resp::ParseResult::Complete { .. } => {
                    return Err(protocol_error("Redis parser left trailing response bytes"));
                }
                crate::resp::ParseResult::Incomplete => {
                    return Err(protocol_error(
                        "Redis scanner accepted an incomplete response",
                    ));
                }
            }
        }
    })
}

fn protocol_error(message: &'static str) -> RedisError {
    RedisError::from_kind(ErrorKind::ResponseError, message)
}

fn protocol_detail(message: &'static str, detail: impl ToString) -> RedisError {
    RedisError::with_detail(ErrorKind::ResponseError, message, detail.to_string())
}

pub(crate) fn map_session_error(error: SessionError<RedisError>) -> RedisError {
    let kind = match &error {
        SessionError::Busy => ErrorKind::Overloaded,
        SessionError::Unavailable => ErrorKind::NoConnection,
        SessionError::Timeout { .. } => ErrorKind::Timeout,
        SessionError::Closed | SessionError::Io(_) => ErrorKind::Io,
        SessionError::Protocol(_)
        | SessionError::UnexpectedResponse
        | SessionError::EmptyRequestFrame => ErrorKind::Io,
        SessionError::Routing(_) => ErrorKind::ClientError,
    };
    RedisError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use brz_net::{HandshakeStatus, Node, NodeOptions, RxBuffer, SessionError, SessionProtocol};
    use bytes::BytesMut;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::{Instant, sleep},
    };

    use super::{RedisProtocol, RedisRequest, RedisResponse, RedisResponseKind};

    fn request(kind: RedisResponseKind) -> RedisRequest {
        let arena = RedisRequest::shared_arena();
        RedisRequest::encode(&arena, &("GET", "key"), kind)
    }

    fn feed(source: &mut RxBuffer, bytes: &[u8]) {
        source.extend_from_slice(bytes).unwrap();
    }

    #[test]
    fn request_is_already_the_final_session_frame() {
        let mut protocol = RedisProtocol::new(None, 0);
        let frame = protocol
            .encode(
                request(RedisResponseKind::Bulk),
                brz_net::RequestToken::from_raw(1),
            )
            .unwrap();
        assert_eq!(frame.as_ref(), b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn fragmented_and_pipelined_replies_remain_fifo() {
        let mut protocol = RedisProtocol::new(None, 0);
        for id in 1..=2 {
            protocol
                .encode(
                    request(RedisResponseKind::Bulk),
                    brz_net::RequestToken::from_raw(id),
                )
                .unwrap();
        }
        let mut source = RxBuffer::with_capacity(8);
        feed(&mut source, b"$5\r\nhel");
        assert!(protocol.decode(&mut source).unwrap().is_none());
        feed(&mut source, b"lo\r\n$1\r\nx\r\n");

        let first = protocol.decode(&mut source).unwrap().unwrap().response;
        let second = protocol.decode(&mut source).unwrap().unwrap().response;
        assert!(matches!(first, RedisResponse::Bulk(Some(value)) if value == "hello"));
        assert!(matches!(second, RedisResponse::Bulk(Some(value)) if value == "x"));
        assert!(protocol.decode(&mut source).unwrap().is_none());
    }

    #[test]
    fn multi_bulk_preserves_nil_and_field_order() {
        let mut protocol = RedisProtocol::new(None, 0);
        protocol
            .encode(
                request(RedisResponseKind::MultiBulk { expected: 3 }),
                brz_net::RequestToken::from_raw(1),
            )
            .unwrap();
        let mut source = RxBuffer::with_capacity(16);
        feed(&mut source, b"*3\r\n$1\r\na\r\n$-1\r\n$2\r\nbb\r\n");

        let response = protocol.decode(&mut source).unwrap().unwrap().response;
        let RedisResponse::MultiBulk(values) = response else {
            panic!("expected multi bulk response");
        };
        let values = crate::RedisValues::<bytes::Bytes>::from_source(values)
            .collect::<crate::RedisResult<Vec<_>>>()
            .unwrap();
        assert_eq!(values[0].as_deref(), Some(b"a".as_slice()));
        assert_eq!(values[1], None);
        assert_eq!(values[2].as_deref(), Some(b"bb".as_slice()));
    }

    #[test]
    fn dynamic_value_decodes_nested_and_variable_length_replies() {
        let mut protocol = RedisProtocol::new(None, 0);
        protocol
            .encode(
                request(RedisResponseKind::Value),
                brz_net::RequestToken::from_raw(1),
            )
            .unwrap();
        let mut source = RxBuffer::with_capacity(8);
        feed(&mut source, b"*3\r\n$1\r\na\r\n:2\r\n");
        assert!(protocol.decode(&mut source).unwrap().is_none());
        feed(&mut source, b"*2\r\n+OK\r\n$-1\r\n");

        let response = protocol.decode(&mut source).unwrap().unwrap().response;
        let value = response.into_value().unwrap();
        assert!(matches!(
            value,
            crate::Value::Array(values)
                if values.len() == 3
                    && values[0].as_bytes() == Some(b"a".as_slice())
                    && values[1] == crate::Value::Int(2)
                    && matches!(&values[2], crate::Value::Array(nested)
                        if nested == &[crate::Value::Okay, crate::Value::Nil])
        ));
    }

    #[test]
    fn large_bulk_reserves_once_and_leaves_pipelined_tail_registered() {
        let mut protocol = RedisProtocol::new(None, 0);
        for id in 1..=2 {
            protocol
                .encode(
                    request(RedisResponseKind::Bulk),
                    brz_net::RequestToken::from_raw(id),
                )
                .unwrap();
        }

        let body = vec![b'x'; 128 * 1024];
        let mut source = RxBuffer::with_capacity(8 * 1024);
        feed(&mut source, b"$131072\r\nfirst-prefix");
        assert!(protocol.decode(&mut source).unwrap().is_none());
        assert!(source.capacity() >= body.len());

        let prefix = b"first-prefix";
        feed(&mut source, &body[prefix.len()..]);
        feed(&mut source, b"\r\n$4\r\ntail\r\n");

        let first = protocol.decode(&mut source).unwrap().unwrap().response;
        let second = protocol.decode(&mut source).unwrap().unwrap().response;
        let RedisResponse::Bulk(Some(first)) = first else {
            panic!("expected first bulk response");
        };
        assert_eq!(first.len(), body.len());
        assert_eq!(&first[..prefix.len()], prefix);
        assert!(first[prefix.len()..].iter().all(|byte| *byte == b'x'));
        assert!(matches!(second, RedisResponse::Bulk(Some(value)) if value == "tail"));
        assert!(source.is_empty());
    }

    #[test]
    fn auth_and_select_complete_before_the_node_is_ready() {
        let mut protocol = RedisProtocol::new(Some("secret".into()), 3);
        let mut write = BytesMut::new();
        assert_eq!(
            protocol.begin_handshake(&mut write).unwrap(),
            HandshakeStatus::Pending
        );
        assert!(write.windows(4).any(|window| window == b"AUTH"));
        assert!(write.windows(6).any(|window| window == b"SELECT"));

        let mut read = BytesMut::from(&b"+OK\r\n+OK\r\n"[..]);
        assert_eq!(
            protocol.decode_handshake(&mut read, &mut write).unwrap(),
            HandshakeStatus::Ready
        );
    }

    #[tokio::test]
    async fn node_drains_a_large_bulk_and_the_following_response() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let wire_request = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut requests = vec![0; wire_request.len() * 2];
            socket.read_exact(&mut requests).await.unwrap();
            assert_eq!(&requests[..wire_request.len()], wire_request);
            assert_eq!(&requests[wire_request.len()..], wire_request);

            let body = vec![b'z'; 256 * 1024];
            socket.write_all(b"$262144\r\n").await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.write_all(b"\r\n$4\r\ntail\r\n").await.unwrap();
        });

        let node = Node::new(
            address,
            RedisProtocol::new(None, 0),
            NodeOptions {
                request_timeout: Duration::from_secs(2),
                connect_timeout: Duration::from_secs(1),
                max_read_buffer_capacity: 1024 * 1024,
                ..NodeOptions::default()
            },
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !node.is_connected() {
            assert!(Instant::now() < deadline);
            sleep(Duration::from_millis(1)).await;
        }

        let first = node.request(request(RedisResponseKind::Bulk)).unwrap();
        let second = node.request(request(RedisResponseKind::Bulk)).unwrap();
        let RedisResponse::Bulk(Some(first)) = first.await.unwrap() else {
            panic!("expected first bulk response");
        };
        let RedisResponse::Bulk(Some(second)) = second.await.unwrap() else {
            panic!("expected second bulk response");
        };
        assert_eq!(first.len(), 256 * 1024);
        assert!(first.iter().all(|byte| *byte == b'z'));
        assert_eq!(second, "tail");
    }

    #[tokio::test]
    async fn node_rejects_a_bulk_above_the_receive_limit() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".len()];
            socket.read_exact(&mut request).await.unwrap();
            socket.write_all(b"$2097152\r\n").await.unwrap();
            sleep(Duration::from_millis(100)).await;
        });

        let node = Node::new(
            address,
            RedisProtocol::new(None, 0),
            NodeOptions {
                request_timeout: Duration::from_secs(2),
                connect_timeout: Duration::from_secs(1),
                max_read_buffer_capacity: 1024 * 1024,
                ..NodeOptions::default()
            },
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !node.is_connected() {
            assert!(Instant::now() < deadline);
            sleep(Duration::from_millis(1)).await;
        }

        let response = node
            .request(request(RedisResponseKind::Bulk))
            .unwrap()
            .await;
        assert!(matches!(response, Err(SessionError::Protocol(_))));
    }
}
