//! Redis wire protocol adapter for the shared Breeze single-session transport.

use brz_net::{DecodedResponse, HandshakeStatus, RequestToken, SessionError, SessionProtocol};
use bytes::{Buf, Bytes, BytesMut};

use crate::resp::parser::{ParseResult, parse_reply};
use crate::{Cmd, ErrorKind, RedisError, RedisResult, Value, cmd};

pub(crate) struct RedisProtocol {
    auth: Option<String>,
    db: i64,
    snapshot: Bytes,
    handshake_replies: usize,
}

impl RedisProtocol {
    pub(crate) fn new(auth: Option<String>, db: i64) -> Self {
        Self {
            auth,
            db,
            snapshot: Bytes::new(),
            handshake_replies: 0,
        }
    }

    fn parse_one(&mut self, source: &mut BytesMut) -> RedisResult<Option<Value>> {
        if self.snapshot.is_empty() {
            if source.is_empty() {
                return Ok(None);
            }
            self.snapshot = source.split().freeze();
        }

        match parse_reply(&self.snapshot)? {
            ParseResult::Complete { value, consumed } => {
                self.snapshot.advance(consumed);
                Ok(Some(value))
            }
            ParseResult::Incomplete => {
                source.extend_from_slice(&self.snapshot);
                self.snapshot = Bytes::new();
                Ok(None)
            }
        }
    }
}

impl SessionProtocol for RedisProtocol {
    type Request = Cmd;
    type Response = Value;
    type Error = RedisError;

    fn reset(&mut self) {
        self.snapshot = Bytes::new();
        self.handshake_replies = 0;
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
            let Some(value) = self.parse_one(source)? else {
                return Ok(HandshakeStatus::Pending);
            };
            match value {
                Value::Okay => self.handshake_replies -= 1,
                Value::ServerError(error) => return Err(error.into()),
                other => {
                    return Err(RedisError::new(
                        ErrorKind::ResponseError,
                        format!("unexpected Redis handshake response: {other:?}"),
                    ));
                }
            }
        }
        Ok(HandshakeStatus::Ready)
    }

    fn encode(
        &mut self,
        request: &Cmd,
        _request_id: RequestToken,
        destination: &mut BytesMut,
    ) -> RedisResult<()> {
        request.encode_into(destination);
        Ok(())
    }

    fn decode(&mut self, source: &mut BytesMut) -> RedisResult<Option<DecodedResponse<Value>>> {
        Ok(self.parse_one(source)?.map(DecodedResponse::fifo))
    }
}

pub(crate) fn map_session_error(error: SessionError<RedisError>) -> RedisError {
    let kind = match &error {
        SessionError::Busy => ErrorKind::Overloaded,
        SessionError::Unavailable => ErrorKind::NoConnection,
        SessionError::Timeout { .. } => ErrorKind::Timeout,
        SessionError::Closed | SessionError::Io(_) => ErrorKind::Io,
        // A wire-format failure poisons the whole session, so it is retriable
        // even when the parser's original classification was ResponseError.
        SessionError::Protocol(_) | SessionError::UnexpectedResponse => ErrorKind::Io,
        SessionError::Routing(_) => ErrorKind::ClientError,
    };
    RedisError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use brz_net::{HandshakeStatus, SessionProtocol};
    use bytes::BytesMut;

    use super::RedisProtocol;
    use crate::{Value, cmd};

    #[test]
    fn command_encodes_directly_into_the_session_buffer() {
        let mut protocol = RedisProtocol::new(None, 0);
        let mut command = cmd("GET");
        command.arg("key");
        let mut destination = BytesMut::new();

        protocol
            .encode(
                &command,
                brz_net::RequestToken::from_raw(1),
                &mut destination,
            )
            .unwrap();

        assert_eq!(destination, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"[..]);
    }

    #[test]
    fn fragmented_and_pipelined_replies_remain_fifo() {
        let mut protocol = RedisProtocol::new(None, 0);
        let mut source = BytesMut::from(&b"$5\r\nhel"[..]);
        assert!(protocol.decode(&mut source).unwrap().is_none());

        source.extend_from_slice(b"lo\r\n:7\r\n");
        let first = protocol.decode(&mut source).unwrap().unwrap().response;
        let second = protocol.decode(&mut source).unwrap().unwrap().response;

        assert!(matches!(first, Value::BulkString(value) if value == "hello"));
        assert_eq!(second, Value::Int(7));
        assert!(protocol.decode(&mut source).unwrap().is_none());
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
}
