//! RESP control ACKs share framing with bounded unsolicited publication input.

use brz_net::{
    DecodedResponse, EphemeralBytes, HandshakeStatus, RequestToken, RxBuffer, SessionError,
    SessionProtocol,
};
use bytes::BytesMut;

use super::*;
use crate::net_transport::{
    RedisProtocol, RedisRequest, RedisResponseKind, decode_unsolicited_value,
};
use crate::{Value, cmd};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Control {
    Subscribe,
    Unsubscribe,
}

pub(super) struct PubSubProtocol {
    handshake: RedisProtocol,
    channel: Bytes,
    sender: Option<mpsc::Sender<RedisMessage>>,
    inbox: Arc<Inbox>,
    pending: Option<Control>,
    subscribed: bool,
}

impl PubSubProtocol {
    pub(super) fn new(
        auth: Option<String>,
        db: i64,
        channel: Bytes,
        sender: mpsc::Sender<RedisMessage>,
        inbox: Arc<Inbox>,
    ) -> Self {
        Self {
            handshake: RedisProtocol::new(auth, db),
            channel,
            sender: Some(sender),
            inbox,
            pending: None,
            subscribed: false,
        }
    }

    fn consume(&mut self, value: Value) -> RedisResult<Option<DecodedResponse<()>>> {
        let Value::Array(mut items) = value else {
            return Err(protocol_error("expected a RESP2 Pub/Sub array"));
        };
        if items.len() != 3 {
            return Err(protocol_error(
                "Pub/Sub reply must have exactly three elements",
            ));
        }
        let third = items.pop().expect("third element");
        let channel = items.pop().expect("channel element");
        let kind = items.pop().expect("kind element");
        let (Value::BulkString(kind), Value::BulkString(channel)) = (kind, channel) else {
            return Err(protocol_error(
                "Pub/Sub kind and channel must be bulk strings",
            ));
        };
        if channel != self.channel {
            return Err(protocol_error("Pub/Sub reply belongs to another channel"));
        }
        match kind.as_ref() {
            b"message" => {
                if !self.subscribed {
                    return Err(protocol_error(
                        "Pub/Sub message arrived outside its subscription",
                    ));
                }
                let Value::BulkString(payload) = third else {
                    return Err(protocol_error("Pub/Sub payload must be a bulk string"));
                };
                let sender = self
                    .sender
                    .as_ref()
                    .ok_or_else(|| protocol_error("Pub/Sub input is closed"))?;
                sender.try_send(RedisMessage { channel, payload }).map_err(
                    |error| match error {
                        mpsc::error::TrySendError::Full(_) => RedisError::new(
                            ErrorKind::Overloaded,
                            "Pub/Sub queue exceeded its 256-message capacity; session closed",
                        ),
                        mpsc::error::TrySendError::Closed(_) => {
                            protocol_error("Pub/Sub receiver was closed")
                        }
                    },
                )?;
                Ok(None)
            }
            b"subscribe" | b"unsubscribe" => {
                let control = if kind.as_ref() == b"subscribe" {
                    Control::Subscribe
                } else {
                    Control::Unsubscribe
                };
                let expected_count = i64::from(control == Control::Subscribe);
                if self.pending != Some(control) || third != Value::Int(expected_count) {
                    return Err(protocol_error(
                        "Pub/Sub acknowledgement does not match its command and count",
                    ));
                }
                self.pending = None;
                self.subscribed = control == Control::Subscribe;
                Ok(Some(DecodedResponse::fifo(())))
            }
            _ => Err(protocol_error("unsupported Pub/Sub reply kind")),
        }
    }
}

impl SessionProtocol for PubSubProtocol {
    type Request = Control;
    type Frame = EphemeralBytes;
    type Response = ();
    type Error = RedisError;

    fn reset(&mut self) {
        self.handshake.reset();
        self.pending = None;
        self.subscribed = false;
    }

    fn accepts_unsolicited(&self) -> bool {
        true
    }
    fn reconnect_after_disconnect(&self) -> bool {
        false
    }

    fn disconnected(&mut self, error: &SessionError<RedisError>) {
        self.inbox.disconnected(error);
        self.sender.take();
    }

    fn begin_handshake(&mut self, destination: &mut BytesMut) -> RedisResult<HandshakeStatus> {
        self.handshake.begin_handshake(destination)
    }

    fn decode_handshake(
        &mut self,
        source: &mut BytesMut,
        destination: &mut BytesMut,
    ) -> RedisResult<HandshakeStatus> {
        self.handshake.decode_handshake(source, destination)
    }

    fn encode(&mut self, control: Control, _token: RequestToken) -> RedisResult<EphemeralBytes> {
        if self.pending.is_some() || self.subscribed != (control == Control::Unsubscribe) {
            return Err(protocol_error("invalid concurrent Pub/Sub control command"));
        }
        let mut command = cmd(match control {
            Control::Subscribe => "SUBSCRIBE",
            Control::Unsubscribe => "UNSUBSCRIBE",
        });
        command.arg_bytes(&self.channel);
        let request = RedisRequest::encode(
            &RedisRequest::shared_arena(),
            &command,
            RedisResponseKind::Value,
        );
        let frame = request.into_frame()?;
        self.pending = Some(control);
        Ok(frame)
    }

    fn decode(&mut self, source: &mut RxBuffer) -> RedisResult<Option<DecodedResponse<()>>> {
        while let Some(value) = decode_unsolicited_value(source)? {
            if let Some(response) = self.consume(value)? {
                return Ok(Some(response));
            }
        }
        Ok(None)
    }
}

fn protocol_error(message: &'static str) -> RedisError {
    RedisError::from_kind(ErrorKind::ResponseError, message)
}
