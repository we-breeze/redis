use super::*;
use brz_net::{RequestToken, RxBuffer, SessionProtocol};

fn protocol(capacity: usize) -> (PubSubProtocol, mpsc::Receiver<RedisMessage>, Arc<Inbox>) {
    let (sender, receiver) = mpsc::channel(capacity);
    let inbox = Arc::new(Inbox::default());
    let protocol = PubSubProtocol::new(None, 0, Bytes::from_static(b"jobs"), sender, inbox.clone());
    (protocol, receiver, inbox)
}

fn encoded(parts: &[&[u8]]) -> Vec<u8> {
    let mut command = crate::Cmd::new();
    for part in parts {
        command.arg_bytes(part);
    }
    let mut output = bytes::BytesMut::new();
    command.encode_into(&mut output);
    output.to_vec()
}

fn subscribe(protocol: &mut PubSubProtocol) {
    let frame = protocol
        .encode(Control::Subscribe, RequestToken::from_raw(1))
        .unwrap();
    assert_eq!(frame.as_ref(), encoded(&[b"SUBSCRIBE", b"jobs"]));
}

const ACK: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$4\r\njobs\r\n:1\r\n";

#[test]
fn split_subscribe_ack_keeps_a_coalesced_binary_publication() {
    let (mut protocol, mut messages, _) = protocol(MESSAGE_CAPACITY);
    subscribe(&mut protocol);
    let mut source = RxBuffer::with_capacity(32);
    source.extend_from_slice(&ACK[..ACK.len() - 1]).unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_none());
    source.extend_from_slice(&ACK[ACK.len() - 1..]).unwrap();
    source
        .extend_from_slice(&encoded(&[b"message", b"jobs", b"\0\xff\r\n"]))
        .unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_some());
    assert!(protocol.decode(&mut source).unwrap().is_none());
    let message = messages.try_recv().unwrap();
    assert_eq!(message.channel, "jobs");
    assert_eq!(message.payload.as_ref(), b"\0\xff\r\n");
    assert!(source.is_empty());
    assert!(messages.try_recv().is_err());
}

#[test]
fn unsubscribe_uses_the_exact_channel_and_zero_subscription_ack() {
    let (mut protocol, _, _) = protocol(MESSAGE_CAPACITY);
    subscribe(&mut protocol);
    let mut source = RxBuffer::with_capacity(128);
    source.extend_from_slice(ACK).unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_some());
    let frame = protocol
        .encode(Control::Unsubscribe, RequestToken::from_raw(2))
        .unwrap();
    assert_eq!(frame.as_ref(), encoded(&[b"UNSUBSCRIBE", b"jobs"]));
    source
        .extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$4\r\njobs\r\n:0\r\n")
        .unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_some());
}

#[test]
fn unsolicited_or_wrong_channel_ack_is_a_protocol_failure() {
    for response in [
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nother\r\n:1\r\n".as_slice(),
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\njobs\r\n:2\r\n".as_slice(),
        b"*3\r\n$11\r\nunsubscribe\r\n$4\r\njobs\r\n:0\r\n".as_slice(),
    ] {
        let (mut protocol, _, _) = protocol(MESSAGE_CAPACITY);
        subscribe(&mut protocol);
        let mut source = RxBuffer::with_capacity(128);
        source.extend_from_slice(response).unwrap();
        assert!(protocol.decode(&mut source).is_err());
    }
}

#[test]
fn overflow_preserves_only_the_finite_prefix_and_an_explicit_error() {
    let (mut protocol, mut messages, inbox) = protocol(2);
    subscribe(&mut protocol);
    let mut source = RxBuffer::with_capacity(128);
    source.extend_from_slice(ACK).unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_some());
    for payload in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        source
            .extend_from_slice(&encoded(&[b"message", b"jobs", payload]))
            .unwrap();
    }
    let error = protocol.decode(&mut source).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    protocol.disconnected(&SessionError::Protocol(Arc::new(error)));
    assert_eq!(messages.try_recv().unwrap().payload, "one");
    assert_eq!(messages.try_recv().unwrap().payload, "two");
    assert!(matches!(
        messages.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    assert_eq!(inbox.take_error().unwrap().kind(), ErrorKind::Overloaded);
    assert!(!protocol.reconnect_after_disconnect());
}

#[test]
fn publication_frame_terminator_is_validated_by_shared_scanner() {
    let (mut protocol, _, _) = protocol(MESSAGE_CAPACITY);
    subscribe(&mut protocol);
    let mut source = RxBuffer::with_capacity(128);
    source.extend_from_slice(ACK).unwrap();
    assert!(protocol.decode(&mut source).unwrap().is_some());
    source
        .extend_from_slice(b"*3\r\n$7\r\nmessage\r\n$4\r\njobs\r\n$3\r\noneXX")
        .unwrap();
    assert!(protocol.decode(&mut source).is_err());
}
