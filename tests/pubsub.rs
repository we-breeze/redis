//! Public subscription APIs against a real RESP TCP peer, without a live Redis dependency.
#[path = "support/runtime.rs"]
mod runtime;
use runtime::runtime_test;

#[path = "pubsub/server.rs"]
mod server;

use std::time::Duration;

use brz_redis::{ErrorKind, Redis, RedisService, RedisServiceOptions};
use server::{Mode, Server, acknowledgement, message};
use tokio::time::timeout;

const CHANNEL: &[u8] = b"callback:channel:123";
const WAIT: Duration = Duration::from_secs(3);

async fn service(server: &Server) -> RedisService {
    RedisService::single(server.endpoint.clone()).await.unwrap()
}

fn names(server: &Server, id: usize) -> Vec<Vec<u8>> {
    server.sessions()[id]
        .commands
        .iter()
        .map(|args| args[0].clone())
        .collect()
}

fn subscribe_count(server: &Server) -> usize {
    server
        .sessions()
        .iter()
        .flat_map(|session| &session.commands)
        .filter(|args| args[0] == b"SUBSCRIBE")
        .count()
}

runtime_test! {
async fn authenticates_selects_and_preserves_first_binary_message_after_split_ack() {
    let server = Server::start(Mode::ManualSubscribeAck).await;
    let redis = RedisService::single_with_options(
        format!("{}:3", server.endpoint),
        RedisServiceOptions::default().with_password(Some("test password".into())),
    ).await.unwrap();
    let channel: &'static [u8] = b"callback:\0\xff";
    let payload = b"prefix\0\xff\r\nsuffix";
    let mut connecting = tokio::spawn(async move { redis.subscribe(channel).await });
    let id = server.subscription().await;
    let ack = acknowledgement(b"subscribe", channel, 1);
    let split = ack.len() - 4;
    server.write(id, ack[..split].to_vec()).await;
    assert!(timeout(Duration::from_millis(25), &mut connecting).await.is_err(),
        "subscribe must wait for the complete confirmation");
    let mut tail = ack[split..].to_vec();
    tail.extend(message(channel, payload));
    server.write(id, tail).await;
    let mut subscription = timeout(WAIT, connecting).await.unwrap().unwrap().unwrap();
    let input = timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().unwrap();
    assert_eq!(input.channel.as_ref(), channel);
    assert_eq!(input.payload.as_ref(), payload);
    let sessions = server.sessions();
    assert_eq!(sessions[id].commands, vec![
        vec![b"AUTH".to_vec(), b"test password".to_vec()],
        vec![b"SELECT".to_vec(), b"3".to_vec()],
        vec![b"SUBSCRIBE".to_vec(), channel.to_vec()],
    ]);
    subscription.close().await.unwrap();
    server.wait_closed(id).await;
    assert_eq!(names(&server, id), [b"AUTH".to_vec(), b"SELECT".to_vec(), b"SUBSCRIBE".to_vec()]);
}
}

runtime_test! {
async fn delivers_all_184_notifications_in_order_without_exposing_confirmation_frames() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    let mut burst = Vec::new();
    for index in 0..184_u64 {
        burst.extend(message(CHANNEL, &index.to_be_bytes()));
    }
    server.write(id, burst).await;
    timeout(WAIT, async {
        for index in 0..184_u64 {
            let input = subscription.next_message().await.unwrap().unwrap();
            assert_eq!(input.channel.as_ref(), CHANNEL);
            assert_eq!(input.payload.as_ref(), index.to_be_bytes().as_slice());
        }
    }).await.unwrap();
    assert!(timeout(Duration::from_millis(25), subscription.next_message()).await.is_err());
    subscription.close().await.unwrap();
    server.wait_closed(id).await;
    assert_eq!(names(&server, id), [b"SUBSCRIBE".to_vec()]);
}
}

runtime_test! {
async fn idle_timeouts_and_cancelled_half_frame_reads_keep_the_same_subscription() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    // Together these exceed the service's default 200ms finite-request timeout.
    for _ in 0..2 {
        assert!(timeout(Duration::from_millis(125), subscription.next_message()).await.is_err());
    }
    let payload: Vec<_> = (0..4096).map(|index| (index % 256) as u8).collect();
    let frame = message(CHANNEL, &payload);
    let split = frame.len() - 7;
    server.write(id, frame[..split].to_vec()).await;
    assert!(timeout(Duration::from_millis(25), subscription.next_message()).await.is_err());
    server.write(id, frame[split..].to_vec()).await;
    let input = timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().unwrap();
    assert_eq!(input.payload.as_ref(), payload.as_slice());
    assert_eq!(subscribe_count(&server), 1);
    assert_eq!(names(&server, id), [b"SUBSCRIBE".to_vec()]);
    subscription.close().await.unwrap();
    server.wait_closed(id).await;
}
}

runtime_test! {
async fn a_slow_consumer_receives_an_ordered_prefix_and_an_explicit_overload() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    let mut burst = Vec::new();
    for index in 0..300_u64 {
        burst.extend(message(CHANNEL, &index.to_be_bytes()));
    }
    server.write(id, burst).await;
    // Do not drain until the bounded queue has failed and closed its socket.
    server.wait_closed(id).await;
    timeout(WAIT, async {
        let mut count = 0_u64;
        loop {
            match subscription.next_message().await {
                Ok(Some(input)) => {
                    assert_eq!(input.payload.as_ref(), count.to_be_bytes().as_slice());
                    count += 1;
                }
                Err(error) => {
                    assert_eq!(error.kind(), ErrorKind::Overloaded);
                    assert_eq!(count, 256, "the documented bounded prefix remains available");
                    break;
                }
                Ok(None) => panic!("overflow must not silently truncate the notification stream"),
            }
        }
        assert!(subscription.next_message().await.unwrap().is_none());
    }).await.unwrap();
    assert_eq!(subscribe_count(&server), 1);
}
}

runtime_test! {
async fn unexpected_eof_keeps_a_valid_prefix_then_reports_failure_without_reconnect() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    server.write(id, message(CHANNEL, b"last complete message")).await;
    server.disconnect(id).await;
    let input = timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().unwrap();
    assert_eq!(input.payload.as_ref(), b"last complete message");
    assert!(timeout(WAIT, subscription.next_message()).await.unwrap().is_err());
    assert!(timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().is_none());
    assert_eq!(subscribe_count(&server), 1);
}
}

runtime_test! {
async fn malformed_or_foreign_notifications_end_the_subscription_explicitly() {
    for invalid in [b"+not-a-notification\r\n".to_vec(), message(b"another-channel", b"foreign")] {
        let server = Server::start(Mode::Automatic).await;
        let redis = service(&server).await;
        let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
        let id = server.subscription().await;
        server.write(id, invalid).await;
        assert!(timeout(WAIT, subscription.next_message()).await.unwrap().is_err());
        server.wait_closed(id).await;
        assert!(timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().is_none());
        assert_eq!(subscribe_count(&server), 1);
    }
}
}

runtime_test! {
async fn unsubscribe_waits_for_its_channel_confirmation_and_close_sends_no_extra_command() {
    let server = Server::start(Mode::ManualUnsubscribeAck).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    let mut ending = tokio::spawn(async move {
        let result = subscription.unsubscribe().await;
        (subscription, result)
    });
    server.wait_for(|sessions| sessions[id].commands.iter()
        .any(|args| args[0] == b"UNSUBSCRIBE").then_some(())).await;
    let ack = acknowledgement(b"unsubscribe", CHANNEL, 0);
    let split = ack.len() - 4;
    server.write(id, ack[..split].to_vec()).await;
    assert!(timeout(Duration::from_millis(25), &mut ending).await.is_err(),
        "unsubscribe must wait for its complete zero-count acknowledgement");
    server.write(id, ack[split..].to_vec()).await;
    let (mut subscription, result) = timeout(WAIT, ending).await.unwrap().unwrap();
    result.unwrap();
    server.wait_closed(id).await;
    assert!(timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().is_none());
    subscription.close().await.unwrap();
    let sessions = server.sessions();
    assert_eq!(sessions[id].commands, vec![
        vec![b"SUBSCRIBE".to_vec(), CHANNEL.to_vec()],
        vec![b"UNSUBSCRIBE".to_vec(), CHANNEL.to_vec()],
    ]);
}
}

runtime_test! {
async fn subscribe_rejects_a_confirmation_for_another_channel_or_subscription_count() {
    for invalid in [
        acknowledgement(b"subscribe", b"foreign-channel", 1),
        acknowledgement(b"subscribe", CHANNEL, 0),
    ] {
        let server = Server::start(Mode::ManualSubscribeAck).await;
        let redis = service(&server).await;
        let connecting = tokio::spawn(async move { redis.subscribe(CHANNEL).await });
        let id = server.subscription().await;
        server.write(id, invalid).await;
        assert!(timeout(WAIT, connecting).await.unwrap().unwrap().is_err());
        server.wait_closed(id).await;
        assert_eq!(subscribe_count(&server), 1);
    }
}
}

runtime_test! {
async fn dropping_a_subscription_and_cancelling_before_confirmation_close_the_socket() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let subscription = redis.subscribe(CHANNEL).await.unwrap();
    let id = server.subscription().await;
    drop(subscription);
    server.wait_closed(id).await;
    assert_eq!(names(&server, id), [b"SUBSCRIBE".to_vec()]);

    let server = Server::start(Mode::ManualSubscribeAck).await;
    let redis = service(&server).await;
    let connecting = tokio::spawn(async move { redis.subscribe(CHANNEL).await });
    let id = server.subscription().await;
    connecting.abort();
    match connecting.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("the pending subscription must be cancelled"),
    }
    server.wait_closed(id).await;
    assert_eq!(names(&server, id), [b"SUBSCRIBE".to_vec()]);
}
}

runtime_test! {
async fn cancelling_during_subscription_authentication_releases_the_new_session() {
    let server = Server::start(Mode::Automatic).await;
    let redis = RedisService::single_with_options(
        server.endpoint.clone(),
        RedisServiceOptions::default().with_password(Some("test password".into())),
    ).await.unwrap();
    let existing = server.sessions().len();
    server.hold_new_auth();
    let connecting = tokio::spawn(async move { redis.subscribe(CHANNEL).await });
    let id = server.wait_for(|sessions| sessions.iter().enumerate().skip(existing)
        .find(|(_, session)| session.commands.iter().any(|args| args[0] == b"AUTH"))
        .map(|(id, _)| id)).await;
    connecting.abort();
    match connecting.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("the subscription authentication must be cancelled"),
    }
    server.wait_closed(id).await;
    assert_eq!(names(&server, id), [b"AUTH".to_vec()]);
}
}

runtime_test! {
async fn ordinary_get_and_publish_keep_their_own_connection_while_a_subscription_is_live() {
    let server = Server::start(Mode::Automatic).await;
    let redis = service(&server).await;
    let mut subscription = redis.subscribe(CHANNEL).await.unwrap();
    let subscription_id = server.subscription().await;
    assert_eq!(redis.get::<_, String>("key").await.unwrap().as_deref(), Some("value"));
    assert_eq!(redis.publish(CHANNEL, b"published").await.unwrap(), 1);
    server.write(subscription_id, message(CHANNEL, b"business input")).await;
    let input = timeout(WAIT, subscription.next_message()).await.unwrap().unwrap().unwrap();
    assert_eq!(input.payload.as_ref(), b"business input");
    let sessions = server.sessions();
    assert_eq!(names(&server, subscription_id), [b"SUBSCRIBE".to_vec()]);
    for command in [b"GET".as_slice(), b"PUBLISH".as_slice()] {
        assert!(sessions.iter().enumerate().any(|(id, session)| id != subscription_id
            && session.commands.iter().any(|args| args[0] == command)));
    }
    subscription.close().await.unwrap();
    server.wait_closed(subscription_id).await;
    assert_eq!(redis.get::<_, String>("still-usable").await.unwrap().as_deref(), Some("value"));
}
}
