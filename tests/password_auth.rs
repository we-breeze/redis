#[path = "support/runtime.rs"]
mod runtime;
use runtime::runtime_test;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use brz_redis::resp::parser::{ParseResult, parse_reply};
use brz_redis::{Redis, RedisService, RedisServiceOptions, Value};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

const PASSWORD: &str = "test password:@%/";
type Sessions = Arc<Mutex<Vec<Vec<String>>>>;

struct AuthRedis {
    endpoint: String,
    sessions: Sessions,
    disconnect: Arc<Notify>,
    task: JoinHandle<()>,
}

impl AuthRedis {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("{}:3", listener.local_addr().unwrap());
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let observed = sessions.clone();
        let disconnect = Arc::new(Notify::new());
        let close_connections = disconnect.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let id = { let mut sessions = observed.lock().unwrap(); sessions.push(Vec::new()); sessions.len()-1 };
                        tasks.spawn(serve(socket, observed.clone(), id));
                    }
                    () = close_connections.notified() => tasks.abort_all(),
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                        if let Err(error) = result { assert!(error.is_cancelled(), "{error}"); }
                    }
                }
            }
        });
        Self {
            endpoint,
            sessions,
            disconnect,
            task,
        }
    }
}
impl Drop for AuthRedis {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut socket: TcpStream, observed: Sessions, id: usize) {
    let mut input = BytesMut::new();
    let mut authenticated = false;
    loop {
        match socket.read_buf(&mut input).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        loop {
            let ParseResult::Complete { value, consumed } =
                parse_reply(&input.clone().freeze()).unwrap()
            else {
                break;
            };
            input.advance(consumed);
            let Value::Array(args) = value else {
                panic!("command must be an array");
            };
            let args: Vec<_> = args
                .into_iter()
                .map(|v| match v {
                    Value::BulkString(b) => b,
                    _ => panic!("expected bulk string"),
                })
                .collect();
            let command = std::str::from_utf8(&args[0]).unwrap();
            observed.lock().unwrap()[id].push(command.into());
            let reply = if command == "AUTH" {
                authenticated = args.len() == 2 && args[1] == PASSWORD;
                if authenticated {
                    &b"+OK\r\n"[..]
                } else {
                    &b"-WRONGPASS invalid username-password pair\r\n"[..]
                }
            } else if !authenticated {
                &b"-NOAUTH Authentication required\r\n"[..]
            } else {
                match command {
                    "SELECT" => {
                        assert_eq!(&args[1][..], b"3");
                        &b"+OK\r\n"[..]
                    }
                    "PING" => &b"+PONG\r\n"[..],
                    "SET" => &b"+OK\r\n"[..],
                    "GET" => &b"$5\r\nvalue\r\n"[..],
                    _ => panic!("unexpected test command: {command}"),
                }
            };
            if socket.write_all(reply).await.is_err() {
                return;
            }
        }
    }
}

fn options(password: Option<&str>) -> RedisServiceOptions {
    RedisServiceOptions::default()
        .with_password(password.map(str::to_owned))
        .with_connect_timeout(Duration::from_millis(200))
        .with_timeout(Duration::from_secs(1))
}

runtime_test! {
async fn password_authenticates_master_and_replica_before_commands() {
    let master = AuthRedis::start().await;
    let replica = AuthRedis::start().await;
    let redis = RedisService::noshard_with_options(
        master.endpoint.clone(),
        [replica.endpoint.clone()],
        options(Some(PASSWORD)),
    )
    .await
    .unwrap();
    redis.set("key", "value").await.unwrap();
    assert_eq!(
        redis.get::<_, String>("key").await.unwrap(),
        Some("value".into())
    );
    for server in [&master, &replica] {
        let sessions = server.sessions.lock().unwrap();
        assert!(!sessions.is_empty());
        assert!(
            sessions
                .iter()
                .all(|commands| commands.starts_with(&["AUTH".into(), "SELECT".into()]))
        );
    }
}
}

runtime_test! {
async fn reconnect_authenticates_again_before_serving_requests() {
    let server = AuthRedis::start().await;
    let redis = RedisService::single_with_options(server.endpoint.clone(), options(Some(PASSWORD)))
        .await
        .unwrap();
    redis.ping().await.unwrap();
    let before = server.sessions.lock().unwrap().len();
    server.disconnect.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server.sessions.lock().unwrap().len() > before && redis.ping().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let sessions = server.sessions.lock().unwrap();
    assert!(
        sessions
            .iter()
            .skip(before)
            .any(|commands| commands.contains(&"PING".into()))
    );
    assert!(
        sessions
            .iter()
            .filter(|s| !s.is_empty())
            .all(|commands| commands[0] == "AUTH")
    );
}
}

runtime_test! {
async fn rejected_or_missing_password_never_exposes_an_authenticated_service() {
    let server = AuthRedis::start().await;
    for password in [None, Some("incorrect")] {
        let result =
            RedisService::single_with_options(server.endpoint.clone(), options(password)).await;
        assert!(result.is_err());
    }
    let sessions = server.sessions.lock().unwrap();
    assert!(
        sessions
            .iter()
            .flatten()
            .all(|command| command == "AUTH" || command == "SELECT")
    );
}
}

#[test]
fn password_is_redacted_from_debug_output() {
    let debug = format!("{:?}", options(Some(PASSWORD)));
    assert!(!debug.contains(PASSWORD));
    assert!(debug.contains("[REDACTED]"));
}
