//! A loopback RESP peer with explicit wire and disconnect fences.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brz_redis::Value;
use brz_redis::resp::parser::{ParseResult, parse_reply};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Copy)]
pub enum Mode {
    Automatic,
    ManualSubscribeAck,
    ManualUnsubscribeAck,
}

#[derive(Clone, Default)]
pub struct Session {
    pub commands: Vec<Vec<Vec<u8>>>,
    pub closed: bool,
}

enum Instruction {
    Write(Vec<u8>, oneshot::Sender<Result<(), String>>),
    Disconnect(oneshot::Sender<()>),
}

#[derive(Default)]
struct State {
    sessions: Vec<Session>,
    controls: Vec<mpsc::Sender<Instruction>>,
    hold_auth_from: Option<usize>,
}

struct Trace {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl Trace {
    fn update(&self, change: impl FnOnce(&mut State)) {
        change(&mut self.state.lock().unwrap());
        self.changed.send_modify(|revision| *revision += 1);
    }
}

struct Closed {
    trace: Arc<Trace>,
    id: usize,
}

impl Drop for Closed {
    fn drop(&mut self) {
        self.trace
            .update(|state| state.sessions[self.id].closed = true);
    }
}

pub struct Server {
    pub endpoint: String,
    trace: Arc<Trace>,
    task: JoinHandle<()>,
}

impl Server {
    pub async fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let (changed, _) = watch::channel(0);
        let trace = Arc::new(Trace {
            state: Mutex::new(State::default()),
            changed,
        });
        let observed = trace.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (socket, _) = connection.unwrap();
                        let (tx, rx) = mpsc::channel(8);
                        let id = {
                            let mut state = observed.state.lock().unwrap();
                            let id = state.sessions.len();
                            state.sessions.push(Session::default());
                            state.controls.push(tx);
                            id
                        };
                        observed.changed.send_modify(|revision| *revision += 1);
                        sessions.spawn(serve(socket, observed.clone(), id, mode, rx));
                    }
                    Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                        result.expect("scripted Redis session must not panic");
                    }
                }
            }
        });
        Self {
            endpoint,
            trace,
            task,
        }
    }

    pub fn sessions(&self) -> Vec<Session> {
        self.trace.state.lock().unwrap().sessions.clone()
    }

    pub fn hold_new_auth(&self) {
        self.trace
            .update(|state| state.hold_auth_from = Some(state.sessions.len()));
    }

    pub async fn wait_for<T>(&self, predicate: impl Fn(&[Session]) -> Option<T>) -> T {
        let mut changed = self.trace.changed.subscribe();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(found) = predicate(&self.trace.state.lock().unwrap().sessions) {
                    return found;
                }
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("scripted Redis wire condition must become observable")
    }

    pub async fn subscription(&self) -> usize {
        self.wait_for(|sessions| {
            sessions
                .iter()
                .position(|session| session.commands.iter().any(|args| args[0] == b"SUBSCRIBE"))
        })
        .await
    }

    pub async fn wait_closed(&self, id: usize) {
        self.wait_for(|sessions| sessions[id].closed.then_some(()))
            .await;
    }

    pub async fn write(&self, id: usize, bytes: impl Into<Vec<u8>>) {
        let control = self.trace.state.lock().unwrap().controls[id].clone();
        let (tx, rx) = oneshot::channel();
        control
            .send(Instruction::Write(bytes.into(), tx))
            .await
            .unwrap();
        rx.await
            .unwrap()
            .expect("scripted Redis write must succeed");
    }

    pub async fn disconnect(&self, id: usize) {
        let control = self.trace.state.lock().unwrap().controls[id].clone();
        let (tx, rx) = oneshot::channel();
        control.send(Instruction::Disconnect(tx)).await.unwrap();
        rx.await.unwrap();
        self.wait_closed(id).await;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(
    mut socket: TcpStream,
    trace: Arc<Trace>,
    id: usize,
    mode: Mode,
    mut controls: mpsc::Receiver<Instruction>,
) {
    let _closed = Closed {
        trace: trace.clone(),
        id,
    };
    let mut input = BytesMut::new();
    loop {
        tokio::select! {
            instruction = controls.recv() => match instruction {
                Some(Instruction::Write(bytes, completed)) => {
                    let result = socket.write_all(&bytes).await.map_err(|error| error.to_string());
                    let failed = result.is_err();
                    let _ = completed.send(result);
                    if failed { return; }
                }
                Some(Instruction::Disconnect(completed)) => {
                    let _ = completed.send(());
                    return;
                }
                None => return,
            },
            read = socket.read_buf(&mut input) => {
                match read {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                loop {
                    let ParseResult::Complete { value, consumed } =
                        parse_reply(&input.clone().freeze()).unwrap()
                    else { break; };
                    input.advance(consumed);
                    let Value::Array(values) = value else { panic!("command must be an array"); };
                    let args: Vec<Vec<u8>> = values.into_iter().map(|value| match value {
                        Value::BulkString(bytes) => bytes.to_vec(),
                        _ => panic!("command arguments must be bulk strings"),
                    }).collect();
                    assert!(!args.is_empty());
                    trace.update(|state| state.sessions[id].commands.push(args.clone()));
                    let hold_auth = trace.state.lock().unwrap().hold_auth_from
                        .is_some_and(|first| id >= first);
                    let response = match args[0].as_slice() {
                        b"AUTH" if hold_auth => continue,
                        b"AUTH" | b"SELECT" => b"+OK\r\n".to_vec(),
                        b"SUBSCRIBE" if matches!(mode, Mode::ManualSubscribeAck) => continue,
                        b"SUBSCRIBE" => acknowledgement(b"subscribe", &args[1], 1),
                        b"UNSUBSCRIBE" if matches!(mode, Mode::ManualUnsubscribeAck) => continue,
                        b"UNSUBSCRIBE" => acknowledgement(b"unsubscribe", &args[1], 0),
                        b"GET" => b"$5\r\nvalue\r\n".to_vec(),
                        b"PUBLISH" => b":1\r\n".to_vec(),
                        other => panic!("unexpected Redis command: {}", String::from_utf8_lossy(other)),
                    };
                    if socket.write_all(&response).await.is_err() { return; }
                }
            }
        }
    }
}

pub fn acknowledgement(kind: &[u8], channel: &[u8], count: usize) -> Vec<u8> {
    let mut result = b"*3\r\n".to_vec();
    bulk(&mut result, kind);
    bulk(&mut result, channel);
    result.extend_from_slice(format!(":{count}\r\n").as_bytes());
    result
}

pub fn message(channel: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut result = b"*3\r\n".to_vec();
    for value in [b"message".as_slice(), channel, payload] {
        bulk(&mut result, value);
    }
    result
}

fn bulk(destination: &mut Vec<u8>, value: &[u8]) {
    destination.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
    destination.extend_from_slice(value);
    destination.extend_from_slice(b"\r\n");
}
