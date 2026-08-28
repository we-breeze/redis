//! Application-facing Redis access with master/slave read splitting.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::connection::{ConnectionLike, RedisFuture};
use crate::direct::{DirectClient, MsServer, ServerConfig};
use crate::{Cmd, ErrorKind, Pipeline, Redis, RedisBytes, RedisError, RedisResult, Value};

/// A [`Redis`] implementation backed by one master and one or more slaves.
///
/// Write commands always go to the master. Read-only commands are distributed
/// round-robin across healthy slaves and fall back to the master when the
/// selected slave fails retriably or every slave is unavailable. The facade
/// also implements [`ConnectionLike`], so the low-level [`crate::Commands`]
/// surface follows the same routing policy.
#[derive(Clone)]
pub struct MsRedis {
    inner: Arc<MsRedisInner>,
}

struct MsRedisInner {
    servers: Box<[MsServer]>,
    next_slave: AtomicUsize,
}

impl MsRedis {
    /// Connect to one master and a non-empty list of slaves.
    ///
    /// Every address uses the direct-mode `host:port[:db]` syntax. Duplicate
    /// slaves and a slave equal to the master are rejected before connecting.
    pub async fn new<M, I, S>(master_endpoint: M, slave_endpoints: I) -> RedisResult<Self>
    where
        M: AsRef<str>,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let master_config = ServerConfig::new(master_endpoint.as_ref())?;
        let slave_configs = slave_endpoints
            .into_iter()
            .map(|endpoint| ServerConfig::new(endpoint.as_ref()))
            .collect::<RedisResult<Vec<_>>>()?;
        validate_topology(
            &slave_configs
                .iter()
                .map(ServerConfig::label)
                .collect::<Vec<_>>(),
        )?;

        Self::from_server_configs(master_config, slave_configs).await
    }

    pub(crate) async fn from_server_configs(
        master_config: ServerConfig,
        slave_configs: Vec<ServerConfig>,
    ) -> RedisResult<Self> {
        validate_topology(
            &slave_configs
                .iter()
                .map(ServerConfig::label)
                .collect::<Vec<_>>(),
        )?;

        let master_label = master_config.label();
        let master = DirectClient::connect(master_config)
            .await
            .map_err(|error| {
                tracing::error!(
                    target: "redis::ms",
                    backend = %master_label,
                    error = %error,
                    "failed to connect master backend"
                );
                error
            })?;

        let mut slaves = Vec::with_capacity(slave_configs.len());
        for config in slave_configs {
            let label = config.label();
            let slave = DirectClient::connect(config.with_read_only(true))
                .await
                .map_err(|error| {
                    tracing::error!(
                        target: "redis::ms",
                        backend = %label,
                        error = %error,
                        "failed to connect slave backend"
                    );
                    error
                })?;
            slaves.push(slave);
        }

        let servers = slaves
            .into_iter()
            .map(|slave| MsServer::new(master.clone(), Some(slave)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            inner: Arc::new(MsRedisInner {
                servers,
                next_slave: AtomicUsize::new(0),
            }),
        })
    }

    /// The writable master backend.
    fn master(&self) -> &DirectClient {
        self.inner.servers[0].master()
    }

    /// All read-only slave backends in configuration order.
    #[cfg(test)]
    fn slaves(&self) -> impl ExactSizeIterator<Item = &DirectClient> {
        self.inner
            .servers
            .iter()
            .map(|server| server.slave().expect("MsRedis always has a slave"))
    }

    fn next_available_server(&self) -> Option<&MsServer> {
        let len = self.inner.servers.len();
        let start = self.inner.next_slave.fetch_add(1, Ordering::Relaxed);
        (0..len)
            .map(|offset| &self.inner.servers[(start.wrapping_add(offset)) % len])
            .find(|server| server.slave().is_some_and(DirectClient::is_available))
    }
}

pub(crate) fn validate_topology(slave_labels: &[String]) -> RedisResult<()> {
    if slave_labels.is_empty() {
        return Err(RedisError::new(
            ErrorKind::ClientError,
            "at least one slave backend is required",
        ));
    }

    let mut unique = HashSet::with_capacity(slave_labels.len());
    for label in slave_labels {
        if !unique.insert(label.as_str()) {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                format!("duplicate slave backend {label}"),
            ));
        }
    }
    Ok(())
}

impl ConnectionLike for MsRedis {
    fn req_command<'a>(&'a self, command: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move {
            if command.is_readonly()
                && let Some(server) = self.next_available_server()
            {
                return server.req_command(command).await;
            }
            self.master().req_command(command).await
        })
    }

    fn req_pipeline<'a>(
        &'a self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            if pipeline.is_readonly()
                && let Some(server) = self.next_available_server()
            {
                let slave = server.slave().expect("MsRedis always has a slave");
                return match slave.req_pipeline(pipeline, offset, count).await {
                    Err(error) if error.is_retriable() => {
                        self.master().req_pipeline(pipeline, offset, count).await
                    }
                    result => result,
                };
            }
            self.master().req_pipeline(pipeline, offset, count).await
        })
    }
}

#[async_trait]
impl Redis for MsRedis {
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::get(self, key).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> RedisResult<()> {
        crate::api::set(self, key, value).await
    }

    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>> {
        crate::api::hget(self, key, field).await
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>> {
        crate::api::hmget(self, key, fields).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::{JoinHandle, JoinSet};

    use crate::Commands;

    use super::*;

    struct FakeRedis {
        endpoint: String,
        seen: Arc<Mutex<Vec<String>>>,
        task: JoinHandle<()>,
    }

    impl FakeRedis {
        async fn start(read_value: &'static str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let server_seen = seen.clone();
            let task = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let Ok((mut socket, _)) = accepted else {
                                break;
                            };
                            let seen = server_seen.clone();
                            connections.spawn(async move {
                                let mut buffer = [0_u8; 4096];
                                loop {
                                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                                    if read == 0 {
                                        break;
                                    }
                                    let command = String::from_utf8_lossy(&buffer[..read])
                                        .to_uppercase();
                                    seen.lock().unwrap().push(command.clone());
                                    let response = if command.contains("HGET")
                                        || command.contains("\r\nGET\r\n")
                                    {
                                        format!("${}\r\n{read_value}\r\n", read_value.len())
                                    } else if command.contains("PING") {
                                        "+PONG\r\n".to_string()
                                    } else {
                                        ":1\r\n".to_string()
                                    };
                                    if socket.write_all(response.as_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                        Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    }
                }
            });
            Self {
                endpoint,
                seen,
                task,
            }
        }

        fn saw(&self, command: &str) -> bool {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .any(|seen| seen.contains(command))
        }
    }

    impl Drop for FakeRedis {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[test]
    fn implements_application_and_connection_contracts() {
        fn assert_redis<T: Redis>() {}
        fn assert_connection<T: ConnectionLike>() {}
        assert_redis::<MsRedis>();
        assert_connection::<MsRedis>();
    }

    #[tokio::test]
    async fn rejects_invalid_topologies_before_connecting() {
        let error = MsRedis::new("127.0.0.1:6379", Vec::<&str>::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::ClientError);

        let error = MsRedis::new("missing-port", ["127.0.0.1:6380"])
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::ClientError);

        let error = validate_topology(&["slave:6379:0".to_string(), "slave:6379:0".to_string()])
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ClientError);

        validate_topology(&["master:6379:0".to_string()]).unwrap();
    }

    #[tokio::test]
    async fn accepts_the_same_endpoint_for_master_and_slave() {
        let server = FakeRedis::start("same-backend").await;
        let redis = MsRedis::new(&server.endpoint, [&server.endpoint])
            .await
            .unwrap();

        let value = Redis::get(&redis, "key").await.unwrap();
        assert_eq!(value.as_deref(), Some(b"same-backend".as_slice()));
        Redis::set(&redis, "key", b"value").await.unwrap();
        assert!(server.saw("GET"));
        assert!(server.saw("SET"));
    }

    #[tokio::test]
    async fn reads_round_robin_across_slaves_and_writes_only_master() {
        let master = FakeRedis::start("master").await;
        let slave_a = FakeRedis::start("slave-a").await;
        let slave_b = FakeRedis::start("slave-b").await;
        let redis = MsRedis::new(&master.endpoint, [&slave_a.endpoint, &slave_b.endpoint])
            .await
            .unwrap();

        let first = Redis::hget(&redis, "key", "field").await.unwrap();
        let second = Redis::hget(&redis, "key", "field").await.unwrap();
        assert_eq!(first.as_deref(), Some(b"slave-a".as_slice()));
        assert_eq!(second.as_deref(), Some(b"slave-b".as_slice()));

        let written: i64 = redis.hset("key", "field", "value").await.unwrap();
        assert_eq!(written, 1);
        assert!(master.saw("HSET"));
        assert!(!slave_a.saw("HSET"));
        assert!(!slave_b.saw("HSET"));
    }

    #[tokio::test]
    async fn read_falls_back_to_master_when_all_slaves_are_unavailable() {
        let master = FakeRedis::start("master").await;
        let slave = FakeRedis::start("slave").await;
        let redis = MsRedis::new(&master.endpoint, [&slave.endpoint])
            .await
            .unwrap();
        redis.slaves().next().unwrap().pooled_client().pause();

        let value = Redis::hget(&redis, "key", "field").await.unwrap();

        assert_eq!(value.as_deref(), Some(b"master".as_slice()));
        assert!(master.saw("HGET"));
        assert!(!slave.saw("HGET"));
    }
}
