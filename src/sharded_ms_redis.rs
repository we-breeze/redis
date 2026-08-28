//! Application-facing sharded Redis access with per-shard master/slave routing.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::direct::{ServerConfig, Shards};
use crate::ms_redis::validate_topology;
use crate::{ErrorKind, MsRedis, Redis, RedisBytes, RedisError, RedisResult};

const DISTRIBUTION: &str = "modula";

/// A [`Redis`] implementation that shards by key, then applies master/slave
/// routing within the selected shard.
///
/// The configured algorithm is interpreted as a Breeze hash algorithm. Shard
/// indexes use the existing `modula` distribution, in the same order as the
/// supplied topology groups. Each group is owned by one [`MsRedis`], so reads
/// are distributed over that group's healthy slaves with fallback to its
/// master.
#[derive(Clone)]
pub struct ShardedMsRedis {
    shards: Arc<Shards<MsRedis>>,
}

struct TopologyGroup {
    master: String,
    master_label: String,
    slaves: Vec<String>,
}

impl ShardedMsRedis {
    /// Connect to a non-empty list of `(master, slaves)` shard groups.
    ///
    /// `sharding_algorithm` accepts the hash names supported by Breeze
    /// sharding, such as `crc32`. Every group must contain one master endpoint
    /// and at least one slave endpoint, all using `host:port[:db]` syntax.
    pub async fn new<M, S>(
        sharding_algorithm: impl AsRef<str>,
        groups: Vec<(M, Vec<S>)>,
    ) -> RedisResult<Self>
    where
        M: AsRef<str>,
        S: AsRef<str>,
    {
        let sharding_algorithm = sharding_algorithm.as_ref();
        if sharding_algorithm.trim().is_empty() {
            return Err(RedisError::new(
                ErrorKind::ClientError,
                "sharding algorithm must not be empty",
            ));
        }

        let groups = validate_groups(groups)?;
        let mut names = Vec::with_capacity(groups.len());
        let mut shards = Vec::with_capacity(groups.len());
        for group in groups {
            names.push(group.master_label);
            shards.push(MsRedis::new(group.master, group.slaves).await?);
        }

        Ok(Self {
            shards: Arc::new(Shards::new(sharding_algorithm, DISTRIBUTION, names, shards)),
        })
    }

    fn shard_for_key(&self, key: &str) -> &MsRedis {
        self.shards.for_key(key.as_bytes())
    }

    fn shard_for_route(&self, routing_key: &[u8]) -> &MsRedis {
        self.shards.for_key(routing_key)
    }
}

fn validate_groups<M, S>(groups: Vec<(M, Vec<S>)>) -> RedisResult<Vec<TopologyGroup>>
where
    M: AsRef<str>,
    S: AsRef<str>,
{
    if groups.is_empty() {
        return Err(RedisError::new(
            ErrorKind::ClientError,
            "at least one master/slave shard group is required",
        ));
    }

    let mut master_labels = HashSet::with_capacity(groups.len());
    groups
        .into_iter()
        .map(|(master, slaves)| {
            let master = master.as_ref().to_owned();
            let master_label = ServerConfig::new(&master)?.label();
            if !master_labels.insert(master_label.clone()) {
                return Err(RedisError::new(
                    ErrorKind::ClientError,
                    format!("duplicate shard master backend {master_label}"),
                ));
            }

            let slaves = slaves
                .into_iter()
                .map(|slave| slave.as_ref().to_owned())
                .collect::<Vec<_>>();
            let slave_labels = slaves
                .iter()
                .map(|slave| ServerConfig::new(slave).map(|config| config.label()))
                .collect::<RedisResult<Vec<_>>>()?;
            validate_topology(&slave_labels)?;

            Ok(TopologyGroup {
                master,
                master_label,
                slaves,
            })
        })
        .collect()
}

#[async_trait]
impl Redis for ShardedMsRedis {
    async fn get(&self, key: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::get(self.shard_for_key(key), key).await
    }

    async fn set(&self, key: &str, value: &[u8]) -> RedisResult<()> {
        Redis::set(self.shard_for_key(key), key, value).await
    }

    async fn get_routed(&self, routing_key: &[u8], key: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::get(self.shard_for_route(routing_key), key).await
    }

    async fn set_routed(&self, routing_key: &[u8], key: &str, value: &[u8]) -> RedisResult<()> {
        Redis::set(self.shard_for_route(routing_key), key, value).await
    }

    async fn hget(&self, key: &str, field: &str) -> RedisResult<Option<RedisBytes>> {
        Redis::hget(self.shard_for_key(key), key, field).await
    }

    async fn hmget(&self, key: &str, fields: &[&str]) -> RedisResult<Vec<Option<RedisBytes>>> {
        Redis::hmget(self.shard_for_key(key), key, fields).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::{JoinHandle, JoinSet};

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

        fn saw_read(&self) -> bool {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .any(|command| command.contains("HGET") || command.contains("\r\nGET\r\n"))
        }
    }

    impl Drop for FakeRedis {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[test]
    fn implements_application_contract() {
        fn assert_redis<T: Redis>() {}
        assert_redis::<ShardedMsRedis>();
    }

    #[tokio::test]
    async fn rejects_invalid_topologies_before_connecting() {
        let error = ShardedMsRedis::new("crc32", Vec::<(&str, Vec<&str>)>::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::ClientError);

        let error = ShardedMsRedis::new(
            "crc32",
            vec![
                ("127.0.0.1:6379", vec!["127.0.0.1:6380"]),
                ("127.0.0.1:6379", vec!["127.0.0.1:6381"]),
            ],
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind(), ErrorKind::ClientError);

        let error = ShardedMsRedis::new("", vec![("127.0.0.1:6379", vec!["127.0.0.1:6380"])])
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::ClientError);
    }

    #[tokio::test]
    async fn routes_keys_before_each_groups_slave_selection() {
        let master_a = FakeRedis::start("master-a").await;
        let slave_a = FakeRedis::start("shard-a").await;
        let master_b = FakeRedis::start("master-b").await;
        let slave_b = FakeRedis::start("shard-b").await;

        let redis = ShardedMsRedis::new(
            "crc32",
            vec![
                (&master_a.endpoint, vec![&slave_a.endpoint]),
                (&master_b.endpoint, vec![&slave_b.endpoint]),
            ],
        )
        .await
        .unwrap();

        let mut saw_a = false;
        let mut saw_b = false;
        for id in 0..256 {
            let key = format!("user:{id}");
            match Redis::get(&redis, &key).await.unwrap().as_deref() {
                Some(b"shard-a") => saw_a = true,
                Some(b"shard-b") => saw_b = true,
                value => panic!("unexpected routed value: {value:?}"),
            }
            if saw_a && saw_b {
                break;
            }
        }

        assert!(saw_a && saw_b, "expected keys to reach both shard groups");
        assert!(slave_a.saw_read());
        assert!(slave_b.saw_read());
        assert!(!master_a.saw_read());
        assert!(!master_b.saw_read());
    }
}
