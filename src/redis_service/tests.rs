use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep};

use crate::resp::parser::{ParseResult, parse_reply};
use crate::sharding::Sharding;
use crate::sharding::hash::{Hash, Hasher};
use crate::{ErrorKind, MeshConfig, Redis, RedisBytes, RedisPipe, RedisResult, RedisValues, Value};

use super::{RedisService, RedisServiceOptions, ShardRouting};

struct FakeRedis {
    endpoint: String,
    accepted: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl FakeRedis {
    async fn start(read_value: &'static str) -> Self {
        Self::start_with_delay(read_value, Duration::ZERO).await
    }

    async fn start_with_delay(read_value: &'static str, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = Arc::clone(&accepted);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let server_seen = Arc::clone(&seen);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else {
                            break;
                        };
                        server_accepted.fetch_add(1, Ordering::Relaxed);
                        let seen = Arc::clone(&server_seen);
                        connections.spawn(async move {
                            let mut read_buffer = BytesMut::with_capacity(4096);
                            loop {
                                let read = socket.read_buf(&mut read_buffer).await.unwrap_or(0);
                                if read == 0 {
                                    break;
                                }
                                let snapshot = read_buffer.split().freeze();
                                let mut position = 0;
                                let mut responses = BytesMut::new();
                                loop {
                                    match parse_reply(&snapshot.slice(position..)).unwrap() {
                                        ParseResult::Complete { value, consumed } => {
                                            position += consumed;
                                            let command = command_name(&value);
                                            let argument_count = command_argument_count(&value);
                                            seen.lock().unwrap().push(command.clone());
                                            if !delay.is_zero() {
                                                tokio::time::sleep(delay).await;
                                            }
                                            match command.as_str() {
                                                "GET" | "HGET" => responses.extend_from_slice(
                                                    format!("${}\r\n{read_value}\r\n", read_value.len()).as_bytes()
                                                ),
                                                "MGET" | "HMGET" => {
                                                    let values = argument_count.saturating_sub(
                                                        if command == "MGET" { 1 } else { 2 }
                                                    );
                                                    responses.extend_from_slice(
                                                        format!("*{values}\r\n").as_bytes()
                                                    );
                                                    for _ in 0..values {
                                                        responses.extend_from_slice(
                                                            format!("${}\r\n{read_value}\r\n", read_value.len()).as_bytes()
                                                        );
                                                    }
                                                }
                                                "SET" | "AUTH" | "SELECT" => responses.extend_from_slice(b"+OK\r\n"),
                                                "PING" => responses.extend_from_slice(b"+PONG\r\n"),
                                                _ => responses.extend_from_slice(b":1\r\n"),
                                            }
                                        }
                                        ParseResult::Incomplete => {
                                            read_buffer.extend_from_slice(&snapshot[position..]);
                                            break;
                                        }
                                    }
                                }
                                if !responses.is_empty()
                                    && socket.write_all(&responses).await.is_err()
                                {
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
            accepted,
            seen,
            task,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    fn saw(&self, command: &str) -> bool {
        self.seen.lock().unwrap().iter().any(|seen| seen == command)
    }
}

impl Drop for FakeRedis {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RecoveringRedis {
    endpoint: String,
    accepted: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl RecoveringRedis {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = accepted.clone();
        let task = tokio::spawn(async move {
            let (mut stalled, _) = listener.accept().await.unwrap();
            server_accepted.fetch_add(1, Ordering::Relaxed);
            let mut buffer = [0_u8; 4096];
            let _ = stalled.read(&mut buffer).await;
            while stalled.read(&mut buffer).await.unwrap_or(0) != 0 {}

            let (mut recovered, _) = listener.accept().await.unwrap();
            server_accepted.fetch_add(1, Ordering::Relaxed);
            loop {
                let length = recovered.read(&mut buffer).await.unwrap_or(0);
                if length == 0 {
                    return;
                }
                if recovered.write_all(b"$9\r\nrecovered\r\n").await.is_err() {
                    return;
                }
            }
        });
        Self {
            endpoint,
            accepted,
            task,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }
}

impl Drop for RecoveringRedis {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn command_name(value: &Value) -> String {
    let Value::Array(arguments) = value else {
        return String::new();
    };
    let Some(Value::BulkString(command)) = arguments.first() else {
        return String::new();
    };
    String::from_utf8_lossy(command).to_ascii_uppercase()
}

fn command_argument_count(value: &Value) -> usize {
    match value {
        Value::Array(arguments) => arguments.len(),
        _ => 0,
    }
}

#[test]
fn redis_defaults_to_two_hundred_millisecond_request_timeout() {
    let options = RedisServiceOptions::default();

    assert_eq!(options.master_timeout, Duration::from_millis(200));
    assert_eq!(options.slave_timeout, Duration::from_millis(200));
}

#[test]
fn unified_timeout_updates_both_roles() {
    let options = RedisServiceOptions::default().with_timeout(Duration::from_secs(3));

    assert_eq!(options.master_timeout, Duration::from_secs(3));
    assert_eq!(options.slave_timeout, Duration::from_secs(3));
}

#[cfg(feature = "metrics")]
#[tokio::test]
async fn metrics_feature_records_the_configured_authority_without_db_suffix() {
    let server = FakeRedis::start("metric-value").await;
    let redis = RedisService::single(format!("{}:3", server.endpoint))
        .await
        .unwrap();

    assert_eq!(
        Redis::get::<_, RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .as_deref(),
        Some(b"metric-value".as_slice())
    );

    let mut snapshot = None;
    brz_metrics::visit(|name, metric_type, candidate| {
        if name == server.endpoint && metric_type == "REDIS" {
            snapshot = Some(candidate);
        }
    });
    let snapshot = snapshot.expect("the configured endpoint must be registered");
    assert_eq!(
        (snapshot.total, snapshot.success, snapshot.failure),
        (1, 1, 0)
    );
}

#[tokio::test]
async fn mesh_discovers_once_then_behaves_like_single() {
    let first = FakeRedis::start("first").await;
    let second = FakeRedis::start("second").await;
    let directory = tempfile::tempdir().unwrap();
    let first_port = first.endpoint.rsplit_once(':').unwrap().1;
    let second_port = second.endpoint.rsplit_once(':').unwrap().1;
    let first_record = directory.path().join(format!(
        "static.config.api.example.com+3+config+cloud+redis+feed+profiles@redis:{first_port}@rs"
    ));
    std::fs::write(&first_record, []).unwrap();

    let redis = RedisService::mesh_with_config(
        MeshConfig::new("feed", "profiles").with_socket_dir(directory.path()),
        RedisServiceOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        Redis::get::<_, RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .as_deref(),
        Some(b"first".as_slice())
    );

    std::fs::remove_file(first_record).unwrap();
    std::fs::write(
        directory.path().join(format!(
            "static.config.api.example.com+3+config+cloud+redis+feed+profiles@redis:{second_port}@rs"
        )),
        [],
    )
    .unwrap();

    assert_eq!(
        Redis::get::<_, RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(second.accepted(), 0);
}

#[test]
fn crc32_java_mapping_keeps_standard_and_short_distinct() {
    let key = b"123456789".as_slice();
    let standard = Hasher::from("crc32").hash(&key);
    let short = Hasher::from("crc32-short").hash(&key);

    // java.util.zip.CRC32("123456789") == 0xCBF43926.
    assert_eq!(standard, 0xcbf4_3926);
    assert_eq!(short, (standard >> 16) & 0x7fff);
    assert_eq!(short, 0x4bf4);
    assert_ne!(standard, short);

    // reference-library HashUtilTest.testGetHashCrc32 uses these exact expected
    // values for Java's `(crc32 / splitCount) % splitCount` mapping.
    let java_uid_crc = Hasher::from("crc32").hash(&b"1821155363".as_slice());
    assert_eq!((java_uid_crc / 32) % 32, 12);
    assert_eq!((java_uid_crc / 128) % 128, 51);
}

#[test]
fn explicit_uid_route_matches_java_range_sharding() {
    let names = (0..16)
        .map(|index| format!("shard-{index}"))
        .collect::<Vec<_>>();
    let sharding = Sharding::new("crc32", "range-256", &names);
    let mut state = 0x4d59_5df4_d0f3_3173_u64;

    for _ in 0..10_000 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let uid = 10_000 + state % 2_999_999_999_990_000;
        let uid = uid.to_string();
        let expected_crc = java_crc32(uid.as_bytes());
        let expected_slot = ((expected_crc / 256) % 256) / 16;

        assert_eq!(sharding.hash(uid.as_bytes()), expected_crc as i64);
        assert_eq!(sharding.shard_idx(uid.as_bytes()), expected_slot as usize);
    }
}

#[test]
fn java_sharding_support_hash_test_vectors_match_range() {
    // Copied from reference-library ShardingSupportHashTest.testGetDbTableUid:
    // hashAlg=crc32, hashGene=1024, tablePerDb=64, noneHash=new.
    let vectors = [
        (1_750_715_731_u64, 15_usize),
        (1_821_155_363, 13),
        (1_779_195_673, 11),
        (1_734_528_095, 0),
        (10_503, 14),
    ];
    let names = (0..16)
        .map(|index| format!("shard-{index}"))
        .collect::<Vec<_>>();
    let sharding = Sharding::new("crc32", "range-1024", &names);

    for (uid, expected_db) in vectors {
        assert_eq!(sharding.shard_idx(uid.to_string().as_bytes()), expected_db);
    }
}

#[test]
fn smartnum_is_only_an_equivalent_key_mapping_not_crc32_short() {
    let uid = b"4066469060".as_slice();
    let redis_key = b"u:4066469060".as_slice();
    let standard = Hasher::from("crc32").hash(&uid);

    assert_eq!(Hasher::from("crc32-smartnum").hash(&redis_key), standard);
    assert_ne!(Hasher::from("crc32-short").hash(&uid), standard);
}

#[tokio::test]
async fn single_uses_independent_read_and_write_connections() {
    let server = FakeRedis::start("single").await;
    let redis = RedisService::single(server.endpoint.clone()).await.unwrap();

    assert_eq!(server.accepted(), 2);
    let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some(b"single".as_slice()));
    crate::Redis::set(&redis, "key", "value").await.unwrap();
    assert!(server.saw("GET"));
    assert!(server.saw("SET"));

    assert_eq!(server.accepted(), 2);
}

#[tokio::test]
async fn hset_uses_the_master_and_decodes_the_integer_response() {
    let master = FakeRedis::start("master").await;
    let slave = FakeRedis::start("slave").await;
    let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
        .await
        .unwrap();

    assert_eq!(
        Redis::hset(&redis, "hash", "field", "value").await.unwrap(),
        1
    );
    assert!(master.saw("HSET"));
    assert!(!slave.saw("HSET"));
}

#[tokio::test]
async fn added_native_commands_use_declared_reader_and_writer_roles() {
    let master = FakeRedis::start("master").await;
    let slave = FakeRedis::start("slave").await;
    let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
        .await
        .unwrap();

    assert_eq!(Redis::incr(&redis, "counter").await.unwrap(), 1);
    assert_eq!(
        Redis::pfcount(&redis, ["metric:a", "metric:b"])
            .await
            .unwrap(),
        1
    );
    let values = Redis::mget::<_, RedisBytes>(&redis, ["metric:a", "metric:b"])
        .await
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>(),
        vec![Some(b"slave".as_slice()), Some(b"slave".as_slice())]
    );

    assert!(master.saw("INCR"));
    assert!(!slave.saw("INCR"));
    assert!(slave.saw("PFCOUNT"));
    assert!(!master.saw("PFCOUNT"));
    assert!(slave.saw("MGET"));
    assert!(!master.saw("MGET"));
}

#[tokio::test]
async fn pipeline_submits_to_one_replica_and_takes_typed_responses_in_order() {
    let master = FakeRedis::start("master").await;
    let first_slave = FakeRedis::start("first").await;
    let second_slave = FakeRedis::start("second").await;
    let redis = RedisService::noshard(
        master.endpoint.clone(),
        [first_slave.endpoint.clone(), second_slave.endpoint.clone()],
    )
    .await
    .unwrap();

    let mut pipe = RedisPipe::with_capacity(3);
    pipe.hget("key", "version").unwrap();
    pipe.hmget("key", ["value", "hash"]).unwrap();
    pipe.hget("key", "version").unwrap();

    let mut responses = redis.pipe(pipe).await.unwrap();
    let first: Option<RedisBytes> = responses.take().await.unwrap();
    let values: RedisValues<RedisBytes> = responses.take().await.unwrap();
    let values = values.collect::<RedisResult<Vec<_>>>().unwrap();
    let last: Option<RedisBytes> = responses.take().await.unwrap();

    assert_eq!(first, last);
    assert_eq!(values, vec![first.clone(), first]);
    assert!(responses.is_empty());

    let first_commands = first_slave.seen.lock().unwrap().len();
    let second_commands = second_slave.seen.lock().unwrap().len();
    assert!(
        (first_commands == 3 && second_commands == 0)
            || (first_commands == 0 && second_commands == 3)
    );
}

#[tokio::test]
async fn pipeline_rejects_multi_shard_service_before_sending() {
    let master_a = FakeRedis::start("master-a").await;
    let slave_a = FakeRedis::start("slave-a").await;
    let master_b = FakeRedis::start("master-b").await;
    let slave_b = FakeRedis::start("slave-b").await;
    let redis = RedisService::sharded(
        vec![
            (master_a.endpoint.clone(), vec![slave_a.endpoint.clone()]),
            (master_b.endpoint.clone(), vec![slave_b.endpoint.clone()]),
        ],
        ShardRouting::new("crc32", "modula"),
    )
    .await
    .unwrap();

    let mut pipe = RedisPipe::with_capacity(2);
    pipe.get("key-a").unwrap();
    pipe.get("key-b").unwrap();

    let error = redis.pipe(pipe).await.err().unwrap();

    assert_eq!(error.kind(), ErrorKind::ClientError);
    assert!(error.to_string().contains("multi-shard"));
    assert!(!slave_a.saw("GET"));
    assert!(!slave_b.saw("GET"));
    assert!(!master_a.saw("GET"));
    assert!(!master_b.saw("GET"));
}

#[tokio::test]
async fn reuses_one_connection_and_splits_reads_from_writes() {
    let master = FakeRedis::start("master").await;
    let slave = FakeRedis::start("slave").await;
    let redis = RedisService::noshard_with_options(
        master.endpoint.clone(),
        [slave.endpoint.clone()],
        RedisServiceOptions::default(),
    )
    .await
    .unwrap();

    // Construction warms exactly one connection for each endpoint.
    while master.accepted() == 0 || slave.accepted() == 0 {
        tokio::task::yield_now().await;
    }
    let initial_master_connections = master.accepted();
    let initial_slave_connections = slave.accepted();

    for _ in 0..20 {
        let value: Option<crate::RedisBytes> =
            crate::Redis::get(&redis, "u:1821155363").await.unwrap();
        assert_eq!(value.as_deref(), Some(b"slave".as_slice()));
    }
    crate::Redis::set(&redis, "u:1821155363", b"\x00\x7f\xff")
        .await
        .unwrap();

    assert!(slave.saw("GET"));
    assert!(!master.saw("GET"));
    assert!(master.saw("SET"));
    assert!(!slave.saw("SET"));
    assert_eq!(master.accepted(), initial_master_connections);
    assert_eq!(slave.accepted(), initial_slave_connections);
}

#[tokio::test]
async fn noshard_does_not_encode_the_key_for_routing() {
    struct CountingKey<'a>(&'a AtomicUsize);

    impl crate::EncodeRedisArg for CountingKey<'_> {
        fn encoded_len(&self) -> usize {
            3
        }

        fn encode<S: crate::RedisArgSink + ?Sized>(&self, sink: &mut S) -> crate::RedisResult<()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            sink.write(b"key");
            Ok(())
        }
    }

    let master = FakeRedis::start("master").await;
    let slave = FakeRedis::start("slave").await;
    let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
        .await
        .unwrap();
    let encodes = AtomicUsize::new(0);

    let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, CountingKey(&encodes))
        .await
        .unwrap();

    assert_eq!(value.as_deref(), Some(b"slave".as_slice()));
    assert_eq!(encodes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn noshard_rejects_an_empty_slave_set() {
    let error = RedisService::noshard("127.0.0.1:6379", Vec::<String>::new())
        .await
        .err()
        .unwrap();

    assert_eq!(error.kind(), ErrorKind::ClientError);
    assert!(error.to_string().contains("at least one slave"));
}

#[tokio::test]
async fn sharded_routes_before_selecting_each_groups_slaves() {
    let master_a = FakeRedis::start("master-a").await;
    let slave_a = FakeRedis::start("slave-a").await;
    let master_b = FakeRedis::start("master-b").await;
    let slave_b = FakeRedis::start("slave-b").await;
    let redis = RedisService::sharded(
        vec![
            (master_a.endpoint.clone(), vec![slave_a.endpoint.clone()]),
            (master_b.endpoint.clone(), vec![slave_b.endpoint.clone()]),
        ],
        ShardRouting::new("crc32", "modula"),
    )
    .await
    .unwrap();
    let sharding = Sharding::new("crc32", "modula", &["a".to_owned(), "b".to_owned()]);
    let mut keys = [None, None];
    for index in 0_u64.. {
        let key = index.to_string();
        let shard = sharding.shard_idx(key.as_bytes());
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }

    let keys = keys.map(Option::unwrap);
    for (key, expected) in keys
        .iter()
        .zip([b"slave-a".as_slice(), b"slave-b".as_slice()])
    {
        let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, key)
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(expected));
    }

    let values = Redis::mget::<_, RedisBytes>(&redis, [&keys[1], &keys[0], &keys[1]])
        .await
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>(),
        [
            Some(b"slave-b".as_slice()),
            Some(b"slave-a".as_slice()),
            Some(b"slave-b".as_slice()),
        ]
    );
    assert!(slave_a.saw("MGET"));
    assert!(slave_b.saw("MGET"));

    assert_eq!(
        Redis::del_many(&redis, [&keys[0], &keys[1]]).await.unwrap(),
        2
    );
    assert!(master_a.saw("DEL"));
    assert!(master_b.saw("DEL"));

    let count_error = Redis::pfcount(&redis, [&keys[0], &keys[1]])
        .await
        .unwrap_err();
    assert_eq!(count_error.kind(), ErrorKind::ClientError);
    assert!(count_error.to_string().contains("same shard"));

    let script_result =
        Redis::eval::<_, _, _, i64>(&redis, "return 1", [&keys[0]], [] as [&str; 0])
            .await
            .unwrap();
    assert_eq!(script_result, 1);
    assert!(master_a.saw("EVAL"));

    let script_error =
        Redis::eval::<_, _, _, i64>(&redis, "return 1", [&keys[0], &keys[1]], [] as [&str; 0])
            .await
            .unwrap_err();
    assert_eq!(script_error.kind(), ErrorKind::ClientError);
    assert!(script_error.to_string().contains("same shard"));
}

#[tokio::test]
async fn concurrent_reads_share_one_physical_slave_connection() {
    let master = FakeRedis::start("master").await;
    let slave = FakeRedis::start("slave").await;
    let redis = RedisService::noshard(master.endpoint.clone(), [slave.endpoint.clone()])
        .await
        .unwrap();

    let mut requests = JoinSet::new();
    for index in 0..128 {
        let redis = redis.clone();
        requests.spawn(async move {
            crate::Redis::get::<_, crate::RedisBytes>(&redis, &format!("key-{index}"))
                .await
                .unwrap()
        });
    }
    while let Some(result) = requests.join_next().await {
        assert_eq!(result.unwrap().as_deref(), Some(b"slave".as_slice()));
    }

    assert_eq!(master.accepted(), 1);
    assert_eq!(slave.accepted(), 1);
}

#[tokio::test]
async fn slave_selection_rotates_after_consuming_time_quota() {
    let master = FakeRedis::start("master").await;
    let slave_a = FakeRedis::start_with_delay("a", Duration::from_millis(3)).await;
    let slave_b = FakeRedis::start_with_delay("b", Duration::from_millis(3)).await;
    let redis = RedisService::noshard_with_options(
        master.endpoint.clone(),
        [slave_a.endpoint.clone(), slave_b.endpoint.clone()],
        RedisServiceOptions::default().with_replica_quota(Duration::from_millis(1)),
    )
    .await
    .unwrap();

    let first = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
        .await
        .unwrap()
        .unwrap();
    let second = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert!(slave_a.saw("GET"));
    assert!(slave_b.saw("GET"));
}

#[tokio::test]
async fn changed_dns_snapshot_is_applied_automatically_and_reuses_nodes() {
    let master = FakeRedis::start("master").await;
    let old_slave = FakeRedis::start("old-slave").await;
    let new_slave = FakeRedis::start("new-slave").await;
    let redis = RedisService::noshard(master.endpoint.clone(), [old_slave.endpoint.clone()])
        .await
        .unwrap();

    assert_eq!(
        crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap()
            .as_deref(),
        Some(b"old-slave".as_slice())
    );
    let topology = redis.inner.topology.load_full();
    assert!(
        topology.discovery.shards[0].slaves[0]
            .endpoints
            .replace([new_slave.endpoint.parse().unwrap()])
    );
    drop(topology);

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
            .await
            .unwrap();
        if value.as_deref() == Some(b"new-slave".as_slice()) {
            break;
        }
        assert_eq!(value.as_deref(), Some(b"old-slave".as_slice()));
        assert!(Instant::now() < deadline);
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(master.accepted(), 1, "unchanged master Node must be reused");
    assert_eq!(old_slave.accepted(), 1);
    assert_eq!(new_slave.accepted(), 1);
}

#[tokio::test]
async fn response_timeout_retries_master_then_reconnects_the_slave() {
    let master = FakeRedis::start("master").await;
    let slave = RecoveringRedis::start().await;
    let redis = RedisService::noshard_with_options(
        master.endpoint.clone(),
        [slave.endpoint.clone()],
        RedisServiceOptions::default().with_slave_timeout(Duration::from_millis(30)),
    )
    .await
    .unwrap();

    let value = crate::Redis::get::<_, crate::RedisBytes>(&redis, "key")
        .await
        .unwrap();
    assert_eq!(value.as_deref(), Some(b"master".as_slice()));

    #[cfg(feature = "metrics")]
    {
        let mut slave_metric = None;
        let mut master_metric = None;
        brz_metrics::visit(|name, metric_type, candidate| {
            if name == slave.endpoint && metric_type == "REDIS" {
                slave_metric = Some(candidate);
            }
            if name == master.endpoint && metric_type == "REDIS" {
                master_metric = Some(candidate);
            }
        });
        let slave_metric = slave_metric.expect("failed slave attempt must be recorded");
        let master_metric = master_metric.expect("master fallback must be recorded");
        assert_eq!(
            (
                slave_metric.total,
                slave_metric.success,
                slave_metric.failure
            ),
            (1, 0, 1)
        );
        assert_eq!(
            (
                master_metric.total,
                master_metric.success,
                master_metric.failure
            ),
            (1, 1, 0)
        );
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while slave.accepted() < 2 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    loop {
        match crate::Redis::get::<_, crate::RedisBytes>(&redis, "key").await {
            Ok(value) => {
                assert_eq!(value.as_deref(), Some(b"recovered".as_slice()));
                break;
            }
            Err(error) if error.kind() == ErrorKind::NoConnection => {
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(error) => panic!("unexpected retry failure: {error}"),
        }
    }
}

fn java_crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    crc ^ u32::MAX
}
