//! Workload generation against a pre-generated key/value pool.
//!
//! Every mode builds the same [`redis::RedisService`]; only its construction
//! source (mesh, one endpoint, or an explicit sharded topology) differs.
//!
//! To keep the per-op allocation count honest (so it reflects the *SDK's*
//! allocations, not the benchmark's), keys and values are pre-generated once
//! into an [`Arc`] and workers index into them with a plain integer — no
//! `format!()` on the hot path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redis::{Redis, RedisBytes, RedisService, RedisValues};

/// A boxed, `Send` future returned by a workload.
pub type WorkFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// The kind of workload to run.
#[derive(Clone, Copy, Debug)]
pub enum WorkloadKind {
    /// One HGET per op — pure single-round-trip read (ping/pong).
    Hget,
    /// One HMGET (4 fields) per op — multi-field read.
    Hmget,
}

impl WorkloadKind {
    pub fn runner(self, pool: Arc<Pool>, verify: bool) -> Box<dyn Workload> {
        match self {
            WorkloadKind::Hget => Box::new(HgetPing { pool, verify }),
            WorkloadKind::Hmget => Box::new(HmgetPing { pool, verify }),
        }
    }
}

/// Hash fields every workload key is seeded with.
pub const FIELDS: [&str; 4] = ["f1", "f2", "f3", "f4"];

/// Seeded values always start with `field || key` (self-describing content,
/// optionally padded to a size distribution), so replies can be checked for
/// request/response mixups whenever `--verify` is on.
/// Returns true if `value` carries that prefix.
pub fn expected_value_matches(field: &str, key: &[u8], value: &[u8]) -> bool {
    value.len() >= field.len() + key.len()
        && value.starts_with(field.as_bytes())
        && &value[field.len()..field.len() + key.len()] == key
}

/// Build the seed value for (key, field): `field || key`, padded with the
/// size-cycled pattern `fill` when it is larger, so the value size spread is
/// preserved while the content stays verifiable.
pub fn seeded_value(field: &str, key: &[u8], fill: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(fill.len().max(field.len() + key.len()));
    v.extend_from_slice(field.as_bytes());
    v.extend_from_slice(key);
    if fill.len() > v.len() {
        v.extend_from_slice(&fill[v.len()..]);
    }
    v
}

/// One logical operation against a connection.
pub trait Workload: Send + Sync {
    fn run<'a>(&'a self, client: &'a RedisService, op: u64) -> WorkFuture<'a>;
}

/// Pre-generated keys (fixed 32-byte) and values (sizes cycling through
/// `1..=max_value_size`), shared (immutable) across all workers. Built once
/// before measurement so the hot path only indexes into it.
pub struct Pool {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    /// Big-value pattern and stride: every `big_stride`-th key gets the big
    /// value; stride 0 = disabled.
    big_value: Option<Vec<u8>>,
    big_stride: usize,
}

impl Pool {
    /// Build a pool of `num_keys` keys (each `key_len` bytes) and values
    /// whose sizes cycle through `1..=max_value_size`. With
    /// `big_value_rate > 0`, every `1/rate`-th key gets a
    /// `big_value_size`-byte value instead.
    pub fn new(
        num_keys: usize,
        key_len: usize,
        max_value_size: usize,
        big_value_rate: f64,
        big_value_size: usize,
    ) -> Self {
        let keys = (0..num_keys).map(|i| key_bytes(i, key_len)).collect();
        let values: Vec<Vec<u8>> = if max_value_size == 0 {
            vec![Vec::new()]
        } else {
            (1..=max_value_size).map(value_bytes).collect()
        };
        let values = if values.is_empty() {
            vec![Vec::new()]
        } else {
            values
        };
        let (big_value, big_stride) = if big_value_rate > 0.0 {
            (
                Some(value_bytes(big_value_size)),
                (1.0 / big_value_rate).round().max(1.0) as usize,
            )
        } else {
            (None, 0)
        };
        Pool {
            keys,
            values,
            big_value,
            big_stride,
        }
    }

    /// Number of distinct keys in the pool.
    #[allow(dead_code)]
    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }

    /// All pre-generated keys, for seeding.
    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    /// Pick key at `index` (caller wraps with modulo).
    pub fn key(&self, index: usize) -> &[u8] {
        &self.keys[index % self.keys.len()]
    }

    /// The fill value for key `index`: the big pattern for big-value keys,
    /// otherwise the size-cycled pattern.
    pub fn value(&self, index: usize) -> &[u8] {
        if self.big_stride > 0 && index.is_multiple_of(self.big_stride) {
            return self.big_value.as_deref().unwrap();
        }
        &self.values[index % self.values.len()]
    }
}

/// Deterministic `key_len`-byte key derived from `i`.
fn key_bytes(i: usize, key_len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key_len);
    // 8-byte big-endian index prefix, then a repeating fill byte.
    buf.extend_from_slice(&i.to_be_bytes());
    let fill = b'k';
    while buf.len() < key_len {
        buf.push(fill);
    }
    buf.truncate(key_len);
    buf
}

/// A `size`-byte value filled with a stable pattern (no allocation on the
/// hot path — built once here).
fn value_bytes(size: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(size);
    let mut b = 0u8;
    for _ in 0..size {
        buf.push(b);
        b = b.wrapping_add(17);
    }
    buf
}

struct HgetPing {
    pool: Arc<Pool>,
    verify: bool,
}
impl Workload for HgetPing {
    fn run<'a>(&'a self, client: &'a RedisService, op: u64) -> WorkFuture<'a> {
        let key = self.pool.key(op as usize);
        Box::pin(async move {
            let result: redis::RedisResult<Option<RedisBytes>> = client.hget(key, FIELDS[0]).await;
            match result {
                Ok(Some(value)) => {
                    let ok = !self.verify || expected_value_matches(FIELDS[0], key, &value);
                    if self.verify && !ok && std::env::var("VERIFY_DEBUG").is_ok() {
                        eprintln!(
                            "verify mismatch: field={} key={:?} got={:?}",
                            FIELDS[0],
                            String::from_utf8_lossy(key),
                            value
                        );
                    }
                    ok
                }
                Ok(None) => !self.verify,
                Err(_) => false,
            }
        })
    }
}

struct HmgetPing {
    pool: Arc<Pool>,
    verify: bool,
}
impl Workload for HmgetPing {
    fn run<'a>(&'a self, client: &'a RedisService, op: u64) -> WorkFuture<'a> {
        let key = self.pool.key(op as usize);
        Box::pin(async move {
            let result: redis::RedisResult<RedisValues<RedisBytes>> =
                client.hmget(key, FIELDS).await;
            match result {
                Ok(values) => {
                    let Ok(values) = values.collect::<redis::RedisResult<Vec<_>>>() else {
                        return false;
                    };
                    if !self.verify {
                        return true;
                    }
                    values.len() == FIELDS.len()
                        && values.iter().zip(FIELDS.iter()).all(|(value, field)| {
                            value
                                .as_deref()
                                .is_some_and(|value| expected_value_matches(field, key, value))
                        })
                }
                Err(_) => false,
            }
        })
    }
}
