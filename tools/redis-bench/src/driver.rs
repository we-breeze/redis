//! Workload generation against a pre-generated key/value pool.
//!
//! Workloads are written against the [`Commands`] trait, which is implemented
//! for every [`ConnectionLike`]. The harness holds the active connection as a
//! trait object (`Arc<dyn ConnectionLike>`) so the same workload code drives
//! both the mesh [`Client`](redis::sidecar::Client) and the harness's direct
//! client.
//!
//! To keep the per-op allocation count honest (so it reflects the *SDK's*
//! allocations, not the benchmark's), keys and values are pre-generated once
//! into an [`Arc`] and workers index into them with a plain integer — no
//! `format!()` on the hot path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redis::Commands;
use redis::connection::ConnectionLike;
use redis::types::Value;

/// A boxed, `Send` future returned by a workload.
pub type WorkFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// The kind of workload to run.
#[derive(Clone, Copy, Debug)]
pub enum WorkloadKind {
    /// One GET per op — pure single-round-trip read (ping/pong).
    Get,
    /// One SET per op — pure single-round-trip write (ping/pong).
    Set,
}

impl WorkloadKind {
    pub fn runner(self, pool: Arc<Pool>) -> Box<dyn Workload> {
        match self {
            WorkloadKind::Get => Box::new(GetPing { pool }),
            WorkloadKind::Set => Box::new(SetPing { pool }),
        }
    }
}

/// One logical operation against a connection.
pub trait Workload: Send + Sync {
    fn run<'a>(&'a self, client: &'a dyn ConnectionLike, op: u64) -> WorkFuture<'a>;
}

/// Pre-generated keys (fixed 32-byte) and values (sizes cycling through
/// `1..=max_value_size`), shared (immutable) across all workers. Built once
/// before measurement so the hot path only indexes into it.
pub struct Pool {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
}

impl Pool {
    /// Build a pool of `num_keys` keys (each `key_len` bytes) and values whose
    /// sizes cycle through `1..=max_value_size` (so the SET workload exercises
    /// a realistic spread of value sizes).
    pub fn new(num_keys: usize, key_len: usize, max_value_size: usize) -> Self {
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
        Pool { keys, values }
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

    /// All pre-generated values, for seeding.
    pub fn values(&self) -> &[Vec<u8>] {
        &self.values
    }

    /// Pick key at `index` (caller wraps with modulo).
    pub fn key(&self, index: usize) -> &[u8] {
        &self.keys[index % self.keys.len()]
    }

    /// Pick a value whose size cycles through `1..=max_value_size`.
    pub fn value(&self, index: usize) -> &[u8] {
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

struct GetPing {
    pool: Arc<Pool>,
}
impl Workload for GetPing {
    fn run<'a>(&'a self, client: &'a dyn ConnectionLike, op: u64) -> WorkFuture<'a> {
        let key = self.pool.key(op as usize);
        Box::pin(async move {
            let _: Value = match client.get(key).await {
                Ok(v) => v,
                Err(_) => return false,
            };
            true
        })
    }
}

struct SetPing {
    pool: Arc<Pool>,
}
impl Workload for SetPing {
    fn run<'a>(&'a self, client: &'a dyn ConnectionLike, op: u64) -> WorkFuture<'a> {
        let key = self.pool.key(op as usize);
        let value = self.pool.value(op as usize);
        Box::pin(async move { client.set::<()>(key, value).await.is_ok() })
    }
}
