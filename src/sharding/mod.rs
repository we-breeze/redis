//! Hash and distribution algorithms for client-side backend sharding.
//!
//! Faithful port of the breeze mesh's `sharding` crate
//! (`github.com/examplecom/breeze`, `sharding/src/{hash,distribution}`), so an
//! SDK accessing backends directly computes **the same shard index as the
//! mesh** for a given `(hash, distribution)` configuration. Keys are plain
//! `&[u8]`; logging goes through `tracing`; `enum_dispatch` is replaced by
//! plain enum dispatch.
//!
//! Names come from the resource configuration, e.g. hash `crc32-underscore`,
//! distribution `modula`, `absmodula`, `ketama`, `range-256`, `modrange`,
//! `slotmod-1024`, `splitmod-32`, `secmod`.

pub mod distribution;
pub mod hash;

pub use distribution::Distribute;
pub use hash::Hasher;

/// A resolved client-side sharding plan: hash algorithm + slot distribution.
#[derive(Clone, Debug)]
pub struct Sharding {
    hasher: Hasher,
    distribute: Distribute,
}

impl Sharding {
    /// Build from configuration names and the backend name list (ketama uses
    /// the names as ring nodes; other distributions only use the count).
    pub fn new(hash_alg: &str, distribution: &str, backends: &[String]) -> Self {
        Sharding {
            hasher: Hasher::from(hash_alg),
            distribute: Distribute::from(distribution, backends),
        }
    }

    /// The shard index for `key`.
    #[inline]
    pub fn shard_idx(&self, key: &[u8]) -> usize {
        self.distribute.index(self.hasher.hash(key))
    }

    /// The raw hash of `key` (e.g. for `hash_range` membership checks).
    #[inline]
    pub fn hash(&self, key: &[u8]) -> i64 {
        self.hasher.hash(key)
    }
}
