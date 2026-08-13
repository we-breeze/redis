// 本目录为 breeze sharding 的原样复制(vendored),风格类告警不参与
// 主 crate 的 lint 标准。
#![allow(clippy::all)]

// #[derive(Debug, Clone)]
// pub struct Sharding {
//     hash: Hasher,
//     distribution: Distribute,
//     num: usize,
// }

pub mod hash;
use hash::*;

pub mod distribution;
pub use distribution::Distribute;
pub use hash::Hasher;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bench;

/// A resolved client-side sharding plan: hash algorithm + slot distribution.
/// (SDK 侧补充的薄封装,便于按 key 直接取分片。)
#[derive(Clone, Debug)]
pub struct Sharding {
    hasher: Hasher,
    distribute: distribution::Distribute,
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
        self.distribute.index(self.hasher.hash(&key))
    }

    /// The raw hash of `key`.
    #[inline]
    pub fn hash(&self, key: &[u8]) -> i64 {
        self.hasher.hash(&key)
    }
}

// use distribution::*;

// use std::ops::Deref;

// impl Sharding {
// dead code 暂时注释掉
// pub fn from(hash_alg: &str, distribution: &str, names: Vec<String>) -> Self {
//     let num = names.len();
//     let h = Hasher::from(hash_alg);
//     let d = Distribute::from(distribution, &names);
//     Self {
//         hash: h,
//         distribution: d,
//         num: num,
//     }
// }
// #[inline]
// pub fn sharding(&self, key: &[u8]) -> usize {
//     let hash = self.hash.hash(&key);
//     let idx = self.distribution.index(hash);
//     assert!(idx < self.num);
//     idx
// }
// // key: sharding idx
// // value: 是keys idx列表
// #[inline]
// pub fn shardings<K: Deref<Target = [u8]>>(&self, keys: Vec<K>) -> Vec<Vec<usize>> {
//     let mut shards = vec![Vec::with_capacity(8); self.num];
//     for (ki, key) in keys.iter().enumerate() {
//         let idx = self.sharding(key);
//         unsafe { shards.get_unchecked_mut(idx).push(ki) };
//     }
//     shards
// }
// }
