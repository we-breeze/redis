//! Slot distribution algorithms, ported 1:1 from breeze
//! `sharding/src/distribution`. A distribution maps a hash value to a shard
//! index in `[0, shard_count)`.

use std::collections::BTreeMap;
use std::ops::Bound::Included;

/// Default slot count for `range`/`modrange` (the mesh's "hash-gen" concept).
const DIST_RANGE_SLOT_COUNT_DEFAULT: u64 = 256;

/// The distribution strategy over a fixed set of backends.
#[derive(Clone, Debug)]
pub enum Distribute {
    /// `ketama` / `ketama_origin` consistent hashing (md5 ring, 40 factors).
    Consistent { buckets: BTreeMap<i64, usize> },
    /// `modula` / `absmodula`.
    Modula(Modula),
    /// `range[-slot]`: `hash/slot%slot/(slot/shards)`.
    Range { slot: u64, interval: u64, shards: usize },
    /// `modrange[-slot]`: `hash%slot/(slot/shards)`.
    ModRange { slot: u64, interval: u64, shards: usize },
    /// `splitmod[-count]`: `hash/count%count%shards`.
    SplitMod { split_count: u64, shard_count: u64 },
    /// `slotmod[-count]`: `hash%count%shards`.
    SlotMod { slot_count: u64, shard_count: u64 },
    /// `secmod`: `hash/shards%shards`.
    SecMod { shard_count: u64 },
}

#[derive(Clone, Debug)]
pub struct Modula {
    shards: usize,
    /// absmodula: cast to i32 and take abs first (hc business compatibility).
    absolute_hash: bool,
}

impl Distribute {
    /// Build from its configuration name and the backend names (only ketama
    /// uses the names themselves). Unknown names fall back to `modula`.
    pub fn from(distribution: &str, names: &[String]) -> Self {
        let dist = distribution.to_ascii_lowercase();
        let idx = dist.find('-');
        let name = &dist[..idx.unwrap_or(dist.len())];
        let num = idx.and_then(|i| dist[i + 1..].parse::<u64>().ok());

        match name {
            "modula" => Self::Modula(Modula {
                shards: names.len(),
                absolute_hash: false,
            }),
            "absmodula" => Self::Modula(Modula {
                shards: names.len(),
                absolute_hash: true,
            }),
            "ketama" => Self::consistent(names, false),
            "ketama_origin" => Self::consistent(names, true),
            "range" => Self::range(num, names.len()),
            "modrange" => Self::mod_range(num, names.len()),
            "splitmod" => Self::SplitMod {
                split_count: num.unwrap_or(32),
                shard_count: names.len() as u64,
            },
            "slotmod" => Self::SlotMod {
                slot_count: num.unwrap_or(1024),
                shard_count: names.len() as u64,
            },
            "secmod" => Self::SecMod {
                shard_count: names.len() as u64,
            },
            _ => {
                tracing::warn!("'{distribution}' is not valid, use modula instead");
                Self::Modula(Modula {
                    shards: names.len(),
                    absolute_hash: false,
                })
            }
        }
    }

    fn range(slot: Option<u64>, shards: usize) -> Self {
        let slot = slot.unwrap_or(DIST_RANGE_SLOT_COUNT_DEFAULT);
        assert!(shards > 0 && slot >= shards as u64);
        Self::Range {
            slot,
            interval: slot / shards as u64,
            shards,
        }
    }

    fn mod_range(slot: Option<u64>, shards: usize) -> Self {
        let slot = slot.unwrap_or(DIST_RANGE_SLOT_COUNT_DEFAULT);
        assert!(shards > 0 && slot >= shards as u64);
        Self::ModRange {
            slot,
            interval: slot / shards as u64,
            shards,
        }
    }

    // `& 0xFF` documents the Java byte widening; kept 1:1 with the mesh.
    #[allow(clippy::identity_op)]
    fn consistent(names: &[String], origin_alg: bool) -> Self {
        let mut buckets = BTreeMap::new();
        for (idx, shard) in names.iter().enumerate() {
            let factor = 40;
            for i in 0..factor {
                let data = format!("{shard}-{i}");
                let out = md5::compute(data.as_str());
                for j in 0..4 {
                    let mut hash = (((out[3 + j * 4] & 0xFF) as i64) << 24)
                        | (((out[2 + j * 4] & 0xFF) as i64) << 16)
                        | (((out[1 + j * 4] & 0xFF) as i64) << 8)
                        | ((out[j * 4] & 0xFF) as i64);
                    // The SDK-corrected ketama applies this extra mod; the
                    // twemproxy-origin variant does not.
                    if !origin_alg {
                        hash = hash.wrapping_rem(i32::MAX as i64);
                    }
                    if hash < 0 {
                        hash = hash.wrapping_mul(-1);
                    }
                    buckets.insert(hash, idx);
                }
            }
        }
        Self::Consistent { buckets }
    }

    /// The shard index for `hash`.
    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        match self {
            Distribute::Consistent { buckets } => {
                // First node at or above hash; wrap to the first node.
                if let Some((_, idx)) = buckets.range((Included(hash), Included(i64::MAX))).next() {
                    return *idx;
                }
                buckets.values().next().copied().unwrap_or(0)
            }
            Distribute::Modula(m) => m.index(hash),
            Distribute::Range {
                slot,
                interval,
                shards,
            } => {
                let mut val = hash
                    .wrapping_div(*slot as i64)
                    .wrapping_rem(*slot as i64);
                if val < 0 {
                    tracing::warn!("negative range pre hash: {val}");
                    val = val.wrapping_abs();
                }
                let rs = val as u64 / interval;
                if rs >= *shards as u64 {
                    tracing::warn!("range slot out of bound, rs:{rs}, shards:{shards}");
                    return shards - 1;
                }
                rs as usize
            }
            Distribute::ModRange {
                slot,
                interval,
                shards,
            } => {
                let mut val = hash.wrapping_rem(*slot as i64);
                if val < 0 {
                    tracing::warn!("negative modrange pre hash: {val}");
                    val = val.wrapping_abs();
                }
                let rs = val as u64 / interval;
                if rs >= *shards as u64 {
                    tracing::warn!("modrange slot out of bound, rs:{rs}, shards:{shards}");
                    return shards - 1;
                }
                rs as usize
            }
            Distribute::SplitMod {
                split_count,
                shard_count,
            } => {
                let mut val = hash
                    .wrapping_div(*split_count as i64)
                    .wrapping_rem(*split_count as i64);
                if val < 0 {
                    tracing::warn!("negative splitmod pre hash: {val}");
                    val = val.wrapping_abs();
                }
                (val as u64 % shard_count) as usize
            }
            Distribute::SlotMod {
                slot_count,
                shard_count,
            } => hash
                .wrapping_rem(*slot_count as i64)
                .wrapping_rem(*shard_count as i64) as usize,
            Distribute::SecMod { shard_count } => {
                if hash < 0 {
                    tracing::error!("negative hash for secmod: {hash}");
                }
                (hash as usize)
                    .wrapping_div(*shard_count as usize)
                    .wrapping_rem(*shard_count as usize)
            }
        }
    }
}

impl Modula {
    #[inline]
    fn index(&self, hash: i64) -> usize {
        if self.shards == 0 {
            return 0;
        }
        // The mesh special-cases power-of-two shard counts with a mask; the
        // result is identical to `%` for non-negative values, and for the
        // absolute variant it matches on all non-MIN i32s. We reproduce the
        // exact bit-level behavior anyway to stay bit-compatible.
        let mask = self.shards - 1;
        if self.shards & mask == 0 {
            if self.absolute_hash {
                (hash as i32).wrapping_abs() as usize & mask
            } else {
                hash as usize & mask
            }
        } else if self.absolute_hash {
            (hash as i32).wrapping_abs() as usize % self.shards
        } else {
            hash as usize % self.shards
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("10.0.0.{i}:6379")).collect()
    }

    #[test]
    fn modula_variants() {
        let d = Distribute::from("modula", &names(4));
        assert_eq!(d.index(5), 1);
        assert_eq!(d.index(0), 0);
        let d = Distribute::from("absmodula", &names(3));
        assert!(d.index(-7) < 3);
        assert_eq!(d.index(7), 7i32.unsigned_abs() as usize % 3);
    }

    #[test]
    fn range_variants() {
        // range-16 over 4 shards: interval 4; index = hash/16%16/4.
        let d = Distribute::from("range-16", &names(4));
        assert_eq!(d.index(0), 0);
        assert_eq!(d.index(4 * 16), 1);
        assert_eq!(d.index(15 * 16), 3);
        // modrange-16: hash%16/4.
        let d = Distribute::from("modrange-16", &names(4));
        assert_eq!(d.index(0), 0);
        assert_eq!(d.index(4), 1);
        assert_eq!(d.index(15), 3);
        assert_eq!(d.index(16), 0);
    }

    #[test]
    fn ketama_is_deterministic_and_in_range() {
        let d = Distribute::from("ketama", &names(4));
        for h in [0i64, 1, 123456789, i64::MAX, i64::MAX - 1] {
            assert!(d.index(h) < 4);
            assert_eq!(d.index(h), d.index(h));
        }
    }

    #[test]
    fn mod_family() {
        let d = Distribute::from("slotmod-8", &names(4));
        #[allow(clippy::identity_op)]
        {
            assert_eq!(d.index(9), 9 % 8 % 4);
        }
        let d = Distribute::from("splitmod-8", &names(4));
        #[allow(clippy::identity_op)]
        {
            assert_eq!(d.index(9), 9 / 8 % 8 % 4);
        }
        let d = Distribute::from("secmod", &names(4));
        #[allow(clippy::identity_op)]
        {
            assert_eq!(d.index(9), 9 / 4 % 4);
        }
    }
}
