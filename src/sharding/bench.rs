//! sharding 微基准:直接运行(非 cargo bench),量化各 hash/distribution
//! 的单次耗时,用于优化前后对比。
//!
//! 运行: cargo test --release --package redis --lib sharding_perf -- --nocapture --ignored

#![cfg(test)]

use super::distribution::Distribute;
use super::hash::{Hash, Hasher};
use std::time::Instant;

fn bench_hash(alg: &str, keys: &[Vec<u8>], rounds: usize) {
    let hasher = Hasher::from(alg);
    let start = Instant::now();
    let mut acc = 0i64;
    for _ in 0..rounds {
        for key in keys {
            acc = acc.wrapping_add(hasher.hash(&key.as_slice()));
        }
    }
    let total = (rounds * keys.len()) as f64;
    let ns = start.elapsed().as_nanos() as f64 / total;
    println!("{alg:<24} {ns:8.1} ns/op (acc={acc})");
}

#[test]
#[ignore = "perf"]
fn sharding_perf() {
    // 32 字节 key,贴近 bench/线上形态。
    let keys: Vec<Vec<u8>> = (0..1000usize)
        .map(|i| {
            let mut k = i.to_be_bytes().to_vec();
            k.extend_from_slice(b"kkkkkkkkkkkkkkkkkkkkkkkk");
            k
        })
        .collect();
    let rounds = 200;

    println!("== hash (32B key) ==");
    for alg in [
        "crc32",
        "crc32-short",
        "crc32-underscore",
        "crc32-num",
        "crc32local",
        "crc32abs",
        "crc64",
        "bkdr",
        "bkdrsub",
        "raw",
        "fnv1_32",
        "fnv1a_64",
    ] {
        bench_hash(alg, &keys, rounds);
    }

    println!("== distribution ==");
    let names: Vec<String> = (0..8).map(|i| format!("10.0.0.{i}:6379")).collect();
    for dist in ["modula", "absmodula", "ketama", "range-256", "modrange-256"] {
        let d = Distribute::from(dist, &names);
        let start = Instant::now();
        let mut acc = 0usize;
        let hashes: Vec<i64> = keys
            .iter()
            .map(|k| Hasher::from("crc32").hash(&k.as_slice()))
            .collect();
        for _ in 0..rounds {
            for h in &hashes {
                acc = acc.wrapping_add(d.index(*h));
            }
        }
        let ns = start.elapsed().as_nanos() as f64 / (rounds * hashes.len()) as f64;
        println!("{dist:<24} {ns:8.1} ns/op (acc={acc})");
    }
}
