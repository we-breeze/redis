//! 复制自 breeze sharding 的算法的正确性校验(独立于被复制的源码,
//! 防止复制/调整过程中引入偏差;向量与独立实现交叉核对)。

use super::distribution::Distribute;
use super::hash::Hash;
use super::hash::Hasher;

fn names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("10.0.0.{i}:6379")).collect()
}

fn hash(alg: &str, key: &[u8]) -> i64 {
    Hasher::from(alg).hash(&key)
}

#[test]
fn name_parsing() {
    assert!(matches!(Hasher::from("crc32"), Hasher::Crc32(_)));
    assert!(matches!(Hasher::from("CRC32"), Hasher::Crc32(_)));
    assert!(matches!(
        Hasher::from("crc32-range-underscore"),
        Hasher::Crc32Delimiter(_)
    ));
    assert!(matches!(Hasher::from("crc32-id"), Hasher::Crc32Num(_)));
    assert!(matches!(
        Hasher::from("bkdrsub"),
        Hasher::BkdrsubDelimiter(_)
    ));
    assert!(matches!(
        Hasher::from("rawsuffix-pound"),
        Hasher::RawSuffix(_)
    ));
}

#[test]
fn known_vectors() {
    assert_eq!(hash("bkdr", b"abc"), 96354);
    assert_eq!(hash("raw", b"12345abc"), 12345);
    assert_eq!(hash("crc32", b"abc"), 891568578);
    assert_eq!(hash("crc32-underscore", b"abc_def"), hash("crc32", b"abc"));
    assert_eq!(hash("crc32-num-3", b"uid12345x"), hash("crc32", b"12345"));
    assert_eq!(
        hash("crc32-smartnum", b"abc12345678def"),
        hash("crc32", b"12345678")
    );
    assert_eq!(
        hash("crc32-mixnum", b"a_123_456_bc"),
        hash("crc32", b"123456")
    );
    assert_eq!(hash("rawsuffix-point", b"abc.789"), 789);
    assert_eq!(hash("bkdrsub", b"abc#123_456"), 847490);
    assert_eq!(hash("rawcrc32local", b"12345_xx"), 12345);
}

#[test]
fn distributions() {
    let d = Distribute::from("modula", &names(4));
    assert_eq!(d.index(5), 1);
    let d = Distribute::from("range-16", &names(4));
    assert_eq!(d.index(4 * 16), 1);
    assert_eq!(d.index(15 * 16), 3);
    let d = Distribute::from("modrange-16", &names(4));
    assert_eq!(d.index(15), 3);
    assert_eq!(d.index(16), 0);
    let d = Distribute::from("ketama", &names(4));
    for h in [0i64, 1, 123456789, i64::MAX] {
        assert!(d.index(h) < 4);
        assert_eq!(d.index(h), d.index(h));
    }
    let d = Distribute::from("slotmod-8", &names(4));
    assert_eq!(d.index(9), 9 % 8 % 4);
    let d = Distribute::from("splitmod-8", &names(4));
    assert_eq!(d.index(9), 9 / 8 % 8 % 4);
    let d = Distribute::from("secmod", &names(4));
    assert_eq!(d.index(9), 9 / 4 % 4);

    let d = super::distribution::DBRange::new(4, 8, 2);
    for h in [0i64, 1, 31, 32, 123456789] {
        assert!(d.index(h) < 2);
        assert_eq!(d.index(h), d.db_idx(h) / 2);
    }
}

/// u32 化改写必须与原版 i64 算法逐位一致。这里内嵌 i64 参照实现
/// (来自改写前的源码),对随机 key 做交叉验证。
#[test]
fn crc32_variants_match_i64_reference() {
    // 参照表运行时按多项式 0xEDB88320 独立生成(同时校验共享表本身)。
    let mut ref_tab = [0i64; 256];
    for (i, entry) in ref_tab.iter_mut().enumerate() {
        let mut c = i as i64;
        for _ in 0..8 {
            c = if c & 1 != 0 {
                0xEDB88320i64 ^ (c >> 1)
            } else {
                c >> 1
            };
        }
        *entry = c;
    }
    let ref_tab = ref_tab;
    let ref_crc32 = |key: &[u8], masked: bool| {
        const SEED: i64 = 0xFFFFFFFF;
        let mut crc: i64 = SEED;
        for &c in key {
            let idx = ((crc ^ (c as i64)) & 0xff) as usize;
            crc = if masked {
                ((crc >> 8) & 0x00FFFFFF) ^ ref_tab[idx]
            } else {
                (crc >> 8) ^ ref_tab[idx]
            };
        }
        crc ^= SEED;
        crc & SEED
    };

    // 确定性伪随机 key 集:覆盖短/长/含分隔符/含二进制字节。
    let mut x = 0x12345678u64;
    let mut rand = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..2000 {
        let len = (rand() % 64) as usize + 1;
        let mut k = Vec::with_capacity(len);
        for _ in 0..len {
            k.push((rand() % 256) as u8);
        }
        keys.push(k);
        // 分隔符形态
        let k2 = format!("pre{i}_suf{}.tail", rand() % 1000);
        keys.push(k2.into_bytes());
        // 数字形态
        keys.push(format!("{}{}", rand() % 100000, rand() % 100).into_bytes());
    }

    for key in &keys {
        assert_eq!(
            hash("crc32", key),
            ref_crc32(key, true),
            "crc32 mismatch for {key:?}"
        );
        // crc32-short: 先 crc32 再截断
        assert_eq!(
            hash("crc32-short", key),
            (ref_crc32(key, true) >> 16) & 0x7fff
        );
        // crc32local: 无掩码循环 + i32 abs(参照原版)
        let local_ref = ref_crc32(key, false) as i32;
        let local_ref = if local_ref < 0 {
            -local_ref as i64
        } else {
            local_ref as i64
        };
        assert_eq!(hash("crc32local", key), local_ref, "crc32local {key:?}");
        // crc32abs
        let abs_ref = ref_crc32(key, true) as i32;
        let abs_ref = if abs_ref < 0 {
            -abs_ref as i64
        } else {
            abs_ref as i64
        };
        assert_eq!(hash("crc32abs", key), abs_ref, "crc32abs {key:?}");
    }
}
