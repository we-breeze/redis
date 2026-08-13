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
    assert!(matches!(Hasher::from("bkdrsub"), Hasher::BkdrsubDelimiter(_)));
    assert!(matches!(Hasher::from("rawsuffix-pound"), Hasher::RawSuffix(_)));
}

#[test]
fn known_vectors() {
    assert_eq!(hash("bkdr", b"abc"), 96354);
    assert_eq!(hash("raw", b"12345abc"), 12345);
    assert_eq!(hash("crc32", b"abc"), 891568578);
    assert_eq!(
        hash("crc32-underscore", b"abc_def"),
        hash("crc32", b"abc")
    );
    assert_eq!(hash("crc32-num-3", b"uid12345x"), hash("crc32", b"12345"));
    assert_eq!(
        hash("crc32-smartnum", b"abc12345678def"),
        hash("crc32", b"12345678")
    );
    assert_eq!(hash("crc32-mixnum", b"a_123_456_bc"), hash("crc32", b"123456"));
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
