//! Hash algorithms, ported 1:1 from breeze `sharding/src/hash`.
//!
//! Every algorithm maps a key to an `i64` (may be negative for some
//! algorithms — distributions handle that exactly like the mesh does).

/// Hash key of the whole key, truncated to a short afterwards.
const CRC32_EXT_SHORT: &str = "short";
/// Hash key is a number.
const CRC32_EXT_NUM: &str = "num";
/// Hash key is the first run of digits at least 5 long.
const CRC32_EXT_SMARTNUM: &str = "smartnum";
/// smartnum hash key minimum length.
const SMARTNUM_MIN_LEN: usize = 5;
/// Hash key is the concatenation of all digit runs.
const CRC32_EXT_MIXNUM: &str = "mixnum";

const KEY_DELIMITER_POINT: &str = "point";
const KEY_DELIMITER_POUND: &str = "pound";
const KEY_DELIMITER_UNDERSCORE: &str = "underscore";
const KEY_DELIMITER_NONE: u8 = 0;

const NAME_RAWSUFFIX: &str = "rawsuffix";

/// Placeholder hash for framework compatibility (mq etc.).
pub const HASH_PADDING: &str = "padding";

/// All supported hash algorithms. Construct with [`Hasher::from`].
#[derive(Debug, Clone)]
pub enum Hasher {
    Padding,
    /// redis raw: a numeric (or numeric-prefixed) key is its own hash.
    Raw,
    Bkdr,
    Bkdrsub(u8),
    /// bkdr over the leading digits, then abs, then crc32 over the digits of
    /// that absolute value.
    BkdrAbsCrc32,
    Crc32,
    /// mc short crc32.
    Crc32Short,
    /// crc32 over digits starting at `start_pos`.
    Crc32Num {
        start_pos: usize,
    },
    /// crc32 over the first digit run of length >= 5.
    Crc32SmartNum,
    /// crc32 over all digits concatenated.
    Crc32MixNum,
    /// crc32 over `key[start_pos..]` up to `delimiter`.
    Crc32Delimiter {
        start_pos: usize,
        delimiter: u8,
    },
    /// reference-library `Util.crc32()` compatible (i32 abs).
    Crc32local,
    Crc32localDelimiter {
        start_pos: usize,
        delimiter: u8,
    },
    Crc32localSmartNum,
    /// `Util.crc32(Longs.toByteArray(id))` compatible.
    LBCrc32localDelimiter,
    /// leading `uid_` number verbatim, else crc32local.
    Rawcrc32local,
    /// i32-based crc32 then abs.
    Crc32Abs,
    Crc32AbsDelimiter {
        start_pos: usize,
        delimiter: u8,
    },
    Crc64,
    Fnv1aF64,
    Random,
    /// digits after a single delimiter, verbatim.
    RawSuffix {
        delimiter: u8,
    },
    Fnv1F32,
}

impl Default for Hasher {
    #[inline]
    fn default() -> Self {
        Self::Crc32Short
    }
}

impl Hasher {
    /// Normalize an algorithm name: lowercase, strip legacy `-range`, map
    /// `-id` to `-num`.
    fn reconcile_name(alg: &str) -> String {
        let mut alg_lower = alg.to_ascii_lowercase();
        if alg_lower.contains("-range") {
            alg_lower = alg_lower.replace("-range", "");
        }
        if alg_lower.contains("-id") {
            alg_lower = alg_lower.replace("-id", "-num");
        }
        alg_lower
    }

    /// Build a hasher from its configuration name, e.g. `crc32`,
    /// `crc32-underscore`, `crc32-num-5`, `bkdrsub`, `rawsuffix-point`.
    /// Unknown names fall back to the mesh's defaults (`crc32-short` for
    /// simple names, `crc32` for unknown extended names).
    pub fn from(alg: &str) -> Self {
        let alg_lower = Hasher::reconcile_name(alg);
        let alg_parts: Vec<&str> = alg_lower.split('-').collect();

        if alg_parts.len() == 1 {
            return match alg_parts[0] {
                HASH_PADDING => Self::Padding,
                "bkdr" => Self::Bkdr,
                "bkdrsub" => Self::Bkdrsub(b'_'),
                "bkdrsubhat" => Self::Bkdrsub(b'^'),
                "bkdrabscrc32" => Self::BkdrAbsCrc32,
                "raw" => Self::Raw,
                "crc32" => Self::Crc32,
                "crc32local" => Self::Crc32local,
                "rawcrc32local" => Self::Rawcrc32local,
                "lbcrc32local" => Self::LBCrc32localDelimiter,
                "crc32abs" => Self::Crc32Abs,
                "crc64" => Self::Crc64,
                "random" => Self::Random,
                "fnv1_32" => Self::Fnv1F32,
                "fnv1a_64" => Self::Fnv1aF64,
                _ => {
                    tracing::warn!("unknown hash:{alg}, use crc32-short instead");
                    Self::Crc32Short
                }
            };
        }

        debug_assert!(alg_parts.len() == 2 || alg_parts.len() == 3);
        match alg_parts[0] {
            "crc32" => match alg_parts[1] {
                CRC32_EXT_SHORT => Self::Crc32Short,
                CRC32_EXT_NUM => Self::Crc32Num {
                    start_pos: parse_prefix_len(&alg_parts),
                },
                CRC32_EXT_SMARTNUM => Self::Crc32SmartNum,
                CRC32_EXT_MIXNUM => Self::Crc32MixNum,
                _ => Self::Crc32Delimiter {
                    start_pos: parse_prefix_len(&alg_parts),
                    delimiter: key_delimiter_name_2u8(&alg_lower, alg_parts[1]),
                },
            },
            "crc32abs" => Self::Crc32AbsDelimiter {
                start_pos: parse_prefix_len(&alg_parts),
                delimiter: key_delimiter_name_2u8(&alg_lower, alg_parts[1]),
            },
            "crc32local" => match alg_parts[1] {
                CRC32_EXT_SMARTNUM => Self::Crc32localSmartNum,
                _ => Self::Crc32localDelimiter {
                    start_pos: parse_prefix_len(&alg_parts),
                    delimiter: key_delimiter_name_2u8(&alg_lower, alg_parts[1]),
                },
            },
            NAME_RAWSUFFIX => Self::RawSuffix {
                delimiter: key_delimiter_name_2u8(&alg_lower, alg_parts[1]),
            },
            _ => {
                tracing::warn!("unknown hash: {alg}, use crc32 instead");
                Self::Crc32
            }
        }
    }

    /// Hash `key`. May return negative values for crc64/fnv variants.
    #[inline]
    pub fn hash(&self, key: &[u8]) -> i64 {
        match self {
            Hasher::Padding => {
                tracing::warn!("padding hash used");
                0
            }
            Hasher::Raw => raw_hash(key),
            Hasher::Bkdr => bkdr(key),
            Hasher::Bkdrsub(delimiter) => bkdrsub(key, *delimiter),
            Hasher::BkdrAbsCrc32 => bkdr_abs_crc32(key),
            Hasher::Crc32 => crc32(key),
            Hasher::Crc32Short => crc32_short(key),
            Hasher::Crc32Num { start_pos } => crc32_num(key, *start_pos),
            Hasher::Crc32SmartNum => {
                let (start, end) = parse_smartnum_hashkey(key);
                crc32_span(key, start, end)
            }
            Hasher::Crc32MixNum => crc32_mixnum(key),
            Hasher::Crc32Delimiter {
                start_pos,
                delimiter,
            } => crc32_delimiter(key, *start_pos, *delimiter),
            Hasher::Crc32local => crc32local(key),
            Hasher::Crc32localDelimiter {
                start_pos,
                delimiter,
            } => crc32local_delimiter(key, *start_pos, *delimiter),
            Hasher::Crc32localSmartNum => {
                let (start, end) = parse_smartnum_hashkey(key);
                crc32local_span(key, start, end)
            }
            Hasher::LBCrc32localDelimiter => lbcrc32local(key),
            Hasher::Rawcrc32local => rawcrc32local(key),
            Hasher::Crc32Abs => crc32_abs(key),
            Hasher::Crc32AbsDelimiter {
                start_pos,
                delimiter,
            } => crc32_abs_delimiter(key, *start_pos, *delimiter),
            Hasher::Crc64 => crc64(key),
            Hasher::Fnv1aF64 => fnv1a_64(key),
            Hasher::Random => rand::random::<u32>() as i64,
            Hasher::RawSuffix { delimiter } => raw_suffix(key, *delimiter),
            Hasher::Fnv1F32 => fnv1_32(key),
        }
    }
}

fn key_delimiter_name_2u8(alg: &str, delimiter_name: &str) -> u8 {
    match delimiter_name {
        KEY_DELIMITER_POINT => b'.',
        KEY_DELIMITER_UNDERSCORE => b'_',
        KEY_DELIMITER_POUND => b'#',
        _ => {
            tracing::warn!("unknown hash alg: {alg}, delimiter disabled");
            KEY_DELIMITER_NONE
        }
    }
}

/// Third name segment is an optional fixed-prefix length.
fn parse_prefix_len(alg_parts: &[&str]) -> usize {
    if alg_parts.len() == 3 {
        if let Ok(prefix_len) = alg_parts[2].parse::<usize>() {
            return prefix_len;
        }
        tracing::debug!("unknown hash name {:?}, ignore prefix", alg_parts);
    }
    0
}

// ---- plain hashes ----

fn bkdr(key: &[u8]) -> i64 {
    let mut h = 0i32;
    let seed = 31i32;
    for &c in key {
        h = h.wrapping_mul(seed).wrapping_add(c as i32);
    }
    if h < 0 {
        h = h.wrapping_mul(-1);
    }
    h as i64
}

/// Hash key is after `#` and before `delimiter` (or the end).
fn bkdrsub(key: &[u8], delimiter: u8) -> i64 {
    const SEED: i32 = 131;
    let mut hash = 0i32;
    let mut found_start = false;
    for &c in key {
        if found_start {
            if c != delimiter {
                hash = hash.wrapping_mul(SEED).wrapping_add(c as i32);
                continue;
            }
            break;
        } else if c == b'#' {
            found_start = true;
        }
    }
    (hash & 0x7FFFFFFF) as i64
}

/// bkdr over the leading digits, abs, then crc32 over the decimal string.
fn bkdr_abs_crc32(key: &[u8]) -> i64 {
    let mut len = 0;
    for &c in key {
        if c.is_ascii_digit() {
            len += 1;
        } else {
            break;
        }
    }
    if len == 0 {
        tracing::warn!("malformed bkdrabscrc32 key: {key:?}");
        return 0;
    }
    let hash = bkdr(&key[..len]);
    crc32(hash.unsigned_abs().to_string().as_bytes())
}

/// A numeric (or numeric-prefixed) key is its own hash.
fn raw_hash(key: &[u8]) -> i64 {
    let mut hash = 0i64;
    for &c in key {
        if !c.is_ascii_digit() {
            return hash;
        }
        hash = hash * 10 + (c - b'0') as i64;
    }
    if hash <= 0 {
        tracing::error!("malformed raw hash/{hash} for key: {key:?}");
    }
    hash
}

/// Digits after a single delimiter; any non-digit suffix yields 0.
fn raw_suffix(key: &[u8], delimiter: u8) -> i64 {
    let mut hash = 0i64;
    let mut found_delimiter = false;
    for &b in key {
        if !found_delimiter {
            if b == delimiter {
                found_delimiter = true;
            }
            continue;
        }
        if !b.is_ascii_digit() {
            return 0;
        }
        hash = hash.wrapping_mul(10) + (b - b'0') as i64;
    }
    hash
}

fn fnv1_32(key: &[u8]) -> i64 {
    const FNV_32_INIT: u32 = 2166136261;
    const FNV_32_PRIME: u32 = 16777619;
    let mut hash = FNV_32_INIT;
    for &c in key {
        hash = hash.wrapping_mul(FNV_32_PRIME);
        hash ^= c as u32;
    }
    hash as i64
}

fn fnv1a_64(key: &[u8]) -> i64 {
    // Truncated to u32 arithmetic, exactly like the mesh implementation.
    const FNV_64_INIT: u64 = 0xcbf29ce484222325;
    const FNV_64_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_64_INIT as u32;
    for &c in key {
        hash ^= c as u32;
        hash = hash.wrapping_mul(FNV_64_PRIME as u32);
    }
    hash as i64
}

// ---- crc32 family (JDK-compatible, i64 arithmetic) ----

const CRC_SEED: i64 = 0xFFFFFFFF;

/// The shared JDK-crc32 loop over a byte span.
#[inline]
fn crc32_bytes(bytes: &[u8]) -> i64 {
    let mut crc: i64 = CRC_SEED;
    for &c in bytes {
        crc = ((crc >> 8) & 0x00FFFFFF) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
    }
    crc ^= CRC_SEED;
    crc & CRC_SEED
}

fn crc32(key: &[u8]) -> i64 {
    let crc = crc32_bytes(key);
    if crc <= 0 {
        tracing::warn!("crc32 - negative hash/{crc} for key/{key:?}");
    }
    crc
}

fn crc32_short(key: &[u8]) -> i64 {
    let crc = crc32_bytes(key);
    (crc >> 16) & 0x7fff
}

fn crc32_num(key: &[u8], start_pos: usize) -> i64 {
    let mut crc: i64 = CRC_SEED;
    for &c in &key[start_pos.min(key.len())..] {
        if !c.is_ascii_digit() {
            break;
        }
        crc = ((crc >> 8) & 0x00FFFFFF) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
    }
    crc ^= CRC_SEED;
    crc & CRC_SEED
}

fn crc32_delimiter(key: &[u8], start_pos: usize, delimiter: u8) -> i64 {
    let mut crc: i64 = CRC_SEED;
    let check = delimiter != KEY_DELIMITER_NONE;
    for &c in &key[start_pos.min(key.len())..] {
        if check && c == delimiter {
            break;
        }
        crc = ((crc >> 8) & 0x00FFFFFF) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
    }
    crc ^= CRC_SEED;
    crc & CRC_SEED
}

fn crc32_span(key: &[u8], start: usize, end: usize) -> i64 {
    crc32_bytes(&key[start..end])
}

/// First digit run of length >= [`SMARTNUM_MIN_LEN`]; malformed keys fall
/// back to the whole key.
fn parse_smartnum_hashkey(key: &[u8]) -> (usize, usize) {
    let mut start = usize::MAX;
    let mut end = usize::MAX;
    for (i, &c) in key.iter().enumerate() {
        if c.is_ascii_digit() && start == usize::MAX {
            start = i;
        } else if start != usize::MAX && !c.is_ascii_digit() {
            end = i;
            if end - start >= SMARTNUM_MIN_LEN {
                break;
            }
            start = usize::MAX;
            end = usize::MAX;
        }
    }
    if start != usize::MAX && end == usize::MAX {
        end = key.len();
    }
    if start == usize::MAX || (end - start) < SMARTNUM_MIN_LEN {
        tracing::error!("malformed smartnum key: {key:?}");
        return (0, key.len());
    }
    (start, end)
}

fn crc32_mixnum(key: &[u8]) -> i64 {
    let mut crc: i64 = CRC_SEED;
    for &c in key {
        if c.is_ascii_digit() {
            crc = ((crc >> 8) & 0x00FFFFFF) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
        }
    }
    crc ^= CRC_SEED;
    crc & CRC_SEED
}

fn crc32_abs(key: &[u8]) -> i64 {
    let crc = crc32_bytes(key) as i32;
    // wrapping_abs: keeps i32::MIN from panicking; the mesh would overflow in
    // the same spot, and every other value matches `.abs()` exactly.
    crc.wrapping_abs() as i64
}

fn crc32_abs_delimiter(key: &[u8], start_pos: usize, delimiter: u8) -> i64 {
    let crc = crc32_delimiter(key, start_pos, delimiter) as i32;
    crc.wrapping_abs() as i64
}

// ---- crc32local family (reference-library Util.crc32 compatible) ----

/// The crc32local loop: no `& 0x00FFFFFF` mask on the shift.
#[inline]
fn crc32local_bytes(bytes: &[u8]) -> i64 {
    let mut crc: i64 = CRC_SEED;
    for &c in bytes {
        crc = (crc >> 8) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
    }
    crc ^ CRC_SEED
}

fn crc32local(key: &[u8]) -> i64 {
    (crc32local_bytes(key) as i32).wrapping_abs() as i64
}

fn crc32local_delimiter(key: &[u8], start_pos: usize, delimiter: u8) -> i64 {
    let mut crc: i64 = CRC_SEED;
    let check = delimiter != KEY_DELIMITER_NONE;
    for &c in &key[start_pos.min(key.len())..] {
        if check && c == delimiter {
            break;
        }
        crc = (crc >> 8) ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
    }
    ((crc ^ CRC_SEED) as i32).wrapping_abs() as i64
}

fn crc32local_span(key: &[u8], start: usize, end: usize) -> i64 {
    (crc32local_bytes(&key[start..end]) as i32).wrapping_abs() as i64
}

/// Parse leading digits as a u64, then crc32local over its big-endian bytes.
fn lbcrc32local(key: &[u8]) -> i64 {
    let mut hkey = 0u64;
    for &c in key {
        if !c.is_ascii_digit() {
            break;
        }
        hkey = hkey.wrapping_mul(10) + (c - b'0') as u64;
    }
    let bytes = hkey.to_be_bytes();
    (crc32local_bytes(&bytes) as i32).wrapping_abs() as i64
}

/// `uid_xxx` → uid verbatim; pure digits → the number; else crc32local.
fn rawcrc32local(key: &[u8]) -> i64 {
    let mut hash = 0i64;
    for &c in key {
        if !c.is_ascii_digit() {
            if c == b'_' {
                return hash;
            }
            hash = 0;
            break;
        }
        hash = hash.wrapping_mul(10) + (c - b'0') as i64;
    }
    if hash == 0 {
        crc32local(key)
    } else if hash > 0 {
        hash
    } else {
        tracing::error!("malformed rawcrc32local for key: {key:?}");
        0
    }
}

// ---- crc64 ----

fn crc64(key: &[u8]) -> i64 {
    let mut crc: u64 = CRC64_TABLE[0];
    for &c in key {
        crc = CRC64_TABLE[((crc ^ c as u64) & 0xff) as usize] ^ (crc >> 8);
    }
    crc as i64
}

include!("crc32_table.rs");
include!("crc64_table.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_parsing() {
        assert!(matches!(Hasher::from("crc32"), Hasher::Crc32));
        assert!(matches!(Hasher::from("CRC32"), Hasher::Crc32));
        assert!(matches!(
            Hasher::from("crc32-range-underscore"),
            Hasher::Crc32Delimiter {
                start_pos: 0,
                delimiter: b'_'
            }
        ));
        assert!(matches!(
            Hasher::from("crc32-id"),
            Hasher::Crc32Num { start_pos: 0 }
        ));
        assert!(matches!(
            Hasher::from("crc32-num-5"),
            Hasher::Crc32Num { start_pos: 5 }
        ));
        assert!(matches!(
            Hasher::from("crc32-point-3"),
            Hasher::Crc32Delimiter {
                start_pos: 3,
                delimiter: b'.'
            }
        ));
        assert!(matches!(Hasher::from("bkdrsub"), Hasher::Bkdrsub(b'_')));
        assert!(matches!(
            Hasher::from("rawsuffix-pound"),
            Hasher::RawSuffix { delimiter: b'#' }
        ));
    }

    #[test]
    fn known_vectors() {
        // Values cross-checked against the breeze sharding crate.
        assert_eq!(Hasher::from("bkdr").hash(b"abc"), 96354);
        assert_eq!(Hasher::from("raw").hash(b"12345abc"), 12345);
        assert_eq!(Hasher::from("crc32").hash(b"abc"), 891568578);
        assert_eq!(
            Hasher::from("crc32-underscore").hash(b"abc_def"),
            Hasher::from("crc32").hash(b"abc")
        );
        assert_eq!(
            Hasher::from("crc32-num-3").hash(b"uid12345x"),
            Hasher::from("crc32").hash(b"12345")
        );
        assert_eq!(
            Hasher::from("crc32-smartnum").hash(b"abc12345678def"),
            Hasher::from("crc32").hash(b"12345678")
        );
        assert_eq!(
            Hasher::from("crc32-mixnum").hash(b"a_123_456_bc"),
            Hasher::from("crc32").hash(b"123456")
        );
        assert_eq!(Hasher::from("rawsuffix-point").hash(b"abc.789"), 789);
        assert_eq!(Hasher::from("bkdrsub").hash(b"abc#123_456"), 847490);
        assert_eq!(Hasher::from("rawcrc32local").hash(b"12345_xx"), 12345);
        assert_eq!(
            Hasher::from("lbcrc32local").hash(b"12345"),
            crc32local(&12345u64.to_be_bytes())
        );
    }
}
