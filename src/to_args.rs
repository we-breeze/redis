//! Converting Rust values into Redis command arguments.
//!
//! [`ToRedisArgs`] writes one or more binary-safe arguments into a
//! [`RedisWrite`] sink (the command buffer), mirroring the `redis-rs` design.
//! [`ToSingleRedisArg`] is a marker for types that always produce exactly one
//! argument, letting single-key/-value commands reject multi-arg inputs at
//! compile time.

use std::collections::{BTreeMap, HashMap};

/// A sink that command arguments are written into.
pub trait RedisWrite {
    /// Append one complete argument.
    fn write_arg(&mut self, arg: &[u8]);

    /// Append one argument produced via [`std::fmt::Display`], avoiding an
    /// intermediate allocation for numbers.
    fn write_arg_fmt(&mut self, arg: impl std::fmt::Display) {
        self.write_arg(arg.to_string().as_bytes());
    }
}

impl RedisWrite for Vec<Vec<u8>> {
    fn write_arg(&mut self, arg: &[u8]) {
        self.push(arg.to_vec());
    }
}

/// A type that can be turned into one or more Redis arguments.
pub trait ToRedisArgs {
    /// Write this value's argument(s) into `out`.
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W);

    /// Collect this value's arguments into a fresh vector. Convenience for
    /// tests and callers that don't have a sink handy.
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        self.write_redis_args(&mut out);
        out
    }

    /// Number of arguments this value produces. Used by variadic commands to
    /// decide framing. Defaults to 1.
    fn num_of_args(&self) -> usize {
        1
    }
}

/// Marker trait: implementors always produce exactly one argument.
pub trait ToSingleRedisArg: ToRedisArgs {}

// --- primitive & string impls ---

macro_rules! itoa_arg {
    ($($t:ty),*) => {$(
        impl ToRedisArgs for $t {
            fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
                let mut buf = itoa::Buffer::new();
                out.write_arg(buf.format(*self).as_bytes());
            }
        }
        impl ToSingleRedisArg for $t {}
    )*};
}
itoa_arg!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

macro_rules! ryu_arg {
    ($($t:ty),*) => {$(
        impl ToRedisArgs for $t {
            fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
                let mut buf = ryu::Buffer::new();
                out.write_arg(buf.format(*self).as_bytes());
            }
        }
        impl ToSingleRedisArg for $t {}
    )*};
}
ryu_arg!(f32, f64);

impl ToRedisArgs for bool {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(if *self { b"1" } else { b"0" });
    }
}
impl ToSingleRedisArg for bool {}

impl ToRedisArgs for String {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(self.as_bytes());
    }
}
impl ToSingleRedisArg for String {}

impl ToRedisArgs for str {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(self.as_bytes());
    }
}
impl ToSingleRedisArg for str {}

impl ToRedisArgs for [u8] {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(self);
    }
}
impl ToSingleRedisArg for [u8] {}

/// A binary value that is always sent as exactly one bulk-string argument.
///
/// `Vec<T>` is treated as *variadic* (each element becomes its own argument),
/// which is what commands like `MGET`/`DEL` want. To send raw bytes as a single
/// value, wrap them in `Bytes` so there is no ambiguity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Bytes(v)
    }
}

impl ToRedisArgs for Bytes {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        out.write_arg(&self.0);
    }
}
impl ToSingleRedisArg for Bytes {}

impl<T: ?Sized + ToRedisArgs> ToRedisArgs for &T {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        (**self).write_redis_args(out);
    }
    fn num_of_args(&self) -> usize {
        (**self).num_of_args()
    }
}

impl<T: ToSingleRedisArg + ?Sized> ToSingleRedisArg for &T {}

impl<T: ToRedisArgs> ToRedisArgs for Option<T> {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        if let Some(inner) = self {
            inner.write_redis_args(out);
        }
    }
    fn num_of_args(&self) -> usize {
        match self {
            Some(inner) => inner.num_of_args(),
            None => 0,
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Vec<T> {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        for item in self {
            item.write_redis_args(out);
        }
    }
    fn num_of_args(&self) -> usize {
        self.iter().map(ToRedisArgs::num_of_args).sum()
    }
}

impl<T: ToRedisArgs, const N: usize> ToRedisArgs for [T; N] {
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        for item in self {
            item.write_redis_args(out);
        }
    }
    fn num_of_args(&self) -> usize {
        self.iter().map(ToRedisArgs::num_of_args).sum()
    }
}

macro_rules! map_args {
    ($($map:ident),*) => {$(
        impl<K: ToRedisArgs, V: ToRedisArgs> ToRedisArgs for $map<K, V> {
            fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
                for (k, v) in self {
                    k.write_redis_args(out);
                    v.write_redis_args(out);
                }
            }
            fn num_of_args(&self) -> usize {
                self.iter().map(|(k, v)| k.num_of_args() + v.num_of_args()).sum()
            }
        }
    )*};
}
map_args!(HashMap, BTreeMap);

macro_rules! tuple_args {
    ($($name:ident),+) => {
        impl<$($name: ToRedisArgs),+> ToRedisArgs for ($($name,)+) {
            #[allow(non_snake_case)]
            fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
                let ($($name,)+) = self;
                $($name.write_redis_args(out);)+
            }
            #[allow(non_snake_case)]
            fn num_of_args(&self) -> usize {
                let ($($name,)+) = self;
                0 $(+ $name.num_of_args())+
            }
        }
    };
}
tuple_args!(A);
tuple_args!(A, B);
tuple_args!(A, B, C);
tuple_args!(A, B, C, D);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_and_strings() {
        assert_eq!(42i64.to_redis_args(), vec![b"42".to_vec()]);
        assert_eq!("hi".to_redis_args(), vec![b"hi".to_vec()]);
        assert_eq!(true.to_redis_args(), vec![b"1".to_vec()]);
    }

    #[test]
    fn option_and_vec() {
        assert_eq!(None::<i64>.to_redis_args(), Vec::<Vec<u8>>::new());
        assert_eq!(Some(7i64).to_redis_args(), vec![b"7".to_vec()]);
        assert_eq!(
            vec!["a", "b"].to_redis_args(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(vec!["a", "b"].num_of_args(), 2);
    }

    #[test]
    fn tuple_pairs() {
        assert_eq!(
            ("f", 1i64).to_redis_args(),
            vec![b"f".to_vec(), b"1".to_vec()]
        );
    }
}
