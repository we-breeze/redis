//! Allocation-free Redis argument encoding.

use bytes::Bytes as SharedBytes;

use crate::{ErrorKind, RedisError, RedisResult, to_args::Bytes as OwnedBytes};

const INLINE_ENCODED_ARG_CAPACITY: usize = 48;

/// Destination for the payload bytes of one Redis bulk argument.
///
/// The request encoder uses a frame sink while sharding may use the same
/// encoded bytes through a hash sink. Framing (`$<length>\r\n...\r\n`) is not
/// part of this sink.
pub trait RedisArgSink {
    fn write(&mut self, bytes: &[u8]);
}

/// Encode exactly one binary-safe Redis argument.
///
/// `encode` must write exactly `encoded_len` bytes and must be deterministic:
/// a key may be encoded once for routing and again for the final request.
pub trait EncodeRedisArg: Sync {
    fn encoded_len(&self) -> usize;

    fn encode<S>(&self, sink: &mut S) -> RedisResult<()>
    where
        S: RedisArgSink + ?Sized;

    /// Fast path for values that already occupy one contiguous byte slice.
    /// Custom encoders may leave this as `None`.
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        None
    }
}

/// Destination for a sequence of complete Redis arguments.
pub trait RedisArgsSink {
    fn write_arg<A>(&mut self, arg: &A) -> RedisResult<()>
    where
        A: EncodeRedisArg + ?Sized;
}

/// Encode zero or more complete Redis arguments without collecting them.
pub trait EncodeRedisArgs: Sync {
    fn num_args(&self) -> usize;

    fn encode_args<S>(&self, sink: &mut S) -> RedisResult<()>
    where
        S: RedisArgsSink + ?Sized;
}

impl EncodeRedisArg for str {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write(self.as_bytes());
        Ok(())
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }
}

impl EncodeRedisArg for String {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_str().encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }
}

impl EncodeRedisArg for [u8] {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write(self);
        Ok(())
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl<const N: usize> EncodeRedisArg for [u8; N] {
    #[inline]
    fn encoded_len(&self) -> usize {
        N
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_slice().encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl EncodeRedisArg for Vec<u8> {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_slice().encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl EncodeRedisArg for SharedBytes {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_ref().encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl EncodeRedisArg for OwnedBytes {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.0.as_slice().encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        Some(&self.0)
    }
}

impl<T: EncodeRedisArg + ?Sized> EncodeRedisArg for &T {
    #[inline]
    fn encoded_len(&self) -> usize {
        (**self).encoded_len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        (**self).encode(sink)
    }

    #[inline]
    fn as_encoded_bytes(&self) -> Option<&[u8]> {
        (**self).as_encoded_bytes()
    }
}

impl EncodeRedisArg for bool {
    #[inline]
    fn encoded_len(&self) -> usize {
        1
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write(if *self { b"1" } else { b"0" });
        Ok(())
    }
}

macro_rules! integer_args {
    ($($type:ty),* $(,)?) => {$(
        impl EncodeRedisArg for $type {
            #[inline]
            fn encoded_len(&self) -> usize {
                let mut buffer = itoa::Buffer::new();
                buffer.format(*self).len()
            }

            #[inline]
            fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
                let mut buffer = itoa::Buffer::new();
                sink.write(buffer.format(*self).as_bytes());
                Ok(())
            }
        }
    )* };
}
integer_args!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

macro_rules! float_args {
    ($($type:ty),* $(,)?) => {$(
        impl EncodeRedisArg for $type {
            #[inline]
            fn encoded_len(&self) -> usize {
                let mut buffer = ryu::Buffer::new();
                buffer.format(*self).len()
            }

            #[inline]
            fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
                let mut buffer = ryu::Buffer::new();
                sink.write(buffer.format(*self).as_bytes());
                Ok(())
            }
        }
    )* };
}
float_args!(f32, f64);

/// Two components concatenated into one binary-safe Redis key.
///
/// No separator is inserted. `RedisKey2("u:", 42)` encodes as `u:42`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisKey2<A, B>(pub A, pub B);

impl<A: EncodeRedisArg, B: EncodeRedisArg> EncodeRedisArg for RedisKey2<A, B> {
    #[inline]
    fn encoded_len(&self) -> usize {
        self.0.encoded_len() + self.1.encoded_len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.0.encode(sink)?;
        self.1.encode(sink)
    }
}

/// Three components concatenated into one binary-safe Redis key.
///
/// No separator is inserted. `RedisKey3("u:", 42, ".suffix")` encodes as
/// `u:42.suffix`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisKey3<A, B, C>(pub A, pub B, pub C);

impl<A: EncodeRedisArg, B: EncodeRedisArg, C: EncodeRedisArg> EncodeRedisArg
    for RedisKey3<A, B, C>
{
    #[inline]
    fn encoded_len(&self) -> usize {
        self.0.encoded_len() + self.1.encoded_len() + self.2.encoded_len()
    }

    #[inline]
    fn encode<S: RedisArgSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.0.encode(sink)?;
        self.1.encode(sink)?;
        self.2.encode(sink)
    }
}

impl<A: EncodeRedisArg> EncodeRedisArgs for [A] {
    #[inline]
    fn num_args(&self) -> usize {
        self.len()
    }

    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        for arg in self {
            sink.write_arg(arg)?;
        }
        Ok(())
    }
}

impl<A: EncodeRedisArg> EncodeRedisArgs for Vec<A> {
    #[inline]
    fn num_args(&self) -> usize {
        self.len()
    }

    #[inline]
    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_slice().encode_args(sink)
    }
}

impl<A: EncodeRedisArg, const N: usize> EncodeRedisArgs for [A; N] {
    #[inline]
    fn num_args(&self) -> usize {
        N
    }

    #[inline]
    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        self.as_slice().encode_args(sink)
    }
}

impl<T: EncodeRedisArgs + ?Sized> EncodeRedisArgs for &T {
    #[inline]
    fn num_args(&self) -> usize {
        (**self).num_args()
    }

    #[inline]
    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        (**self).encode_args(sink)
    }
}

macro_rules! tuple_args {
    ($count:expr; $($name:ident),+ $(,)?) => {
        impl<$($name: EncodeRedisArg),+> EncodeRedisArgs for ($($name,)+) {
            #[inline]
            fn num_args(&self) -> usize {
                $count
            }

            #[allow(non_snake_case)]
            fn encode_args<S: RedisArgsSink + ?Sized>(
                &self,
                sink: &mut S,
            ) -> RedisResult<()> {
                let ($($name,)+) = self;
                $(sink.write_arg($name)?;)+
                Ok(())
            }
        }
    };
}
tuple_args!(1; A);
tuple_args!(2; A, B);
tuple_args!(3; A, B, C);
tuple_args!(4; A, B, C, D);

pub(crate) fn encode_arg_to_vec<A: EncodeRedisArg + ?Sized>(arg: &A) -> RedisResult<Vec<u8>> {
    struct VecSink(Vec<u8>);

    impl RedisArgSink for VecSink {
        fn write(&mut self, bytes: &[u8]) {
            self.0.extend_from_slice(bytes);
        }
    }

    let expected = arg.encoded_len();
    let mut sink = VecSink(Vec::with_capacity(expected));
    arg.encode(&mut sink)?;
    if sink.0.len() != expected {
        return Err(invalid_encoded_len());
    }
    Ok(sink.0)
}

pub(crate) enum ContiguousArg<'a> {
    Borrowed(&'a [u8]),
    Inline {
        bytes: [u8; INLINE_ENCODED_ARG_CAPACITY],
        len: u8,
    },
    Heap(Vec<u8>),
}

impl AsRef<[u8]> for ContiguousArg<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Inline { bytes, len } => &bytes[..usize::from(*len)],
            Self::Heap(bytes) => bytes,
        }
    }
}

pub(crate) fn encode_arg_contiguous<A: EncodeRedisArg + ?Sized>(
    arg: &A,
) -> RedisResult<ContiguousArg<'_>> {
    if let Some(bytes) = arg.as_encoded_bytes() {
        return Ok(ContiguousArg::Borrowed(bytes));
    }

    let expected = arg.encoded_len();
    if expected > INLINE_ENCODED_ARG_CAPACITY {
        return encode_arg_to_vec(arg).map(ContiguousArg::Heap);
    }

    struct InlineSink<'a> {
        bytes: &'a mut [u8],
        offset: usize,
        overflowed: bool,
    }

    impl RedisArgSink for InlineSink<'_> {
        fn write(&mut self, bytes: &[u8]) {
            let Some(end) = self.offset.checked_add(bytes.len()) else {
                self.overflowed = true;
                return;
            };
            let Some(destination) = self.bytes.get_mut(self.offset..end) else {
                self.overflowed = true;
                return;
            };
            destination.copy_from_slice(bytes);
            self.offset = end;
        }
    }

    let mut bytes = [0_u8; INLINE_ENCODED_ARG_CAPACITY];
    let mut sink = InlineSink {
        bytes: &mut bytes[..expected],
        offset: 0,
        overflowed: false,
    };
    arg.encode(&mut sink)?;
    if sink.overflowed || sink.offset != expected {
        return Err(invalid_encoded_len());
    }

    Ok(ContiguousArg::Inline {
        bytes,
        len: expected as u8,
    })
}

fn invalid_encoded_len() -> RedisError {
    RedisError::new(
        ErrorKind::ClientError,
        "EncodeRedisArg wrote a different number of bytes than encoded_len",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_arguments_encode_without_intermediate_ownership_changes() {
        assert_eq!(encode_arg_to_vec("key").unwrap(), b"key");
        assert_eq!(encode_arg_to_vec(&42_i64).unwrap(), b"42");
        assert_eq!(encode_arg_to_vec(&true).unwrap(), b"1");
        assert_eq!(
            SharedBytes::from_static(b"value").as_encoded_bytes(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn redis_key_components_form_one_composite_key() {
        assert_eq!(
            encode_arg_to_vec(&RedisKey3("u:", 12345_u64, ".suffix")).unwrap(),
            b"u:12345.suffix"
        );
        assert_eq!(
            encode_arg_to_vec(&RedisKey3(RedisKey2("tenant:", 7_u32), ":user:", 42_i64)).unwrap(),
            b"tenant:7:user:42"
        );
    }

    #[test]
    fn routing_encoding_borrows_or_uses_inline_storage_for_common_keys() {
        assert!(matches!(
            encode_arg_contiguous("already-contiguous").unwrap(),
            ContiguousArg::Borrowed(_)
        ));

        let encoded = encode_arg_contiguous(&RedisKey3("u:", 12345_u64, ".suffix")).unwrap();
        assert!(matches!(encoded, ContiguousArg::Inline { .. }));
        assert_eq!(encoded.as_ref(), b"u:12345.suffix");

        let long = "x".repeat(INLINE_ENCODED_ARG_CAPACITY + 1);
        let key = RedisKey3("", 1_u8, long);
        let encoded = encode_arg_contiguous(&key).unwrap();
        assert!(matches!(encoded, ContiguousArg::Heap(_)));
    }

    #[test]
    fn argument_sequences_keep_count_and_order() {
        struct Collector(Vec<Vec<u8>>);

        impl RedisArgsSink for Collector {
            fn write_arg<A: EncodeRedisArg + ?Sized>(&mut self, arg: &A) -> RedisResult<()> {
                self.0.push(encode_arg_to_vec(arg)?);
                Ok(())
            }
        }

        let args = ["value", "compress", "hash"];
        let mut collector = Collector(Vec::new());
        args.encode_args(&mut collector).unwrap();
        assert_eq!(args.num_args(), 3);
        assert_eq!(
            collector.0,
            vec![b"value".to_vec(), b"compress".to_vec(), b"hash".to_vec()]
        );

        let args = ("field", 42_i64);
        let mut collector = Collector(Vec::new());
        args.encode_args(&mut collector).unwrap();
        assert_eq!(args.num_args(), 2);
        assert_eq!(collector.0, vec![b"field".to_vec(), b"42".to_vec()]);
    }
}
