//! Typed and lazily iterated Redis bulk responses.

use std::{marker::PhantomData, ops::Range};

use brz_net::RxFrame;
use bytes::Bytes;

use crate::{ErrorKind, RedisError, RedisResult};

/// Binary-safe bytes returned by Redis.
pub type RedisBytes = Bytes;

/// Convert one non-nil Redis bulk string into an application value.
///
/// Nil is represented by the surrounding `Option` returned by Redis commands,
/// so implementations only receive an actual payload here. Taking `Bytes` by
/// value preserves the zero-copy path for consumers that keep it as bytes.
pub trait FromRedisBulk: Sized {
    fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self>;
}

impl FromRedisBulk for RedisBytes {
    #[inline]
    fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self> {
        Ok(value)
    }
}

impl FromRedisBulk for Vec<u8> {
    #[inline]
    fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self> {
        Ok(value.to_vec())
    }
}

impl FromRedisBulk for String {
    fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self> {
        std::str::from_utf8(&value)
            .map(str::to_owned)
            .map_err(|_| type_error("reply is not valid UTF-8"))
    }
}

impl FromRedisBulk for bool {
    #[inline]
    fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self> {
        Ok(value.as_ref() == b"1")
    }
}

macro_rules! parsed_bulk {
    ($($type:ty),* $(,)?) => {$(
        impl FromRedisBulk for $type {
            fn from_redis_bulk(value: RedisBytes) -> RedisResult<Self> {
                let value = std::str::from_utf8(&value)
                    .map_err(|_| type_error("reply is not valid UTF-8"))?;
                value
                    .trim()
                    .parse::<$type>()
                    .map_err(|_| type_error("reply has the wrong numeric type"))
            }
        }
    )*};
}
parsed_bulk!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64,);

fn type_error(message: &'static str) -> RedisError {
    RedisError::new(ErrorKind::TypeError, message)
}

/// Lazy, ordered values returned by `HMGET` and similar commands.
///
/// The complete RESP response has already been validated and detached from
/// the connection. Iteration only materializes the requested bulk slices; it
/// never performs socket I/O and cannot hold up the Redis FIFO session.
pub struct RedisValues<R> {
    source: RedisValuesSource,
    marker: PhantomData<fn() -> R>,
}

#[derive(Debug)]
pub(crate) enum RedisValuesSource {
    #[cfg(test)]
    Materialized(std::vec::IntoIter<Option<RedisBytes>>),
    Contiguous {
        frame: RedisBytes,
        cursor: usize,
        remaining: usize,
    },
    Wrapped {
        frame: RxFrame,
        cursor: usize,
        remaining: usize,
    },
}

impl<R> RedisValues<R> {
    #[cfg(test)]
    pub(crate) fn materialized(values: Vec<Option<RedisBytes>>) -> Self {
        Self::from_source(RedisValuesSource::Materialized(values.into_iter()))
    }

    pub(crate) fn from_source(source: RedisValuesSource) -> Self {
        Self {
            source,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.source.remaining()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<R: FromRedisBulk> Iterator for RedisValues<R> {
    type Item = RedisResult<Option<R>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source
            .next_bytes()
            .map(|value| value.map(R::from_redis_bulk).transpose())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

impl<R: FromRedisBulk> ExactSizeIterator for RedisValues<R> {}

impl RedisValuesSource {
    fn remaining(&self) -> usize {
        match self {
            #[cfg(test)]
            Self::Materialized(values) => values.len(),
            Self::Contiguous { remaining, .. } | Self::Wrapped { remaining, .. } => *remaining,
        }
    }

    fn next_bytes(&mut self) -> Option<Option<RedisBytes>> {
        match self {
            #[cfg(test)]
            Self::Materialized(values) => values.next(),
            Self::Contiguous {
                frame,
                cursor,
                remaining,
            } => {
                if *remaining == 0 {
                    return None;
                }
                let range = next_bulk_range(frame.len(), cursor, |index| frame[index]);
                *remaining -= 1;
                Some(range.map(|range| frame.slice(range)))
            }
            Self::Wrapped {
                frame,
                cursor,
                remaining,
            } => {
                if *remaining == 0 {
                    return None;
                }
                let range = next_bulk_range(frame.len(), cursor, |index| {
                    frame.byte(index).expect("validated Redis response byte")
                });
                *remaining -= 1;
                Some(range.map(|range| frame.copy_range(range)))
            }
        }
    }
}

/// Parse one already-validated `$<length>\r\n<body>\r\n` element.
fn next_bulk_range(
    frame_len: usize,
    cursor: &mut usize,
    byte: impl Fn(usize) -> u8,
) -> Option<Range<usize>> {
    assert!(*cursor < frame_len && byte(*cursor) == b'$');
    let length_start = *cursor + 1;
    let mut line_end = length_start;
    while byte(line_end) != b'\r' || byte(line_end + 1) != b'\n' {
        line_end += 1;
        assert!(line_end + 1 < frame_len);
    }

    let negative = byte(length_start) == b'-';
    let digits = if negative {
        length_start + 1..line_end
    } else {
        length_start..line_end
    };
    let mut length = 0_usize;
    for index in digits {
        length = length * 10 + usize::from(byte(index) - b'0');
    }

    let body_start = line_end + 2;
    if negative {
        *cursor = body_start;
        return None;
    }

    let body_end = body_start + length;
    debug_assert_eq!(byte(body_end), b'\r');
    debug_assert_eq!(byte(body_end + 1), b'\n');
    *cursor = body_end + 2;
    Some(body_start..body_end)
}

#[cfg(test)]
mod tests {
    use brz_net::RxBuffer;

    use super::*;

    #[test]
    fn materialized_values_decode_lazily() {
        let mut values = RedisValues::<i64>::materialized(vec![
            Some(Bytes::from_static(b"7")),
            None,
            Some(Bytes::from_static(b"9")),
        ]);

        assert_eq!(values.len(), 3);
        assert_eq!(values.next().unwrap().unwrap(), Some(7));
        assert_eq!(values.next().unwrap().unwrap(), None);
        assert_eq!(values.next().unwrap().unwrap(), Some(9));
        assert!(values.next().is_none());
    }

    #[test]
    fn wrapped_response_iterates_without_a_range_vector() {
        let mut buffer = RxBuffer::with_capacity(32);
        buffer
            .extend_from_slice(b"*3\r\n$1\r\na\r\n$-1\r\n$2\r\nbb\r\n")
            .unwrap();
        let frame = buffer.take(buffer.len());
        let source = RedisValuesSource::Wrapped {
            frame,
            cursor: 4,
            remaining: 3,
        };
        let values = RedisValues::<Bytes>::from_source(source)
            .collect::<RedisResult<Vec<_>>>()
            .unwrap();

        assert_eq!(values[0].as_deref(), Some(b"a".as_slice()));
        assert_eq!(values[1], None);
        assert_eq!(values[2].as_deref(), Some(b"bb".as_slice()));
    }
}
