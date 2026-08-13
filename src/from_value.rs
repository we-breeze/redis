//! Converting a parsed [`Value`] back into a typed Rust result.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use crate::error::{ErrorKind, RedisError, RedisResult};
use crate::types::Value;

/// A type that can be constructed from a Redis reply [`Value`].
pub trait FromRedisValue: Sized {
    /// Convert `value` into `Self`, or fail with a [`ErrorKind::TypeError`].
    fn from_redis_value(value: &Value) -> RedisResult<Self>;

    /// Convert a batch of reply values, e.g. the elements of an array. The
    /// default converts each element independently.
    fn from_redis_values(values: &[Value]) -> RedisResult<Vec<Self>> {
        values.iter().map(Self::from_redis_value).collect()
    }
}

fn type_error(what: &'static str) -> RedisError {
    RedisError::from_kind(ErrorKind::TypeError, what)
}

fn as_bytes(value: &Value) -> RedisResult<&[u8]> {
    match value {
        Value::BulkString(b) => Ok(b),
        Value::SimpleString(s) => Ok(s.as_bytes()),
        Value::VerbatimString { text, .. } => Ok(text.as_bytes()),
        _ => Err(type_error("expected a string reply")),
    }
}

impl FromRedisValue for Value {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        Ok(value.clone())
    }
}

impl FromRedisValue for () {
    fn from_redis_value(_value: &Value) -> RedisResult<Self> {
        Ok(())
    }
}

macro_rules! int_from_value {
    ($($t:ty),*) => {$(
        impl FromRedisValue for $t {
            fn from_redis_value(value: &Value) -> RedisResult<Self> {
                match value {
                    Value::Int(v) => <$t>::try_from(*v)
                        .map_err(|_| type_error("integer out of range")),
                    Value::Double(v) => Ok(*v as $t),
                    Value::BulkString(_) | Value::SimpleString(_) => {
                        let s = std::str::from_utf8(as_bytes(value)?)
                            .map_err(|_| type_error("reply is not valid UTF-8"))?;
                        s.trim().parse::<$t>()
                            .map_err(|_| type_error("reply is not an integer"))
                    }
                    _ => Err(type_error("expected an integer reply")),
                }
            }
        }
    )*};
}
int_from_value!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

macro_rules! float_from_value {
    ($($t:ty),*) => {$(
        impl FromRedisValue for $t {
            fn from_redis_value(value: &Value) -> RedisResult<Self> {
                match value {
                    Value::Double(v) => Ok(*v as $t),
                    Value::Int(v) => Ok(*v as $t),
                    _ => {
                        let s = std::str::from_utf8(as_bytes(value)?)
                            .map_err(|_| type_error("reply is not valid UTF-8"))?;
                        s.trim().parse::<$t>()
                            .map_err(|_| type_error("reply is not a float"))
                    }
                }
            }
        }
    )*};
}
float_from_value!(f32, f64);

impl FromRedisValue for bool {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::Int(v) => Ok(*v != 0),
            Value::Nil => Ok(false),
            Value::Okay => Ok(true),
            Value::SimpleString(s) => Ok(s == "OK" || s == "1"),
            Value::BulkString(b) => Ok(b.as_ref() == b"1"),
            _ => Err(type_error("expected a boolean-ish reply")),
        }
    }
}

impl FromRedisValue for String {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Okay => Ok("OK".to_string()),
            Value::SimpleString(s) => Ok(s.clone()),
            Value::VerbatimString { text, .. } => Ok(text.clone()),
            _ => String::from_utf8(as_bytes(value)?.to_vec())
                .map_err(|_| type_error("reply is not valid UTF-8")),
        }
    }
}

impl FromRedisValue for crate::to_args::Bytes {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        Ok(crate::to_args::Bytes(as_bytes(value)?.to_vec()))
    }
}

impl FromRedisValue for bytes::Bytes {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::BulkString(bytes) => Ok(bytes.clone()),
            Value::SimpleString(value) => Ok(bytes::Bytes::copy_from_slice(value.as_bytes())),
            Value::VerbatimString { text, .. } => {
                Ok(bytes::Bytes::copy_from_slice(text.as_bytes()))
            }
            _ => Err(type_error("expected a string reply")),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Nil => Ok(None),
            other => Ok(Some(T::from_redis_value(other)?)),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Vec<T> {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Array(items) | Value::Set(items) => T::from_redis_values(items),
            Value::Nil => Ok(Vec::new()),
            // A map decays to a flat [k, v, k, v, ...] list, matching how RESP2
            // returns HGETALL.
            Value::Map(pairs) => {
                let mut flat = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    flat.push(T::from_redis_value(k)?);
                    flat.push(T::from_redis_value(v)?);
                }
                Ok(flat)
            }
            _ => Err(type_error("expected an array reply")),
        }
    }
}

/// Interpret a reply as a sequence of key/value pairs (RESP2 flat array or
/// RESP3 map).
fn pairs_from_value<K, V>(value: &Value) -> RedisResult<Vec<(K, V)>>
where
    K: FromRedisValue,
    V: FromRedisValue,
{
    match value {
        Value::Map(pairs) => pairs
            .iter()
            .map(|(k, v)| Ok((K::from_redis_value(k)?, V::from_redis_value(v)?)))
            .collect(),
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(type_error("expected an even-length array for pairs"));
            }
            items
                .chunks_exact(2)
                .map(|pair| {
                    Ok((
                        K::from_redis_value(&pair[0])?,
                        V::from_redis_value(&pair[1])?,
                    ))
                })
                .collect()
        }
        Value::Nil => Ok(Vec::new()),
        _ => Err(type_error("expected a map or paired-array reply")),
    }
}

impl<K, V> FromRedisValue for HashMap<K, V>
where
    K: FromRedisValue + Eq + Hash,
    V: FromRedisValue,
{
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        Ok(pairs_from_value(value)?.into_iter().collect())
    }
}

impl<K, V> FromRedisValue for BTreeMap<K, V>
where
    K: FromRedisValue + Ord,
    V: FromRedisValue,
{
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        Ok(pairs_from_value(value)?.into_iter().collect())
    }
}

macro_rules! tuple_from_value {
    ($($name:ident),+) => {
        impl<$($name: FromRedisValue),+> FromRedisValue for ($($name,)+) {
            #[allow(non_snake_case)]
            fn from_redis_value(value: &Value) -> RedisResult<Self> {
                let items = match value {
                    Value::Array(items) | Value::Set(items) => items.as_slice(),
                    _ => return Err(type_error("expected an array reply for a tuple")),
                };
                let mut iter = items.iter();
                $(
                    let $name = iter
                        .next()
                        .ok_or_else(|| type_error("array too short for tuple"))?;
                    let $name = $name::from_redis_value($name)?;
                )+
                Ok(($($name,)+))
            }
        }
    };
}
tuple_from_value!(A);
tuple_from_value!(A, B);
tuple_from_value!(A, B, C);
tuple_from_value!(A, B, C, D);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn scalars() {
        assert_eq!(i64::from_redis_value(&Value::Int(5)).unwrap(), 5);
        assert_eq!(
            String::from_redis_value(&Value::BulkString(Bytes::from_static(b"hi"))).unwrap(),
            "hi"
        );
        assert_eq!(String::from_redis_value(&Value::Okay).unwrap(), "OK");
        assert!(bool::from_redis_value(&Value::Int(1)).unwrap());
    }

    #[test]
    fn options_and_vecs() {
        assert_eq!(
            Option::<String>::from_redis_value(&Value::Nil).unwrap(),
            None
        );
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(Vec::<i64>::from_redis_value(&arr).unwrap(), vec![1, 2]);
    }

    #[test]
    fn maps_from_flat_array() {
        let arr = Value::Array(vec![
            Value::BulkString(Bytes::from_static(b"f1")),
            Value::BulkString(Bytes::from_static(b"v1")),
        ]);
        let map: HashMap<String, String> = HashMap::from_redis_value(&arr).unwrap();
        assert_eq!(map.get("f1").map(String::as_str), Some("v1"));
    }

    #[test]
    fn int_out_of_range_errors() {
        let err = u8::from_redis_value(&Value::Int(-1)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TypeError);
    }
}
