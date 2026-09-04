//! Routing plans for Redis commands whose arguments contain multiple keys.

use brz_net::{ShardRouter, Sharded};

use crate::{
    Cmd, EncodeRedisArg, EncodeRedisArgs, ErrorKind, RedisArgsSink, RedisError, RedisResult, cmd,
};

pub(crate) enum KeyRoute<'a, C> {
    Single(&'a C),
    Multiple,
}

pub(crate) struct KeyListArgs<'a, K: ?Sized> {
    command: &'static str,
    keys: &'a K,
}

impl<'a, K: ?Sized> KeyListArgs<'a, K> {
    pub(crate) fn new(command: &'static str, keys: &'a K) -> Self {
        Self { command, keys }
    }
}

impl<K> EncodeRedisArgs for KeyListArgs<'_, K>
where
    K: EncodeRedisArgs + ?Sized,
{
    fn num_args(&self) -> usize {
        1_usize.saturating_add(self.keys.num_args())
    }

    fn encode_args<S: RedisArgsSink + ?Sized>(&self, sink: &mut S) -> RedisResult<()> {
        sink.write_arg(self.command)?;
        self.keys.encode_args(sink)
    }
}

pub(crate) struct ShardCommand<'a, C> {
    pub(crate) target: &'a C,
    pub(crate) positions: Vec<usize>,
    pub(crate) command: Cmd,
}

pub(crate) fn classify_keys<'a, C, R, K>(
    shards: &'a Sharded<C, R>,
    keys: &K,
) -> RedisResult<KeyRoute<'a, C>>
where
    R: ShardRouter<[u8]>,
    K: EncodeRedisArgs + ?Sized,
{
    struct Classifier<'a, C, R> {
        shards: &'a Sharded<C, R>,
        selected: Option<&'a C>,
        multiple: bool,
        seen: usize,
    }

    impl<C, R> RedisArgsSink for Classifier<'_, C, R>
    where
        R: ShardRouter<[u8]>,
    {
        fn write_arg<A>(&mut self, key: &A) -> RedisResult<()>
        where
            A: EncodeRedisArg + ?Sized,
        {
            self.seen = self.seen.saturating_add(1);
            let target = target_for_key(self.shards, key)?;
            if self
                .selected
                .is_some_and(|selected| !std::ptr::eq(selected, target))
            {
                self.multiple = true;
            } else if self.selected.is_none() {
                self.selected = Some(target);
            }
            Ok(())
        }
    }

    require_keys(keys)?;
    let mut classifier = Classifier {
        shards,
        selected: None,
        multiple: false,
        seen: 0,
    };
    keys.encode_args(&mut classifier)?;
    validate_count(keys, classifier.seen)?;
    if classifier.multiple {
        Ok(KeyRoute::Multiple)
    } else {
        classifier
            .selected
            .map(KeyRoute::Single)
            .ok_or_else(no_keys)
    }
}

pub(crate) fn group_keys<'a, C, R, K>(
    shards: &'a Sharded<C, R>,
    command_name: &'static str,
    keys: &K,
) -> RedisResult<Vec<ShardCommand<'a, C>>>
where
    R: ShardRouter<[u8]>,
    K: EncodeRedisArgs + ?Sized,
{
    struct Grouper<'a, C, R> {
        shards: &'a Sharded<C, R>,
        command_name: &'static str,
        groups: Vec<ShardCommand<'a, C>>,
        seen: usize,
    }

    impl<C, R> RedisArgsSink for Grouper<'_, C, R>
    where
        R: ShardRouter<[u8]>,
    {
        fn write_arg<A>(&mut self, key: &A) -> RedisResult<()>
        where
            A: EncodeRedisArg + ?Sized,
        {
            let target = target_for_key(self.shards, key)?;
            let group_index = self
                .groups
                .iter()
                .position(|group| std::ptr::eq(group.target, target));
            let group = match group_index {
                Some(index) => &mut self.groups[index],
                None => {
                    self.groups.push(ShardCommand {
                        target,
                        positions: Vec::new(),
                        command: cmd(self.command_name),
                    });
                    self.groups
                        .last_mut()
                        .expect("a shard Redis command was just inserted")
                }
            };
            group.positions.push(self.seen);
            group.command.arg_encoded(key)?;
            self.seen = self.seen.saturating_add(1);
            Ok(())
        }
    }

    require_keys(keys)?;
    let mut grouper = Grouper {
        shards,
        command_name,
        groups: Vec::new(),
        seen: 0,
    };
    keys.encode_args(&mut grouper)?;
    validate_count(keys, grouper.seen)?;
    Ok(grouper.groups)
}

fn target_for_key<'a, C, R, K>(shards: &'a Sharded<C, R>, key: &K) -> RedisResult<&'a C>
where
    R: ShardRouter<[u8]>,
    K: EncodeRedisArg + ?Sized,
{
    if shards.shard_count() == 1 {
        return shards.get(&[][..]).map_err(map_routing_error);
    }
    let encoded = crate::arg::encode_arg_contiguous(key)?;
    shards.get(encoded.as_ref()).map_err(map_routing_error)
}

fn require_keys<K: EncodeRedisArgs + ?Sized>(keys: &K) -> RedisResult<()> {
    if keys.num_args() == 0 {
        Err(no_keys())
    } else {
        Ok(())
    }
}

fn validate_count<K: EncodeRedisArgs + ?Sized>(keys: &K, seen: usize) -> RedisResult<()> {
    if seen == keys.num_args() {
        Ok(())
    } else {
        Err(RedisError::new(
            ErrorKind::ClientError,
            "EncodeRedisArgs emitted a different number of keys than num_args",
        ))
    }
}

fn no_keys() -> RedisError {
    RedisError::new(
        ErrorKind::ClientError,
        "Redis multi-key command requires at least one key",
    )
}

fn map_routing_error(error: brz_net::NetError) -> RedisError {
    RedisError::new(ErrorKind::ClientError, error.to_string())
}

#[cfg(test)]
mod tests {
    use brz_net::{ShardRouter, Sharded};

    use super::{KeyRoute, classify_keys, group_keys};

    struct FirstByteRouter;

    impl ShardRouter<[u8]> for FirstByteRouter {
        fn route(&self, key: &[u8], shard_count: usize) -> usize {
            usize::from(key.first().copied().unwrap_or_default()) % shard_count
        }
    }

    #[test]
    fn classifies_same_and_cross_shard_key_sets() {
        let shards = Sharded::new(FirstByteRouter, [10, 20]).unwrap();

        assert!(matches!(
            classify_keys(&shards, &["b", "d"]).unwrap(),
            KeyRoute::Single(10)
        ));
        assert!(matches!(
            classify_keys(&shards, &["a", "b"]).unwrap(),
            KeyRoute::Multiple
        ));
    }

    #[test]
    fn groups_keys_by_shard_and_preserves_source_positions() {
        let shards = Sharded::new(FirstByteRouter, [10, 20]).unwrap();
        let groups = group_keys(&shards, "MGET", &["a", "b", "c", "d"]).unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].target, &20);
        assert_eq!(groups[0].positions, [0, 2]);
        assert_eq!(groups[0].command.arg_at(1), Some(b"a".as_slice()));
        assert_eq!(groups[0].command.arg_at(2), Some(b"c".as_slice()));
        assert_eq!(groups[1].target, &10);
        assert_eq!(groups[1].positions, [1, 3]);
    }
}
