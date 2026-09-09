/// 二阶modula，算法： hash / shards % shards
#[derive(Clone, Debug, Default)]
pub struct SecMod {
    shard_count: usize,
}

impl SecMod {
    pub fn from(shards: usize) -> Self {
        assert!(shards > 0);
        Self {
            shard_count: shards,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        // secmod 要求非负 hash；负数输入的分片结果不保证兼容性
        if hash < 0 {
            log::error!("found negative hash for secmod:{}", hash);
        }
        let idx = (hash as usize)
            .wrapping_div(self.shard_count)
            .wrapping_rem(self.shard_count);

        idx
    }
}
