//! The Redis command surface, generated from a single declarative list via
//! [`implement_commands!`](crate::implement_commands).
//!
//! The command set mirrors the common subset of the Java `JedisClient`
//! contract: keys/TTL, strings, hashes, lists, sets, sorted-sets, scanning,
//! and scripting. Method names are snake-cased Redis verbs.

pub mod macros;

crate::implement_commands! {
    // ---- connection / server ----
    /// `PING` — check the connection.
    fn ping() => ["PING"];
    /// `ECHO message` — echo the given string.
    fn echo(message) => ["ECHO"];
    /// `INFO` — server information and statistics.
    fn info() => ["INFO"];
    /// `SELECT db` — switch the logical database.
    fn select(db) => ["SELECT"];

    // ---- keys / ttl ----
    /// `DEL key [key ...]` — delete keys.
    fn del(keys) => ["DEL"];
    /// `UNLINK key [key ...]` — asynchronously delete keys.
    fn unlink(keys) => ["UNLINK"];
    /// `EXISTS key [key ...]` — count of existing keys.
    fn exists(keys) => ["EXISTS"];
    /// `EXPIRE key seconds`.
    fn expire(key, seconds) => ["EXPIRE"];
    /// `PEXPIRE key milliseconds`.
    fn pexpire(key, milliseconds) => ["PEXPIRE"];
    /// `EXPIREAT key unix-time-seconds`.
    fn expire_at(key, timestamp) => ["EXPIREAT"];
    /// `PEXPIREAT key unix-time-millis`.
    fn pexpire_at(key, timestamp) => ["PEXPIREAT"];
    /// `TTL key` — remaining time-to-live in seconds.
    fn ttl(key) => ["TTL"];
    /// `PTTL key` — remaining time-to-live in milliseconds.
    fn pttl(key) => ["PTTL"];
    /// `PERSIST key` — remove the expiry.
    fn persist(key) => ["PERSIST"];
    /// `TYPE key` — the type of the value stored at key.
    fn key_type(key) => ["TYPE"];
    /// `RENAME key newkey`.
    fn rename(key, new_key) => ["RENAME"];
    /// `RENAMENX key newkey`.
    fn rename_nx(key, new_key) => ["RENAMENX"];
    /// `KEYS pattern` — keys matching a glob pattern (avoid in production).
    fn keys(pattern) => ["KEYS"];
    /// `SCAN cursor` — one page of the keyspace; returns `(cursor, keys)`.
    fn scan(cursor) => ["SCAN"];

    // ---- strings ----
    /// `GET key`.
    fn get(key) => ["GET"];
    /// `SET key value`.
    fn set(key, value) => ["SET"];
    /// `SETNX key value` — set only if absent.
    fn set_nx(key, value) => ["SETNX"];
    /// `SETEX key seconds value`.
    fn set_ex(key, seconds, value) => ["SETEX"];
    /// `PSETEX key milliseconds value`.
    fn pset_ex(key, milliseconds, value) => ["PSETEX"];
    /// `GETSET key value` — set and return the previous value.
    fn get_set(key, value) => ["GETSET"];
    /// `MGET key [key ...]`.
    fn mget(keys) => ["MGET"];
    /// `MSET key value [key value ...]`.
    fn mset(items) => ["MSET"];
    /// `MSETNX key value [key value ...]`.
    fn mset_nx(items) => ["MSETNX"];
    /// `APPEND key value`.
    fn append(key, value) => ["APPEND"];
    /// `STRLEN key`.
    fn strlen(key) => ["STRLEN"];
    /// `INCR key`.
    fn incr(key) => ["INCR"];
    /// `DECR key`.
    fn decr(key) => ["DECR"];
    /// `INCRBY key delta`.
    fn incr_by(key, delta) => ["INCRBY"];
    /// `DECRBY key delta`.
    fn decr_by(key, delta) => ["DECRBY"];
    /// `INCRBYFLOAT key delta`.
    fn incr_by_float(key, delta) => ["INCRBYFLOAT"];
    /// `SETBIT key offset value`.
    fn setbit(key, offset, value) => ["SETBIT"];
    /// `GETBIT key offset`.
    fn getbit(key, offset) => ["GETBIT"];

    // ---- hashes ----
    /// `HGET key field`.
    fn hget(key, field) => ["HGET"];
    /// `HSET key field value`.
    fn hset(key, field, value) => ["HSET"];
    /// `HSETNX key field value`.
    fn hset_nx(key, field, value) => ["HSETNX"];
    /// `HMGET key field [field ...]`.
    fn hmget(key, fields) => ["HMGET"];
    /// `HMSET key field value [field value ...]`.
    fn hmset(key, items) => ["HMSET"];
    /// `HGETALL key`.
    fn hgetall(key) => ["HGETALL"];
    /// `HDEL key field [field ...]`.
    fn hdel(key, fields) => ["HDEL"];
    /// `HEXISTS key field`.
    fn hexists(key, field) => ["HEXISTS"];
    /// `HINCRBY key field delta`.
    fn hincr_by(key, field, delta) => ["HINCRBY"];
    /// `HINCRBYFLOAT key field delta`.
    fn hincr_by_float(key, field, delta) => ["HINCRBYFLOAT"];
    /// `HKEYS key`.
    fn hkeys(key) => ["HKEYS"];
    /// `HVALS key`.
    fn hvals(key) => ["HVALS"];
    /// `HLEN key`.
    fn hlen(key) => ["HLEN"];
    /// `HSCAN key cursor` — returns `(cursor, [field, value, ...])`.
    fn hscan(key, cursor) => ["HSCAN"];

    // ---- lists ----
    /// `LPUSH key value [value ...]`.
    fn lpush(key, values) => ["LPUSH"];
    /// `RPUSH key value [value ...]`.
    fn rpush(key, values) => ["RPUSH"];
    /// `LPUSHX key value`.
    fn lpush_x(key, value) => ["LPUSHX"];
    /// `RPUSHX key value`.
    fn rpush_x(key, value) => ["RPUSHX"];
    /// `LPOP key`.
    fn lpop(key) => ["LPOP"];
    /// `RPOP key`.
    fn rpop(key) => ["RPOP"];
    /// `LLEN key`.
    fn llen(key) => ["LLEN"];
    /// `LRANGE key start stop`.
    fn lrange(key, start, stop) => ["LRANGE"];
    /// `LTRIM key start stop`.
    fn ltrim(key, start, stop) => ["LTRIM"];
    /// `LINDEX key index`.
    fn lindex(key, index) => ["LINDEX"];
    /// `LSET key index value`.
    fn lset(key, index, value) => ["LSET"];
    /// `LREM key count value`.
    fn lrem(key, count, value) => ["LREM"];
    /// `LINSERT key <BEFORE|AFTER> pivot value`.
    fn linsert(key, position, pivot, value) => ["LINSERT"];
    /// `RPOPLPUSH source destination`.
    fn rpoplpush(source, destination) => ["RPOPLPUSH"];

    // ---- sets ----
    /// `SADD key member [member ...]`.
    fn sadd(key, members) => ["SADD"];
    /// `SREM key member [member ...]`.
    fn srem(key, members) => ["SREM"];
    /// `SMEMBERS key`.
    fn smembers(key) => ["SMEMBERS"];
    /// `SISMEMBER key member`.
    fn sismember(key, member) => ["SISMEMBER"];
    /// `SCARD key`.
    fn scard(key) => ["SCARD"];
    /// `SPOP key`.
    fn spop(key) => ["SPOP"];
    /// `SRANDMEMBER key`.
    fn srandmember(key) => ["SRANDMEMBER"];
    /// `SMOVE source destination member`.
    fn smove(source, destination, member) => ["SMOVE"];
    /// `SINTER key [key ...]`.
    fn sinter(keys) => ["SINTER"];
    /// `SUNION key [key ...]`.
    fn sunion(keys) => ["SUNION"];
    /// `SDIFF key [key ...]`.
    fn sdiff(keys) => ["SDIFF"];

    // ---- sorted sets ----
    /// `ZADD key score member`.
    fn zadd(key, score, member) => ["ZADD"];
    /// `ZREM key member [member ...]`.
    fn zrem(key, members) => ["ZREM"];
    /// `ZSCORE key member`.
    fn zscore(key, member) => ["ZSCORE"];
    /// `ZCARD key`.
    fn zcard(key) => ["ZCARD"];
    /// `ZCOUNT key min max`.
    fn zcount(key, min, max) => ["ZCOUNT"];
    /// `ZRANK key member`.
    fn zrank(key, member) => ["ZRANK"];
    /// `ZREVRANK key member`.
    fn zrevrank(key, member) => ["ZREVRANK"];
    /// `ZINCRBY key delta member`.
    fn zincr_by(key, delta, member) => ["ZINCRBY"];
    /// `ZRANGE key start stop`.
    fn zrange(key, start, stop) => ["ZRANGE"];
    /// `ZRANGE key start stop WITHSCORES` — returns `[member, score, ...]`.
    fn zrange_withscores(key, start, stop) => ["ZRANGE"] => ["WITHSCORES"];
    /// `ZREVRANGE key start stop`.
    fn zrevrange(key, start, stop) => ["ZREVRANGE"];
    /// `ZREVRANGE key start stop WITHSCORES`.
    fn zrevrange_withscores(key, start, stop) => ["ZREVRANGE"] => ["WITHSCORES"];
    /// `ZRANGEBYSCORE key min max`.
    fn zrangebyscore(key, min, max) => ["ZRANGEBYSCORE"];
    /// `ZRANGEBYSCORE key min max WITHSCORES`.
    fn zrangebyscore_withscores(key, min, max) => ["ZRANGEBYSCORE"] => ["WITHSCORES"];
    /// `ZREMRANGEBYRANK key start stop`.
    fn zremrangebyrank(key, start, stop) => ["ZREMRANGEBYRANK"];
    /// `ZREMRANGEBYSCORE key min max`.
    fn zremrangebyscore(key, min, max) => ["ZREMRANGEBYSCORE"];

    // ---- scripting ----
    /// `EVAL script numkeys [key ... arg ...]`.
    fn eval(script, numkeys, keys_and_args) => ["EVAL"];
    /// `EVALSHA sha1 numkeys [key ... arg ...]`.
    fn eval_sha(sha, numkeys, keys_and_args) => ["EVALSHA"];
    /// `SCRIPT LOAD script` — returns the script's SHA1.
    fn script_load(script) => ["SCRIPT", "LOAD"];

    // ---- mesh routing (value-returning forms) ----
    /// `keyshard key [key ...]` — ask the mesh which shard(s) keys map to.
    fn keyshard(keys) => ["keyshard"];
    /// `master` — pin this connection's subsequent commands to the master.
    fn master() => ["master"];
    /// `sendtoall` — broadcast the next command to all shards.
    fn sendtoall() => ["sendtoall"];
}
