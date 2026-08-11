//! Lua scripting with the standard `EVALSHA`-then-`EVAL` fallback.
//!
//! A [`Script`] precomputes its SHA1 so calls send the compact `EVALSHA`; if
//! the server reports `NOSCRIPT`, the invocation transparently falls back to a
//! full `EVAL` (which also caches the script). This mirrors the Java client's
//! `evalsha` behavior.

use crate::cmd::Cmd;
use crate::connection::ConnectionLike;
use crate::error::{ErrorKind, RedisResult};
use crate::from_value::FromRedisValue;
use crate::to_args::ToRedisArgs;

/// A Lua script and its cached SHA1 digest.
#[derive(Clone, Debug)]
pub struct Script {
    code: String,
    hash: String,
}

impl Script {
    /// Prepare a script, computing its SHA1 up front.
    pub fn new(code: &str) -> Self {
        let hash = sha1_smol::Sha1::from(code.as_bytes()).digest().to_string();
        Script {
            code: code.to_string(),
            hash,
        }
    }

    /// The script's SHA1 (as sent to `EVALSHA`).
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Begin an invocation to which keys and args are added.
    pub fn prepare(&self) -> ScriptInvocation<'_> {
        ScriptInvocation {
            script: self,
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Convenience: start an invocation with one key.
    pub fn key(&self, key: impl ToRedisArgs) -> ScriptInvocation<'_> {
        let mut inv = self.prepare();
        inv.add_key(key);
        inv
    }

    /// Convenience: start an invocation with one argument.
    pub fn arg(&self, arg: impl ToRedisArgs) -> ScriptInvocation<'_> {
        let mut inv = self.prepare();
        inv.add_arg(arg);
        inv
    }
}

/// A pending script call with its `KEYS` and `ARGV`.
pub struct ScriptInvocation<'a> {
    script: &'a Script,
    keys: Vec<Vec<u8>>,
    args: Vec<Vec<u8>>,
}

impl ScriptInvocation<'_> {
    /// Add a `KEYS[i]` entry.
    pub fn key(mut self, key: impl ToRedisArgs) -> Self {
        self.add_key(key);
        self
    }

    /// Add an `ARGV[i]` entry.
    pub fn arg(mut self, arg: impl ToRedisArgs) -> Self {
        self.add_arg(arg);
        self
    }

    fn add_key(&mut self, key: impl ToRedisArgs) {
        key.write_redis_args(&mut self.keys);
    }

    fn add_arg(&mut self, arg: impl ToRedisArgs) {
        arg.write_redis_args(&mut self.args);
    }

    fn build(&self, verb: &str, script_arg: &str) -> Cmd {
        let mut cmd = crate::cmd::cmd(verb);
        cmd.arg(script_arg);
        cmd.arg(self.keys.len());
        for key in &self.keys {
            cmd.arg_bytes(key);
        }
        for arg in &self.args {
            cmd.arg_bytes(arg);
        }
        cmd
    }

    /// Run the script, sending `EVALSHA` first and falling back to `EVAL` on
    /// `NOSCRIPT`.
    pub async fn invoke_async<RV, C>(&self, con: &C) -> RedisResult<RV>
    where
        RV: FromRedisValue,
        C: ConnectionLike + ?Sized,
    {
        let evalsha = self.build("EVALSHA", &self.script.hash);
        match con.req_command(&evalsha).await?.into_result() {
            Ok(value) => RV::from_redis_value(&value),
            Err(err) if err.kind() == ErrorKind::NoScript => {
                let eval = self.build("EVAL", &self.script.code);
                let value = con.req_command(&eval).await?.into_result()?;
                RV::from_redis_value(&value)
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_sha1_hex() {
        let script = Script::new("return 1");
        assert_eq!(script.hash().len(), 40);
        assert!(script.hash().bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn builds_evalsha_frame() {
        let script = Script::new("return KEYS[1]");
        let inv = script.key("k").arg("a");
        let cmd = inv.build("EVALSHA", script.hash());
        assert_eq!(cmd.arg_at(0).unwrap(), b"EVALSHA");
        assert_eq!(cmd.arg_at(2).unwrap(), b"1"); // numkeys
        assert_eq!(cmd.arg_at(3).unwrap(), b"k");
        assert_eq!(cmd.arg_at(4).unwrap(), b"a");
    }
}
