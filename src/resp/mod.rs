//! RESP wire-format: [`encoder`] serializes commands, [`parser`] deserializes
//! replies.

pub mod encoder;
pub mod parser;

pub use encoder::{encode_command, encode_pipeline};
pub use parser::{ParseResult, parse_reply};
