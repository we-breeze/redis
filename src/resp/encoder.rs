//! RESP2 command encoding.
//!
//! Commands are always sent as a RESP2 multibulk array of bulk strings — this
//! is understood by every Redis server regardless of the negotiated protocol
//! version, so encoding never needs to branch on RESP2 vs RESP3.

use bytes::BufMut;

/// Encode a single command (a list of already-serialized argument byte slices)
/// into a RESP multibulk frame, appending to `out`.
pub fn encode_command(args: &[Vec<u8>], out: &mut impl BufMut) {
    write_array_header(args.len(), out);
    for arg in args {
        write_bulk_string(arg, out);
    }
}

/// Slice-based variant of [`encode_command`]: avoids materializing per-arg
/// vectors (used by the flat-buffer [`Cmd`](crate::cmd::Cmd)).
pub fn encode_command_slices<'a>(
    args: impl Iterator<Item = &'a [u8]>,
    count: usize,
    out: &mut impl BufMut,
) {
    write_array_header(count, out);
    for arg in args {
        write_bulk_string(arg, out);
    }
}

/// Encode a pipeline of commands back-to-back into a single buffer.
pub fn encode_pipeline(commands: &[Vec<Vec<u8>>], out: &mut impl BufMut) {
    for args in commands {
        encode_command(args, out);
    }
}

fn write_array_header(len: usize, out: &mut impl BufMut) {
    out.put_u8(b'*');
    write_usize(len, out);
    out.put_slice(b"\r\n");
}

fn write_bulk_string(data: &[u8], out: &mut impl BufMut) {
    out.put_u8(b'$');
    write_usize(data.len(), out);
    out.put_slice(b"\r\n");
    out.put_slice(data);
    out.put_slice(b"\r\n");
}

fn write_usize(value: usize, out: &mut impl BufMut) {
    let mut buf = itoa::Buffer::new();
    out.put_slice(buf.format(value).as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_multibulk() {
        let mut out = Vec::new();
        encode_command(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()], &mut out);
        assert_eq!(out, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[test]
    fn encodes_empty_arg() {
        let mut out = Vec::new();
        encode_command(&[b"GET".to_vec(), b"".to_vec()], &mut out);
        assert_eq!(out, b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n");
    }

    #[test]
    fn encodes_pipeline() {
        let mut out = Vec::new();
        encode_pipeline(&[vec![b"PING".to_vec()], vec![b"PING".to_vec()]], &mut out);
        assert_eq!(out, b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
    }
}
