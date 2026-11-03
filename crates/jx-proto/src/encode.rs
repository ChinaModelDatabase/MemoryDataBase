use crate::RespValue;
use bytes::{BufMut, BytesMut};

pub fn encode(value: &RespValue, out: &mut BytesMut) {
    match value {
        RespValue::SimpleString(s) => {
            out.put_u8(b'+');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            out.put_u8(b'-');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            out.put_u8(b':');
            out.put_slice(n.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::BulkString(None) => out.put_slice(b"$-1\r\n"),
        RespValue::BulkString(Some(b)) => {
            out.put_u8(b'$');
            out.put_slice(b.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        RespValue::Array(None) => out.put_slice(b"*-1\r\n"),
        RespValue::Array(Some(items)) => {
            out.put_u8(b'*');
            out.put_slice(items.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for it in items {
                encode(it, out);
            }
        }
        RespValue::Null => out.put_slice(b"_\r\n"),
        RespValue::Map(entries) => {
            out.put_u8(b'%');
            out.put_slice(entries.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for (k, v) in entries {
                encode(k, out);
                encode(v, out);
            }
        }
    }
}

pub fn encode_to_bytes(value: &RespValue) -> bytes::Bytes {
    let mut buf = BytesMut::new();
    encode(value, &mut buf);
    buf.freeze()
}
