use bytes::{Bytes, BytesMut};
use jx_proto::{encode, encode_to_bytes, RespValue};

#[test]
fn encode_simple_string() {
    let mut out = BytesMut::new();
    encode(&RespValue::SimpleString("OK".into()), &mut out);
    assert_eq!(&out[..], b"+OK\r\n");
}

#[test]
fn encode_error_and_int() {
    assert_eq!(
        &encode_to_bytes(&RespValue::err("ERR bad"))[..],
        b"-ERR bad\r\n"
    );
    assert_eq!(&encode_to_bytes(&RespValue::int(42))[..], b":42\r\n");
}

#[test]
fn encode_bulk_and_null() {
    assert_eq!(
        &encode_to_bytes(&RespValue::bulk(Bytes::from_static(b"hi")))[..],
        b"$2\r\nhi\r\n"
    );
    assert_eq!(&encode_to_bytes(&RespValue::null_bulk())[..], b"$-1\r\n");
}

#[test]
fn encode_array_with_null_bulk() {
    let v = RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"a")),
        RespValue::null_bulk(),
    ]);
    assert_eq!(&encode_to_bytes(&v)[..], b"*2\r\n$1\r\na\r\n$-1\r\n");
}
