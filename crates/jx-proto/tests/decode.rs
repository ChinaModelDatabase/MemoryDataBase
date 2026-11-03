use bytes::Bytes;
use jx_proto::{RespDecoder, RespValue};

#[test]
fn decode_partial_then_complete() {
    let mut d = RespDecoder::new();
    d.feed(b"*2\r\n$3\r\nSET\r\n$3\r\n");
    assert!(d.next_value().unwrap().is_none());
    d.feed(b"foo\r\n");
    let v = d.next_value().unwrap().expect("value");
    assert_eq!(
        v,
        RespValue::Array(Some(vec![
            RespValue::bulk(Bytes::from_static(b"SET")),
            RespValue::bulk(Bytes::from_static(b"foo")),
        ]))
    );
}

#[test]
fn decode_multiple_pipelined() {
    let mut d = RespDecoder::new();
    d.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
    assert!(matches!(d.next_value().unwrap(), Some(RespValue::Array(_))));
    assert!(matches!(d.next_value().unwrap(), Some(RespValue::Array(_))));
    assert!(d.next_value().unwrap().is_none());
}

#[test]
fn decode_error_frame() {
    let mut d = RespDecoder::new();
    d.feed(b"-ERR oops\r\n");
    assert_eq!(d.next_value().unwrap(), Some(RespValue::err("ERR oops")));
}

#[test]
fn decode_rejects_bad_crlf() {
    let mut d = RespDecoder::new();
    d.feed(b"+OK\n");
    assert!(d.next_value().is_err());
}

#[test]
fn decode_null_bulk_and_array() {
    let mut d = RespDecoder::new();
    d.feed(b"$-1\r\n*-1\r\n");
    assert_eq!(d.next_value().unwrap(), Some(RespValue::null_bulk()));
    assert_eq!(d.next_value().unwrap(), Some(RespValue::null_array()));
}

#[test]
fn decode_inline_ping_to_array() {
    let mut d = RespDecoder::new();
    d.feed(b"PING\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![RespValue::bulk(Bytes::from_static(
            b"PING"
        ))]))
    );
}

#[test]
fn decode_inline_multiple_commands() {
    let mut d = RespDecoder::new();
    d.feed(b"SET foo bar\r\nGET foo\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"SET")),
            RespValue::bulk(Bytes::from_static(b"foo")),
            RespValue::bulk(Bytes::from_static(b"bar")),
        ]))
    );
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"GET")),
            RespValue::bulk(Bytes::from_static(b"foo")),
        ]))
    );
    assert!(d.next_value().unwrap().is_none());
}

#[test]
fn decode_inline_partial_line_waits_for_crlf() {
    let mut d = RespDecoder::new();
    d.feed(b"PIN");
    assert!(d.next_value().unwrap().is_none());
    d.feed(b"G\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![RespValue::bulk(Bytes::from_static(
            b"PING"
        ))]))
    );
}

#[test]
fn decode_inline_accepts_lf_only_line() {
    let mut d = RespDecoder::new();
    d.feed(b"SET lf v\nGET lf\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"SET")),
            RespValue::bulk(Bytes::from_static(b"lf")),
            RespValue::bulk(Bytes::from_static(b"v")),
        ]))
    );
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"GET")),
            RespValue::bulk(Bytes::from_static(b"lf")),
        ]))
    );
    assert!(d.next_value().unwrap().is_none());
}

#[test]
fn decode_inline_skips_empty_lines() {
    let mut d = RespDecoder::new();
    d.feed(b"\r\n\r\nPING\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![RespValue::bulk(Bytes::from_static(
            b"PING"
        ))]))
    );
}

#[test]
fn decode_inline_collapses_runs_of_spaces() {
    let mut d = RespDecoder::new();
    d.feed(b"SET   foo   bar\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"SET")),
            RespValue::bulk(Bytes::from_static(b"foo")),
            RespValue::bulk(Bytes::from_static(b"bar")),
        ]))
    );
}

#[test]
fn decode_inline_following_array_command() {
    let mut d = RespDecoder::new();
    d.feed(b"*1\r\n$4\r\nPING\r\nPING\r\n");
    assert!(matches!(d.next_value().unwrap(), Some(RespValue::Array(_))));
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::array(vec![RespValue::bulk(Bytes::from_static(
            b"PING"
        ))]))
    );
}

#[test]
fn decode_rejects_typed_frame_without_cr() {
    let mut d = RespDecoder::new();
    d.feed(b":42\n");
    assert!(d.next_value().is_err());
}

#[test]
fn decode_bulk_bad_trailer_rejected() {
    let mut d = RespDecoder::new();
    d.feed(b"$3\r\nfooXX");
    assert!(d.next_value().is_err());
}

#[test]
fn decode_valid_bulk_still_works() {
    let mut d = RespDecoder::new();
    d.feed(b"$3\r\nfoo\r\n");
    assert_eq!(
        d.next_value().unwrap(),
        Some(RespValue::BulkString(Some(Bytes::from_static(b"foo"))))
    );
}

#[test]
fn decode_rejects_huge_array_count() {
    let mut d = RespDecoder::new();
    d.feed(b"*1000001\r\n");
    assert!(d.next_value().is_err());
}

#[test]
fn decode_rejects_huge_bulk_len() {
    let mut d = RespDecoder::new();
    d.feed(b"$536870913\r\n");
    assert!(d.next_value().is_err());
}

#[test]
fn decode_large_pipeline_no_error() {
    let mut d = RespDecoder::new();
    let mut chunk = Vec::new();
    for i in 0..100 {
        chunk.extend_from_slice("*1\r\n$3\r\nGET\r\n".to_string().as_bytes());
        let _ = i;
    }
    d.feed(&chunk);
    let mut n = 0;
    while let Some(_v) = d.next_value().unwrap() {
        n += 1;
    }
    assert_eq!(n, 100);
}
