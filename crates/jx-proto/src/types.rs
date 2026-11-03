use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
    /// RESP3 map (`%`): alternating key/value entries, used by HELLO when a
    /// client negotiates protocol version 3.
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 null (`_`); RESP2 connections keep using null bulk (`$-1`).
    Null,
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".into())
    }
    pub fn err(msg: impl Into<String>) -> Self {
        RespValue::Error(msg.into())
    }
    pub fn bulk(bytes: impl Into<Bytes>) -> Self {
        RespValue::BulkString(Some(bytes.into()))
    }
    pub fn null_bulk() -> Self {
        RespValue::BulkString(None)
    }
    pub fn int(n: i64) -> Self {
        RespValue::Integer(n)
    }
    pub fn array(items: Vec<RespValue>) -> Self {
        RespValue::Array(Some(items))
    }
    pub fn null_array() -> Self {
        RespValue::Array(None)
    }
    pub fn map(entries: Vec<(RespValue, RespValue)>) -> Self {
        RespValue::Map(entries)
    }
    pub fn null() -> Self {
        RespValue::Null
    }
}
