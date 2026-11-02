use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
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
}
