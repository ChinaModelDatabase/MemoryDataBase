use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("RESP protocol decode error")]
pub struct DecodeError;
