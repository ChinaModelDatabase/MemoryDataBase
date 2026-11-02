mod decode;
mod encode;
mod error;
mod types;

pub use decode::RespDecoder;
pub use encode::{encode, encode_to_bytes};
pub use error::DecodeError;
pub use types::RespValue;

#[test]
fn workspace_compiles() {
    assert_eq!(2 + 2, 4);
}
