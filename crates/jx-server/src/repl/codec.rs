//! Replication frame codec.
//!
//! Wire format (big-endian):
//!
//! ```text
//! [u32 payload_len][u8 type][payload]
//! ```
//!
//! - `payload_len` counts only the payload bytes (not the type byte).
//! - Frame type `0x01` Command: payload is raw RESP command bytes.
//! - Frame type `0x02` Heartbeat: payload is a `u64` BE offset (exactly 8 bytes).
//! - Frame type `0x03` Fullresync: payload is UTF-8 text `"{repl_id}\n{offset}"`
//!   (offset as decimal ASCII). Chosen over binary `repl_id + u64 BE` for
//!   debuggability.

use bytes::{BufMut, Bytes, BytesMut};

/// A single replication frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplFrame {
    /// Raw RESP command bytes.
    Command(Bytes),
    /// Master replication offset carried by a heartbeat.
    Heartbeat(u64),
    /// Full resync: replica restarts from `offset` with id `repl_id`.
    Fullresync { repl_id: String, offset: u64 },
}

const TYPE_COMMAND: u8 = 0x01;
const TYPE_HEARTBEAT: u8 = 0x02;
const TYPE_FULLRESYNC: u8 = 0x03;

/// Encode one frame as `[u32 payload_len][u8 type][payload]`.
pub fn encode_frame(f: &ReplFrame) -> Bytes {
    match f {
        ReplFrame::Command(payload) => frame(TYPE_COMMAND, payload),
        ReplFrame::Heartbeat(offset) => {
            let mut payload = BytesMut::with_capacity(8);
            payload.put_u64(*offset);
            frame(TYPE_HEARTBEAT, &payload)
        }
        ReplFrame::Fullresync { repl_id, offset } => {
            let text = format!("{repl_id}\n{offset}");
            frame(TYPE_FULLRESYNC, text.as_bytes())
        }
    }
}

/// Extract all complete frames from `buf`, consuming their bytes.
///
/// Incomplete trailing bytes are left in `buf`. Unknown type or a payload
/// that does not match its frame type returns `Err(())` and leaves `buf`
/// unchanged at the start of the offending frame.
#[allow(clippy::result_unit_err)]
pub fn decode_frames(buf: &mut BytesMut) -> Result<Vec<ReplFrame>, ()> {
    let mut frames = Vec::new();
    loop {
        if buf.len() < 5 {
            break;
        }
        let payload_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total = 5 + payload_len;
        if buf.len() < total {
            break;
        }
        let typ = buf[4];
        let payload = &buf[5..total];
        let frame = match typ {
            TYPE_COMMAND => ReplFrame::Command(Bytes::copy_from_slice(payload)),
            TYPE_HEARTBEAT => {
                if payload_len != 8 {
                    return Err(());
                }
                let mut off = [0u8; 8];
                off.copy_from_slice(payload);
                ReplFrame::Heartbeat(u64::from_be_bytes(off))
            }
            TYPE_FULLRESYNC => {
                let text = std::str::from_utf8(payload).map_err(|_| ())?;
                let (repl_id, offset) = text.split_once('\n').ok_or(())?;
                let offset: u64 = offset.parse().map_err(|_| ())?;
                ReplFrame::Fullresync {
                    repl_id: repl_id.to_string(),
                    offset,
                }
            }
            _ => return Err(()),
        };
        let _ = buf.split_to(total);
        frames.push(frame);
    }
    Ok(frames)
}

fn frame(typ: u8, payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(5 + payload.len());
    out.put_u32(payload.len() as u32);
    out.put_u8(typ);
    out.put_slice(payload);
    out.freeze()
}
