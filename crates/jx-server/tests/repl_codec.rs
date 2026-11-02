use bytes::{Bytes, BytesMut};
use jx_server::repl::codec::{decode_frames, encode_frame, ReplFrame};

#[test]
fn command_frame_roundtrip() {
    let f = ReplFrame::Command(Bytes::from_static(b"*1\r\n$3\r\nSET\r\n"));
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], ReplFrame::Command(b) if b.as_ref() == b"*1\r\n$3\r\nSET\r\n"));
    assert!(buf.is_empty());
}

#[test]
fn partial_frame_waits() {
    let f = ReplFrame::Heartbeat(42);
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..enc.len() - 1]);
    assert!(decode_frames(&mut buf).unwrap().is_empty());
    buf.extend_from_slice(&enc[enc.len() - 1..]);
    assert_eq!(decode_frames(&mut buf).unwrap().len(), 1);
}

#[test]
fn heartbeat_frame_roundtrip() {
    let f = ReplFrame::Heartbeat(u64::MAX);
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], ReplFrame::Heartbeat(off) if *off == u64::MAX));
}

#[test]
fn fullresync_frame_roundtrip_text_payload() {
    let f = ReplFrame::Fullresync {
        repl_id: "deadbeefcafe0123".to_string(),
        offset: 123456789,
    };
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        ReplFrame::Fullresync { repl_id, offset } => {
            assert_eq!(repl_id, "deadbeefcafe0123");
            assert_eq!(*offset, 123456789);
        }
        other => panic!("expected Fullresync, got {other:?}"),
    }
}

#[test]
fn multiple_frames_in_one_buffer() {
    let f1 = ReplFrame::Command(Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
    let f2 = ReplFrame::Heartbeat(7);
    let mut enc = BytesMut::new();
    enc.extend_from_slice(&encode_frame(&f1));
    enc.extend_from_slice(&encode_frame(&f2));
    let mut buf = enc;
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 2);
    assert!(matches!(&frames[0], ReplFrame::Command(_)));
    assert!(matches!(&frames[1], ReplFrame::Heartbeat(7)));
    assert!(buf.is_empty());
}

#[test]
fn mixed_complete_and_partial_frames() {
    let f1 = ReplFrame::Heartbeat(1);
    let f2 = ReplFrame::Heartbeat(2);
    let enc1 = encode_frame(&f1);
    let enc2 = encode_frame(&f2);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&enc1);
    buf.extend_from_slice(&enc2[..enc2.len() - 1]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], ReplFrame::Heartbeat(1)));
    assert!(!buf.is_empty());
    buf.extend_from_slice(&enc2[enc2.len() - 1..]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], ReplFrame::Heartbeat(2)));
    assert!(buf.is_empty());
}

#[test]
fn unknown_type_is_err() {
    // len=1, type=0x7f, payload=[0x00]
    let mut enc = BytesMut::new();
    enc.extend_from_slice(&1u32.to_be_bytes());
    enc.extend_from_slice(&[0x7f, 0x00]);
    let mut buf = enc;
    assert!(decode_frames(&mut buf).is_err());
}

#[test]
fn heartbeat_bad_len_is_err() {
    // Heartbeat must carry exactly 8 payload bytes; 7 is a bad len.
    let mut enc = BytesMut::new();
    enc.extend_from_slice(&7u32.to_be_bytes());
    enc.extend_from_slice(&[0x02]);
    enc.extend_from_slice(&[0u8; 7]);
    let mut buf = enc;
    assert!(decode_frames(&mut buf).is_err());
}

#[test]
fn fullresync_bad_payload_is_err() {
    // Missing newline separator.
    let payload = b"no-newline-here";
    let mut enc = BytesMut::new();
    enc.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    enc.extend_from_slice(&[0x03]);
    enc.extend_from_slice(payload);
    let mut buf = enc;
    assert!(decode_frames(&mut buf).is_err());

    // Offset is not a valid u64.
    let payload = b"id\nnot-a-number";
    let mut enc = BytesMut::new();
    enc.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    enc.extend_from_slice(&[0x03]);
    enc.extend_from_slice(payload);
    let mut buf = enc;
    assert!(decode_frames(&mut buf).is_err());
}

#[test]
fn frame_layout_is_len_type_payload() {
    let f = ReplFrame::Command(Bytes::from_static(b"ABC"));
    let enc = encode_frame(&f);
    assert_eq!(enc.len(), 4 + 1 + 3);
    assert_eq!(&enc[0..4], &3u32.to_be_bytes());
    assert_eq!(enc[4], 0x01);
    assert_eq!(&enc[5..], b"ABC");
}
