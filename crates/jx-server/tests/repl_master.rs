use bytes::Bytes;
use jx_server::repl::master::MasterRepl;

#[test]
fn on_write_appends_backlog() {
    let m = MasterRepl::new_for_test(1024);
    m.on_write(&[
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"v"),
    ]);
    assert!(m.offset() > 0);
    assert!(m.backlog_slice_from(0).is_some());
}

#[test]
fn on_write_ignores_read_commands() {
    let m = MasterRepl::new_for_test(1024);
    m.on_write(&[Bytes::from_static(b"GET"), Bytes::from_static(b"k")]);
    assert_eq!(m.offset(), 0);
    assert!(m.backlog_slice_from(0).is_none());
    assert_eq!(m.connected_replicas(), 0);
}

#[test]
fn on_write_sequential_offsets_accumulate() {
    let m = MasterRepl::new_for_test(1024);
    m.on_write(&[
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"1"),
    ]);
    let o1 = m.offset();
    m.on_write(&[
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"2"),
    ]);
    let o2 = m.offset();
    assert!(o2 > o1, "second write must advance offset: {o1} -> {o2}");
    assert!(m.backlog_slice_from(o1).is_some());
}

#[test]
fn on_write_tees_del_and_flushall() {
    let m = MasterRepl::new_for_test(1024);
    m.on_write(&[Bytes::from_static(b"DEL"), Bytes::from_static(b"k")]);
    assert!(m.offset() > 0, "DEL is a write command");
    m.on_write(&[Bytes::from_static(b"FLUSHALL")]);
    assert!(m.offset() > 0, "FLUSHALL is a write command");
}

#[test]
fn backlog_overflow_base_advances() {
    let m = MasterRepl::new_for_test(16);
    let arg = Bytes::from_static(b"SETAAAAAAAAAAAAAAAAAAAA");
    m.on_write(&[Bytes::from_static(b"SET"), arg.clone(), arg]);
    let o1 = m.offset();
    let arg2 = Bytes::from_static(b"SETBBBBBBBBBBBBBBBBBBBB");
    m.on_write(&[Bytes::from_static(b"SET"), arg2.clone(), arg2]);
    let o2 = m.offset();
    assert!(o2 > o1);
    // Offset points one past the last teed byte; the last byte itself must
    // still be retained even after overflow drops the front of the ring.
    assert!(m.backlog_slice_from(o2 - 1).is_some());
}
