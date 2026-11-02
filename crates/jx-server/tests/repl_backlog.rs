use jx_server::repl::backlog::Backlog;

#[test]
fn append_and_slice_from() {
    let mut b = Backlog::new(16);
    b.append(0, b"hello");
    b.append(5, b"world");
    assert_eq!(b.slice_from(0), Some(&b"helloworld"[..]));
    assert_eq!(b.slice_from(5), Some(&b"world"[..]));
    assert_eq!(b.base_offset(), 0);
    assert_eq!(b.len(), 10);
}

#[test]
fn overflow_drops_oldest() {
    let mut b = Backlog::new(8);
    b.append(0, b"0123456789"); // 10 bytes > 8
    assert!(b.base_offset() > 0);
    assert!(b.slice_from(0).is_none());
    let base = b.base_offset();
    assert!(b.slice_from(base).is_some());
    assert_eq!(b.len(), 8);
}
