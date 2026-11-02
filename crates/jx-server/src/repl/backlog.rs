use std::collections::VecDeque;

/// Replication backlog ring buffer.
///
/// Stores raw bytes only; `start_offset` is the logical replication offset of
/// `buf[0]`. `append` assumes sequential offsets (the master tees stream
/// offsets sequentially): the `offset` argument is only checked in
/// `debug_assert` against `start_offset + len()`. If it does not match, bytes
/// are still appended at the current end — `offset` is not used for
/// positioning. Overflow drops bytes from the front and advances
/// `base_offset()` past the dropped bytes.
pub struct Backlog {
    cap: usize,
    start_offset: u64,
    buf: VecDeque<u8>,
}

impl Backlog {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            start_offset: 0,
            buf: VecDeque::new(),
        }
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) {
        debug_assert_eq!(
            offset,
            self.start_offset + self.buf.len() as u64,
            "backlog appends must be sequential"
        );
        for &b in bytes {
            self.buf.push_back(b);
        }
        while self.buf.len() > self.cap {
            self.buf.pop_front();
            self.start_offset += 1;
        }
        self.buf.make_contiguous();
    }

    pub fn slice_from(&self, offset: u64) -> Option<&[u8]> {
        if offset < self.start_offset {
            return None;
        }
        let idx = (offset - self.start_offset) as usize;
        if idx >= self.buf.len() {
            return None;
        }
        let (front, back) = self.buf.as_slices();
        debug_assert!(back.is_empty(), "backlog buffer must stay contiguous");
        Some(&front[idx..])
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn base_offset(&self) -> u64 {
        self.start_offset
    }
}
