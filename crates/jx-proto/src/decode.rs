use crate::{DecodeError, RespValue};
use bytes::{Buf, Bytes, BytesMut};

const MAX_ARRAY_LEN: usize = 1_000_000;
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;

pub struct RespDecoder {
    buf: BytesMut,
    consumed: usize,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(4096),
            consumed: 0,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    pub fn next_value(&mut self) -> Result<Option<RespValue>, DecodeError> {
        loop {
            if self.consumed >= self.buf.len() {
                self.buf.clear();
                self.consumed = 0;
                return Ok(None);
            }
            // Inline clients (telnet, redis-cli --pipe) emit stray blank lines
            // between commands; drop them instead of treating them as input.
            if self.buf[self.consumed..].starts_with(b"\r\n") {
                self.consumed += 2;
                continue;
            }
            if self.buf[self.consumed..].starts_with(b"\n") {
                self.consumed += 1;
                continue;
            }
            break;
        }
        match try_parse(&self.buf[self.consumed..])? {
            Some((value, n)) => {
                self.consumed += n;
                if self.consumed >= self.buf.len() {
                    self.buf.clear();
                    self.consumed = 0;
                } else if self.consumed >= 4096 {
                    self.buf.advance(self.consumed);
                    self.consumed = 0;
                }
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

impl Default for RespDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn read_line(buf: &[u8]) -> Result<Option<(Bytes, usize)>, DecodeError> {
    let Some(pos) = buf.iter().position(|&b| b == b'\n') else {
        return Ok(None);
    };
    if pos == 0 || buf[pos - 1] != b'\r' {
        return Err(DecodeError);
    }
    Ok(Some((Bytes::copy_from_slice(&buf[..pos - 1]), pos + 1)))
}

/// Like `read_line` but tolerates a missing CR (inline-only leniency).
fn read_line_lenient(buf: &[u8]) -> Result<Option<(Bytes, usize)>, DecodeError> {
    let Some(pos) = buf.iter().position(|&b| b == b'\n') else {
        return Ok(None);
    };
    let end = if pos > 0 && buf[pos - 1] == b'\r' {
        pos - 1
    } else {
        pos
    };
    Ok(Some((Bytes::copy_from_slice(&buf[..end]), pos + 1)))
}

fn parse_len(line: &[u8]) -> Result<Option<usize>, DecodeError> {
    let s = std::str::from_utf8(line).map_err(|_| DecodeError)?;
    let n: i64 = s.parse().map_err(|_| DecodeError)?;
    if n < 0 {
        Ok(None)
    } else {
        Ok(Some(n as usize))
    }
}

fn try_parse(buf: &[u8]) -> Result<Option<(RespValue, usize)>, DecodeError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let kind = buf[0];
    let mut pos = 1;
    match kind {
        b'+' | b'-' | b':' => {
            let Some((line, used)) = read_line(&buf[pos..])? else {
                return Ok(None);
            };
            pos += used;
            let s = String::from_utf8(line.to_vec()).map_err(|_| DecodeError)?;
            Ok(Some((
                match kind {
                    b'+' => RespValue::SimpleString(s),
                    b'-' => RespValue::Error(s),
                    _ => RespValue::Integer(s.parse().map_err(|_| DecodeError)?),
                },
                pos,
            )))
        }
        b'$' => {
            let Some((line, used)) = read_line(&buf[pos..])? else {
                return Ok(None);
            };
            pos += used;
            let Some(len) = parse_len(&line)? else {
                return Ok(Some((RespValue::BulkString(None), pos)));
            };
            if len > MAX_BULK_LEN {
                return Err(DecodeError);
            }
            if buf.len() < pos + len + 2 {
                return Ok(None);
            }
            let body = Bytes::copy_from_slice(&buf[pos..pos + len]);
            pos += len;
            if buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
                return Err(DecodeError);
            }
            pos += 2;
            Ok(Some((RespValue::BulkString(Some(body)), pos)))
        }
        b'*' => {
            let Some((line, used)) = read_line(&buf[pos..])? else {
                return Ok(None);
            };
            pos += used;
            let Some(n) = parse_len(&line)? else {
                return Ok(Some((RespValue::Array(None), pos)));
            };
            if n > MAX_ARRAY_LEN {
                return Err(DecodeError);
            }
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                match try_parse(&buf[pos..])? {
                    Some((v, used)) => {
                        items.push(v);
                        pos += used;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((RespValue::Array(Some(items)), pos)))
        }
        _ => {
            // RESP2 inline command: a bare line of whitespace-separated args.
            // Redis trims an optional CR, so LF-only lines (redis-cli --pipe
            // on LF-terminated files) are accepted too.
            let Some((line, used)) = read_line_lenient(buf)? else {
                return Ok(None);
            };
            if line.len() > MAX_BULK_LEN {
                return Err(DecodeError);
            }
            let mut items = Vec::new();
            let mut start: Option<usize> = None;
            for (i, &c) in line.iter().enumerate() {
                if c == b' ' || c == b'\t' {
                    if let Some(s) = start.take() {
                        items.push(RespValue::bulk(Bytes::copy_from_slice(&line[s..i])));
                    }
                } else if start.is_none() {
                    start = Some(i);
                }
            }
            if let Some(s) = start {
                items.push(RespValue::bulk(Bytes::copy_from_slice(&line[s..])));
            }
            Ok(Some((RespValue::Array(Some(items)), used)))
        }
    }
}
