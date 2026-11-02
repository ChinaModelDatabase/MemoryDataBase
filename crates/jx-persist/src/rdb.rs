use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use jx_crypto::{sm4_gcm_open, sm4_gcm_seal};
use jx_engine::actor::ShardRuntime;
use jx_engine::{JxValue, KeyMeta};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const RDB_MAGIC: &[u8; 8] = b"JX RDB\0\0";
const RDB_VERSION: u16 = 1;
const ENC_PREFIX: &[u8; 8] = b"JXRDBENC";
const MAX_SHARD_ENTRIES: usize = 10_000_000;
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

// **Shard-count constraint:** RDB files are shard-partitioned. A file written
// by an engine with N shards can only be loaded by an engine with exactly N
// shards (`load_rdb` refuses mismatches). Changing the `shards` config between
// saves and loads therefore requires a fresh save (or AOF replay, which is
// shard-agnostic). See ACCEPTANCE.md "Known gaps".

/// Unique temp path for an RDB write: pid distinguishes processes, seq
/// distinguishes concurrent saves within one process.
fn unique_tmp_path(path: &Path) -> std::path::PathBuf {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("rdb.{}.{seq}.tmp", std::process::id()))
}

fn put_bytes(out: &mut BytesMut, b: &[u8]) {
    out.put_u32(b.len() as u32);
    out.put_slice(b);
}

fn write_value(out: &mut BytesMut, v: &JxValue) {
    match v {
        JxValue::Str(b) => {
            out.put_u8(1);
            put_bytes(out, b);
        }
        JxValue::Int(n) => {
            out.put_u8(2);
            out.put_i64(*n);
        }
        JxValue::Hash(h) => {
            out.put_u8(3);
            out.put_u32(h.len() as u32);
            for (k, val) in h {
                put_bytes(out, k);
                put_bytes(out, val);
            }
        }
        JxValue::List(l) => {
            out.put_u8(4);
            out.put_u32(l.len() as u32);
            for x in l {
                put_bytes(out, x);
            }
        }
        JxValue::Set(s) => {
            out.put_u8(5);
            out.put_u32(s.len() as u32);
            for x in s {
                put_bytes(out, x);
            }
        }
        JxValue::ZSet(z) => {
            out.put_u8(6);
            out.put_u32(z.entries.len() as u32);
            for (m, score) in &z.entries {
                put_bytes(out, m);
                out.put_f64(*score);
            }
        }
    }
}

fn read_value(input: &mut &[u8]) -> Result<JxValue> {
    use anyhow::bail;
    let Some(&tag) = input.first() else {
        bail!("eof in value")
    };
    *input = &input[1..];
    Ok(match tag {
        1 => JxValue::Str(read_bytes(input)?),
        2 => {
            if input.len() < 8 {
                bail!("eof in int")
            }
            let n = i64::from_be_bytes(input[..8].try_into()?);
            *input = &input[8..];
            JxValue::Int(n)
        }
        3 => {
            let n = read_u32(input)? as usize;
            let mut h = std::collections::HashMap::new();
            for _ in 0..n {
                let k = read_bytes(input)?;
                let v = read_bytes(input)?;
                h.insert(k, v);
            }
            JxValue::Hash(h)
        }
        4 => {
            let n = read_u32(input)? as usize;
            let mut l = std::collections::VecDeque::new();
            for _ in 0..n {
                l.push_back(read_bytes(input)?);
            }
            JxValue::List(l)
        }
        5 => {
            let n = read_u32(input)? as usize;
            let mut s = std::collections::HashSet::new();
            for _ in 0..n {
                s.insert(read_bytes(input)?);
            }
            JxValue::Set(s)
        }
        6 => {
            let n = read_u32(input)? as usize;
            let mut z = jx_engine::ZSet::default();
            for _ in 0..n {
                let m = read_bytes(input)?;
                let sc = read_f64(input)?;
                z.entries.insert(m, sc);
            }
            JxValue::ZSet(z)
        }
        _ => bail!("unknown value tag {tag}"),
    })
}

fn read_bytes(input: &mut &[u8]) -> Result<bytes::Bytes> {
    let n = read_u32(input)? as usize;
    if input.len() < n {
        anyhow::bail!("eof in bytes")
    }
    let b = bytes::Bytes::copy_from_slice(&input[..n]);
    *input = &input[n..];
    Ok(b)
}

fn read_u32(input: &mut &[u8]) -> Result<u32> {
    if input.len() < 4 {
        anyhow::bail!("eof in u32")
    }
    let n = u32::from_be_bytes(input[..4].try_into()?);
    *input = &input[4..];
    Ok(n)
}

fn read_u64(input: &mut &[u8]) -> Result<u64> {
    if input.len() < 8 {
        anyhow::bail!("eof in u64")
    }
    let n = u64::from_be_bytes(input[..8].try_into()?);
    *input = &input[8..];
    Ok(n)
}

fn read_f64(input: &mut &[u8]) -> Result<f64> {
    if input.len() < 8 {
        anyhow::bail!("eof in f64")
    }
    let n = f64::from_be_bytes(input[..8].try_into()?);
    *input = &input[8..];
    Ok(n)
}

/// Serialize runtime state to RDB bytes (plaintext; encryption only when a
/// key is supplied). Used by the replication FULLRESYNC path.
pub fn snapshot_to_bytes(rt: &ShardRuntime, key: Option<&[u8; 32]>) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_slice(RDB_MAGIC);
    buf.put_u16(RDB_VERSION);
    buf.put_u32(rt.shard_count() as u32);
    for sid in 0..rt.shard_count() {
        let entries = rt.snapshot_shard(sid);
        buf.put_u32(entries.len() as u32);
        for (k, v, m) in &entries {
            put_bytes(&mut buf, k);
            buf.put_u32(if m.expire_at_ms.is_some() { 1 } else { 0 });
            if let Some(exp) = m.expire_at_ms {
                buf.put_u64(exp);
            }
            write_value(&mut buf, v);
        }
    }
    let payload = match key {
        Some(k) => {
            let sealed = sm4_gcm_seal(k, &buf).context("sm4 seal rdb")?;
            let mut out = BytesMut::new();
            out.put_slice(ENC_PREFIX);
            out.put_slice(&sealed);
            out.freeze()
        }
        None => buf.freeze(),
    };
    Ok(payload.to_vec())
}

pub fn save_rdb(rt: &ShardRuntime, path: &Path, key: Option<&[u8; 32]>) -> Result<()> {
    let t0 = std::time::Instant::now();
    let payload = snapshot_to_bytes(rt, key)?;
    let tmp = unique_tmp_path(path);
    {
        let mut f = std::fs::File::create(&tmp).context("create rdb tmp")?;
        f.write_all(&payload).context("write rdb tmp")?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, path).context("rename rdb")?;
    jx_engine::stats::record_rdb_save(t0.elapsed().as_millis() as u64);
    Ok(())
}

pub fn load_rdb(rt: &ShardRuntime, path: &Path, key: Option<&[u8; 32]>) -> Result<usize> {
    let raw = std::fs::read(path).context("read rdb")?;
    load_rdb_bytes(rt, &raw, key)
}

/// Load RDB bytes into the runtime (replication snapshot path; same
/// shard-count constraint as [`load_rdb`]).
pub fn load_rdb_bytes(rt: &ShardRuntime, raw: &[u8], key: Option<&[u8; 32]>) -> Result<usize> {
    let data = if raw.starts_with(ENC_PREFIX) {
        let k = key.context("rdb is encrypted but no key provided")?;
        sm4_gcm_open(k, &raw[8..]).context("sm4 open rdb")?
    } else {
        raw.to_vec()
    };
    if data.len() < 8 + 2 + 4 || &data[..8] != RDB_MAGIC {
        anyhow::bail!("bad rdb magic");
    }
    if u16::from_be_bytes(data[8..10].try_into()?) != RDB_VERSION {
        anyhow::bail!("unsupported rdb version");
    }
    let mut input: &[u8] = &data[10..];
    let nshards = read_u32(&mut input)? as usize;
    if nshards != rt.shard_count() {
        anyhow::bail!(
            "rdb shard count {} != runtime {}",
            nshards,
            rt.shard_count()
        );
    }
    let mut total = 0usize;
    for sid in 0..nshards {
        let n = read_u32(&mut input)? as usize;
        if n > MAX_SHARD_ENTRIES {
            anyhow::bail!("rdb shard entry count {n} exceeds cap {MAX_SHARD_ENTRIES}");
        }
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let k = read_bytes(&mut input)?;
            let has_exp = read_u32(&mut input)?;
            if has_exp > 1 {
                anyhow::bail!("bad has_exp flag {has_exp}");
            }
            let expire = if has_exp == 1 {
                Some(read_u64(&mut input)?)
            } else {
                None
            };
            let v = read_value(&mut input)?;
            entries.push((
                k,
                v,
                KeyMeta {
                    expire_at_ms: expire,
                    last_access_ms: 0,
                    freq: 1,
                },
            ));
        }
        rt.load_shard(sid, entries);
        total += n;
    }
    Ok(total)
}
