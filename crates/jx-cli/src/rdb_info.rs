use anyhow::{Context, Result};
use std::path::Path;

pub struct RdbInfo {
    pub encrypted: bool,
    pub magic: String,
    pub version: Option<u16>,
    pub shard_count: Option<usize>,
    pub keys: Option<usize>,
}

impl RdbInfo {
    pub fn render(&self) -> String {
        if self.encrypted && self.keys.is_none() {
            "encrypted=yes magic=JXRDBENC (provide --key to count)".to_string()
        } else {
            format!(
                "encrypted={} magic={} version={} shard_count={} keys={}",
                self.encrypted,
                self.magic,
                self.version
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into()),
                self.shard_count
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into()),
                self.keys
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into()),
            )
        }
    }
}

fn parse_plain_header(data: &[u8]) -> Result<(String, u16, usize)> {
    if data.len() < 14 || !data.starts_with(jx_persist::RDB_MAGIC) {
        anyhow::bail!("bad rdb magic");
    }
    let magic = String::from_utf8_lossy(jx_persist::RDB_MAGIC)
        .trim_end_matches('\0')
        .to_string();
    let version = u16::from_be_bytes(data[8..10].try_into()?);
    let nshards = u32::from_be_bytes(data[10..14].try_into()?) as usize;
    Ok((magic, version, nshards))
}

pub fn run(path: &Path, key: Option<&[u8; 32]>) -> Result<RdbInfo> {
    let raw = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if raw.starts_with(b"JXRDBENC") {
        let Some(k) = key else {
            return Ok(RdbInfo {
                encrypted: true,
                magic: "JXRDBENC".into(),
                version: None,
                shard_count: None,
                keys: None,
            });
        };
        let data = jx_crypto::sm4_gcm_open(k, &raw[8..]).context("sm4 open rdb")?;
        let (magic, version, nshards) = parse_plain_header(&data)?;
        let rt = jx_engine::actor::spawn_shard_runtime(nshards, 1024, jx_engine::system_now_ms);
        let keys = jx_persist::load_rdb(&rt, path, Some(k))?;
        Ok(RdbInfo {
            encrypted: true,
            magic,
            version: Some(version),
            shard_count: Some(nshards),
            keys: Some(keys),
        })
    } else {
        let (magic, version, nshards) = parse_plain_header(&raw)?;
        let rt = jx_engine::actor::spawn_shard_runtime(nshards, 1024, jx_engine::system_now_ms);
        let keys = jx_persist::load_rdb(&rt, path, key)?;
        Ok(RdbInfo {
            encrypted: false,
            magic,
            version: Some(version),
            shard_count: Some(nshards),
            keys: Some(keys),
        })
    }
}
