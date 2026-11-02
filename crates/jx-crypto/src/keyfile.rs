use anyhow::{Context, Result};
use rand::RngCore;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub fn generate_keyfile(path: &Path) -> Result<()> {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("create keyfile {} (already exists?)", path.display()))?;
    f.write_all(&key).context("write keyfile")?;
    f.sync_all().context("sync keyfile")?;
    Ok(())
}

pub fn load_keyfile(path: &Path) -> Result<[u8; 32]> {
    let data = std::fs::read(path).with_context(|| format!("read keyfile {}", path.display()))?;
    if data.len() != 32 {
        anyhow::bail!("keyfile must be exactly 32 bytes, got {}", data.len());
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&data);
    Ok(key)
}
