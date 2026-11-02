use anyhow::Result;
use std::path::Path;

pub fn run(out: &Path) -> Result<()> {
    jx_crypto::generate_keyfile(out)
}
