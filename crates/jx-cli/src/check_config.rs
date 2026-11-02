use anyhow::Result;
use std::path::Path;

pub fn run(path: &Path) -> Result<jx_server::config::Config> {
    jx_server::config::load_config(path)
}
