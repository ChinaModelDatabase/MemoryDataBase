pub mod bench;
pub mod check_config;
pub mod keygen;
pub mod rdb_info;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "jx-cli", about = "MemoryDataBase ops CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    CheckConfig {
        #[arg(long)]
        config: PathBuf,
    },
    RdbInfo {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        key: Option<PathBuf>,
    },
    Bench {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 6379)]
        port: u16,
        #[arg(long, default_value_t = 32)]
        clients: usize,
        #[arg(long, default_value_t = 50000)]
        requests: usize,
        #[arg(long, default_value_t = 64)]
        value_size: usize,
        #[arg(
            long,
            default_value_t = 1,
            help = "Pipeline depth (commands in flight per round-trip). Throughput is per-phase: SET and GET are timed separately, each ops/s uses that phase's wall only. Latency stats are per-request (avg_us/p50_us/p99_us) when pipeline=1; batch wall times (batch_avg_us/p50_batch_us/p99_batch_us) when pipeline>1. errs counts RESP error replies."
        )]
        pipeline: usize,
    },
}

pub fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Keygen { out } => keygen::run(&out)?,
        Cmd::CheckConfig { config } => {
            check_config::run(&config)?;
            println!("config OK");
        }
        Cmd::RdbInfo { path, key } => {
            let k = key
                .as_ref()
                .map(|p| jx_crypto::load_keyfile(p))
                .transpose()?;
            let info = rdb_info::run(&path, k.as_ref())?;
            println!("{}", info.render());
        }
        Cmd::Bench {
            host,
            port,
            clients,
            requests,
            value_size,
            pipeline,
        } => {
            bench::run(&host, port, clients, requests, value_size, pipeline)?;
        }
    }
    Ok(())
}
