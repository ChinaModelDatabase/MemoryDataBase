use anyhow::{bail, Context, Result};
use std::net::SocketAddr;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Nearest-rank percentiles over latency samples in nanoseconds.
/// Returns (p50_ns, p99_ns).
pub fn percentiles(samples_ns: &[u64]) -> (u64, u64) {
    (
        nearest_rank(samples_ns, 50.0),
        nearest_rank(samples_ns, 99.0),
    )
}

fn nearest_rank(samples_ns: &[u64], p: f64) -> u64 {
    if samples_ns.is_empty() {
        return 0;
    }
    let mut s = samples_ns.to_vec();
    s.sort_unstable();
    let n = s.len();
    let rank = (p / 100.0 * n as f64).ceil() as usize;
    s[rank.clamp(1, n) - 1]
}

/// Per-phase bench summary. Latency samples are nanoseconds:
/// - `pipeline == 1`: one sample per request (request latency)
/// - `pipeline > 1`: one sample per batch (batch wall time)
pub struct BenchStats {
    pub ops: usize,
    pub errs: usize,
    /// Phase wall time in nanoseconds (max over concurrent clients).
    pub wall_ns: u128,
    /// Average latency in microseconds (one decimal; derived from nanos).
    pub avg_us: f64,
    /// p50 latency in microseconds.
    pub p50_us: f64,
    /// p99 latency in microseconds.
    pub p99_us: f64,
    /// Pipeline depth; decides print labels (per-request vs batch).
    pub pipeline: usize,
}

/// Summarize nanosecond latency samples for one phase.
pub fn summarize(
    samples_ns: Vec<u64>,
    ops: usize,
    errs: usize,
    wall_ns: u128,
    pipeline: usize,
) -> BenchStats {
    let (p50_ns, p99_ns) = percentiles(&samples_ns);
    let avg_ns = if samples_ns.is_empty() {
        0.0
    } else {
        let sum: u64 = samples_ns.iter().sum();
        sum as f64 / samples_ns.len() as f64
    };
    BenchStats {
        ops,
        errs,
        wall_ns,
        avg_us: avg_ns / 1000.0,
        p50_us: p50_ns as f64 / 1000.0,
        p99_us: p99_ns as f64 / 1000.0,
        pipeline,
    }
}

/// Format one phase line. Throughput uses this phase's wall only.
/// pipeline==1: `avg_us`/`p50_us`/`p99_us`; pipeline>1: `batch_avg_us`/`p50_batch_us`/`p99_batch_us`.
pub fn format_stats(label: &str, s: &BenchStats) -> String {
    let ops_per_s = if s.wall_ns == 0 {
        0.0
    } else {
        s.ops as f64 * 1_000_000_000.0 / s.wall_ns as f64
    };
    if s.pipeline > 1 {
        format!(
            "{label} ops={} ops/s={:.0} batch_avg_us={:.1} p50_batch_us={:.1} p99_batch_us={:.1} errs={}",
            s.ops, ops_per_s, s.avg_us, s.p50_us, s.p99_us, s.errs
        )
    } else {
        format!(
            "{label} ops={} ops/s={:.0} avg_us={:.1} p50_us={:.1} p99_us={:.1} errs={}",
            s.ops, ops_per_s, s.avg_us, s.p50_us, s.p99_us, s.errs
        )
    }
}

fn print_stats(label: &str, s: &BenchStats) {
    println!("{}", format_stats(label, s));
}

fn encode_cmd(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

enum Reply {
    Ok,
    /// RESP error reply (`-...`); counted as a failure, does not abort.
    Err,
}

async fn read_reply<R: AsyncBufReadExt + Unpin>(r: &mut R) -> Result<Reply> {
    let mut line = Vec::new();
    let n = r.read_until(b'\n', &mut line).await?;
    if n == 0 {
        bail!("connection closed while reading reply");
    }
    match line.first() {
        Some(b'+') | Some(b':') => Ok(Reply::Ok),
        Some(b'-') => Ok(Reply::Err),
        Some(b'$') => {
            let len_str = String::from_utf8_lossy(&line[1..]).trim().to_string();
            let len: i64 = len_str.parse().context("bad bulk len")?;
            if len >= 0 {
                let mut buf = vec![0u8; len as usize + 2];
                r.read_exact(&mut buf).await?;
            }
            Ok(Reply::Ok)
        }
        other => bail!("unexpected reply type {other:?}"),
    }
}

/// Per-client result for both phases. Latency samples are nanoseconds
/// (per-request when pipeline==1, per-batch when pipeline>1).
struct ClientResult {
    set_ops: usize,
    set_errs: usize,
    set_samples_ns: Vec<u64>,
    set_wall_ns: u128,
    get_ops: usize,
    get_errs: usize,
    get_samples_ns: Vec<u64>,
    get_wall_ns: u128,
}

async fn run_client(
    host: String,
    port: u16,
    client_id: usize,
    requests: usize,
    pipeline: usize,
    value: Vec<u8>,
) -> Result<ClientResult> {
    let addr: SocketAddr = format!("{host}:{port}").parse().context("bad host:port")?;
    let stream = TcpStream::connect(addr).await.context("connect")?;
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);

    let pipeline = pipeline.max(1);

    let mut set_samples_ns: Vec<u64> = Vec::new();
    let mut set_errs = 0usize;
    let mut i = 0;
    let phase_t = Instant::now();
    while i < requests {
        let batch = pipeline.min(requests - i);
        let t0 = Instant::now();
        for j in 0..batch {
            let key = format!("bench:{client_id}:{}", i + j);
            let cmd = encode_cmd(&[b"SET", key.as_bytes(), &value]);
            wr.write_all(&cmd).await?;
        }
        wr.flush().await?;
        for _ in 0..batch {
            if matches!(read_reply(&mut rd).await?, Reply::Err) {
                set_errs += 1;
            }
        }
        set_samples_ns.push(t0.elapsed().as_nanos() as u64);
        i += batch;
    }
    let set_wall_ns = phase_t.elapsed().as_nanos();

    let mut get_samples_ns: Vec<u64> = Vec::new();
    let mut get_errs = 0usize;
    let mut i = 0;
    let phase_t = Instant::now();
    while i < requests {
        let batch = pipeline.min(requests - i);
        let t0 = Instant::now();
        for j in 0..batch {
            let key = format!("bench:{client_id}:{}", i + j);
            let cmd = encode_cmd(&[b"GET", key.as_bytes()]);
            wr.write_all(&cmd).await?;
        }
        wr.flush().await?;
        for _ in 0..batch {
            if matches!(read_reply(&mut rd).await?, Reply::Err) {
                get_errs += 1;
            }
        }
        get_samples_ns.push(t0.elapsed().as_nanos() as u64);
        i += batch;
    }
    let get_wall_ns = phase_t.elapsed().as_nanos();

    Ok(ClientResult {
        set_ops: requests,
        set_errs,
        set_samples_ns,
        set_wall_ns,
        get_ops: requests,
        get_errs,
        get_samples_ns,
        get_wall_ns,
    })
}

pub fn run(
    host: &str,
    port: u16,
    clients: usize,
    requests: usize,
    value_size: usize,
    pipeline: usize,
) -> Result<()> {
    let clients = clients.max(1);
    let requests = requests.max(1);
    let value_size = value_size.max(1);
    let pipeline = pipeline.max(1);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    let value = vec![b'x'; value_size];
    let host = host.to_string();

    rt.block_on(async move {
        let per_client = requests / clients;
        let rem = requests % clients;
        let mut handles = Vec::new();
        for c in 0..clients {
            let n = per_client + usize::from(c < rem);
            if n == 0 {
                continue;
            }
            handles.push(tokio::spawn(run_client(
                host.clone(),
                port,
                c,
                n,
                pipeline,
                value.clone(),
            )));
        }

        let mut set_ops = 0usize;
        let mut set_errs = 0usize;
        let mut set_samples_ns = Vec::new();
        let mut set_wall_ns: u128 = 0;
        let mut get_ops = 0usize;
        let mut get_errs = 0usize;
        let mut get_samples_ns = Vec::new();
        let mut get_wall_ns: u128 = 0;
        for h in handles {
            let r = h.await.context("join client")??;
            set_ops += r.set_ops;
            set_errs += r.set_errs;
            set_samples_ns.extend(r.set_samples_ns);
            set_wall_ns = set_wall_ns.max(r.set_wall_ns);
            get_ops += r.get_ops;
            get_errs += r.get_errs;
            get_samples_ns.extend(r.get_samples_ns);
            get_wall_ns = get_wall_ns.max(r.get_wall_ns);
        }

        print_stats(
            "SET",
            &summarize(set_samples_ns, set_ops, set_errs, set_wall_ns, pipeline),
        );
        print_stats(
            "GET",
            &summarize(get_samples_ns, get_ops, get_errs, get_wall_ns, pipeline),
        );
        Ok(())
    })
}
