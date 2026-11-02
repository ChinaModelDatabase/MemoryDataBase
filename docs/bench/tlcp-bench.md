# TLCP Benchmark Baseline

This document defines the methodology, environment, workload, and reporting format for TLCP performance baselines. TLCP (GB/T 38636-2020) introduces additional handshake and record-layer overhead (SM2/SM3/SM4-GCM) compared to plaintext RESP.

## Goals

- Quantify TLCP vs plaintext throughput/latency under identical conditions.
- Establish reproducible baselines for x86_64, aarch64, loongarch64.
- Report per-connection and pipeline effects to guide capacity planning.

## Test Environment

Record the following for each run:

| Field | Example |
|---|---|
| Platform | x86_64 (Intel/AMD), aarch64 (Kunpeng/FT), loongarch64 (Loongson) |
| CPU | model, cores/threads, freq (base/max), L1/L2/L3 |
| Memory | total, channels, freq |
| OS | Kylin V10 / UOS V20 / openEuler 22.03 / Anolis 8.9 (kernel version, glibc) |
| Rust | rustc --version, cargo --version, target triple |
| Build | debug/release, RUSTFLAGS, LTO |
| Network | loopback (127.0.0.1) vs physical; MTU, offload |
| Server | MemoryDataBase commit SHA, config (shards, maxclients, appendonly, maxmemory) |
| Client | redis-benchmark / memtier_benchmark / jx-cli bench version |

## Workloads

| Workload | Value Size | Key Space | Pipeline | Notes |
|---|---|---|---|---|
| SET/GET small | 64 bytes | 1M | 1, 16, 32, 64 | Baseline hot path |
| SET/GET medium | 512 bytes | 1M | 16, 32 | Memory bandwidth sensitive |
| Mixed 80/20 (GET/SET) | 64 bytes | 1M | 32 | Realistic cache |
| PING_INLINE/PING_BULK | 0/4 bytes | N/A | 1, 32 | Latency-focused |
| Long-lived conns | varies | 1M | 16 | Connection churn low |

## TLCP Setup

- Certificates: dual certs (signing+encryption) + CA per GB/T 38636-2020. Generate with GM tools (GmSSL) consistent with deployment.
- `tls-mode tlcp`; configure `tlcp-sign-cert/key`, `tlcp-enc-cert/key`, `tlcp-ca` in `MemoryDataBase.conf`.
- Clients: use TLCP-capable client (GmSSL-based redis-cli/tlcp client) or equivalent harness. Ensure same cipher suite negotiation.
- Compare against `tls-mode plain` on same port binding or separate runs.

## Metrics

Collect (per phase: SET, GET, or mixed):

- Throughput: OPS/sec (requests/sec). For pipelined, count commands.
- Latency: avg, p50, p95, p99, p99.9, max (microseconds/milliseconds). Report per-request when pipeline==1; per-batch or effective per-command when pipeline>1 (state methodology).
- CPU: user/sys, %util per core, context switches, softirqs (if available).
- Memory: RSS, heap, per-shard mem.
- Network: bytes in/out, retransmits.
- Errors: timeouts, conn errors, `ERR shard command queue full` occurrences.
- Handshake: TLCP handshake time (ms), count.

## Tools & Commands

### jx-cli bench (built-in)

```bash
jx-cli bench \
  --host 127.0.0.1 --port 6379 \
  --clients 64 --requests 50000 \
  --value-size 64 --pipeline 32
```

Notes: jx-cli bench reports per-phase OPS/s and latency stats (see help text). For TLCP, run via TLCP tunnel or use a TLCP client.

### redis-benchmark

```bash
redis-benchmark -h 127.0.0.1 -p 6379 -c 64 -n 100000 -d 64 -P 32 -t set,get,ping_inline
```

For TLCP, use `redis-benchmark` compiled against GmSSL/Tongsuo with TLCP support, or stunnel not recommended (adds proxy overhead).

## Reporting Template

```markdown
### Run: tlcp-baseline-<date>-<arch>
- Commit: abc123
- Config: shards=8, maxclients=10000, appendonly=no, maxmemory=0
- Server: release build, LTO thin
- Client: redis-benchmark + TLCP client build SHA

#### Plaintext
| Workload | pipeline | OPS/sec | avg_us | p50_us | p95_us | p99_us | p99.9_us | errors |
|---|---|---|---|---|---|---|---|---|
| SET | 32 | ... | ... | ... | ... | ... | ... | 0 |

#### TLCP
| Workload | pipeline | OPS/sec | avg_us | p50_us | p95_us | p99_us | p99.9_us | hs_ms_avg | errors |
|---|---|---|---|---|---|---|---|---|---|
| SET | 32 | ... | ... | ... | ... | ... | ... | ... | 0 |

#### Overhead
TLCP/Plaintext OPS ratio: X% (Y% overhead). Handshake amortized over connections/requests.
```

## Acceptance & Notes

- TLCP baseline is **informational** (not required to meet 500k OPS plaintext target). Document overhead clearly.
- Run each workload >= 3 times, report median + stddev or min/median/max.
- Warm up: discard first 10% or run full warm iteration.
- Ensure CPU governor `performance`, no CPU throttling, network loopback clean.
- Capture `dmesg`, `sysctl`, `lscpu`, `/proc/cpuinfo` in environment section.
