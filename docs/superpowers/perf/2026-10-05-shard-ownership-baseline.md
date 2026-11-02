# MemoryDataBase — shard-ownership 性能基线报告 (2026-10-05)

在 `MemoryDataBase/perf-shard` 分支（exclusive shard ownership + in-place RESP decoder + pipeline response batching + hot-path dispatch 微优化）上，用 `scripts/perf-matrix.sh` 实测的验收矩阵基线。

**结论：未达标。** pipeline=1（主验收）SET ≈ 168–172k OPS/s，GET ≈ 177k OPS/s，与目标 **≥ 500,000 OPS/s** 相差约 **2.9–3.0×**。pipeline=32 相对旧基线有明显提升（约 +40–50%），但仍不足 50 万。本文件记录真实数字与差距，不作达标声明。

## 机器与工具链

| 项 | 值 |
|---|---|
| OS | macOS (Darwin 27.0.0) arm64 |
| 主机 | MacBookAir.local |
| CPU | Apple M5 |
| CPU 数 | 10 (`sysctl -n hw.ncpu`) |
| rustc | 1.99.0 (b940084d7 2026-09-28) |
| cargo | 1.99.0 (5f94df478 2026-08-27) |
| 分支 | `MemoryDataBase/perf-shard` |
| 构建 | `cargo build --release -p jx-server -p jx-cli` |
| 服务器配置 | plain RESP, 127.0.0.1:6399, shards=10 (nproc), maxmemory 4gb, maxmemory-policy noeviction, appendonly no, dir=temp |
| 压测工具 | `jx-cli bench`（SET phase + GET phase，无 mixed 模式） |
| 验收线 | pipeline=1 SET 与 GET ops/s 均 ≥ 500,000 |

## 实测矩阵

以下数字来自 `bash scripts/perf-matrix.sh`（2026-10-05T03:53:40Z）及一次独立 primary/secondary 复跑；errs 全部为 0。

### pipeline=1（主验收，每请求往返）

两轮独立运行：

| 运行 | SET ops/s | SET avg | SET p50 | SET p99 | GET ops/s | GET avg | GET p50 | GET p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Run 1 (matrix) | 168,343 | 371.7 µs | 361.8 µs | 740.2 µs | 176,824 | 356.5 µs | 353.8 µs | 657.9 µs |
| Run 2 (confirm) | 171,952 | 366.0 µs | 358.2 µs | 714.0 µs | 176,904 | 356.2 µs | 350.6 µs | 676.3 µs |

原始输出（Run 1）：

```
SET ops=100000 ops/s=168343 avg_us=371.7 p50_us=361.8 p99_us=740.2 errs=0
GET ops=100000 ops/s=176824 avg_us=356.5 p50_us=353.8 p99_us=657.9 errs=0
```

原始输出（Run 2）：

```
SET ops=100000 ops/s=171952 avg_us=366.0 p50_us=358.2 p99_us=714.0 errs=0
GET ops=100000 ops/s=176904 avg_us=356.2 p50_us=350.6 p99_us=676.3 errs=0
```

**PASS/FAIL：** SET 与 GET 均 **FAIL** vs 500,000 OPS/s（达到目标的 ~34–35%）。

### pipeline=32（次要，批量往返）

| 运行 | SET batch ops/s | SET batch_avg | GET batch ops/s | GET batch_avg |
|---|---:|---:|---:|---:|
| Run 1 (matrix) | 376,841 | 4949.1 µs | 398,214 | 4799.1 µs |
| Run 2 (confirm) | 375,778 | 5091.5 µs | 414,794 | 4692.1 µs |

延迟标签是 batch 延迟（一次 32 条命令的往返），不能与 pipeline=1 的 per-request 延迟直接比较。

### 客户端并发扫描（pipeline=1）

| clients | SET ops/s | SET p50 | SET p99 | GET ops/s | GET p50 | GET p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 159,066 | 95.1 µs | 191.0 µs | 160,992 | 94.2 µs | 196.4 µs |
| 64 | 168,343 | 361.8 µs | 740.2 µs | 176,824 | 353.8 µs | 657.9 µs |
| 256 | 168,205 | 1356.4 µs | 3127.2 µs | 175,734 | 1342.7 µs | 3112.2 µs |

观察：聚合吞吐在 clients=64 之后即饱和于 ~168–176k，再加并发只抬升单请求延迟（排队），不抬升吞吐。clients=16 时单请求延迟已约 95–100 µs——这是当前 per-command 路径的本征开销量级。

### 80/20 mixed

**限制：** 当前 `jx-cli bench` 无 `--mixed` / `--ratio` 参数，工作负载固定为 SET phase 后 GET phase，无法直接跑 80/20。

**替代：** 矩阵中的 GET phase 即 GET-after-preload（预写后纯读），可作为读偏向（GET-heavy）代理；其 OPS/s 即上表 GET 行。若需严格 80/20，需扩展 bench 或引入 redis-benchmark/memtier。

### TLCP

本任务不跑 TLCP（按 ACCEPTANCE.md，TLCP 性能不要求 50 万，输出单独报告）。如需复现，用 `scripts/tlcp-interop.sh` 并在 TLS 配置下另跑 bench。

## 与旧基线 (2026-10-04) 对比

旧基线（同一开发机、同一 bench 口径，架构开销未消除时）：pipeline=1 SET ≈ 169k / GET ≈ 175k；pipeline=32 SET ≈ 258–260k / GET ≈ 297–307k。

| 场景 | 旧基线 (2026-10-04) | 本次 (2026-10-05) | 变化 |
|---|---:|---:|---:|
| pipeline=1 SET | ~169–170k | 168–172k | 持平（≈ +1%） |
| pipeline=1 GET | ~176–177k | ~177k | 持平 |
| pipeline=32 SET | ~258–260k | ~376–377k | **+~46%** |
| pipeline=32 GET | ~297–307k | ~398–415k | **+~36–40%** |

**要点：** exclusive shard ownership + decoder + pipeline batching + 微优化的收益主要体现在 **pipeline（批量）路径**；pipeline=1 的每请求路径吞吐几乎未变。50 万主验收线仍然 FAIL。

## 差距分析（诚实版）

目标：明文 RESP 小 value GET/SET ≥ 500,000 OPS/s。实测 pipeline=1 ≈ 170k。**差距 ≈ 2.9–3.0×，未达标。**

### 本次已落地的优化在测什么

1. **Exclusive shard ownership**（`jx-engine/src/actor.rs`）：shard 线程独占 `ShardState`，Engine 内部不再有 per-shard Mutex。消除了旧架构的双重串行化（actor + Mutex）。
2. **In-place RESP decoder state machine**（`jx-proto`）：解析侧减少分配。
3. **Pipeline response batching**（`jx-server`）：回复侧按 pipeline batch 聚合编码，减少 syscall/format。这解释了 pipeline=32 的 +40% 提升。
4. **Hot-path dispatch / integer paths**（`jx-engine`）：命令分发与整数路径微优化。

### 为何 pipeline=1 仍然 ~170k（剩余瓶颈）

clients=16 时单请求延迟已达 ~95–100 µs。按 clients/latency 反推，聚合吞吐上限 ≈ 16/99µs ≈ 160k——与实测吻合。也就是说：**即使不加并发，每条 pipeline=1 命令的路径成本就决定了 ~170k 天花板。** 剩余开销主要来自：

1. **每命令 channel hop**：每条命令仍经 crossbeam channel 投递到 shard actor 线程，reply 再回传。两次线程交接 + 调度延迟按命令计费，pipeline=1 无法摊薄。旧基线里这是双倍（actor + Engine Mutex）；现在减掉了 Mutex，但 **channel hop 本身仍在**。
2. **per-request 网络路径**：连接任务在 tokio runtime 上，localhost TCP 仍要 write + read 系统调用 + 调度；pipeline=1 下没有 writev/readv/批量回复摊薄。decoder/编码优化对单条 RTT 的绝对收益有限。
3. **并发下的排队**：clients 从 64→256，p50 从 ~360µs→~1350µs，吞吐不变。说明服务端每命令处理/队列已串行化到吞吐墙；加并发只是把 RTT 变长。
4. **客户端单进程**：`jx-cli bench` 每 client 一个 tokio task、串行往返。在 ~170k 量级客户端未必是主瓶颈，但要证明「服务器侧已达 50 万」必须换多进程客户端（memtier/redis-benchmark）复测。
5. **硬件口径**：本数字是 **开发机（MacBookAir, Apple M5, macOS arm64, 10 CPU）** 基线，不是信创 Linux 生产硬件认证数字。

### 离 50 万还缺什么（按收益排序）

1. **消除或合并每命令 channel hop**：shard 独占已就位，下一步是 **pipeline 感知的批量投递**（一次 channel send 携带 N 条命令）或在连接侧对同 shard 命令本地聚合后再投递；目标把每命令的 actor 往返从「每条付一次」变为「每 batch 付一次」。pipeline=32 的 +40% 已经说明 batch 化方向有效，需要把它扩展到 pipeline=1 的调度结构上（例如：先无阻塞执行/本地缓冲，或 per-connection request pipeline 直接喂给 shard）。
2. **连接层 I/O 合并**：即使 pipeline=1，也要用 writev/单次 read 批量收发（小 value 时一条 TCP 写多条命令可减少 syscall）；回复侧按 batch 编码已有，读路径是否也有对应合并需确认。
3. **热路径再压**：`format!` 分配（key 字符串、命令编码）、`Bytes` clone、reply 类型分配——encoder/decoder 已改 state machine，但 bench 侧与 server 侧仍有逐命令 alloc 痕迹。
4. **多客户端验证**：达到服务器侧 50 万后，必须用 memtier_benchmark/redis-benchmark（多进程、可控 pipeline）在 Linux 信创机上复测；本报告不构成生产达标证据。
5. **调参无意义**：在完成 1–2 之前，调 shard 数 / pipeline / clients 只能改变延迟分布，不能把 pipeline=1 从 170k 抬到 500k。

## 复现

```bash
bash /Users/caosunliu/opencode/MemoryDataBase/scripts/perf-matrix.sh
# 可选覆盖:
#   PERF_PORT=6399 PERF_ACCEPT_OPS=500000
#   PERF_CLIENTS=64 PERF_TARGET_OPS=100000 PERF_VALUE_SIZE=64
#   PERF_PRIMARY_PIPELINE=1 PERF_SECONDARY_PIPELINE=32
#   PERF_PRIMARY_CLIENTS=64
```

`scripts/perf-report.sh` 现为薄封装，直接 `exec` 到 `perf-matrix.sh`，保持旧文档/命令兼容。

脚本流程：打印 uname/cpu/rustc → `cargo build --release -p jx-server -p jx-cli` → 临时 plain 配置（shards=nproc, maxmemory 4gb, noeviction, appendonly no）→ 启动 server → 跑 primary (pipeline=1) 并解析 SET/GET ops/s 打印 PASS/FAIL vs 500k → pipeline=32 → clients 16/64/256 扫描 → 打印 mixed 限制说明 → `trap` 保证退出时 kill server。

## 原始输出（matrix run，2026-10-05T03:53:40Z）

```
== MemoryDataBase perf acceptance matrix ==
date:      2026-10-05T03:53:40Z
uname:     Darwin 27.0.0 arm64
host:      MacBookAir.local
cpu:       Apple M5
cpu count: 10
rustc:     rustc 1.99.0 (b940084d7 2026-09-28)
cargo:     cargo 1.99.0 (5f94df478 2026-08-27)
accept:    pipeline=1 SET/GET ops/s >= 500000

== bench: primary acceptance (clients=64 pipeline=1 requests=100000 value=64) ==
SET ops=100000 ops/s=168343 avg_us=371.7 p50_us=361.8 p99_us=740.2 errs=0
GET ops=100000 ops/s=176824 avg_us=356.5 p50_us=353.8 p99_us=657.9 errs=0

== primary acceptance vs 500000 OPS/s ==
SET ops/s=168343  GET ops/s=176824  => FAIL

== bench: pipeline=32 (secondary) (clients=64 pipeline=32) ==
SET ops=100000 ops/s=376841 batch_avg_us=4949.1 p50_batch_us=4929.3 p99_batch_us=7987.5 errs=0
GET ops=100000 ops/s=398214 batch_avg_us=4799.1 p50_batch_us=4805.2 p99_batch_us=7870.0 errs=0

== bench: pipeline=1 clients=16 ==
SET ops=100000 ops/s=159066 avg_us=99.5 p50_us=95.1 p99_us=191.0 errs=0
GET ops=100000 ops/s=160992 avg_us=98.8 p50_us=94.2 p99_us=196.4 errs=0

== bench: pipeline=1 clients=256 ==
SET ops=100000 ops/s=168205 avg_us=1418.2 p50_us=1356.4 p99_us=3127.2 errs=0
GET ops=100000 ops/s=175734 avg_us=1387.5 p50_us=1342.7 p99_us=3112.2 errs=0
```
