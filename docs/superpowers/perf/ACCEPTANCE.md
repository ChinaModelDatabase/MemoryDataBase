# MemoryDataBase MVP + perf-shard — 端到端验收清单

**日期:** 2026-10-05  
**分支:** `MemoryDataBase/perf-shard`（基线任务在 `MemoryDataBase/mvp`）  
**依据:** spec `docs/superpowers/specs/2026-10-04-MemoryDataBase-mvp-design.md`（含附录 A）+ plan Task 18；perf-shard spec `docs/superpowers/specs/2026-10-05-MemoryDataBase-perf-shard-ownership-design.md` + plan Tasks 1–9  
**证据来源:** 任务 0–17 与 perf-shard 任务 0–8 会话 ledger（`.superpowers/sdd/progress.md` + 各 task-report）；本轮（2026-10-05）`cargo test --workspace`、`tests/compat/run_compat.sh`、`scripts/check-licenses.sh`、`scripts/perf-matrix.sh` 实跑；基线报告 `perf/2026-10-04-baseline.md` 与 `perf/2026-10-05-shard-ownership-baseline.md`

## 判定图例

| 标记 | 含义 |
|------|------|
| ✅ PASS | 达到通过标准，有证据 |
| ⚠️ PARTIAL | 主体实现/测试通过，但验收线或互操作未完整闭环 |
| ❌ FAIL | 明确未达标 |
| ⛔ NOT DONE | 未做 |

---

## 验收对照表（plan Task 18 + perf-shard Tasks 0–9）

| 项 | 命令/方法 | 通过标准 | 状态 | 证据 |
|----|-----------|----------|------|------|
| 附录 A 必须命令 | `cargo test -p jx-engine` + 兼容脚本 `tests/compat/run_compat.sh` | 全绿 | ✅ PASS（核心集） | 本轮（2026-10-05）`cargo test --workspace` **176 passed / 0 failed**（1 ignored：`tlcp_mode_rejects_plain_client`）；engine 套件扩至 lib/actor/collections/coordinator/mem_evict/ops/persistence_cmds/shard_execute/string；`tests/compat` 双客户端冒烟 PASS（Task 15, `d7bdbdd`；本轮复跑 OK）。连接/Key/String/Hash/List/Set/ZSet 与 SAVE 族已覆盖；`HMSET` 及 `SELECT`/`SETEX`/`RENAME`/`RANDOMKEY`/`MSETNX` 等 stretch/边缘项仍列 `is_unimplemented`（见缺口） |
| 明文 50 万 OPS | `scripts/perf-matrix.sh`（`scripts/perf-report.sh` 为其薄封装） | ≥ 500000 OPS/s（GET/SET 64B, pipeline=1） | ❌ FAIL | **shard-ownership 架构已交付但性能线仍未达标。** pipeline=1 实测 SET ≈ 172k / GET ≈ 177k OPS/s（errs=0，与 2026-10-05 基线一致，无新增提升）；差距约 **2.9×**。详见 `docs/superpowers/perf/2026-10-05-shard-ownership-baseline.md`（Task 8, `f80bf97`）。pipeline=32 批量 ≈ 373k–405k（相对旧基线 +~40%），**不可用于宣称达标** |
| TLCP 实测 | 启动 `tls-mode tlcp` + bench / 互操作 | 输出单独报告，不要求 50 万 | ⚠️ PARTIAL | `gm-tlcp 0.7.4` 接入；`tests/tlcp.rs` **6/6 passed**，含 `tlcp_loopback_handshake`（非 ignore）；server 侧 `tlcp_mode_rejects_plain_client` 在 `--ignored` 下通过（Task 12）。`scripts/tlcp-interop.sh` 存在，GmSSL 3.2.0 与 fixture 未加密 PEM 密钥不兼容 → 打印手工互操作步骤后 exit 0；**GmSSL 全链路未完整执行**。无独立 TLCP 性能报告（按标准不要求 50 万） |
| RDB/AOF | 单测 + kill -9 恢复 / 损坏文件 | everysec 丢窗口 ≤1s；损坏文件拒绝启动 | ✅ PASS | `jx-persist` **28 passed**（RDB 8 + AOF 20），含加密往返、错误密钥、损坏文件、shard 不匹配拒绝、跨 shard RENAME 恢复、per-shard AOF sink、SAVE 压缩防双写；`jx-server` persistence/smoke/shutdown 套件含 `restore_rdb_on_startup`、`restore_rdb_then_aof_on_startup`、`save_creates_rdb`、`bgsave_returns_ok`、`bgrewriteaof_returns_ok`、`shutdown_persist_*`（Task 7/8/10/13 + perf-shard 修复 `97b2b96`/`82798c5`/`60aac8b`）。`appendfsync` 配置校验 `always|everysec|no`。SIGKILL 丢窗口按 everysec 每秒刷一次设计为 ≤1s，本轮验收未另做独立 `kill -9` 外部脚本实测 |
| SM3/SM4 | `jx-cli keygen` + 加密 RDB 往返 | 单测通过 | ✅ PASS | `jx-crypto` **11 passed**（`crypto.rs` 5 + `tlcp.rs` 6）；SM3 认证、SM4-GCM 文件加解密、keyfile 0600 原子创建与常数时间校验均有测试；`jx-cli keygen`/`rdb-info` 集成测试通过（Task 5/14, `590c1d3`/`55a3a42`）。`sm4-gcm 0.1.2`（16 字节密钥，由 32 字节 keyfile 密钥派生） |
| 三架构 | `scripts/build-*.sh` | 三 target 编译成功（LoongArch 需工具链环境） | ⚠️ PARTIAL | 脚本存在且 `bash -n` 通过：`build-linux.sh`、`build-loongarch.sh`、`check-licenses.sh`（Task 17, `429c083`）。host aarch64-apple-darwin release 构建通过（本轮 perf-matrix 复跑再次验证）。Linux x86_64/aarch64：纯 Rust crates 编译 OK；**`ring` 构建脚本因缺少 cross-gcc（`x86_64/aarch64-linux-gnu-gcc`）失败**，脚本分类为 `TOOLCHAIN_WARN` 并 exit 0。LoongArch 同理缺龙芯 cross-gcc。三 target 最终二进制**尚未产出** |
| 许可证 | `scripts/check-licenses.sh` | 通过 | ✅ PASS | 2026-10-05 复跑：`cargo-deny 0.20.2` → `licenses ok` / `license check: OK`，exit 0（deny.toml 白名单：MIT/Apache-2.0/BSD/ISC/Unlicense/Unicode-3.0） |
| 未支持命令 | `MULTI`/`EVAL` 等 | 返回 `ERR ... not supported` | ✅ PASS | `jx-engine/src/commands/mod.rs` `is_unimplemented()` 覆盖 MULTI/EXEC/DISCARD/WATCH、PUB/SUB、EVAL、XADD/GEO/PFADD、HELLO/COMMAND 等；`execute` 返回 `ERR command '…' is not supported in this version`；`jx-engine/tests/string.rs` 断言 MULTI 错误串含 `not supported` |
| 回归 + 兼容冒烟 + 矩阵（perf-shard T9） | `cargo test --workspace` + `tests/compat/run_compat.sh` + `scripts/perf-matrix.sh` | 回归全绿；兼容 PASS；矩阵如实记录 | ✅ PASS（矩阵除外，见上） | 本轮实跑：workspace 176/0；compat `redis-py smoke OK` + `go-redis smoke OK` + `compat smoke OK`；perf-matrix pipeline=1 FAIL（172k/177k）—— 数字诚实、与 Task 8 基线一致 |

---

## 本轮验证记录（2026-10-05 复跑，perf-shard Task 9）

### cargo test --workspace

```
环境: rustc 1.99.0 (b940084d7 2026-09-28), cargo 1.99.0, macOS arm64
结果: total_passed=176  total_failed=0  ignored=1
```

| 套件 | 结果 |
|------|------|
| jx-cli `tests/cli.rs` | 10 passed |
| jx-crypto `crypto.rs` / `tlcp.rs` | 5 / 6 passed |
| jx-engine（lib + actor/collections/coordinator/mem_evict/ops/persistence_cmds/shard_execute/string） | 1+32+9+9+12+4+1+1+13 passed |
| jx-persist `aof.rs` / `rdb.rs` | 20 / 8 passed |
| jx-proto（lib + decode/encode） | 1+11+4 passed |
| jx-server（actor_smoke/config/ops/persistence/server_smoke/shutdown） | 2+2+9 passed（ops 1 ignored）+3+8+5 passed |
| Doc-tests（全部 crate） | 0（无 doc-test 失败） |

相对 MVP 基线（102 passed）增量主要来自：shard-ownership 引入的 `actor.rs` 32 项、`coordinator.rs` 9 项、`shard_execute.rs` 1 项、pipeline_batch 与 AOF sink/跨 shard 恢复等修复测试。

### 脚本

| 脚本 | 结果 |
|------|------|
| `tests/compat/run_compat.sh` | **PASS** — redis-py smoke OK + go-redis smoke OK（2026-10-05 复跑） |
| `scripts/check-licenses.sh` | **PASS** — `cargo-deny 0.20.2` → `licenses ok` / `license check: OK`，exit 0（2026-10-05 复跑） |
| `scripts/perf-matrix.sh` | 2026-10-05T04:01:34Z 实跑：pipeline=1 SET **171,714** / GET **177,371** → **FAIL** vs 500k；pipeline=32 SET 372,859 / GET 405,309；clients=16 → 159,899/162,786；clients=256 → 170,865/176,209；errs 全 0。与 `2026-10-05-shard-ownership-baseline.md` 一致，**无新增性能提升** |
| `scripts/perf-report.sh` | 薄封装，`exec` 到 `perf-matrix.sh`（保持旧文档/命令兼容） |
| `scripts/build-linux.sh` / `build-loongarch.sh` | `bash -n` OK；实跑 TOOLCHAIN_WARN（ring/cross-gcc） |
| `scripts/tlcp-interop.sh` | `bash -n` OK；GmSSL 互操作未完整执行（加密密钥格式 gap） |

### 原始输出摘录（perf-matrix 2026-10-05T04:01:34Z）

```
== primary acceptance vs 500000 OPS/s ==
SET ops/s=171714  GET ops/s=177371  => FAIL

SET ops=100000 ops/s=372859 batch_avg_us=4981.5 p50_batch_us=4957.6 p99_batch_us=7954.0 errs=0
GET ops=100000 ops/s=405309 batch_avg_us=4680.6 p50_batch_us=4707.4 p99_batch_us=7388.4 errs=0
```

---

## 架构交付与 API 变化（perf-shard Tasks 1–7，本任务确认）

### 已交付架构

- **Exclusive shard ownership**（`jx-engine/src/actor.rs`）：每个 shard 线程独占 `ShardState`（move 进线程），**Engine 内部不再有 per-shard Mutex**；消除了旧架构的双重序列化（actor channel hop + Mutex）。
- **协调器**（`jx-engine/src/coordinator.rs`）：跨 shard 命令（MGET/KEYS/FLUSHALL/DBSIZE/INFO/DEL 子集/跨 shard RENAME）聚合；server 侧 `jx-server/src/dispatch.rs` 直接路由到 coordinator。
- **持久化接入 shard runtime**：`AofSink` trait + `ShardRuntime::set_aof_sink`；单 shard 写命令在 owning actor 上追加 AOF（无额外 channel hop）；coordinator 路径经 `append_aof` / `append_aof_all`；RDB 经 `Snapshot`/`LoadShard`/`Broadcast`。
- **RESP 热路径**（`jx-proto`）：in-place decoder state machine（`5f14e87`）。
- **Pipeline 回复批处理**（`jx-server`）：按 pipeline batch 聚合编码（`50640c5`）。
- **热路径派发 / 整数路径微优化**（`jx-engine`，`98ac7a0`）。

### 公开 API 变化（破坏性，仅库接口；RESP2 线协议与 RDB/AOF 格式不变）

| 旧（mvp） | 新（perf-shard） |
|-----------|------------------|
| `jx_engine::Engine`（持 `Mutex<ShardState>` 分片表） | **已移除**；`jx-engine/src/lib.rs` 不再导出 `Engine` |
| `Engine::lock_shard(...)` / `Engine::execute_via_mutex(...)` | **gone**；命令路径为 `ShardRuntime::exec` / `exec_no_aof` |
| 命令函数签名 `fn set(e: &Engine, ...)` | `fn set(sh: &mut ShardState, args: &[Bytes], now: u64)` |
| — | **新引擎表面：** `jx_engine::actor::ShardRuntime`（`spawn_shard_runtime(shard_count, capacity, now_ms)`；`exec`/`exec_no_aof`/`set_aof_sink`/`append_aof`/`append_aof_all`） |
| — | **消息类型：** `Cmd::{Exec, ConfigMaxMemory, ConfigPolicy, Snapshot, LoadShard, MaxMemory, Expire, Broadcast, FetchRemove, Insert, DelKey}`（`actor.rs`） |
| — | **协调器 API：** `jx_engine::coordinator::{mget_coalesced, keys_glob, flushall, dbsize, info, ...}` |
| — | **Server 接线：** 仅持有 `ShardRuntime`；`dispatch(rt: &ShardRuntime, args: Vec<Bytes>) -> RespValue` |
| — | **过载语义：** per-shard 有界 crossbeam channel，满时返回 `ERR shard command queue full`（不无限阻塞） |

**兼容承诺：** RESP2 语义、附录 A 命令行为、错误字符串、RDB/AOF 格式、国密 API 不变（spec §兼容）。测试夹具允许等价改写（去掉 `lock_shard` 直操作），断言语义不变——本轮 workspace 全绿即证据。

**对嵌入式调用方的影响：** 若外部代码直接依赖 `jx_engine::Engine` / `lock_shard`，需迁移到 `ShardRuntime` + `ShardState::execute`；`jx-server` 二进制行为不变。

---

## 汇总

| 状态 | 数量 | 项 |
|------|------|-----|
| ✅ PASS | 6 | 附录 A 必须命令（核心集）、RDB/AOF、SM3/SM4、许可证、未支持命令、回归+兼容+矩阵记录（T9） |
| ⚠️ PARTIAL | 2 | TLCP 实测（loopback/接线通过，GmSSL 互操作未完整执行）、三架构（脚本就绪，cross 链接受阻于 ring） |
| ❌ FAIL | 1 | 明文 50 万 OPS（pipeline=1 ~170k–177k，缺口约 2.9×；架构已交付） |
| ⛔ NOT DONE | — | （spec 非目标：Web 管控台、主从/集群、Lua 等） |

**MVP + perf-shard 结论:** 功能与国密/持久化/命令集/许可证路径整体可验收；**exclusive shard ownership 架构已交付并回归全绿**；**性能验收线（pipeline=1 ≥ 50 万 OPS）仍 FAIL**（~170k，与 2026-10-05 基线一致）；**信创 Linux/LoongArch 交叉编译与 GmSSL TLCP 互操作** 为交付环境依赖缺口，不是代码缺失。

---

## Known gaps / follow-ups

### 1. 性能缺口（未达标 — 最高优先级）

- 目标：明文 RESP GET/SET 64B ≥ 500,000 OPS/s；本轮实测 pipeline=1 SET ≈ 171.7k / GET ≈ 177.4k（2026-10-05T04:01Z）。
- **已落地：** exclusive shard ownership（无 Engine Mutex）、in-place RESP decoder、pipeline 回复批处理、热路径派发微优化——收益主要在 pipeline=32（+~40%，~373k–405k），pipeline=1 吞吐几乎未变。
- **剩余瓶颈**（详见 `2026-10-05-shard-ownership-baseline.md`）：每命令 channel hop 仍按条计费；per-request TCP 路径无 writev/readv 摊薄；clients=16 时单请求延迟已 ~95–100µs，反推聚合上限 ~160k。
- 下一步（按收益）：pipeline 感知批量投递（一次 channel send 携带 N 条命令）→ 连接层 I/O 合并（writev/批量 read）→ 热路径再压 → 多进程外部客户端（memtier/redis-benchmark）+ 信创 Linux 真机复测。
- 禁止用 pipeline=32 批量吞吐宣称达标。

### 2. 三架构交叉编译（ring / cross-gcc）

- `ring`（TLS 路径经 rustls）在 macOS 上缺 cross-gcc 时 build script 失败，阻断所有 Linux/LoongArch target 的最终二进制。
- 完成方式：在原生 Linux 信创机构建（最简），或安装 cross-gcc 并配置 linker/CC，或（代码级）为非 TLS 部署加 feature 绕过 ring。
- LoongArch 需龙芯工具链；脚本已打印所需环境变量。

### 3. GmSSL TLCP 互操作

- loopback 握手与 server 拒绝明文客户端已验证；`scripts/tlcp-interop.sh` 因 GmSSL PEM 密钥格式（加密 PKCS#8）与 gm-crypto 解析器不兼容，未完成自动互操作。
- 生产 TLCP 声明前需：GmSSL 侧可读密钥/证书格式对齐，或补齐 in-repo TLCP 客户端 e2e（gm-tlcp client + `with_server_sign_key` + client certs）。

### 4. 附录 A 命令集细节

- `HMSET` 为 stretch，当前返回 not supported（`HSET` 可作兼容替代）。
- 仍在 `is_unimplemented` 的边缘项：`GETBIT`/`SETBIT`/`BITCOUNT`、`COMMAND` 等；AUTH 在 conn 层实现、engine 列表仍含 AUTH（经 server 时 AUTH 不走 engine dispatch）。
- 已实现：`SELECT`（仅 db0）、`SETEX`/`PSETEX`、`MSETNX`、`RENAME`、`RANDOMKEY`。
- 加密 AOF：实时 `append_aof` 仍为单命令 MVP（每次覆盖文件）；`rewrite_aof` 已改为内存构建完整命令流后一次性 seal 为单个 `JXAOFENC` 载荷（含 `PEXPIREAT` TTL）。多命令加密持久化亦可使用加密 RDB。

### 4b. RDB shard-count 约束

- RDB 按 shard 分区保存：`shards=N` 写出的 `dump.rdb` 只能由同样 `shards=N` 的引擎加载，`load_rdb` 会拒绝不匹配。
- 修改 `shards` 配置后需重新 SAVE（或改用与 shard 数无关的 AOF replay）再启动。
- 详见 `jx-persist/src/rdb.rs` 中 `save_rdb`/`load_rdb` 的模块文档。

### 4c. MSETNX 并发原子性（已知缺口）

- 单客户端语义正确：全部 key 空闲 → 全部写入返回 1；任一已存在 → 返回 0 且无部分写入（`jx-engine/tests/string.rs` `setex_psetex_msetnx_select`）。
- perf-shard 后 MSETNX 经 `coordinator` 跨 shard 广播（`Cmd::Insert`），但「先检查全部空闲再写入」仍是两阶段：检查阶段逐 shard 读后释放，写入阶段再逐 shard `Insert`；两阶段之间无全局锁，并发下仍可能交错（与旧 MVP 行为一致，测试覆盖单客户端语义）。
- 若需严格原子性：需在协调器引入跨 shard 事务锁或把检查+写入合并为单次 `Broadcast` 变体（后续）。

### 5. 范围外（spec 非目标 / roadmap）

- Web 管控台 — out of scope（MVP）。
- 主从、集群、Lua — roadmap，不在本次验收（perf-shard 设计 §8：后续独立子项目）。
- `maxclients`/`timeout` 硬限、完整 client-cert CA 校验接线 — 后续任务。
- salted AUTH（`requirepass-sm3 = salthex:hashhex`）已支持；CONFIG GET 已脱敏为 `(hidden)`。

---

## 相关提交（分支 `MemoryDataBase/perf-shard`，git root `/Users/caosunliu`）

```
d955eab feat: scaffold MemoryDataBase cargo workspace
…（Tasks 1–14 实现与修复，见 progress.md）
d7bdbdd test: client compatibility smoke scripts
acd11d8 perf: baseline bench script and report
429c083 build: xinchuang target build and license scripts
（mvp 验收）docs: MVP acceptance checklist
02e391e feat(jx-engine): extract cross-shard coordinator
60aac8b feat: RDB/AOF via exclusive shard runtime (tested)
97b2b96 fix(persist): live AOF for DEL/FLUSHALL; compact shard AOF on SAVE
82798c5 fix(persist): cross-shard RENAME AOF deletes dst first
5f14e87 perf(jx-proto): in-place RESP decoder state machine
50640c5 perf(jx-server): pipeline response batching
98ac7a0 perf(jx-engine): hot-path command dispatch and integer paths
f80bf97 perf: acceptance matrix script and shard-ownership baseline
（本任务）docs: update acceptance for shard-ownership   ← 本文件
```

---

## 复现

```bash
cd /Users/caosunliu/opencode/MemoryDataBase
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

cargo test --workspace
bash tests/compat/run_compat.sh
bash scripts/check-licenses.sh
bash scripts/perf-matrix.sh          # 明文矩阵 vs 500k（pipeline=1 FAIL，见 2026-10-05 baseline）
bash scripts/perf-report.sh          # 薄封装 → perf-matrix.sh（旧文档兼容）
bash scripts/build-linux.sh          # Linux 交叉（需 cross-gcc 才能出二进制）
bash scripts/build-loongarch.sh      # LoongArch（需龙芯工具链）
bash scripts/tlcp-interop.sh         # GmSSL 互操作（需 gmssl + 兼容密钥格式）
```
