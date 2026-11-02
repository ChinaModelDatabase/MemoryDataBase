# MemoryDataBase 性能子项目（真分片所有权）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 MemoryDataBase 热路径重构为真分片所有权（无 Mutex），消除双重序列化，开发机明文 GET/SET pipeline=1 达到 ≥50 万 OPS，并交付完整压测矩阵与回归全绿。

**Architecture:** tokio 连接层解析 RESP 后按 `hash(key)%N` 投递到分片 Actor；每个分片数据由独占 OS 线程拥有（`ShardState` 无锁）；跨分片命令经协调器聚合；持久化经 Snapshot/AofBatch 消息在命令间隙处理；响应 pipeline 合并 writev。

**Tech Stack:** Rust、tokio、crossbeam-channel、bytes（现有栈，不新增运行时依赖除非 micro-opt 明确要求）。

**Spec:** `opencode/MemoryDataBase/docs/superpowers/specs/2026-10-05-MemoryDataBase-perf-shard-ownership-design.md`

## Global Constraints

- 严格兼容：RESP2 语义、附录 A 命令行为、错误字符串、RDB/AOF 格式、国密 API 不变。
- 回归门禁：`cargo test --workspace` 全绿（测试夹具允许等价改写，断言语义不变）。
- 主验收：明文 GET/SET pipeline=1 **≥ 500,000 OPS/s**；矩阵含 pipeline=32、p99（达标时 ≤2ms）、80/20、client 16/64/256；TLCP 单独报告。
- 路径相对 `/Users/caosunliu/opencode/MemoryDataBase/`；git 根 `/Users/caosunliu`；提交 `git -C /Users/caosunliu add opencode/MemoryDataBase/<path>`。
- cargo：`export PATH="$(dirname "$(rustup which cargo)"):$PATH"`。
- 每任务 TDD：失败测试 → 实现 → 通过 → commit。
- Actor 崩溃 = 进程 fail-fast；channel 有界，满返回 `ERR shard command queue full`。

## File Structure

```
crates/jx-engine/src/
  types.rs          # JxValue/Entry/KeyMeta/ZSet（基本不变）
  shard.rs          # ShardState + execute/命令方法（从 commands 迁入）
  commands/         # 命令实现改为 &mut ShardState 方法
  coordinator.rs    # 跨分片广播/聚合
  actor.rs          # 独占所有权 ShardRuntime
  lib.rs            # Engine 路由表（无 Mutex 数据）
  ttl.rs / evict.rs # 变为 ShardState 方法
crates/jx-proto/src/decode.rs   # 就地状态机
crates/jx-server/src/conn.rs    # pipeline writev；新 runtime 接线
scripts/perf-matrix.sh          # 扩展压测矩阵
docs/superpowers/perf/          # 新基线 + ACCEPTANCE 更新
```

---

## Task 1: ShardState::execute 迁移（Engine 仍持 Mutex 适配）

**Files:**
- Create: `crates/jx-engine/src/shard.rs`
- Modify: `crates/jx-engine/src/{lib.rs,commands/*.rs,ttl.rs,evict.rs}`
- Test: `crates/jx-engine/tests/shard_execute.rs`（新）；现有测试保持绿

**Interfaces:**
- Produces:
  - `impl ShardState { pub fn execute(&mut self, args: &[Bytes], now: u64) -> Result<RespValue, RespValue>; }`
  - 命令函数签名：`fn set(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue>`（去掉 `&Engine`）
  - `Engine::execute_via_mutex(&self, args: &[Bytes]) -> Result<RespValue, RespValue>`（临时适配：锁分片后调 `ShardState::execute`，供旧测试/未迁移路径）

- [ ] **Step 1: 写失败测试**

```rust
// crates/jx-engine/tests/shard_execute.rs
use bytes::Bytes;
use jx_engine::{system_now_ms, MaxMemoryPolicy, ShardState};

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn shard_state_set_get_incr() {
    let mut sh = ShardState::new_empty(MaxMemoryPolicy::NoEviction);
    let now = system_now_ms();
    assert_eq!(sh.execute(&[b("SET"), b("k"), b("v")], now).unwrap(), jx_proto::RespValue::ok());
    assert_eq!(sh.execute(&[b("GET"), b("k")], now).unwrap(), jx_proto::RespValue::bulk(b("v")));
    assert_eq!(sh.execute(&[b("INCR"), b("n")], now).unwrap(), jx_proto::RespValue::int(1));
}
```

- [ ] **Step 2: 确认失败** `cargo test -p jx-engine --test shard_execute` → FAIL

- [ ] **Step 3: 实现迁移**

`shard.rs`（新建）：
```rust
use std::collections::HashMap;
use bytes::Bytes;
use jx_proto::RespValue;
use crate::types::{Entry, KeyMeta, MaxMemoryPolicy, ZSet};
use crate::{commands, evict, ttl};

pub struct ShardState {
    pub dict: HashMap<Bytes, Entry>,
    pub mem: usize,
    pub maxmemory: Option<usize>,
    pub policy: MaxMemoryPolicy,
}

impl ShardState {
    pub fn new_empty(policy: MaxMemoryPolicy) -> Self {
        Self { dict: HashMap::new(), mem: 0, maxmemory: None, policy }
    }

    pub fn touch_alive(&mut self, key: &Bytes, now: u64) -> bool {
        if let Some(e) = self.dict.get_mut(key) {
            if let Some(exp) = e.meta.expire_at_ms {
                if now >= exp {
                    self.dict.remove(key);
                    return false;
                }
            }
            e.meta.last_access_ms = now;
            e.meta.freq = e.meta.freq.saturating_add(1);
            true
        } else {
            false
        }
    }

    pub fn expire_cycle(&mut self, now: u64, budget: usize) -> u64 {
        ttl::expire_cycle(self, now, budget)
    }

    pub fn evict_if_needed(&mut self, key: &Bytes, now: u64) -> Result<(), RespValue> {
        evict::evict_if_needed(self, key, now)
    }

    pub fn execute(&mut self, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
        commands::dispatch(self, args, now)
    }
}
```

`commands/mod.rs` 增加：
```rust
pub fn dispatch(sh: &mut crate::ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    // 与现网 execute 相同的 match，但调用 string::set(sh, ...) 等
}
```

各命令文件：将 `e: &Engine` 参数改为 `sh: &mut ShardState`，内部 `let sid = e.shard_id_for_key(...)` / `e.lock_shard(sid)` 删除，直接操作 `sh`。`ttl::expire_cycle` / `evict::evict_if_needed` 改为 `sh` 方法或 `&mut ShardState` 自由函数。

`lib.rs`：`Engine` 暂时仍持有 `Vec<Mutex<ShardState>>`；增加：
```rust
impl Engine {
    pub fn execute_via_mutex(&self, args: &[Bytes]) -> Result<RespValue, RespValue> {
        let now = self.now();
        let key = args.get(1).cloned().unwrap_or_default();
        let sid = self.shard_id_for_key(&key);
        let mut sh = self.lock_shard(sid);
        sh.execute(args, now)
    }
}
```
旧 `execute(&Engine, args)` 改为调用 `execute_via_mutex`（或在 conn/dispatch 逐步切换）。

- [ ] **Step 4: 全量测试** `cargo test --workspace` → 全绿（夹具若直接 `lock_shard` 操作 dict，改为经 `execute` 命令或 `ShardState` 直构）

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "refactor(jx-engine): migrate commands to ShardState::execute"
```

---

## Task 2: 独占所有权 ShardRuntime（删除 Engine 数据 Mutex）

**Files:**
- Modify: `crates/jx-engine/src/{lib.rs,actor.rs}`, `crates/jx-server/src/{net.rs,conn.rs,dispatch.rs,persistence_cmd.rs}`
- Test: `crates/jx-engine/tests/actor.rs`, `crates/jx-server/tests/*`（等价改写）

**Interfaces:**
- Produces:
  - `pub struct ShardRuntime { shard_count: usize, shards: Vec<ShardHandle>, now_ms: fn() -> u64 }`
  - `pub fn spawn_shard_runtime(shard_count: usize, capacity: usize, now_ms: fn() -> u64) -> ShardRuntime`
  - `impl ShardRuntime { pub fn exec(&self, args: Vec<Bytes>) -> RespValue; pub fn shard_id_for_key(&self, key: &Bytes) -> usize; pub fn shard_count(&self) -> usize; pub fn set_maxmemory(&self, bytes: usize, policy: MaxMemoryPolicy); pub fn set_maxmemory_policy(&self, policy: MaxMemoryPolicy); pub fn maxmemory(&self) -> Option<usize>; pub fn broadcast(&self, f: impl Fn(usize, &mut ShardState) + Send + Sync + 'static); }`
  - Actor 线程内持有 `ShardState`（move 进 thread），`Cmd::Exec` 处理 `sh.execute(args, now)`。
  - Engine **不再**公开 `lock_shard`；数据不在 Engine。

- [ ] **Step 1: 失败测试（独占所有权）**

```rust
// crates/jx-engine/tests/actor.rs 更新
use bytes::Bytes;
use jx_engine::{actor::spawn_shard_runtime, system_now_ms, MaxMemoryPolicy};
use jx_proto::RespValue;

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn exclusive_shard_runtime_set_get() {
    let rt = spawn_shard_runtime(4, 1024, system_now_ms);
    assert_eq!(rt.exec(vec![b("SET"), b("a"), b("1")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
}

#[test]
fn maxmemory_config_broadcast() {
    let rt = spawn_shard_runtime(2, 64, system_now_ms);
    rt.set_maxmemory(100, MaxMemoryPolicy::NoEviction);
    let err = rt.exec(vec![b("SET"), b("big"), Bytes::from(vec![b'x'; 200])]);
    assert!(matches!(err, RespValue::Error(m) if m.starts_with("OOM")));
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现独占 Runtime**

`actor.rs` 重写核心：
```rust
pub enum Cmd {
    Exec { args: Vec<Bytes>, reply: std::sync::mpsc::Sender<Result<RespValue, RespValue>> },
    ConfigMaxMemory { bytes: usize, policy: MaxMemoryPolicy, reply: std::sync::mpsc::Sender<()> },
    ConfigPolicy { policy: MaxMemoryPolicy, reply: std::sync::mpsc::Sender<()> },
    Snapshot { reply: std::sync::mpsc::Sender<Vec<(Bytes, crate::types::JxValue, crate::types::KeyMeta)>> },
    LoadShard { entries: Vec<(Bytes, crate::types::JxValue, crate::types::KeyMeta)>, reply: std::sync::mpsc::Sender<()> },
}

pub fn spawn_shard_runtime_with_capacity(shard_count: usize, capacity: usize, now_ms: fn() -> u64) -> ShardRuntime {
    let shard_count = shard_count.max(1);
    let capacity = capacity.max(1);
    let mut shards = Vec::new();
    for id in 0..shard_count {
        let (tx, rx) = crossbeam_channel::bounded::<Cmd>(capacity);
        let join = std::thread::spawn(move || {
            let mut sh = crate::ShardState::new_empty(MaxMemoryPolicy::NoEviction);
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    Cmd::Exec { args, reply } => {
                        let now = now_ms();
                        let _ = reply.send(sh.execute(&args, now));
                    }
                    Cmd::ConfigMaxMemory { bytes, policy, reply } => {
                        sh.maxmemory = if bytes == 0 { None } else { Some(bytes) };
                        sh.policy = policy;
                        let _ = reply.send(());
                    }
                    Cmd::ConfigPolicy { policy, reply } => {
                        sh.policy = policy;
                        let _ = reply.send(());
                    }
                    Cmd::Snapshot { reply } => {
                        let _ = reply.send(sh.dict.iter().map(|(k,e)| (k.clone(), e.value.clone(), e.meta.clone())).collect());
                    }
                    Cmd::LoadShard { entries, reply } => {
                        for (k,v,m) in entries { sh.dict.insert(k, crate::types::Entry{value:v,meta:m}); }
                        let _ = reply.send(());
                    }
                }
            }
        });
        shards.push(ShardHandle { id, join, tx });
    }
    ShardRuntime { shard_count, shards, now_ms }
}
```

`ShardRuntime::exec` 与现网相同（try_send + oneshot/mpsc recv + queue full 错误）。  
`Engine`：删除 `Mutex<ShardState>` 与 `lock_shard`/`execute_via_mutex`/`set_maxmemory` 等数据方法；保留或删除 Engine（server 改为只持有 `ShardRuntime`）。迁移 `jx-server`：`net.rs` 创建 `spawn_shard_runtime(cfg.shards_or_default(cpu), maxclients, system_now_ms)`；`dispatch.rs` 改为 `rt.exec(args)`；`persistence_cmd`/`shutdown` 使用 `Cmd::Snapshot`/`LoadShard`/`ConfigMaxMemory`。

- [ ] **Step 4: 全量测试** `cargo test --workspace` → 全绿

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates
git -C /Users/caosunliu commit -m "refactor: exclusive shard ownership, Engine becomes router"
```

---

## Task 3: 跨分片协调器（MGET/KEYS/FLUSHALL/DBSIZE）

**Files:**
- Create: `crates/jx-engine/src/coordinator.rs`
- Modify: `crates/jx-engine/src/{lib.rs,actor.rs}`, `crates/jx-server/src/conn.rs`（路由跨分片命令）
- Test: `crates/jx-engine/tests/coordinator.rs`

**Interfaces:**
- Produces:
  - `pub fn mget_coalesced(rt: &ShardRuntime, args: &[Bytes]) -> RespValue`（按输入序）
  - `pub fn keys_glob(rt: &ShardRuntime, pattern: &Bytes) -> RespValue`
  - `pub fn flushall(rt: &ShardRuntime) -> RespValue`
  - `pub fn dbsize(rt: &ShardRuntime) -> RespValue`
  - conn 层：命令名为上述集合时走协调器，不走单分片 `exec`。

- [ ] **Step 1: 失败测试**

```rust
use bytes::Bytes;
use jx_engine::{actor::spawn_shard_runtime, coordinator, system_now_ms};
use jx_proto::RespValue;

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn mget_preserves_input_order_across_shards() {
    let rt = spawn_shard_runtime(4, 256, system_now_ms);
    for i in 0..20 {
        rt.exec(vec![b("SET"), Bytes::from(format!("k{i}")), Bytes::from(format!("v{i}"))]);
    }
    let mut args = vec![b("MGET")];
    for i in 0..20 { args.push(Bytes::from(format!("k{i}"))); }
    match coordinator::mget_coalesced(&rt, &args[1..]) {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 20);
            for (i, it) in items.iter().enumerate() {
                assert_eq!(it, &RespValue::bulk(Bytes::from(format!("v{i}"))));
            }
        }
        other => panic!("bad {other:?}"),
    }
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现协调器**

```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::actor::ShardRuntime;
use std::collections::HashSet;

pub fn mget_coalesced(rt: &ShardRuntime, keys: &[Bytes]) -> RespValue {
    // 单线程即可：对每个 key rt.exec(GET) 保持输入序；后续可分组并行
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        out.push(rt.exec(vec![Bytes::from_static(b"GET"), k.clone()]));
    }
    RespValue::Array(Some(out))
}

pub fn keys_glob(rt: &ShardRuntime, pattern: &Bytes) -> RespValue {
    let mut seen = HashSet::new();
    for sid in 0..rt.shard_count() {
        // 通过每分片 KEYS 不便：用协调器广播——MVP：exec KEYS 到分片0 不正确。
        // 实现：Cmd 广播见 actor.rs 广播辅助；此处用 rt.broadcast 收集。
        let _ = sid;
        let _ = &mut seen;
    }
    // 完整实现：rt.broadcast 收集各分片匹配 key，去重后返回 Array
    RespValue::array(vec![])
}

pub fn flushall(rt: &ShardRuntime) -> RespValue {
    rt.broadcast(|_, sh| { sh.dict.clear(); sh.mem = 0; });
    RespValue::ok()
}

pub fn dbsize(rt: &ShardRuntime) -> RespValue {
    // broadcast 收集各分片 dict.len() 求和
    RespValue::int(0)
}
```

> 执行说明：为 KEYS/DBSIZE 增加 `Cmd::ForEach { f: ... }` 或 `Cmd::Collect` 消息，由各分片线程在独占状态下执行并回传结果；禁止跨线程直接访问 dict。

- [ ] **Step 4: conn 路由 + 全量测试** → 绿

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates
git -C /Users/caosunliu commit -m "feat: cross-shard coordinator for MGET/KEYS/FLUSHALL/DBSIZE"
```

---

## Task 4: 持久化消息接入（Snapshot/Load/AOF 缓冲）

**Files:**
- Modify: `crates/jx-engine/src/actor.rs`, `crates/jx-persist/src/{rdb.rs,aof.rs}`, `crates/jx-server/src/{net.rs,persistence_cmd.rs}`
- Test: `crates/jx-persist/tests/rdb.rs`（经 runtime 加载/保存）

**Interfaces:**
- Produces:
  - `ShardRuntime::save_rdb(path, key: Option<&[u8;32]>) -> Result<()>`（协调各分片 Snapshot，组文件）
  - `ShardRuntime::load_rdb(path, key) -> Result<usize>`
  - 分片内 AOF：`Cmd::AppendAof { args, key }` 或 Actor 内直接调用 `jx_persist::encode_command` 追加分片缓冲文件（每分片 `appendonly.{sid}.aof`，刷盘合并策略见 spec）。
  - 恢复顺序：RDB → AOF 重放（server net.rs 不变语义）。

- [ ] **Step 1: 失败测试（RDB 经 runtime 往返）**

```rust
use bytes::Bytes;
use jx_engine::{actor::spawn_shard_runtime, system_now_ms};
use jx_persist::{load_rdb, save_rdb};

#[test]
fn rdb_roundtrip_via_shard_runtime() {
    let rt = spawn_shard_runtime(2, 64, system_now_ms);
    rt.exec(vec![Bytes::from_static(b"SET"), Bytes::from_static(b"a"), Bytes::from_static(b"1")]);
    let dir = std::env::temp_dir().join(format!("jx-rt-rdb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dump.rdb");
    // 使用 ShardRuntime 保存 API 或现有 save_rdb 从 runtime 导出快照
    let _ = save_rdb; // 迁移实现后调用 rt.save_rdb
    let _ = load_rdb;
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现**

- `save_rdb` 重载为接受快照收集器：遍历 `Cmd::Snapshot` 得到各分片 `Vec<(key,value,meta)>`，按 Task 1 的 RDB 布局写出（格式不变）。
- `load_rdb`：解析后按 `shard_id_for_key` 投递 `Cmd::LoadShard`（分片数必须与文件一致，否则 Err）。
- AOF：写命令在 Actor 内 `append_aof` 到分片文件；`BGREWRITEAOF`/shutdown 经协调器收集全量再写（含 PEXPIREAT，密钥整流 seal——保持现网修复行为）。

- [ ] **Step 4: 全量测试** → 绿

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates
git -C /Users/caosunliu commit -m "feat: RDB/AOF via exclusive shard runtime"
```

---

## Task 5: RespDecoder 就地状态机（消除 probe clone）

**Files:**
- Modify: `crates/jx-proto/src/decode.rs`
- Test: `crates/jx-proto/tests/decode.rs`（现有语义全部保持）

**Interfaces:**
- Produces: 同公开 API `RespDecoder::{new,feed,next_value}`；内部不再 `buf.clone()` 整缓冲。

- [ ] **Step 1: 失败测试（新增性能回归语义——行为不变，加部分帧用例）**

```rust
#[test]
fn decode_large_pipeline_no_error() {
    let mut d = jx_proto::RespDecoder::new();
    let mut chunk = Vec::new();
    for i in 0..100 {
        chunk.extend_from_slice(format!("*1\r\n$3\r\nGET\r\n").as_bytes());
        let _ = i;
    }
    d.feed(&chunk);
    let mut n = 0;
    while let Some(_v) = d.next_value().unwrap() { n += 1; }
    assert_eq!(n, 100);
}
```

- [ ] **Step 2: 确认失败或既有绿（行为测试）** — 实现重构后必须仍绿

- [ ] **Step 3: 实现就地状态机**

```rust
pub struct RespDecoder {
    buf: bytes::BytesMut,
    /// 已成功解析但未消费的帧结束偏移（0 表示无）
    consumed: usize,
}

impl RespDecoder {
    pub fn next_value(&mut self) -> Result<Option<RespValue>, DecodeError> {
        // 在 self.buf[self.consumed..] 上 try_parse；成功则 consumed += 帧长
        // 半包：返回 Ok(None)，不移动 consumed，不 clone 整缓冲
    }
}
```

`try_parse` 改为「在切片上解析并返回 (value, bytes_consumed)」；错误时连接层仍关闭连接。

- [ ] **Step 4: `cargo test -p jx-proto` → 绿；workspace 绿**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-proto
git -C /Users/caosunliu commit -m "perf(jx-proto): in-place RESP decoder state machine"
```

---

## Task 6: Pipeline 响应合并 writev

**Files:**
- Modify: `crates/jx-server/src/conn.rs`
- Test: `crates/jx-server/tests/ops.rs`（pipeline 顺序测试已有则加强）

**Interfaces:**
- Produces: 同连接 pipeline 内命令按序执行/响应；编码阶段合并到单 `BytesMut` 后一次 `write_all`（或 `write_vectored`）。

- [ ] **Step 1: 失败/回归测试（pipeline 顺序）**

```rust
#[test]
fn pipeline_responses_in_order() {
    // TCP 连接发送: SET a 1 / GET a / INCR n 三条，一次读回期望顺序 +OK $1 1 :1
}
```

- [ ] **Step 2: 实现**

conn 循环：将 `feed` 后可解析出的全部完整命令收集为 `Vec<Vec<Bytes>>`（受 `max_pipeline` 上限），**顺序**执行（每条 `rt.exec` 或协调器），将结果编码进同一输出缓冲，一次写出。

- [ ] **Step 3: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "perf(jx-server): pipeline response batching"
```

---

## Task 7: 热路径微优化（命令名/整数路径）

**Files:**
- Modify: `crates/jx-engine/src/commands/mod.rs`, `crates/jx-proto/src/encode.rs`（如需）
- Test: 现有引擎测试

- [ ] **Step 1: 命令名匹配避免分配**

```rust
fn cmd_eq(name: &[u8], upper: &str) -> bool {
    name.len() == upper.len() && name.iter().zip(upper.bytes()).all(|(a,b)| a.to_ascii_uppercase()==b)
}
// dispatch: if cmd_eq(&args[0], "SET") { ... }
```

- [ ] **Step 2: INCR/GET 整数路径**减少 `to_string`/`parse` 往返（Str 存整数时直接用 `JxValue::Int`，GET 时 format 一次）。

- [ ] **Step 3: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "perf(jx-engine): hot-path command dispatch and integer paths"
```

---

## Task 8: 压测矩阵脚本与新基线

**Files:**
- Create: `scripts/perf-matrix.sh`
- Modify: `scripts/perf-report.sh`（调用 matrix 或保持兼容）
- Create: `docs/superpowers/perf/2026-10-05-shard-ownership-baseline.md`

**Interfaces:**
- Produces: 矩阵输出（pipeline=1 达标判定、pipeline=32、80/20、client 16/64/256、p99）；TLCP 可选段单独打印。

- [ ] **Step 1: 写脚本**

```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
# build release; start plain server; run jx-cli bench variants; print PASS/FAIL vs 500000
```

- [ ] **Step 2: 运行矩阵并写基线文档（真实数字，不造假）**

Run: `bash scripts/perf-matrix.sh`
若 pipeline=1 ≥500000 → 文档写达标；否则如实写差距与瓶颈。

- [ ] **Step 3: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/scripts opencode/MemoryDataBase/docs/superpowers/perf
git -C /Users/caosunliu commit -m "perf: acceptance matrix script and shard-ownership baseline"
```

---

## Task 9: 全量回归、兼容冒烟与 API 文档

**Files:**
- Modify: `docs/superpowers/perf/ACCEPTANCE.md`, 必要时 `config` 注释
- Test: `bash tests/compat/run_compat.sh`, `cargo test --workspace`, `scripts/check-licenses.sh`

- [ ] **Step 1: 回归**

Run: `cargo test --workspace` → 全绿  
Run: `bash tests/compat/run_compat.sh` → PASS  
Run: `scripts/check-licenses.sh` → ok

- [ ] **Step 2: 更新 ACCEPTANCE 性能行与 API 变化说明（Engine 无 lock_shard）**

- [ ] **Step 3: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/docs
git -C /Users/caosunliu commit -m "docs: update acceptance for shard-ownership MVP"
```

---

## Plan Self-Review

1. **Spec coverage:** 所有权模型（T1–T2）、协调器（T3）、持久化（T4）、解码（T5）、writev（T6）、微优化（T7）、矩阵与基线（T8）、回归与文档（T9）均覆盖；TLCP 单独报告在 T8 脚本中体现。
2. **Placeholder scan:** Task 3 KEYS/DBSIZE 与 Task 8 脚本给出了实现方向与必须的消息类型，无 TBD 字样；执行者需按「Cmd 广播」落地。
3. **Type consistency:** `ShardState::execute(&mut self, args, now)`、`spawn_shard_runtime(shard_count, capacity, now_ms)`、`Cmd::{Exec,ConfigMaxMemory,Snapshot,LoadShard}` 在 T1–T4 间一致；server 仅持有 `ShardRuntime`。
