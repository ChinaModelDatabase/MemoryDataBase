# MemoryDataBase 主从复制 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现异步主从复制：RDB 全量 + 增量命令流、副本只读、手动提升；standalone 行为不变。

**Architecture:** 主节点维护复制 backlog（环形缓冲）并在写命令成功后 tee 到 streaming 副本；副本经独立 repl-port 拉取 FULLRESYNC（复用分片 Snapshot）后重放增量；客户端写副本返回 READONLY；`REPLICAOF NO ONE` 手动提升。

**Tech Stack:** 现有 Rust 栈（tokio、crossbeam-channel、bytes、jx-persist RDB、jx-engine ShardRuntime），不新增运行时依赖。

**Spec:** `opencode/MemoryDataBase/docs/superpowers/specs/2026-10-05-MemoryDataBase-replication-design.md`

## Global Constraints

- 兼容：`role standalone` 行为与现网一致；RESP 错误字符串 `READONLY You can't write against a read only replica.`
- 异步复制：写命令不等待副本 ACK；RPO 文档化。
- 全量以主为准：副本清空后加载主快照。
- 回归：`cargo test --workspace` 全绿；compat 冒烟通过。
- 路径相对 `/Users/caosunliu/opencode/MemoryDataBase/`；git 根 `/Users/caosunliu`。
- cargo：`export PATH="$(dirname "$(rustup which cargo)"):$PATH"`。
- 每任务 TDD；commit 仅 `opencode/MemoryDataBase/...` 路径。
- 新模块建议：`crates/jx-server/src/repl/`（backlog、codec、master、replica、role），避免塞进 actor.rs。

## File Structure

```
crates/jx-server/src/repl/
  mod.rs       # Role, ReplConfig, 公开 API
  backlog.rs   # 环形缓冲
  codec.rs     # 复制帧编解码
  master.rs    # repl-port 监听、FULLRESYNC、tee
  replica.rs   # REPLICAOF 客户端、全量应用、流式重放
crates/jx-server/src/{config.rs,conn.rs,net.rs}  # 配置与接线
docs/superpowers/ha/replication-runbook.md
```

---

## Task 1: Role 配置与类型

**Files:**
- Create: `crates/jx-server/src/repl/mod.rs`
- Modify: `crates/jx-server/src/config.rs`, `crates/jx-server/src/lib.rs`
- Test: `crates/jx-server/tests/repl_config.rs`

**Interfaces:**
- Produces:
  - `pub enum Role { Standalone, Master, Replica }`
  - `pub struct ReplConfig { pub role: Role, pub replicaof: Option<(String, u16)>, pub repl_port: u16, pub backlog_size: usize, pub repl_timeout_secs: u64, pub masterauth: Option<String> }`
  - `Config` 增加 `role`, `replicaof`, `repl_port`, `backlog_size`, `repl_timeout`, `masterauth` 字段；解析 key：`role`, `replicaof`, `repl-port`, `backlog-size`, `repl-timeout`, `masterauth`

- [ ] **Step 1: 失败测试**

```rust
// crates/jx-server/tests/repl_config.rs
use jx_server::config::{load_config, Role};
use std::io::Write;

#[test]
fn parse_role_and_replicaof() {
    let dir = std::env::temp_dir().join(format!("jx-repl-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "bind 127.0.0.1").unwrap();
    writeln!(f, "port 6379").unwrap();
    writeln!(f, "role replica").unwrap();
    writeln!(f, "replicaof 127.0.0.1 6380").unwrap();
    writeln!(f, "repl-port 6381").unwrap();
    writeln!(f, "backlog-size 1mb").unwrap();
    let cfg = load_config(&path).unwrap();
    assert_eq!(cfg.role, Role::Replica);
    assert_eq!(cfg.replicaof.as_deref(), Some(("127.0.0.1", 6380)));
    assert_eq!(cfg.repl_port, 6381);
    assert_eq!(cfg.backlog_size, 1024 * 1024);
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** `cargo test -p jx-server --test repl_config` → FAIL

- [ ] **Step 3: 实现** — `repl/mod.rs` 定义 Role/ReplConfig；config.rs 增加字段与解析（`replicaof` 格式 `host port`）；`repl_port` 默认 `port+1`；`backlog-size` 复用 `parse_size`；lib.rs `pub mod repl;`

- [ ] **Step 4: `cargo test -p jx-server` → 绿**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat(jx-server): replication role config"
```

---

## Task 2: Backlog 环形缓冲

**Files:**
- Create: `crates/jx-server/src/repl/backlog.rs`
- Test: `crates/jx-server/tests/repl_backlog.rs`

**Interfaces:**
- Produces:
  - `pub struct Backlog { cap: usize, start_offset: u64, buf: VecDeque<u8> }`
  - `impl Backlog { pub fn new(cap: usize) -> Self; pub fn append(&mut self, offset: u64, bytes: &[u8]); pub fn slice_from(&self, offset: u64) -> Option<&[u8]>; pub fn len(&self) -> usize; pub fn base_offset(&self) -> u64; }`
  - 溢出：从头丢弃，使 `base_offset()` 前移

- [ ] **Step 1: 失败测试**

```rust
use jx_server::repl::backlog::Backlog;

#[test]
fn append_and_slice_from() {
    let mut b = Backlog::new(16);
    b.append(0, b"hello");
    b.append(5, b"world");
    assert_eq!(b.slice_from(0), Some(&b"helloworld"[..]));
    assert_eq!(b.slice_from(5), Some(&b"world"[..]));
    assert_eq!(b.base_offset(), 0);
}

#[test]
fn overflow_drops_oldest() {
    let mut b = Backlog::new(8);
    b.append(0, b"0123456789"); // 10 bytes > 8
    assert!(b.base_offset() > 0);
    assert!(b.slice_from(0).is_none());
    let base = b.base_offset();
    assert!(b.slice_from(base).is_some());
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现** — VecDeque 环形；append 时若超出 cap 从 front 弹出并记录丢弃字节数对应的 offset 前移。

- [ ] **Step 4: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat(jx-server): replication backlog ring buffer"
```

---

## Task 3: 复制帧编解码

**Files:**
- Create: `crates/jx-server/src/repl/codec.rs`
- Test: `crates/jx-server/tests/repl_codec.rs`

**Interfaces:**
- Produces:
  - `pub enum ReplFrame { Command(Bytes), Heartbeat(u64), Fullresync { repl_id: String, offset: u64 } }`
  - `pub fn encode_frame(f: &ReplFrame) -> Bytes`
  - `pub fn decode_frames(buf: &mut BytesMut) -> Result<Vec<ReplFrame>, ()>`（半包返回已完整帧列表，剩余留在 buf）

- [ ] **Step 1: 失败测试**

```rust
use bytes::{Bytes, BytesMut};
use jx_server::repl::codec::{decode_frames, encode_frame, ReplFrame};

#[test]
fn command_frame_roundtrip() {
    let f = ReplFrame::Command(Bytes::from_static(b"*1\r\n$3\r\nSET\r\n"));
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..]);
    let frames = decode_frames(&mut buf).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(matches!(&frames[0], ReplFrame::Command(b) if b.as_ref() == b"*1\r\n$3\r\nSET\r\n"));
}

#[test]
fn partial_frame_waits() {
    let f = ReplFrame::Heartbeat(42);
    let enc = encode_frame(&f);
    let mut buf = BytesMut::from(&enc[..enc.len() - 1]);
    assert!(decode_frames(&mut buf).unwrap().is_empty());
    buf.extend_from_slice(&enc[enc.len() - 1..]);
    assert_eq!(decode_frames(&mut buf).unwrap().len(), 1);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现** — `[u32 len][u8 type][payload]`；type 0x01 Command、0x02 Heartbeat(u64 BE)、0x03 Fullresync（payload 为 `repl_id\noffset`）。

- [ ] **Step 4: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat(jx-server): replication frame codec"
```

---

## Task 4: Master repl-port 与写命令 tee

**Files:**
- Create: `crates/jx-server/src/repl/master.rs`
- Modify: `crates/jx-server/src/{net.rs,conn.rs}`, `crates/jx-persist/src/rdb.rs`（如需导出快照字节）
- Test: `crates/jx-server/tests/repl_master.rs`

**Interfaces:**
- Produces:
  - `pub struct MasterRepl { backlog: Mutex<Backlog>, offset: AtomicU64, replicas: Mutex<Vec<ReplicaConn>>, repl_id: String }`
  - `pub fn spawn_master_repl(rt: Arc<ShardRuntime>, repl_port: u16, backlog_size: usize, now_ms: fn() -> u64) -> Arc<MasterRepl>`
  - `impl MasterRepl { pub fn on_write(&self, args: &[Bytes]); pub fn offset(&self) -> u64; pub fn connected_replicas(&self) -> usize; }`
  - `on_write`：encode_command → backlog.append → 向每个 streaming replica 发送 ReplFrame::Command（try_send，失败标记断开）

- [ ] **Step 1: 失败测试**

```rust
// 用 mock：直接调用 MasterRepl 内部 backlog（或单测 on_write 后 slice_from 可见）
use bytes::Bytes;
use jx_server::repl::master::MasterRepl;

#[test]
fn on_write_appends_backlog() {
    let m = MasterRepl::new_for_test(1024);
    m.on_write(&[Bytes::from_static(b"SET"), Bytes::from_static(b"k"), Bytes::from_static(b"v")]);
    assert!(m.offset() > 0);
    assert!(m.backlog_slice_from(0).is_some());
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现** — conn 成功写命令后调用 `master.on_write(&args)`（仅 role=master/standalone-with-tee 可选；standalone 默认不 tee）。FULLRESYNC：从 `Cmd::Snapshot` 收集各分片，`jx_persist` 导出 RDB 字节发送（可复用临时文件读入或增加 `snapshot_to_bytes` 辅助）。

- [ ] **Step 4: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server opencode/MemoryDataBase/crates/jx-persist
git -C /Users/caosunliu commit -m "feat(jx-server): master repl backlog and write tee"
```

---

## Task 5: 副本 REPLICAOF 与全量/增量应用

**Files:**
- Create: `crates/jx-server/src/repl/replica.rs`
- Modify: `crates/jx-server/src/{conn.rs,net.rs}`, `crates/jx-engine/src/actor.rs`（replica 角色写拒绝或 conn 层拦截）
- Test: `crates/jx-server/tests/repl_replica.rs`

**Interfaces:**
- Produces:
  - `pub struct ReplicaState { pub role_view: Role, pub master_offset: u64, pub status: String }`
  - `pub fn spawn_replica(rt: Arc<ShardRuntime>, master: (String, u16), repl_port: u16, masterauth: Option<String>) -> ()`（后台 tokio task）
  - conn 层：`role == Replica` 时客户端写命令（非复制通道）返回 READONLY 错误字符串
  - 复制通道重放：`rt.exec_no_aof(args)`（绕过客户端只读检查）

- [ ] **Step 1: 失败测试**

```rust
#[test]
fn replica_client_write_returns_readonly() {
    // 启动 replica 角色 server（role replica, replicaof 指向不存在主也可先测 READONLY）
    // TCP: SET k v → 错误包含 READONLY
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现** — 后台连接 master repl-port；握手 `REPLICAOF sync`（自定义文本或帧 0x03）；收 FULLRESYNC + RDB 字节 → 清空本地（FLUSHALL via rt）→ 反序列化加载（复用 jx_persist load 逻辑，若不便可经临时文件）；随后循环 decode_frames，Command → `exec_no_aof`，Heartbeat 更新 offset。

- [ ] **Step 4: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat(jx-server): replica REPLICAOF sync and READONLY"
```

---

## Task 6: REPLICAOF 命令、INFO replication、提升

**Files:**
- Modify: `crates/jx-server/src/{conn.rs}`, `crates/jx-engine/src/commands/server.rs`, `crates/jx-server/src/repl/{mod.rs,master.rs,replica.rs}`
- Test: `crates/jx-server/tests/repl_ops.rs`

**Interfaces:**
- Produces:
  - 客户端命令 `REPLICAOF host port` / `REPLICAOF NO ONE`（仅 role 允许时；standalone 可接受并切换为 replica/master）
  - `INFO` 含 `# Replication\r\nrole:master|replica|standalone\r\nmaster_repl_offset:N\r\nconnected_slaves:N\r\n`

- [ ] **Step 1: 失败测试**

```rust
#[test]
fn info_contains_replication_section() { /* INFO 文本含 role: 与 master_repl_offset: */ }

#[test]
fn replicaof_no_one_promotes() { /* replica 上 REPLICAOF NO ONE 后 SET 成功 */ }
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现** — REPLICAOF 在 conn/repl 层处理（不进 ShardState）；NO ONE：停止 replica task，role→master，接受写。INFO：由 conn 从 Runtime/ReplState 填充。

- [ ] **Step 4: 测试绿 + Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates
git -C /Users/caosunliu commit -m "feat: REPLICAOF commands and INFO replication"
```

---

## Task 7: 双节点集成测试与 runbook

**Files:**
- Create: `crates/jx-server/tests/repl_e2e.rs`, `docs/superpowers/ha/replication-runbook.md`
- Modify: `config/MemoryDataBase.conf.example`

**Interfaces:**
- Produces: e2e 测试（同进程两个 runtime 或两个端口进程）：全量一致、增量一致、READONLY、NO ONE、断线重连；runbook：切换步骤、RPO、双主风险。

- [ ] **Step 1: 失败测试（e2e 骨架）**

```rust
#[test]
fn e2e_fullsync_and_incremental() {
    // master :port1 role master; replica :port2 role replica replicaof 127.0.0.1 port1
    // master SET k v; 等待; replica GET k == v
}
```

- [ ] **Step 2: 实现 e2e + runbook + example 配置注释**

- [ ] **Step 3: `cargo test --workspace` + `bash tests/compat/run_compat.sh` → 绿**

- [ ] **Step 4: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates opencode/MemoryDataBase/docs opencode/MemoryDataBase/config
git -C /Users/caosunliu commit -m "test: replication e2e and runbook"
```

---

## Plan Self-Review

1. **Spec coverage:** 角色/配置（T1）、backlog（T2）、协议（T3）、master tee+全量（T4）、replica 同步+READONLY（T5）、REPLICAOF/INFO/提升（T6）、e2e/runbook（T7）。哨兵/集群仅 spec 附录，不在本计划。
2. **Placeholder scan:** FULLRESYNC 文本握手在 T5 给出方向（帧 0x03）；实现者按 codec 对齐。
3. **Type consistency:** `ReplFrame`/`Backlog`/`MasterRepl::on_write`/`exec_no_aof` 贯穿 T2–T6；conn 只读字符串与 spec 一致。
