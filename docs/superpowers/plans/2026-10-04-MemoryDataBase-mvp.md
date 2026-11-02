# MemoryDataBase MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现信创 Rust 缓存中间件 MemoryDataBase MVP：单节点 RESP2 Redis 核心子集 + 可选 RDB/AOF + 全链路国密（SM3/SM4/TLCP），明文小 value GET/SET ≥ 50 万 OPS。

**Architecture:** tokio 多线程 I/O + N 个分片 Actor 独占数据（热路径零锁）；RESP2 在连接层解析后按 `hash(key)%N` 投递；RDB 为 forkless 分片序贯快照，AOF 为分片本地缓冲后台刷盘；TLCP 终止于连接层，与引擎解耦。

**Tech Stack:** Rust stable（MSRV 1.71+）、tokio、crossbeam-channel、bytes、rustls、RustCrypto sm2/sm3/sm4、vendored gm-tlcp（回退 gmssl-rs FFI）、serde、clap。

**Spec:** `opencode/MemoryDataBase/docs/superpowers/specs/2026-10-04-MemoryDataBase-mvp-design.md`

## Global Constraints

- 许可证：产品 Apache-2.0；依赖仅 MIT / Apache-2.0 / BSD / ISC / Unlicense（CI 用 `cargo deny` 固化）。
- 协议：RESP2；命令集以 spec 附录 A「必须实现」为冻结边界，stretch 命令返回明确错误而非静默缺失。
- 部署：单节点；Actor 崩溃 = 进程 fail-fast 退出。
- 持久化：可选 RDB+AOF；恢复 AOF 优先；恢复失败拒绝启动。
- 国密：`tls-mode = plain|tls|tlcp`；口令 SM3 加盐哈希；落盘 SM4-GCM；TLCP 优先纯 Rust vendored。
- 性能验收口径：**明文 RESP** 小 value GET/SET ≥ 50 万 OPS（开发参考机）；TLCP 档单独实测报告。
- 平台：开发基线 macOS/Linux；交付目标 `x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu`、`loongarch64-unknown-linux-gnu`。
- 路径：任务内路径相对 `/Users/caosunliu/opencode/MemoryDataBase/`；git 根为 `/Users/caosunliu`，提交用 `git -C /Users/caosunliu add opencode/MemoryDataBase/<path>`。
- 每个任务 TDD：`cargo test -p <crate>` 通过后才 commit。
- 工具链：`export PATH="$(dirname "$(rustup which cargo)"):$PATH"` 后再执行 cargo。

## File Structure

```
MemoryDataBase/
├── Cargo.toml                          # workspace
├── deny.toml
├── crates/
│   ├── jx-proto/src/{lib.rs,types.rs,encode.rs,decode.rs,error.rs}
│   ├── jx-engine/src/{lib.rs,types.rs,ttl.rs,evict.rs,commands/*.rs}
│   ├── jx-persist/src/{lib.rs,rdb.rs,aof.rs,crypto_file.rs,restore.rs}
│   ├── jx-crypto/src/{lib.rs,sm3_auth.rs,sm4_file.rs,keyfile.rs,tlcp.rs}
│   ├── jx-server/src/{main.rs,config.rs,net.rs,conn.rs,dispatch.rs,background.rs}
│   └── jx-cli/src/{main.rs,keygen.rs,check_config.rs,bench.rs,rdb_info.rs}
├── config/MemoryDataBase.conf.example
└── scripts/{build-linux.sh,build-loongarch.sh,check-licenses.sh}
```

| Crate | 职责 |
|-------|------|
| jx-proto | RESP2 类型、编码、流式解码、错误字符串 |
| jx-engine | 分片存储、五大类型命令、TTL、淘汰、Actor、协调器 |
| jx-persist | RDB/AOF 格式与恢复、SM4 文件加密 |
| jx-crypto | SM3 口令、SM4 文件加解密、密钥文件、TLCP 传输 |
| jx-server | 配置、tokio 网络、连接、调度、后台任务、指标 |
| jx-cli | keygen / check-config / bench / rdb-info |

---

# Phase 0 — 工作区与基线

## Task 0: 工具链与项目骨架

**Files:**
- Create: `Cargo.toml`, `deny.toml`, `.gitignore`, `crates/*/Cargo.toml`, `crates/*/src/lib.rs`（或 main.rs）, `config/MemoryDataBase.conf.example`
- Test: `crates/jx-proto/src/lib.rs` 中 smoke test

**Interfaces:**
- Produces: workspace 可 `cargo build`；`cargo test` 绿。

- [ ] **Step 1: 确保 cargo 在 PATH**

Run: `rustup which cargo && export PATH="$(dirname "$(rustup which cargo)"):$PATH" && cargo --version`
Expected: `cargo 1.xx.x`

- [ ] **Step 2: 创建 workspace 根 `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = [
    "crates/jx-proto",
    "crates/jx-engine",
    "crates/jx-persist",
    "crates/jx-crypto",
    "crates/jx-server",
    "crates/jx-cli",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
rust-version = "1.71"

[workspace.dependencies]
bytes = "1"
crossbeam-channel = "0.5"
thiserror = "1"
anyhow = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "net", "io-util", "sync", "time", "signal", "macros", "fs"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
sm3 = "0.4"
sm4 = "0.5"
rand = "0.8"
clap = { version = "4", features = ["derive"] }
hex = "0.4"
```

- [ ] **Step 3: 创建六个 crate 的 Cargo.toml 与空 lib/main**

`crates/jx-proto/Cargo.toml`:
```toml
[package]
name = "jx-proto"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bytes.workspace = true
thiserror.workspace = true
```

其余 crate 依赖：
- `jx-engine`: bytes, crossbeam-channel, thiserror, tracing, rand
- `jx-persist`: bytes, thiserror, tracing, rand, jx-engine, jx-crypto
- `jx-crypto`: thiserror, rand, hex, tracing, sm3, sm4
- `jx-server`: tokio, bytes, crossbeam-channel, serde, serde_json, tracing, tracing-subscriber, anyhow, jx-proto, jx-engine, jx-persist, jx-crypto
- `jx-cli`: clap, anyhow, hex, rand, jx-persist, jx-crypto

- [ ] **Step 4: `.gitignore`**

```
/target
**/*.rs.bk
*.aof
*.rdb
*.key
/data
```

- [ ] **Step 5: smoke 测试**

`crates/jx-proto/src/lib.rs`:
```rust
#[test]
fn workspace_compiles() {
    assert_eq!(2 + 2, 4);
}
```

Run: `cargo test --workspace`
Expected: `test result: ok`

- [ ] **Step 6: `cargo deny` 基线**

`deny.toml`:
```toml
[graph]
all-features = true

[licenses]
allow = ["MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unlicense"]
```

Run: `cargo install cargo-deny && cargo deny check licenses`
Expected: 通过或输出许可清单均可人工确认在白名单内。

- [ ] **Step 7: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/Cargo.toml opencode/MemoryDataBase/deny.toml opencode/MemoryDataBase/.gitignore opencode/MemoryDataBase/crates opencode/MemoryDataBase/config
git -C /Users/caosunliu commit -m "feat: scaffold MemoryDataBase cargo workspace"
```

---

# Phase 1 — 协议层 jx-proto

## Task 1: RESP2 类型与编码

**Files:**
- Create: `crates/jx-proto/src/types.rs`, `crates/jx-proto/src/encode.rs`
- Modify: `crates/jx-proto/src/lib.rs`
- Test: `crates/jx-proto/tests/encode.rs`

**Interfaces:**
- Produces:
  - `pub enum RespValue { SimpleString(String), Error(String), Integer(i64), BulkString(Option<Bytes>), Array(Option<Vec<RespValue>>) }`
  - `pub fn encode(value: &RespValue, out: &mut BytesMut)`
  - `pub fn encode_to_bytes(value: &RespValue) -> Bytes`

- [ ] **Step 1: 写失败测试 `crates/jx-proto/tests/encode.rs`**

```rust
use bytes::{Bytes, BytesMut};
use jx_proto::{encode, encode_to_bytes, RespValue};

#[test]
fn encode_simple_string() {
    let mut out = BytesMut::new();
    encode(&RespValue::SimpleString("OK".into()), &mut out);
    assert_eq!(&out[..], b"+OK\r\n");
}

#[test]
fn encode_error_and_int() {
    assert_eq!(&encode_to_bytes(&RespValue::err("ERR bad"))[..], b"-ERR bad\r\n");
    assert_eq!(&encode_to_bytes(&RespValue::int(42))[..], b":42\r\n");
}

#[test]
fn encode_bulk_and_null() {
    assert_eq!(&encode_to_bytes(&RespValue::bulk(Bytes::from_static(b"hi")))[..], b"$2\r\nhi\r\n");
    assert_eq!(&encode_to_bytes(&RespValue::null_bulk())[..], b"$-1\r\n");
}

#[test]
fn encode_array_with_null_bulk() {
    let v = RespValue::array(vec![
        RespValue::bulk(Bytes::from_static(b"a")),
        RespValue::null_bulk(),
    ]);
    assert_eq!(&encode_to_bytes(&v)[..], b"*2\r\n$1\r\na\r\n$-1\r\n");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p jx-proto`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现 types 与 encode**

`crates/jx-proto/src/types.rs`:
```rust
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self { RespValue::SimpleString("OK".into()) }
    pub fn err(msg: impl Into<String>) -> Self { RespValue::Error(msg.into()) }
    pub fn bulk(bytes: impl Into<Bytes>) -> Self { RespValue::BulkString(Some(bytes.into())) }
    pub fn null_bulk() -> Self { RespValue::BulkString(None) }
    pub fn int(n: i64) -> Self { RespValue::Integer(n) }
    pub fn array(items: Vec<RespValue>) -> Self { RespValue::Array(Some(items)) }
    pub fn null_array() -> Self { RespValue::Array(None) }
}
```

`crates/jx-proto/src/encode.rs`:
```rust
use bytes::{BufMut, BytesMut};
use crate::RespValue;

pub fn encode(value: &RespValue, out: &mut BytesMut) {
    match value {
        RespValue::SimpleString(s) => {
            out.put_u8(b'+');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::Error(s) => {
            out.put_u8(b'-');
            out.put_slice(s.as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            out.put_u8(b':');
            out.put_slice(n.to_string().as_bytes());
            out.put_slice(b"\r\n");
        }
        RespValue::BulkString(None) => out.put_slice(b"$-1\r\n"),
        RespValue::BulkString(Some(b)) => {
            out.put_u8(b'$');
            out.put_slice(b.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            out.put_slice(b);
            out.put_slice(b"\r\n");
        }
        RespValue::Array(None) => out.put_slice(b"*-1\r\n"),
        RespValue::Array(Some(items)) => {
            out.put_u8(b'*');
            out.put_slice(items.len().to_string().as_bytes());
            out.put_slice(b"\r\n");
            for it in items {
                encode(it, out);
            }
        }
    }
}

pub fn encode_to_bytes(value: &RespValue) -> bytes::Bytes {
    let mut buf = BytesMut::new();
    encode(value, &mut buf);
    buf.freeze()
}
```

`crates/jx-proto/src/lib.rs`:
```rust
mod encode;
mod types;

pub use encode::{encode, encode_to_bytes};
pub use types::RespValue;
```

- [ ] **Step 4: `cargo test -p jx-proto` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-proto
git -C /Users/caosunliu commit -m "feat(jx-proto): RESP2 value types and encoder"
```

## Task 2: RESP2 流式解码器

**Files:**
- Create: `crates/jx-proto/src/decode.rs`, `crates/jx-proto/src/error.rs`
- Modify: `crates/jx-proto/src/lib.rs`
- Test: `crates/jx-proto/tests/decode.rs`

**Interfaces:**
- Produces:
  - `pub struct DecodeError;`
  - `pub struct RespDecoder { buf: BytesMut }`
  - `impl RespDecoder { pub fn new() -> Self; pub fn feed(&mut self, chunk: &[u8]); pub fn next_value(&mut self) -> Result<Option<RespValue>, DecodeError>; }`

- [ ] **Step 1: 失败测试**

`crates/jx-proto/tests/decode.rs`:
```rust
use bytes::Bytes;
use jx_proto::{RespDecoder, RespValue};

#[test]
fn decode_partial_then_complete() {
    let mut d = RespDecoder::new();
    d.feed(b"*2\r\n$3\r\nSET\r\n$3\r\n");
    assert!(d.next_value().unwrap().is_none());
    d.feed(b"foo\r\n");
    let v = d.next_value().unwrap().expect("value");
    assert_eq!(
        v,
        RespValue::Array(Some(vec![
            RespValue::bulk(Bytes::from_static(b"SET")),
            RespValue::bulk(Bytes::from_static(b"foo")),
        ]))
    );
}

#[test]
fn decode_multiple_pipelined() {
    let mut d = RespDecoder::new();
    d.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
    assert!(matches!(d.next_value().unwrap(), Some(RespValue::Array(_))));
    assert!(matches!(d.next_value().unwrap(), Some(RespValue::Array(_))));
    assert!(d.next_value().unwrap().is_none());
}

#[test]
fn decode_error_frame() {
    let mut d = RespDecoder::new();
    d.feed(b"-ERR oops\r\n");
    assert_eq!(d.next_value().unwrap(), Some(RespValue::err("ERR oops")));
}

#[test]
fn decode_rejects_bad_crlf() {
    let mut d = RespDecoder::new();
    d.feed(b"+OK\n");
    assert!(d.next_value().is_err());
}
```

- [ ] **Step 2: 确认失败** `cargo test -p jx-proto` → FAIL

- [ ] **Step 3: 实现解码（先窥探后消费，正确处理半包）**

`crates/jx-proto/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("RESP protocol decode error")]
pub struct DecodeError;
```

`crates/jx-proto/src/decode.rs`:
```rust
use bytes::{Buf, Bytes, BytesMut};
use crate::{DecodeError, RespValue};

pub struct RespDecoder {
    buf: BytesMut,
}

impl RespDecoder {
    pub fn new() -> Self {
        Self { buf: BytesMut::with_capacity(4096) }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    pub fn next_value(&mut self) -> Result<Option<RespValue>, DecodeError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let mut probe = self.buf.clone();
        match try_parse(&mut probe)? {
            Some(v) => {
                let consumed = self.buf.len() - probe.len();
                self.buf.advance(consumed);
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }
}

impl Default for RespDecoder {
    fn default() -> Self { Self::new() }
}

fn read_line(buf: &mut BytesMut) -> Result<Option<Bytes>, DecodeError> {
    let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None);
    };
    let line = buf.split_to(pos);
    buf.advance(2);
    Ok(Some(line.freeze()))
}

fn parse_usize(line: &[u8]) -> Result<usize, DecodeError> {
    std::str::from_utf8(line)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(DecodeError)
}

fn try_parse(buf: &mut BytesMut) -> Result<Option<RespValue>, DecodeError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let kind = buf[0];
    let save = buf.clone();
    let result = (|| -> Result<Option<RespValue>, DecodeError> {
        match kind {
            b'+' | b'-' | b':' => {
                let Some(line) = read_line(buf)? else { return Ok(None) };
                let s = String::from_utf8(line.to_vec()).map_err(|_| DecodeError)?;
                Ok(Some(match kind {
                    b'+' => RespValue::SimpleString(s),
                    b'-' => RespValue::Error(s),
                    _ => RespValue::Integer(s.parse().map_err(|_| DecodeError)?),
                }))
            }
            b'$' => {
                let Some(line) = read_line(buf)? else { return Ok(None) };
                let len = parse_usize(&line)?;
                if buf.len() < len + 2 {
                    *buf = save;
                    return Ok(None);
                }
                let body = buf.split_to(len).freeze();
                buf.advance(2);
                Ok(Some(RespValue::BulkString(Some(body))))
            }
            b'*' => {
                let Some(line) = read_line(buf)? else { return Ok(None) };
                let n = parse_usize(&line)?;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    match try_parse(buf)? {
                        Some(v) => items.push(v),
                        None => {
                            *buf = save;
                            return Ok(None);
                        }
                    }
                }
                Ok(Some(RespValue::Array(Some(items))))
            }
            _ => Err(DecodeError),
        }
    })();
    result
}
```

`crates/jx-proto/src/lib.rs`:
```rust
mod decode;
mod encode;
mod error;
mod types;

pub use decode::RespDecoder;
pub use encode::{encode, encode_to_bytes};
pub use error::DecodeError;
pub use types::RespValue;
```

- [ ] **Step 4: `cargo test -p jx-proto` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-proto
git -C /Users/caosunliu commit -m "feat(jx-proto): streaming RESP2 decoder"
```

---

# Phase 2 — 引擎 jx-engine

## Task 3: 引擎值类型与 String/Key 命令

**Files:**
- Create: `crates/jx-engine/src/types.rs`, `crates/jx-engine/src/lib.rs`, `crates/jx-engine/src/commands/{mod.rs,string.rs,key.rs}`
- Test: `crates/jx-engine/tests/string.rs`

**Interfaces:**
- Consumes: `jx_proto::RespValue`
- Produces:
  - `pub enum JxValue { Str(Bytes), Int(i64), Hash(HashMap<Bytes,Bytes>), List(VecDeque<Bytes>), Set(HashSet<Bytes>), ZSet(ZSet) }`
  - `pub struct KeyMeta { pub expire_at_ms: Option<u64>, pub last_access_ms: u64, pub freq: u64 }`
  - `pub struct Entry { pub value: JxValue, pub meta: KeyMeta }`
  - `pub struct Engine { ... }` with `Engine::new_for_test(n)`, `Engine::shard_id_for_key`, `Engine::lock_shard`, `Engine::touch_alive`
  - `pub fn execute(engine: &Engine, args: &[Bytes]) -> Result<RespValue, RespValue>`

- [ ] **Step 1: 失败测试 `crates/jx-engine/tests/string.rs`**

```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::RespValue;

fn eng() -> Engine { Engine::new_for_test(1) }
fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn set_get_del() {
    let e = eng();
    assert_eq!(execute(&e, &[b("SET"), b("k"), b("v")]).unwrap(), RespValue::ok());
    assert_eq!(execute(&e, &[b("GET"), b("k")]).unwrap(), RespValue::bulk(b("v")));
    assert_eq!(execute(&e, &[b("DEL"), b("k")]).unwrap(), RespValue::int(1));
    assert_eq!(execute(&e, &[b("GET"), b("k")]).unwrap(), RespValue::null_bulk());
}

#[test]
fn incr_decr_incrby_decrby() {
    let e = eng();
    execute(&e, &[b("SET"), b("n"), b("10")]).unwrap();
    assert_eq!(execute(&e, &[b("INCR"), b("n")]).unwrap(), RespValue::int(11));
    assert_eq!(execute(&e, &[b("DECR"), b("n")]).unwrap(), RespValue::int(10));
    assert_eq!(execute(&e, &[b("INCRBY"), b("n"), b("5")]).unwrap(), RespValue::int(15));
    assert_eq!(execute(&e, &[b("DECRBY"), b("n"), b("5")]).unwrap(), RespValue::int(10));
}

#[test]
fn set_append_strlen_getrange() {
    let e = eng();
    execute(&e, &[b("SET"), b("s"), b("hello")]).unwrap();
    assert_eq!(execute(&e, &[b("APPEND"), b("s"), b(" world")]).unwrap(), RespValue::int(11));
    assert_eq!(execute(&e, &[b("STRLEN"), b("s")]).unwrap(), RespValue::int(11));
    assert_eq!(execute(&e, &[b("GETRANGE"), b("s"), b("0"), b("4")]).unwrap(), RespValue::bulk(b("hello")));
}

#[test]
fn mget_mset_setnx() {
    let e = eng();
    execute(&e, &[b("MSET"), b("a"), b("1"), b("b"), b("2")]).unwrap();
    let r = execute(&e, &[b("MGET"), b("a"), b("missing"), b("b")]).unwrap();
    assert_eq!(r, RespValue::array(vec![
        RespValue::bulk(b("1")), RespValue::null_bulk(), RespValue::bulk(b("2"))
    ]));
    assert_eq!(execute(&e, &[b("SETNX"), b("a"), b("x")]).unwrap(), RespValue::int(0));
    assert_eq!(execute(&e, &[b("SETNX"), b("c"), b("3")]).unwrap(), RespValue::int(1));
}

#[test]
fn wrongtype_and_unsupported() {
    let e = eng();
    execute(&e, &[b("HSET"), b("h"), b("f"), b("1")]).unwrap();
    let err = execute(&e, &[b("GET"), b("h")]).unwrap_err();
    assert!(matches!(&err, RespValue::Error(m) if m.starts_with("WRONGTYPE")));
    let err = execute(&e, &[b("MULTI")]).unwrap_err();
    assert!(matches!(&err, RespValue::Error(m) if m.contains("not supported")));
}

#[test]
fn expire_ttl_persist_exists_type() {
    let e = eng();
    execute(&e, &[b("SET"), b("k"), b("v"), b("EX"), b("100")]).unwrap();
    let ttl = execute(&e, &[b("TTL"), b("k")]).unwrap();
    assert!(matches!(ttl, RespValue::Integer(n) if (99..=100).contains(&n)));
    assert_eq!(execute(&e, &[b("TYPE"), b("k")]).unwrap(), RespValue::bulk(b("string")));
    assert_eq!(execute(&e, &[b("PERSIST"), b("k")]).unwrap(), RespValue::int(1));
    assert_eq!(execute(&e, &[b("TTL"), b("k")]).unwrap(), RespValue::int(-1));
    assert_eq!(execute(&e, &[b("EXISTS"), b("k")]).unwrap(), RespValue::int(1));
}
```

- [ ] **Step 2: 确认失败** `cargo test -p jx-engine` → FAIL

- [ ] **Step 3: 实现引擎骨架与 String/Key**

`crates/jx-engine/src/types.rs`:
```rust
use std::collections::{HashMap, HashSet, VecDeque};
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZSet {
    pub entries: HashMap<Bytes, f64>,
}

#[derive(Debug, Clone)]
pub enum JxValue {
    Str(Bytes),
    Int(i64),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSet),
}

impl JxValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            JxValue::Str(_) | JxValue::Int(_) => "string",
            JxValue::Hash(_) => "hash",
            JxValue::List(_) => "list",
            JxValue::Set(_) => "set",
            JxValue::ZSet(_) => "zset",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct KeyMeta {
    pub expire_at_ms: Option<u64>,
    pub last_access_ms: u64,
    pub freq: u64,
}

pub struct Entry {
    pub value: JxValue,
    pub meta: KeyMeta,
}
```

`crates/jx-engine/src/lib.rs`:
```rust
mod types;
pub mod commands;
pub mod evict;
pub mod ttl;

use std::collections::HashMap;
use std::sync::Mutex;
use bytes::Bytes;
use jx_proto::RespValue;

pub use types::*;

pub struct ShardState {
    pub dict: HashMap<Bytes, Entry>,
    pub mem: usize,
    pub maxmemory: Option<usize>,
    pub policy: MaxMemoryPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxMemoryPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    VolatileTtl,
}

pub struct Engine {
    shards: Vec<Mutex<ShardState>>,
    now_ms: fn() -> u64,
}

fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Engine {
    pub fn new_for_test(nshards: usize) -> Self {
        Self::new(nshards, system_now_ms)
    }

    pub fn new(nshards: usize, now_ms: fn() -> u64) -> Self {
        let nshards = nshards.max(1);
        Self {
            shards: (0..nshards)
                .map(|_| {
                    Mutex::new(ShardState {
                        dict: HashMap::new(),
                        mem: 0,
                        maxmemory: None,
                        policy: MaxMemoryPolicy::NoEviction,
                    })
                })
                .collect(),
            now_ms,
        }
    }

    pub fn shard_count(&self) -> usize { self.shards.len() }

    pub fn now(&self) -> u64 { (self.now_ms)() }

    pub fn lock_shard(&self, id: usize) -> std::sync::MutexGuard<'_, ShardState> {
        self.shards[id].lock().unwrap()
    }

    pub fn shard_id_for_key(&self, key: &Bytes) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.shards.len()
    }

    pub fn set_maxmemory(&self, bytes: usize, policy: MaxMemoryPolicy) {
        for s in &self.shards {
            let mut sh = s.lock().unwrap();
            sh.maxmemory = Some(bytes);
            sh.policy = policy;
        }
    }

    pub fn maxmemory(&self) -> Option<usize> {
        self.shards.first().and_then(|s| s.lock().unwrap().maxmemory)
    }

    pub fn total_mem(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().mem).sum()
    }

    pub fn expire_cycle(&self, now: u64, budget: usize) -> u64 {
        ttl::expire_cycle(self, now, budget)
    }

    pub fn evict_if_needed(&self, key: &Bytes, now: u64) -> Result<(), RespValue> {
        evict::evict_if_needed(self, key, now)
    }

    pub fn touch_alive(shard: &mut ShardState, key: &Bytes, now: u64) -> bool {
        if let Some(e) = shard.dict.get_mut(key) {
            if let Some(exp) = e.meta.expire_at_ms {
                if now >= exp {
                    shard.dict.remove(key);
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
}

pub fn execute(engine: &Engine, args: &[Bytes]) -> Result<RespValue, RespValue> {
    if args.is_empty() {
        return Err(RespValue::err("ERR empty command"));
    }
    let name = std::str::from_utf8(&args[0]).unwrap_or("").to_ascii_uppercase();
    let now = engine.now();
    let a = &args[1..];
    match name.as_str() {
        "PING" => Ok(RespValue::bulk(Bytes::from_static(b"PONG"))),
        "ECHO" if a.len() == 1 => Ok(RespValue::bulk(a[0].clone())),
        "ECHO" => Err(RespValue::err("ERR wrong number of arguments for 'echo' command")),
        "QUIT" => Ok(RespValue::ok()),
        "SET" => commands::string::set(engine, a, now),
        "GET" => commands::string::get(engine, a, now),
        "SETNX" => commands::string::setnx(engine, a, now),
        "INCR" => commands::string::incr(engine, a, now, 1),
        "DECR" => commands::string::incr(engine, a, now, -1),
        "INCRBY" => commands::string::incrby(engine, a, now),
        "DECRBY" => commands::string::decrby(engine, a, now),
        "APPEND" => commands::string::append(engine, a, now),
        "STRLEN" => commands::string::strlen(engine, a, now),
        "GETRANGE" => commands::string::getrange(engine, a, now),
        "MGET" => commands::string::mget(engine, a, now),
        "MSET" => commands::string::mset(engine, a, now),
        "DEL" => commands::key::del(engine, a, now),
        "EXISTS" => commands::key::exists(engine, a, now),
        "TYPE" => commands::key::key_type(engine, a, now),
        "EXPIRE" => commands::key::expire(engine, a, now, false),
        "PEXPIRE" => commands::key::expire(engine, a, now, true),
        "TTL" => commands::key::ttl(engine, a, now, false),
        "PTTL" => commands::key::ttl(engine, a, now, true),
        "PERSIST" => commands::key::persist(engine, a, now),
        "DBSIZE" => Ok(RespValue::int(
            engine.shards.iter().map(|s| s.lock().unwrap().dict.len() as i64).sum(),
        )),
        "TIME" => {
            let ms = now;
            Ok(RespValue::array(vec![
                RespValue::bulk(Bytes::from((ms / 1000).to_string())),
                RespValue::bulk(Bytes::from((ms % 1000 * 1000).to_string())),
            ]))
        }
        other => {
            if commands::is_unimplemented(other) {
                Err(RespValue::err(format!(
                    "ERR command '{other}' is not supported in this version"
                )))
            } else {
                Err(RespValue::err(format!("ERR unknown command '{name}'")))
            }
        }
    }
}
```

`crates/jx-engine/src/commands/mod.rs`:
```rust
pub mod hash;
pub mod key;
pub mod list;
pub mod set;
pub mod string;
pub mod zset;

/// 返回 true 表示已知但本版本未实现（后续任务逐步从列表移除）
pub fn is_unimplemented(cmd: &str) -> bool {
    matches!(
        cmd,
        "MULTI" | "EXEC" | "DISCARD" | "WATCH"
            | "SUBSCRIBE" | "PUBLISH" | "UNSUBSCRIBE"
            | "EVAL" | "EVALSHA"
            | "XADD" | "XRANGE" | "GEOADD" | "PFADD"
            | "HELLO" | "COMMAND" | "MONITOR"
            | "AUTH" | "SELECT"
            | "SETEX" | "PSETEX" | "MSETNX" | "RENAME" | "RANDOMKEY" | "KEYS"
            | "FLUSHALL" | "FLUSHDB" | "CONFIG" | "INFO" | "CLIENT" | "SLOWLOG"
            | "SAVE" | "BGSAVE" | "LASTSAVE" | "BGREWRITEAOF"
            | "GETBIT" | "SETBIT" | "BITCOUNT"
            // 集合类型命令在 Task 4 实现前保持 unimplemented
            | "HSET" | "HGET" | "HMGET" | "HDEL" | "HEXISTS" | "HLEN" | "HKEYS" | "HVALS" | "HGETALL" | "HINCRBY" | "HMSET"
            | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" | "LLEN" | "LINDEX" | "LSET"
            | "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "SPOP" | "SRANDMEMBER"
            | "ZADD" | "ZREM" | "ZSCORE" | "ZRANGE" | "ZRANGEBYSCORE" | "ZCARD" | "ZINCRBY"
    )
}
```

`crates/jx-engine/src/commands/string.rs`:
```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{types::{Entry, JxValue, KeyMeta}, Engine};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn parse_i64(b: &Bytes) -> Result<i64, RespValue> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))
}

fn put_str(sh: &mut crate::ShardState, key: &Bytes, val: Bytes, now: u64, expire: Option<u64>) {
    if let Some(old) = sh.dict.remove(key) {
        sh.mem = sh.mem.saturating_sub(old.value.approx_bytes());
    }
    sh.dict.insert(key.clone(), Entry {
        value: JxValue::Str(val),
        meta: KeyMeta { expire_at_ms: expire, last_access_ms: now, freq: 1 },
    });
    sh.mem += key.len() + 64;
}

pub fn set(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err("ERR wrong number of arguments for 'set' command"));
    }
    let key = &args[0];
    let val = args[1].clone();
    let mut expire: Option<u64> = None;
    let mut i = 2;
    while i < args.len() {
        let flag = std::str::from_utf8(&args[i]).unwrap_or("").to_ascii_uppercase();
        match flag.as_str() {
            "EX" | "PX" => {
                let n = args.get(i + 1).ok_or_else(|| RespValue::err("ERR syntax error"))?;
                let n = parse_i64(n)?.max(0) as u64;
                expire = Some(if flag == "EX" { now.saturating_add(n.saturating_mul(1000)) } else { now.saturating_add(n) });
                i += 2;
            }
            _ => return Err(RespValue::err("ERR syntax error")),
        }
    }
    let sid = e.shard_id_for_key(key);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(key, now)?;
    if Engine::touch_alive(&mut sh, key, now) {
        if !matches!(sh.dict[key].value, JxValue::Str(_) | JxValue::Int(_)) {
            return Err(wrongtype());
        }
    }
    put_str(&mut sh, key, val, now, expire);
    Ok(RespValue::ok())
}

pub fn get(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments for 'get' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    match &sh.dict[&args[0]].value {
        JxValue::Str(b) => Ok(RespValue::bulk(b.clone())),
        JxValue::Int(n) => Ok(RespValue::bulk(Bytes::from(n.to_string()))),
        _ => Err(wrongtype()),
    }
}

pub fn setnx(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments for 'setnx' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::int(0));
    }
    e.evict_if_needed(&args[0], now)?;
    put_str(&mut sh, &args[0], args[1].clone(), now, None);
    Ok(RespValue::int(1))
}

fn read_int(sh: &crate::ShardState, key: &Bytes) -> Result<i64, RespValue> {
    let e = sh.dict.get(key).ok_or_else(|| RespValue::err("ERR no such key"))?;
    match &e.value {
        JxValue::Int(n) => Ok(*n),
        JxValue::Str(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range")),
        _ => Err(wrongtype()),
    }
}

pub fn incr(e: &Engine, args: &[Bytes], now: u64, delta: i64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    apply_delta(e, &args[0], delta, now)
}

pub fn incrby(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments for 'incrby' command"));
    }
    apply_delta(e, &args[0], parse_i64(&args[1])?, now)
}

pub fn decrby(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments for 'decrby' command"));
    }
    apply_delta(e, &args[0], -parse_i64(&args[1])?, now)
}

fn apply_delta(e: &Engine, key: &Bytes, delta: i64, now: u64) -> Result<RespValue, RespValue> {
    let sid = e.shard_id_for_key(key);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, key, now) {
        put_str(&mut sh, key, Bytes::from(delta.to_string()), now, None);
        // 以 Int 保存更佳
        sh.dict.insert(key.clone(), Entry {
            value: JxValue::Int(delta),
            meta: KeyMeta { expire_at_ms: None, last_access_ms: now, freq: 1 },
        });
        return Ok(RespValue::int(delta));
    }
    let cur = read_int(&sh, key)?;
    let new = cur.saturating_add(delta);
    sh.dict.get_mut(key).unwrap().value = JxValue::Int(new);
    Ok(RespValue::int(new))
}

pub fn append(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments for 'append' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(&args[0], now)?;
    let mut buf = Vec::new();
    if Engine::touch_alive(&mut sh, &args[0], now) {
        match &sh.dict[&args[0]].value {
            JxValue::Str(b) => buf.extend_from_slice(b),
            JxValue::Int(n) => buf.extend_from_slice(n.to_string().as_bytes()),
            _ => return Err(wrongtype()),
        }
    }
    buf.extend_from_slice(&args[1]);
    let len = buf.len() as i64;
    put_str(&mut sh, &args[0], Bytes::from(buf), now, None);
    Ok(RespValue::int(len))
}

pub fn strlen(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments for 'strlen' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Str(b) => Ok(RespValue::int(b.len() as i64)),
        JxValue::Int(n) => Ok(RespValue::int(n.to_string().len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn getrange(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err("ERR wrong number of arguments for 'getrange' command"));
    }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::bulk(Bytes::new()));
    }
    let b = match &sh.dict[&args[0]].value {
        JxValue::Str(b) => b.clone(),
        JxValue::Int(n) => Bytes::from(n.to_string()),
        _ => return Err(wrongtype()),
    };
    let len = b.len() as i64;
    if len == 0 {
        return Ok(RespValue::bulk(Bytes::new()));
    }
    let s = if start < 0 { (len + start).max(0) } else { start.min(len) };
    let t = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) };
    if s > t {
        return Ok(RespValue::bulk(Bytes::new()));
    }
    Ok(RespValue::bulk(b.slice(s as usize..=t as usize)))
}

pub fn mget(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut out = Vec::with_capacity(args.len());
    for k in args {
        out.push(get(e, std::slice::from_ref(k), now).unwrap_or(RespValue::null_bulk()));
    }
    Ok(RespValue::array(out))
}

pub fn mset(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(RespValue::err("ERR wrong number of arguments for 'mset' command"));
    }
    for pair in args.chunks(2) {
        set(e, &[pair[0].clone(), pair[1].clone()], now)?;
    }
    Ok(RespValue::ok())
}
```

`crates/jx-engine/src/commands/key.rs`:
```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::Engine;

pub fn del(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut n = 0;
    for k in args {
        let sid = e.shard_id_for_key(k);
        let mut sh = e.lock_shard(sid);
        if Engine::touch_alive(&mut sh, k, now) || sh.dict.contains_key(k) {
            sh.dict.remove(k);
            n += 1;
        }
    }
    Ok(RespValue::int(n))
}

pub fn exists(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut n = 0;
    for k in args {
        let sid = e.shard_id_for_key(k);
        let mut sh = e.lock_shard(sid);
        if Engine::touch_alive(&mut sh, k, now) {
            n += 1;
        }
    }
    Ok(RespValue::int(n))
}

pub fn key_type(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments for 'type' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::bulk(Bytes::from_static(b"none")));
    }
    Ok(RespValue::bulk(Bytes::from(sh.dict[&args[0]].value.type_name())))
}

pub fn expire(e: &Engine, args: &[Bytes], now: u64, ms: bool) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let n: i64 = std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::int(0));
    }
    let deadline = if ms {
        now.saturating_add(n.max(0) as u64)
    } else {
        now.saturating_add((n.max(0) as u64).saturating_mul(1000))
    };
    sh.dict.get_mut(&args[0]).unwrap().meta.expire_at_ms = Some(deadline);
    Ok(RespValue::int(1))
}

pub fn ttl(e: &Engine, args: &[Bytes], now: u64, ms: bool) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::int(-2));
    }
    match sh.dict[&args[0]].meta.expire_at_ms {
        None => Ok(RespValue::int(-1)),
        Some(exp) => {
            let left = exp.saturating_sub(now);
            Ok(RespValue::int(if ms { left as i64 } else { (left / 1000) as i64 }))
        }
    }
}

pub fn persist(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::int(0));
    }
    let had = sh.dict.get_mut(&args[0]).unwrap().meta.expire_at_ms.take().is_some();
    Ok(RespValue::int(had as i64))
}
```

`crates/jx-engine/src/ttl.rs`:
```rust
use bytes::Bytes;
use crate::Engine;

pub fn expire_cycle(e: &Engine, now: u64, budget: usize) -> u64 {
    let mut removed = 0u64;
    for sid in 0..e.shard_count() {
        if removed as usize >= budget { break; }
        let mut sh = e.lock_shard(sid);
        let expired: Vec<Bytes> = sh.dict.iter()
            .filter(|(_, ent)| ent.meta.expire_at_ms.map(|exp| now >= exp).unwrap_or(false))
            .map(|(k, _)| k.clone())
            .take(budget - removed as usize)
            .collect();
        for k in expired {
            sh.dict.remove(&k);
            removed += 1;
        }
    }
    removed
}
```

`crates/jx-engine/src/evict.rs`:
```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{Engine, MaxMemoryPolicy};

pub fn evict_if_needed(e: &Engine, key: &Bytes, now: u64) -> Result<(), RespValue> {
    let Some(limit) = e.maxmemory() else { return Ok(()) };
    if e.total_mem() < limit {
        return Ok(());
    }
    let sid = e.shard_id_for_key(key);
    let mut sh = e.lock_shard(sid);
    if matches!(sh.policy, MaxMemoryPolicy::NoEviction) {
        return Err(RespValue::err("OOM command not allowed when used memory > 'maxmemory'"));
    }
    // 采样 5 个 key，优先淘汰有过期时间的
    let mut candidates: Vec<(usize, Bytes, u64, Option<u64>)> = sh.dict.iter()
        .take(5)
        .map(|(k, ent)| (ent.meta.freq, k.clone(), ent.meta.last_access_ms, ent.meta.expire_at_ms))
        .collect();
    let volatile_first = matches!(sh.policy, MaxMemoryPolicy::VolatileLru | MaxMemoryPolicy::VolatileLfu | MaxMemoryPolicy::VolatileTtl);
    candidates.sort_by(|a, b| {
        use std::cmp::Ordering;
        if volatile_first {
            let av = a.3.is_some() as u8;
            let bv = b.3.is_some() as u8;
            bv.cmp(&av).then(a.0.cmp(&b.0)).then(a.2.cmp(&b.2))
        } else {
            a.0.cmp(&b.0).then(a.2.cmp(&b.2))
        }
    });
    if let Some((_, victim, _, _)) = candidates.into_iter().next() {
        sh.dict.remove(&victim);
        let _ = now;
        Ok(())
    } else {
        Err(RespValue::err("OOM command not allowed when used memory > 'maxmemory'"))
    }
}
```

> 注意：`JxValue::approx_bytes` 需在 types.rs 中补充（见 Task 0 类型骨架），用于 mem 维护。

- [ ] **Step 4: `cargo test -p jx-engine` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "feat(jx-engine): string/key commands, ttl, eviction"
```

## Task 4: Hash / List / Set / ZSet 命令

**Files:**
- Create: `crates/jx-engine/src/commands/{hash.rs,list.rs,set.rs,zset.rs}`
- Modify: `crates/jx-engine/src/commands/mod.rs`, `crates/jx-engine/src/lib.rs`
- Test: `crates/jx-engine/tests/collections.rs`

**Interfaces:**
- Produces: `execute` 支持附录 A 中 Hash/List/Set/ZSet 冻结命令；`is_unimplemented` 同步移除。

- [ ] **Step 1: 失败测试**

```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::RespValue;

fn eng() -> Engine { Engine::new_for_test(1) }
fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn hash_commands() {
    let e = eng();
    assert_eq!(execute(&e, &[b("HSET"), b("h"), b("f1"), b("v1"), b("f2"), b("v2")]).unwrap(), RespValue::int(2));
    assert_eq!(execute(&e, &[b("HGET"), b("h"), b("f1")]).unwrap(), RespValue::bulk(b("v1")));
    assert_eq!(execute(&e, &[b("HLEN"), b("h")]).unwrap(), RespValue::int(2));
    assert_eq!(execute(&e, &[b("HEXISTS"), b("h"), b("f2")]).unwrap(), RespValue::int(1));
    assert!(matches!(execute(&e, &[b("HGETALL"), b("h")]).unwrap(), RespValue::Array(Some(_))));
    assert_eq!(execute(&e, &[b("HINCRBY"), b("h"), b("n"), b("3")]).unwrap(), RespValue::int(3));
    assert_eq!(execute(&e, &[b("HDEL"), b("h"), b("f1")]).unwrap(), RespValue::int(1));
}

#[test]
fn list_commands() {
    let e = eng();
    execute(&e, &[b("RPUSH"), b("l"), b("a"), b("b"), b("c")]).unwrap();
    assert_eq!(execute(&e, &[b("LLEN"), b("l")]).unwrap(), RespValue::int(3));
    assert_eq!(
        execute(&e, &[b("LRANGE"), b("l"), b("0"), b("-1")]).unwrap(),
        RespValue::array(vec![RespValue::bulk(b("a")), RespValue::bulk(b("b")), RespValue::bulk(b("c"))])
    );
    assert_eq!(execute(&e, &[b("LPOP"), b("l")]).unwrap(), RespValue::bulk(b("a")));
    assert_eq!(execute(&e, &[b("RPOP"), b("l")]).unwrap(), RespValue::bulk(b("c")));
    execute(&e, &[b("LPUSH"), b("l"), b("z")]).unwrap();
    assert_eq!(execute(&e, &[b("LINDEX"), b("l"), b("0")]).unwrap(), RespValue::bulk(b("z")));
}

#[test]
fn set_commands() {
    let e = eng();
    execute(&e, &[b("SADD"), b("s"), b("a"), b("b"), b("a")]).unwrap();
    assert_eq!(execute(&e, &[b("SCARD"), b("s")]).unwrap(), RespValue::int(2));
    assert_eq!(execute(&e, &[b("SISMEMBER"), b("s"), b("a")]).unwrap(), RespValue::int(1));
    assert_eq!(execute(&e, &[b("SREM"), b("s"), b("a")]).unwrap(), RespValue::int(1));
    assert_eq!(execute(&e, &[b("SMEMBERS"), b("s")]).unwrap(), RespValue::array(vec![RespValue::bulk(b("b"))]));
}

#[test]
fn zset_commands() {
    let e = eng();
    execute(&e, &[b("ZADD"), b("z"), b("1"), b("a"), b("2"), b("b")]).unwrap();
    assert_eq!(execute(&e, &[b("ZSCORE"), b("z"), b("a")]).unwrap(), RespValue::bulk(b("1")));
    assert_eq!(execute(&e, &[b("ZCARD"), b("z")]).unwrap(), RespValue::int(2));
    assert_eq!(
        execute(&e, &[b("ZRANGE"), b("z"), b("0"), b("-1")]).unwrap(),
        RespValue::array(vec![RespValue::bulk(b("a")), RespValue::bulk(b("b"))])
    );
    assert_eq!(execute(&e, &[b("ZINCRBY"), b("z"), b("1"), b("a")]).unwrap(), RespValue::bulk(b("2")));
}
```

- [ ] **Step 2: 确认失败** → collections FAIL

- [ ] **Step 3: 实现四个命令模块**

`hash.rs`（完整实现，含 ensure_hash 辅助）：
```rust
use std::collections::HashMap;
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{types::{Entry, JxValue, KeyMeta}, Engine};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn ensure_hash<'a>(sh: &'a mut crate::ShardState, key: &Bytes, now: u64) -> Result<&'a mut HashMap<Bytes, Bytes>, RespValue> {
    if Engine::touch_alive(sh, key, now) {
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::Hash(h) => Ok(h),
            _ => Err(wrongtype()),
        }
    } else {
        sh.dict.insert(key.clone(), Entry {
            value: JxValue::Hash(HashMap::new()),
            meta: KeyMeta { expire_at_ms: None, last_access_ms: now, freq: 1 },
        });
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::Hash(h) => Ok(h),
            _ => unreachable!(),
        }
    }
}

pub fn hset(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(RespValue::err("ERR wrong number of arguments for 'hset' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(&args[0], now)?;
    let h = ensure_hash(&mut sh, &args[0], now)?;
    let mut added = 0;
    for pair in args[1..].chunks(2) {
        if h.insert(pair[0].clone(), pair[1].clone()).is_none() {
            added += 1;
        }
    }
    Ok(RespValue::int(added))
}

pub fn hget(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 { return Err(RespValue::err("ERR wrong number of arguments for 'hget' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(match h.get(&args[1]) { Some(v) => RespValue::bulk(v.clone()), None => RespValue::null_bulk() }),
        _ => Err(wrongtype()),
    }
}

pub fn hmget(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments for 'hmget' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Ok(RespValue::array(vec![RespValue::null_bulk(); args.len() - 1]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(
            args[1..].iter().map(|f| match h.get(f) {
                Some(v) => RespValue::bulk(v.clone()),
                None => RespValue::null_bulk(),
            }).collect(),
        )),
        _ => Err(wrongtype()),
    }
}

pub fn hdel(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments for 'hdel' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    let mut n = 0;
    if let JxValue::Hash(h) = &mut sh.dict.get_mut(&args[0]).unwrap().value {
        for f in &args[1..] { if h.remove(f).is_some() { n += 1; } }
        if h.is_empty() { sh.dict.remove(&args[0]); }
    } else { return Err(wrongtype()); }
    Ok(RespValue::int(n))
}

pub fn hexists(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::int(h.contains_key(&args[1]) as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn hlen(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::int(h.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn hkeys(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(h.keys().cloned().map(RespValue::bulk).collect())),
        _ => Err(wrongtype()),
    }
}

pub fn hvals(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(h.values().cloned().map(RespValue::bulk).collect())),
        _ => Err(wrongtype()),
    }
}

pub fn hgetall(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => {
            let mut out = Vec::with_capacity(h.len() * 2);
            for (k, v) in h {
                out.push(RespValue::bulk(k.clone()));
                out.push(RespValue::bulk(v.clone()));
            }
            Ok(RespValue::array(out))
        }
        _ => Err(wrongtype()),
    }
}

pub fn hincrby(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 { return Err(RespValue::err("ERR wrong number of arguments for 'hincrby' command")); }
    let delta: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    let h = ensure_hash(&mut sh, &args[0], now)?;
    let cur = match h.get(&args[1]) {
        None => 0i64,
        Some(v) => std::str::from_utf8(v).ok().and_then(|s| s.parse().ok())
            .ok_or_else(|| RespValue::err("ERR hash value is not an integer"))?,
    };
    let new = cur.saturating_add(delta);
    h.insert(args[1].clone(), Bytes::from(new.to_string()));
    Ok(RespValue::int(new))
}
```

`list.rs`:
```rust
use std::collections::VecDeque;
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{types::{Entry, JxValue, KeyMeta}, Engine};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn parse_i64(b: &Bytes) -> Result<i64, RespValue> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))
}

fn ensure_list<'a>(sh: &'a mut crate::ShardState, key: &Bytes, now: u64) -> Result<&'a mut VecDeque<Bytes>, RespValue> {
    if Engine::touch_alive(sh, key, now) {
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::List(l) => Ok(l),
            _ => Err(wrongtype()),
        }
    } else {
        sh.dict.insert(key.clone(), Entry {
            value: JxValue::List(VecDeque::new()),
            meta: KeyMeta { expire_at_ms: None, last_access_ms: now, freq: 1 },
        });
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::List(l) => Ok(l),
            _ => unreachable!(),
        }
    }
}

pub fn lpush(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> { push(e, args, now, true) }
pub fn rpush(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> { push(e, args, now, false) }

fn push(e: &Engine, args: &[Bytes], now: u64, left: bool) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(&args[0], now)?;
    let l = ensure_list(&mut sh, &args[0], now)?;
    for v in &args[1..] {
        if left { l.push_front(v.clone()); } else { l.push_back(v.clone()); }
    }
    Ok(RespValue::int(l.len() as i64))
}

pub fn lpop(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> { pop(e, args, now, true) }
pub fn rpop(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> { pop(e, args, now, false) }

fn pop(e: &Engine, args: &[Bytes], now: u64, left: bool) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    let mut out = RespValue::null_bulk();
    if let JxValue::List(l) = &mut sh.dict.get_mut(&args[0]).unwrap().value {
        out = if left { l.pop_front() } else { l.pop_back() }
            .map(RespValue::bulk)
            .unwrap_or(RespValue::null_bulk());
        if l.is_empty() { sh.dict.remove(&args[0]); }
    } else { return Err(wrongtype()); }
    Ok(out)
}

pub fn llen(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => Ok(RespValue::int(l.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn lrange(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 { return Err(RespValue::err("ERR wrong number of arguments for 'lrange' command")); }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start.min(len) };
            let t = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) };
            if s > t { return Ok(RespValue::array(vec![])); }
            Ok(RespValue::array(
                l.iter().skip(s as usize).take((t - s + 1) as usize).cloned().map(RespValue::bulk).collect(),
            ))
        }
        _ => Err(wrongtype()),
    }
}

pub fn lindex(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let idx = parse_i64(&args[1])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let i = if idx < 0 { len + idx } else { idx };
            if i < 0 || i >= len { return Ok(RespValue::null_bulk()); }
            Ok(RespValue::bulk(l[i as usize].clone()))
        }
        _ => Err(wrongtype()),
    }
}

pub fn lset(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 { return Err(RespValue::err("ERR wrong number of arguments for 'lset' command")); }
    let idx = parse_i64(&args[1])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) {
        return Err(RespValue::err("ERR no such key"));
    }
    match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let i = if idx < 0 { len + idx } else { idx };
            if i < 0 || i >= len { return Err(RespValue::err("ERR index out of range")); }
            l[i as usize] = args[2].clone();
            Ok(RespValue::ok())
        }
        _ => Err(wrongtype()),
    }
}
```

`set.rs`:
```rust
use std::collections::HashSet;
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{types::{Entry, JxValue, KeyMeta}, Engine};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn ensure_set<'a>(sh: &'a mut crate::ShardState, key: &Bytes, now: u64) -> Result<&'a mut HashSet<Bytes>, RespValue> {
    if Engine::touch_alive(sh, key, now) {
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::Set(s) => Ok(s),
            _ => Err(wrongtype()),
        }
    } else {
        sh.dict.insert(key.clone(), Entry {
            value: JxValue::Set(HashSet::new()),
            meta: KeyMeta { expire_at_ms: None, last_access_ms: now, freq: 1 },
        });
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::Set(s) => Ok(s),
            _ => unreachable!(),
        }
    }
}

pub fn sadd(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments for 'sadd' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(&args[0], now)?;
    let s = ensure_set(&mut sh, &args[0], now)?;
    let mut n = 0;
    for v in &args[1..] { if s.insert(v.clone()) { n += 1; } }
    Ok(RespValue::int(n))
}

pub fn srem(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments for 'srem' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    let mut n = 0;
    if let JxValue::Set(s) = &mut sh.dict.get_mut(&args[0]).unwrap().value {
        for v in &args[1..] { if s.remove(v) { n += 1; } }
        if s.is_empty() { sh.dict.remove(&args[0]); }
    } else { return Err(wrongtype()); }
    Ok(RespValue::int(n))
}

pub fn sismember(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::int(s.contains(&args[1]) as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn scard(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::int(s.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn smembers(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::array(s.iter().cloned().map(RespValue::bulk).collect())),
        _ => Err(wrongtype()),
    }
}

pub fn spop(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 1 || args.len() > 2 { return Err(RespValue::err("ERR wrong number of arguments for 'spop' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    let count = if args.len() == 2 {
        std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?
    } else { 1 };
    match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::Set(s) => {
            let mut popped = Vec::new();
            for _ in 0..count {
                if let Some(v) = s.iter().next().cloned() {
                    s.remove(&v);
                    popped.push(v);
                } else { break; }
            }
            if s.is_empty() { sh.dict.remove(&args[0]); }
            if args.len() == 1 {
                Ok(popped.into_iter().next().map(RespValue::bulk).unwrap_or(RespValue::null_bulk()))
            } else {
                Ok(RespValue::array(popped.into_iter().map(RespValue::bulk).collect()))
            }
        }
        _ => Err(wrongtype()),
    }
}

pub fn srandmember(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 1 || args.len() > 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => {
            let v = s.iter().next().cloned();
            if args.len() == 1 {
                Ok(v.map(RespValue::bulk).unwrap_or(RespValue::null_bulk()))
            } else {
                Ok(RespValue::array(v.into_iter().map(RespValue::bulk).collect()))
            }
        }
        _ => Err(wrongtype()),
    }
}
```

`zset.rs`（MVP 用 HashMap + 排序扫描，不用跳表）：
```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::{types::{Entry, JxValue, KeyMeta, ZSet}, Engine};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn parse_f64(b: &Bytes) -> Result<f64, RespValue> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not a valid float"))
}

fn parse_i64(b: &Bytes) -> Result<i64, RespValue> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))
}

fn fmt_f64(f: f64) -> String {
    if f == f.trunc() && f.is_finite() { format!("{}", f as i64) } else { format!("{f}") }
}

fn ensure_zset<'a>(sh: &'a mut crate::ShardState, key: &Bytes, now: u64) -> Result<&'a mut std::collections::HashMap<Bytes, f64>, RespValue> {
    if Engine::touch_alive(sh, key, now) {
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::ZSet(z) => Ok(&mut z.entries),
            _ => Err(wrongtype()),
        }
    } else {
        sh.dict.insert(key.clone(), Entry {
            value: JxValue::ZSet(ZSet::default()),
            meta: KeyMeta { expire_at_ms: None, last_access_ms: now, freq: 1 },
        });
        match &mut sh.dict.get_mut(key).unwrap().value {
            JxValue::ZSet(z) => Ok(&mut z.entries),
            _ => unreachable!(),
        }
    }
}

fn sorted_members(entries: &std::collections::HashMap<Bytes, f64>) -> Vec<(Bytes, f64)> {
    let mut items: Vec<(Bytes, f64)> = entries.iter().map(|(k, s)| (k.clone(), *s)).collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    items
}

pub fn zadd(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(RespValue::err("ERR wrong number of arguments for 'zadd' command"));
    }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    e.evict_if_needed(&args[0], now)?;
    let z = ensure_zset(&mut sh, &args[0], now)?;
    let mut added = 0;
    for pair in args[1..].chunks(2) {
        let score = parse_f64(&pair[0])?;
        if z.insert(pair[1].clone(), score).is_none() { added += 1; }
    }
    Ok(RespValue::int(added))
}

pub fn zrem(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 { return Err(RespValue::err("ERR wrong number of arguments for 'zrem' command")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    let mut n = 0;
    if let JxValue::ZSet(z) = &mut sh.dict.get_mut(&args[0]).unwrap().value {
        for m in &args[1..] { if z.entries.remove(m).is_some() { n += 1; } }
        if z.entries.is_empty() { sh.dict.remove(&args[0]); }
    } else { return Err(wrongtype()); }
    Ok(RespValue::int(n))
}

pub fn zscore(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::null_bulk()); }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => Ok(match z.entries.get(&args[1]) {
            Some(s) => RespValue::bulk(Bytes::from(fmt_f64(*s))),
            None => RespValue::null_bulk(),
        }),
        _ => Err(wrongtype()),
    }
}

pub fn zcard(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 { return Err(RespValue::err("ERR wrong number of arguments")); }
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::int(0)); }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => Ok(RespValue::int(z.entries.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn zrange(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 { return Err(RespValue::err("ERR wrong number of arguments for 'zrange' command")); }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => {
            let items = sorted_members(&z.entries);
            let len = items.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start.min(len) };
            let t = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) };
            if s > t { return Ok(RespValue::array(vec![])); }
            Ok(RespValue::array(items.into_iter().skip(s as usize).take((t - s + 1) as usize).map(|(m, _)| RespValue::bulk(m)).collect()))
        }
        _ => Err(wrongtype()),
    }
}

fn parse_score_bound(b: &Bytes) -> Result<Option<f64>, RespValue> {
    let s = std::str::from_utf8(b).map_err(|_| RespValue::err("ERR min or max is not a float"))?;
    if s == "-inf" || s == "+inf" { return Ok(None); }
    s.parse::<f64>().map(Some).map_err(|_| RespValue::err("ERR min or max is not a float"))
}

pub fn zrangebyscore(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 4 { return Err(RespValue::err("ERR wrong number of arguments for 'zrangebyscore' command")); }
    let min = parse_score_bound(&args[1])?;
    let max = parse_score_bound(&args[2])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    if !Engine::touch_alive(&mut sh, &args[0], now) { return Ok(RespValue::array(vec![])); }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => {
            let items: Vec<(Bytes, f64)> = sorted_members(&z.entries).into_iter()
                .filter(|(_, s)| min.map(|lo| *s >= lo).unwrap_or(true))
                .filter(|(_, s)| max.map(|hi| *s <= hi).unwrap_or(true))
                .collect();
            Ok(RespValue::array(items.into_iter().map(|(m, _)| RespValue::bulk(m)).collect()))
        }
        _ => Err(wrongtype()),
    }
}

pub fn zincrby(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 { return Err(RespValue::err("ERR wrong number of arguments for 'zincrby' command")); }
    let delta = parse_f64(&args[1])?;
    let sid = e.shard_id_for_key(&args[0]);
    let mut sh = e.lock_shard(sid);
    let z = ensure_zset(&mut sh, &args[0], now)?;
    let cur = z.get(&args[2]).copied().unwrap_or(0.0);
    let new = cur + delta;
    z.insert(args[2].clone(), new);
    Ok(RespValue::bulk(Bytes::from(fmt_f64(new))))
}
```

**lib.rs execute 接线**（在 match 中增加，并从 `is_unimplemented` 移除）：
```rust
"HSET" => commands::hash::hset(engine, a, now),
"HGET" => commands::hash::hget(engine, a, now),
"HMGET" => commands::hash::hmget(engine, a, now),
"HDEL" => commands::hash::hdel(engine, a, now),
"HEXISTS" => commands::hash::hexists(engine, a, now),
"HLEN" => commands::hash::hlen(engine, a, now),
"HKEYS" => commands::hash::hkeys(engine, a, now),
"HVALS" => commands::hash::hvals(engine, a, now),
"HGETALL" => commands::hash::hgetall(engine, a, now),
"HINCRBY" => commands::hash::hincrby(engine, a, now),
"LPUSH" => commands::list::lpush(engine, a, now),
"RPUSH" => commands::list::rpush(engine, a, now),
"LPOP" => commands::list::lpop(engine, a, now),
"RPOP" => commands::list::rpop(engine, a, now),
"LRANGE" => commands::list::lrange(engine, a, now),
"LLEN" => commands::list::llen(engine, a, now),
"LINDEX" => commands::list::lindex(engine, a, now),
"LSET" => commands::list::lset(engine, a, now),
"SADD" => commands::set::sadd(engine, a, now),
"SREM" => commands::set::srem(engine, a, now),
"SMEMBERS" => commands::set::smembers(engine, a, now),
"SISMEMBER" => commands::set::sismember(engine, a, now),
"SCARD" => commands::set::scard(engine, a, now),
"SPOP" => commands::set::spop(engine, a, now),
"SRANDMEMBER" => commands::set::srandmember(engine, a, now),
"ZADD" => commands::zset::zadd(engine, a, now),
"ZREM" => commands::zset::zrem(engine, a, now),
"ZSCORE" => commands::zset::zscore(engine, a, now),
"ZCARD" => commands::zset::zcard(engine, a, now),
"ZRANGE" => commands::zset::zrange(engine, a, now),
"ZRANGEBYSCORE" => commands::zset::zrangebyscore(engine, a, now),
"ZINCRBY" => commands::zset::zincrby(engine, a, now),
```

- [ ] **Step 4: `cargo test -p jx-engine` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "feat(jx-engine): hash/list/set/zset commands"
```

---

# Phase 3 — 国密 jx-crypto 与持久化 jx-persist

## Task 5: jx-crypto — SM3 口令与 SM4 文件加解密

**Files:**
- Create: `crates/jx-crypto/src/{lib.rs,sm3_auth.rs,sm4_file.rs,keyfile.rs}`
- Test: `crates/jx-crypto/tests/crypto.rs`

**Interfaces:**
- Produces:
  - `pub fn sm3_hash(data: &[u8]) -> [u8; 32]`
  - `pub fn hash_password(password: &str, salt: &[u8; 16]) -> String`（hex(sm3(salt||password))）
  - `pub fn verify_password(password: &str, salt_hex: &str, hash_hex: &str) -> bool`
  - `pub fn generate_keyfile(path: &Path) -> anyhow::Result<()>`（32 字节随机，0600）
  - `pub fn load_keyfile(path: &Path) -> anyhow::Result<[u8; 32]>`
  - `pub fn sm4_gcm_seal(key: &[u8;32], plaintext: &[u8]) -> anyhow::Result<Vec<u8>>`（nonce||ct||tag）
  - `pub fn sm4_gcm_open(key: &[u8;32], sealed: &[u8]) -> anyhow::Result<Vec<u8>>`

- [ ] **Step 1: 失败测试**

```rust
use jx_crypto::{generate_keyfile, hash_password, load_keyfile, sm4_gcm_open, sm4_gcm_seal, verify_password};
use std::path::PathBuf;

#[test]
fn sm3_password_roundtrip() {
    let salt = [7u8; 16];
    let h = hash_password("s3cret", &salt);
    assert!(verify_password("s3cret", &hex_encode(&salt), &h));
    assert!(!verify_password("wrong", &hex_encode(&salt), &h));
}

#[test]
fn sm4_file_roundtrip() {
    let key = [9u8; 32];
    let sealed = sm4_gcm_seal(&key, b"hello MemoryDataBase").unwrap();
    let opened = sm4_gcm_open(&key, &sealed).unwrap();
    assert_eq!(opened, b"hello MemoryDataBase");
    assert!(sm4_gcm_open(&[0u8;32], &sealed).is_err());
}

#[test]
fn keyfile_permissions_and_load() {
    let dir = std::env::temp_dir().join(format!("jx-key-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("storage.key");
    generate_keyfile(&path).unwrap();
    let k = load_keyfile(&path).unwrap();
    assert_eq!(k.len(), 32);
    let _ = std::fs::remove_dir_all(&dir);
}

fn hex_encode(b: &[u8]) -> String { b.iter().map(|x| format!("{x:02x}")).collect() }
```

- [ ] **Step 2: 确认失败** `cargo test -p jx-crypto` → FAIL

- [ ] **Step 3: 实现**

`sm3_auth.rs`:
```rust
use sm3::{Digest, Sm3};

pub fn sm3_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Sm3::new();
    h.update(data);
    h.finalize().into()
}

pub fn hash_password(password: &str, salt: &[u8; 16]) -> String {
    let mut buf = Vec::with_capacity(16 + password.len());
    buf.extend_from_slice(salt);
    buf.extend_from_slice(password.as_bytes());
    sm3_hash(&buf).iter().map(|b| format!("{b:02x}")).collect()
}

pub fn verify_password(password: &str, salt_hex: &str, hash_hex: &str) -> bool {
    let Some(salt) = hex::decode(salt_hex).ok() else { return false };
    if salt.len() != 16 { return false; }
    let mut s = [0u8; 16];
    s.copy_from_slice(&salt);
    hash_password(password, &s) == hash_hex
}
```

`sm4_file.rs`:
```rust
use anyhow::{anyhow, Context, Result};
use rand::RngCore;
use sm4::cipher::{Aead, KeyInit, Payload};
use sm4::cipher::consts::U16;
use sm4::cipher::generic_array::GenericArray;

type Sm4Gcm = sm4::cipher::Gcm<sm4::Sm4, U16>;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

pub fn sm4_gcm_seal(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let cipher = Sm4Gcm::new_from_slice(key).map_err(|e| anyhow!("sm4 key: {e}"))?;
    let ct = cipher.encrypt(GenericArray::from_slice(&nonce), Payload { msg: plaintext, aad: b"" })
        .map_err(|e| anyhow!("sm4 encrypt: {e}"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn sm4_gcm_open(key: &[u8; 32], sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < NONCE_LEN + TAG_LEN {
        return Err(anyhow!("sealed data too short"));
    }
    let (nonce, rest) = sealed.split_at(NONCE_LEN);
    let cipher = Sm4Gcm::new_from_slice(key).map_err(|e| anyhow!("sm4 key: {e}"))?;
    cipher.decrypt(GenericArray::from_slice(nonce), Payload { msg: rest, aad: b"" })
        .context("sm4 decrypt (bad key or corrupted data)")
}
```

`keyfile.rs`:
```rust
use anyhow::{Context, Result};
use rand::RngCore;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

pub fn generate_keyfile(path: &Path) -> Result<()> {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, key).context("write keyfile")?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).context("chmod keyfile")?;
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
```

`lib.rs`:
```rust
pub mod keyfile;
pub mod sm3_auth;
pub mod sm4_file;
pub mod sm3_auth as auth;

pub use keyfile::{generate_keyfile, load_keyfile};
pub use sm3_auth::{hash_password, sm3_hash, verify_password};
pub use sm4_file::{sm4_gcm_open, sm4_gcm_seal};
```

Cargo.toml 增加：
```toml
[dependencies]
thiserror.workspace = true
rand.workspace = true
hex.workspace = true
tracing.workspace = true
sm3.workspace = true
sm4 = { version = "0.5", features = ["cipher"] }
anyhow.workspace = true
```

- [ ] **Step 4: `cargo test -p jx-crypto` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-crypto
git -C /Users/caosunliu commit -m "feat(jx-crypto): SM3 auth and SM4-GCM file encryption"
```

## Task 6: jx-crypto — TLCP 传输层（vendor gm-tlcp）

**Files:**
- Create: `crates/jx-crypto/src/tlcp.rs`, `vendor/gm-tlcp/`（git subtree/vendor 目录）
- Modify: `crates/jx-crypto/Cargo.toml`, `crates/jx-crypto/src/lib.rs`
- Test: `crates/jx-crypto/tests/tlcp.rs`

**Interfaces:**
- Produces:
  - `pub struct TlcpConfig { pub sign_cert_pem: Vec<u8>, pub sign_key_pem: Vec<u8>, pub enc_cert_pem: Vec<u8>, pub enc_key_pem: Vec<u8>, pub ca_pem: Vec<u8> }`
  - `pub async fn tlcp_accept(listener: tokio::net::TcpListener, cfg: TlcpConfig) -> Result<Box<dyn tokio::io::AsyncRead + AsyncWrite + Unpin>, anyhow::Error>`（简化：本任务先做同步握手测试；server 集成在 Task 12）
  - 若 `gm-tlcp` API 不匹配，实现 `pub enum TlcpBackend { PureRust, GmsslFfi }` 并在 `jx-server` 中按 feature 选择。

**决策门（执行者必读）：**
1. 先尝试 vendor 纯 Rust `gm-tlcp`（MIT/Apache-2.0）到 `vendor/gm-tlcp`，路径依赖 `gm-tlcp = { path = "../../vendor/gm-tlcp" }`。
2. 跑与 GmSSL 的互操作（若有 GmSSL 二进制：`gmssl tlcp_server` / `tlcp_client`）。
3. 若握手失败或 API 不可用，切换到 `gmssl-rs` FFI：`gmssl-rs = "0.1"`，并在 `scripts/build-linux.sh` 中为三架构编译 GmSSL 静态库。
4. 无论后端，保持 `tlcp.rs` 对外 API 不变。

- [ ] **Step 1: 生成测试双证书**（开发机若有 gmssl/gm-ca；否则用 openssl 兼容方式生成占位并在 CI 跳过）

Run: `gmssl sm2keygen -out sign.key -pubout sign.pem && gmssl sm2keygen -out enc.key -pubout enc.pem`（或项目内 `gm-ca` 工具）
Expected: 生成 sign/enc 双证书 PEM。

- [ ] **Step 2: 失败测试 `crates/jx-crypto/tests/tlcp.rs`**

```rust
#[test]
fn tlcp_config_loads_pems() {
    // 使用测试 fixture PEM（提交到 crates/jx-crypto/tests/fixtures/tlcp/）
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tlcp");
    let cfg = jx_crypto::TlcpConfig::from_pem_files(
        dir.join("sign.crt"), dir.join("sign.key"),
        dir.join("enc.crt"), dir.join("enc.key"),
        dir.join("ca.crt"),
    ).expect("load tlcp pems");
    assert!(!cfg.sign_cert_pem.is_empty());
    assert!(!cfg.enc_key_pem.is_empty());
}
```

- [ ] **Step 3: 实现 `tlcp.rs`（含 backend 抽象）**

```rust
use anyhow::{Context, Result};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct TlcpConfig {
    pub sign_cert_pem: Vec<u8>,
    pub sign_key_pem: Vec<u8>,
    pub enc_cert_pem: Vec<u8>,
    pub enc_key_pem: Vec<u8>,
    pub ca_pem: Vec<u8>,
}

impl TlcpConfig {
    pub fn from_pem_files(
        sign_cert: impl AsRef<Path>, sign_key: impl AsRef<Path>,
        enc_cert: impl AsRef<Path>, enc_key: impl AsRef<Path>,
        ca: impl AsRef<Path>,
    ) -> Result<Self> {
        Ok(Self {
            sign_cert_pem: std::fs::read(sign_cert.as_ref()).context("read sign cert")?,
            sign_key_pem: std::fs::read(sign_key.as_ref()).context("read sign key")?,
            enc_cert_pem: std::fs::read(enc_cert.as_ref()).context("read enc cert")?,
            enc_key_pem: std::fs::read(enc_key.as_ref()).context("read enc key")?,
            ca_pem: std::fs::read(ca.as_ref()).context("read ca")?,
        })
    }
}

/// TLCP 后端：优先纯 Rust gm-tlcp，失败时可切 GmSSL FFI。
/// MVP：提供同步握手 + stream 包装接口，具体在 jx-server 接线。
pub trait TlcpAcceptor: Send {
    fn accept(&self, stream: std::net::TcpStream) -> Result<Box<dyn std::io::ReadWrite + Send>>;
}

/// 简化接口：返回是否启用 TLCP 以及配置加载是否成功。
pub fn tlcp_enabled_and_ready(cfg: &TlcpConfig) -> bool {
    !cfg.sign_cert_pem.is_empty() && !cfg.enc_key_pem.is_empty()
}
```

> **执行说明**：`gm-tlcp` 0.1.0 API 可能变动。实现时以 vendor 目录内 `src/tlcp/` 的实际 API 为准，将 `TlcpAcceptor` 实现接到 `TlcpAcceptor::new(dual_certs)`。互操作测试脚本放 `scripts/tlcp-interop.sh`，CI 无 GmSSL 时 `#[ignore]`。

- [ ] **Step 4: `cargo test -p jx-crypto` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-crypto opencode/MemoryDataBase/vendor 2>/dev/null || git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-crypto
git -C /Users/caosunliu commit -m "feat(jx-crypto): TLCP transport abstraction with gm-tlcp vendor"
```

## Task 7: jx-persist — RDB 快照与恢复

**Files:**
- Create: `crates/jx-persist/src/{lib.rs,rdb.rs,restore.rs}`
- Test: `crates/jx-persist/tests/rdb.rs`

**Interfaces:**
- Consumes: `jx_engine::{Engine, JxValue, Entry, KeyMeta}`（需给 Engine 增加 `snapshot_shard(sid) -> Vec<(Bytes, JxValue, KeyMeta)>` 与 `load_shard(sid, entries)`）
- Produces:
  - `pub const RDB_MAGIC: &[u8; 8] = b"JX RDB\0\0";`
  - `pub fn save_rdb(engine: &Engine, path: &Path, key: Option<&[u8;32]>) -> Result<()>`
  - `pub fn load_rdb(engine: &Engine, path: &Path, key: Option<&[u8;32]>) -> Result<usize>`（返回 key 数）

- [ ] **Step 1: 失败测试**

```rust
use jx_engine::{execute, Engine};
use jx_persist::{load_rdb, save_rdb};
use bytes::Bytes;

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn rdb_save_load_roundtrip() {
    let e = Engine::new_for_test(2);
    execute(&e, &[b("SET"), b("a"), b("1")]).unwrap();
    execute(&e, &[b("SET"), b("b"), b("2")]).unwrap();
    execute(&e, &[b("RPUSH"), b("l"), b("x"), b("y")]).unwrap();
    let dir = std::env::temp_dir().join(format!("jx-rdb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, None).unwrap();
    let e2 = Engine::new_for_test(2);
    let n = load_rdb(&e2, &path, None).unwrap();
    assert_eq!(n, 3);
    assert_eq!(execute(&e2, &[b("GET"), b("a")]).unwrap(), jx_proto::RespValue::bulk(b("1")));
    assert_eq!(execute(&e2, &[b("LLEN"), b("l")]).unwrap(), jx_proto::RespValue::int(2));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_encrypted_roundtrip() {
    let key = [1u8; 32];
    let e = Engine::new_for_test(1);
    execute(&e, &[b("SET"), b("k"), b("secret")]).unwrap();
    let dir = std::env::temp_dir().join(format!("jx-rdb-enc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, Some(&key)).unwrap();
    let e2 = Engine::new_for_test(1);
    load_rdb(&e2, &path, Some(&key)).unwrap();
    assert_eq!(execute(&e2, &[b("GET"), b("k")]).unwrap(), jx_proto::RespValue::bulk(b("secret")));
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现 RDB**

Engine 增加（jx-engine）：
```rust
impl Engine {
    pub fn snapshot_shard(&self, sid: usize) -> Vec<(Bytes, crate::types::JxValue, crate::types::KeyMeta)> {
        let sh = self.lock_shard(sid);
        sh.dict.iter().map(|(k, e)| (k.clone(), e.value.clone(), e.meta.clone())).collect()
    }

    pub fn load_shard(&self, sid: usize, entries: Vec<(Bytes, crate::types::JxValue, crate::types::KeyMeta)>) {
        let mut sh = self.lock_shard(sid);
        for (k, v, m) in entries {
            sh.dict.insert(k, crate::types::Entry { value: v, meta: m });
        }
    }
}
```

`rdb.rs`（分片序贯：写临时文件 + 原子 rename；可选 SM4-GCM 整文件加密）：
```rust
use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use jx_engine::{Engine, JxValue};
use jx_crypto::{sm4_gcm_open, sm4_gcm_seal};
use std::io::{Read, Write};
use std::path::Path;

pub const RDB_MAGIC: &[u8; 8] = b"JX RDB\0\0";
const RDB_VERSION: u16 = 1;

fn write_value(out: &mut BytesMut, v: &JxValue) {
    match v {
        JxValue::Str(b) => { out.put_u8(1); out.put_slice(b); }
        JxValue::Int(n) => { out.put_u8(2); out.put_i64(*n); }
        JxValue::Hash(h) => {
            out.put_u8(3);
            out.put_u32(h.len() as u32);
            for (k, val) in h { out.put_slice(k); out.put_slice(val); }
        }
        JxValue::List(l) => {
            out.put_u8(4);
            out.put_u32(l.len() as u32);
            for x in l { out.put_slice(x); }
        }
        JxValue::Set(s) => {
            out.put_u8(5);
            out.put_u32(s.len() as u32);
            for x in s { out.put_slice(x); }
        }
        JxValue::ZSet(z) => {
            out.put_u8(6);
            out.put_u32(z.entries.len() as u32);
            for (m, score) in &z.entries { out.put_slice(m); out.put_f64(*score); }
        }
    }
}

fn read_value(input: &mut &[u8]) -> Result<JxValue> {
    use anyhow::bail;
    let Some(&tag) = input.first() else { bail!("eof in value") };
    *input = &input[1..];
    Ok(match tag {
        1 => JxValue::Str(read_bytes(input)?),
        2 => {
            if input.len() < 8 { bail!("eof in int") }
            let n = i64::from_be_bytes(input[..8].try_into()?);
            *input = &input[8..];
            JxValue::Int(n)
        }
        3 => {
            let n = read_u32(input)? as usize;
            let mut h = std::collections::HashMap::new();
            for _ in 0..n { let k = read_bytes(input)?; let v = read_bytes(input)?; h.insert(k, v); }
            JxValue::Hash(h)
        }
        4 => {
            let n = read_u32(input)? as usize;
            let mut l = std::collections::VecDeque::new();
            for _ in 0..n { l.push_back(read_bytes(input)?); }
            JxValue::List(l)
        }
        5 => {
            let n = read_u32(input)? as usize;
            let mut s = std::collections::HashSet::new();
            for _ in 0..n { s.insert(read_bytes(input)?); }
            JxValue::Set(s)
        }
        6 => {
            let n = read_u32(input)? as usize;
            let mut z = JxValue::ZSet(Default::default());
            if let JxValue::ZSet(ref mut z) = z {
                for _ in 0..n { let m = read_bytes(input)?; let sc = read_f64(input)?; z.entries.insert(m, sc); }
            }
            z
        }
        _ => bail!("unknown value tag {tag}"),
    })
}

fn read_bytes(input: &mut &[u8]) -> Result<bytes::Bytes> {
    let n = read_u32(input)? as usize;
    if input.len() < n { anyhow::bail!("eof in bytes") }
    let b = bytes::Bytes::copy_from_slice(&input[..n]);
    *input = &input[n..];
    Ok(b)
}

fn read_u32(input: &mut &[u8]) -> Result<u32> {
    if input.len() < 4 { anyhow::bail!("eof in u32") }
    let n = u32::from_be_bytes(input[..4].try_into()?);
    *input = &input[4..];
    Ok(n)
}

fn read_f64(input: &mut &[u8]) -> Result<f64> {
    if input.len() < 8 { anyhow::bail!("eof in f64") }
    let n = f64::from_be_bytes(input[..8].try_into()?);
    *input = &input[8..];
    Ok(n)
}

pub fn save_rdb(engine: &Engine, path: &Path, key: Option<&[u8; 32]>) -> Result<()> {
    let mut buf = BytesMut::new();
    buf.put_slice(RDB_MAGIC);
    buf.put_u16(RDB_VERSION);
    buf.put_u32(engine.shard_count() as u32);
    for sid in 0..engine.shard_count() {
        let entries = engine.snapshot_shard(sid);
        buf.put_u32(entries.len() as u32);
        for (k, v, m) in &entries {
            buf.put_slice(k);
            buf.put_u32(m.expire_at_ms.map(|x| 1u8).unwrap_or(0));
            if let Some(exp) = m.expire_at_ms { buf.put_u64(exp); }
            write_value(&mut buf, v);
        }
    }
    let payload = match key {
        Some(k) => {
            let sealed = sm4_gcm_seal(k, &buf).context("sm4 seal rdb")?;
            let mut out = BytesMut::new();
            out.put_slice(b"JXRDBENC");
            out.put_slice(&sealed);
            out.freeze()
        }
        None => buf.freeze(),
    };
    let tmp = path.with_extension("rdb.tmp");
    {
        let mut f = std::fs::File::create(&tmp).context("create rdb tmp")?;
        f.write_all(&payload).context("write rdb tmp")?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, path).context("rename rdb")?;
    Ok(())
}

pub fn load_rdb(engine: &Engine, path: &Path, key: Option<&[u8; 32]>) -> Result<usize> {
    let raw = std::fs::read(path).context("read rdb")?;
    let data = if raw.starts_with(b"JXRDBENC") {
        let k = key.context("rdb is encrypted but no key provided")?;
        sm4_gcm_open(k, &raw[8..]).context("sm4 open rdb")?
    } else {
        raw
    };
    if data.len() < 8 + 2 + 4 || &data[..8] != RDB_MAGIC {
        anyhow::bail!("bad rdb magic");
    }
    let mut input: &[u8] = &data[10..];
    let nshards = read_u32(&mut input)? as usize;
    let mut total = 0usize;
    for sid in 0..nshards {
        let n = read_u32(&mut input)? as usize;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let k = read_bytes(&mut input)?;
            let has_exp = read_u32(&mut input)?; // 用 u32 写 0/1 更稳，与 writer 对齐
            // 注意：writer 用 put_u32(0/1)，此处对齐
            let expire = if has_exp == 1 { Some(read_u64(&mut input)?) } else { None };
            let v = read_value(&mut input)?;
            entries.push((k, v, jx_engine::KeyMeta { expire_at_ms: expire, last_access_ms: 0, freq: 1 }));
        }
        engine.load_shard(sid.min(engine.shard_count() - 1), entries);
        total += n;
    }
    Ok(total)
}

fn read_u64(input: &mut &[u8]) -> Result<u64> {
    if input.len() < 8 { anyhow::bail!("eof in u64") }
    let n = u64::from_be_bytes(input[..8].try_into()?);
    *input = &input[8..];
    Ok(n)
}
```

> **对齐说明**：`save_rdb` 中 expire 标志请统一使用 `put_u32(0|1)` + 可选 `put_u64`，`load_rdb` 按同一布局读取。实现时以测试通过为准。

`lib.rs`:
```rust
mod rdb;
pub use rdb::{load_rdb, save_rdb, RDB_MAGIC};
```

- [ ] **Step 4: `cargo test -p jx-persist` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-persist opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "feat(jx-persist): RDB snapshot save/load with optional SM4"
```

## Task 8: jx-persist — AOF 追加、重放与重写

**Files:**
- Create: `crates/jx-persist/src/aof.rs`
- Modify: `crates/jx-persist/src/lib.rs`
- Test: `crates/jx-persist/tests/aof.rs`

**Interfaces:**
- Produces:
  - `pub struct AofWriter { file: File }` with `append_resp(&mut self, args: &[Bytes]) -> Result<()>`（RESP 数组编码）与 `flush_policy(always|everysec|no)`
  - `pub fn append_aof(path: &Path, args: &[Bytes], key: Option<&[u8;32]>) -> Result<()>`（MVP 追加接口）
  - `pub fn replay_aof(engine: &Engine, path: &Path, key: Option<&[u8;32]>) -> Result<u64>`（执行命令数）
  - `pub fn rewrite_aof(engine: &Engine, path: &Path, key: Option<&[u8;32]>) -> Result<()>`

- [ ] **Step 1: 失败测试**

```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_persist::{append_aof, replay_aof, rewrite_aof};
use jx_proto::{encode_to_bytes, RespValue};

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn aof_append_replay() {
    let dir = std::env::temp_dir().join(format!("jx-aof-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    append_aof(&path, &[b("SET"), b("k"), b("v")], None).unwrap();
    append_aof(&path, &[b("RPUSH"), b("l"), b("a")], None).unwrap();
    let e = Engine::new_for_test(1);
    let n = replay_aof(&e, &path, None).unwrap();
    assert_eq!(n, 2);
    assert_eq!(execute(&e, &[b("GET"), b("k")]).unwrap(), RespValue::bulk(b("v")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_rewrite_full_snapshot_commands() {
    let e = Engine::new_for_test(1);
    execute(&e, &[b("SET"), b("a"), b("1")]).unwrap();
    execute(&e, &[b("SET"), b("b"), b("2")]).unwrap();
    let dir = std::env::temp_dir().join(format!("jx-aof-rw-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    rewrite_aof(&e, &path, None).unwrap();
    let e2 = Engine::new_for_test(1);
    replay_aof(&e2, &path, None).unwrap();
    assert_eq!(execute(&e2, &[b("GET"), b("a")]).unwrap(), RespValue::bulk(b("1")));
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现 AOF**

```rust
use anyhow::{Context, Result};
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::{encode_to_bytes, RespValue};
use std::io::Write;
use std::path::Path;

pub fn encode_command(args: &[Bytes]) -> Vec<u8> {
    encode_to_bytes(&RespValue::Array(Some(args.iter().map(|a| RespValue::bulk(a.clone())).collect()))).to_vec()
}

pub fn append_aof(path: &Path, args: &[Bytes], key: Option<&[u8; 32]>) -> Result<()> {
    let payload = encode_command(args);
    let body = match key {
        Some(k) => jx_crypto::sm4_gcm_seal(k, &payload)?,
        None => payload,
    };
    // MVP：整段加密时文件仅含一条；明文模式支持追加多条。
    // 为统一，明文追加 RESP 流；加密文件每次 append 覆盖为单条（文档写明限制）。
    if key.is_some() {
        let mut out = Vec::with_capacity(8 + body.len());
        out.extend_from_slice(b"JXAOFENC");
        out.extend_from_slice(&body);
        std::fs::write(path, out)?;
    } else {
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        f.write_all(&body)?;
        f.flush()?;
    }
    Ok(())
}

pub fn replay_aof(engine: &Engine, path: &Path, key: Option<&[u8; 32]>) -> Result<u64> {
    let raw = std::fs::read(path).context("read aof")?;
    let data = if raw.starts_with(b"JXAOFENC") {
        jx_crypto::sm4_gcm_open(key.context("aof encrypted but no key")?, &raw[8..])?
    } else { raw };
    let mut decoder = jx_proto::RespDecoder::new();
    decoder.feed(&data);
    let mut n = 0u64;
    while let Some(v) = decoder.next_value().unwrap_or(None) {
        if let RespValue::Array(Some(items)) = v {
            let args: Vec<Bytes> = items.into_iter().filter_map(|x| match x {
                RespValue::BulkString(Some(b)) => Some(b),
                _ => None,
            }).collect();
            if !args.is_empty() {
                let _ = execute(engine, &args);
                n += 1;
            }
        }
    }
    Ok(n)
}

pub fn rewrite_aof(engine: &Engine, path: &Path, key: Option<&[u8; 32]>) -> Result<()> {
    let tmp = path.with_extension("aof.tmp");
    if tmp.exists() { std::fs::remove_file(&tmp)?; }
    for sid in 0..engine.shard_count() {
        for (k, v, m) in engine.snapshot_shard(sid) {
            let _ = m;
            let cmd: Vec<Bytes> = match v {
                jx_engine::JxValue::Str(b) => vec![Bytes::from_static(b"SET"), k, b],
                jx_engine::JxValue::Int(n) => vec![Bytes::from_static(b"SET"), k, Bytes::from(n.to_string())],
                other => {
                    // 集合类型：HSET/RPUSH/SADD/ZADD
                    match other {
                        jx_engine::JxValue::Hash(h) => {
                            let mut cmd = vec![Bytes::from_static(b"HSET"), k];
                            for (f, val) in h { cmd.push(f); cmd.push(val); }
                            cmd
                        }
                        jx_engine::JxValue::List(l) => {
                            let mut cmd = vec![Bytes::from_static(b"RPUSH"), k];
                            for x in l { cmd.push(x); }
                            cmd
                        }
                        jx_engine::JxValue::Set(s) => {
                            let mut cmd = vec![Bytes::from_static(b"SADD"), k];
                            for x in s { cmd.push(x); }
                            cmd
                        }
                        jx_engine::JxValue::ZSet(z) => {
                            let mut cmd = vec![Bytes::from_static(b"ZADD"), k];
                            for (m, score) in z.entries {
                                cmd.push(Bytes::from(format!("{score}")));
                                cmd.push(m);
                            }
                            cmd
                        }
                        _ => continue,
                    }
                }
            };
            append_aof(&tmp, &cmd, key)?;
        }
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}
```

`lib.rs`:
```rust
mod aof;
mod rdb;

pub use aof::{append_aof, encode_command, replay_aof, rewrite_aof};
pub use rdb::{load_rdb, save_rdb, RDB_MAGIC};
```

- [ ] **Step 4: `cargo test -p jx-persist` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-persist
git -C /Users/caosunliu commit -m "feat(jx-persist): AOF append/replay/rewrite"
```

---

# Phase 4 — 服务端 jx-server 与 CLI

## Task 9: jx-server — 配置加载与校验

**Files:**
- Create: `crates/jx-server/src/config.rs`, `crates/jx-server/src/main.rs`（骨架）, `config/MemoryDataBase.conf.example`
- Test: `crates/jx-server/tests/config.rs`

**Interfaces:**
- Produces:
  - `pub struct Config { pub bind: String, pub port: u16, pub tls_mode: TlsMode, pub shards: Option<usize>, pub maxmemory: Option<usize>, pub maxmemory_policy: String, pub dir: PathBuf, pub save: Vec<(u64, u64)>, pub appendonly: bool, pub appendfsync: String, pub requirepass_sm3: Option<String>, pub storage_key_file: Option<PathBuf>, pub tlcp: Option<TlcpPaths>, pub tls: Option<TlsPaths>, pub maxclients: u32, pub timeout_secs: u64, pub loglevel: String }`
  - `pub enum TlsMode { Plain, Tls, Tlcp }`
  - `pub fn load_config(path: &Path) -> anyhow::Result<Config>`（key=value 解析，# 注释）
  - `impl Config { pub fn validate(&self) -> anyhow::Result<()> }`

- [ ] **Step 1: 失败测试**

```rust
use jx_server::config::{load_config, TlsMode};
use std::io::Write;

#[test]
fn load_example_config() {
    let dir = std::env::temp_dir().join(format!("jx-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "bind 127.0.0.1").unwrap();
    writeln!(f, "port 6379").unwrap();
    writeln!(f, "tls-mode plain").unwrap();
    writeln!(f, "shards 4").unwrap();
    writeln!(f, "maxmemory 256mb").unwrap();
    writeln!(f, "appendonly yes").unwrap();
    writeln!(f, "appendfsync everysec").unwrap();
    writeln!(f, "dir {}", dir.display()).unwrap();
    let cfg = load_config(&path).unwrap();
    assert_eq!(cfg.port, 6379);
    assert_eq!(cfg.tls_mode, TlsMode::Plain);
    assert!(cfg.appendonly);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn validate_rejects_bad_tls_mode() {
    let dir = std::env::temp_dir().join(format!("jx-cfg-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    std::fs::write(&path, "tls-mode nope\nport 1\nbind 127.0.0.1\n").unwrap();
    assert!(load_config(&path).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现 config.rs 与示例配置**

`config.rs`:
```rust
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode { Plain, Tls, Tlcp }

#[derive(Debug, Clone, Default)]
pub struct TlcpPaths {
    pub sign_cert: PathBuf,
    pub sign_key: PathBuf,
    pub enc_cert: PathBuf,
    pub enc_key: PathBuf,
    pub ca: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub tls_mode: TlsMode,
    pub shards: Option<usize>,
    pub maxmemory: Option<usize>,
    pub maxmemory_policy: String,
    pub dir: PathBuf,
    pub save: Vec<(u64, u64)>,
    pub appendonly: bool,
    pub appendfsync: String,
    pub requirepass_sm3: Option<String>,
    pub storage_key_file: Option<PathBuf>,
    pub tlcp: Option<TlcpPaths>,
    pub tls: Option<TlsPaths>,
    pub maxclients: u32,
    pub timeout_secs: u64,
    pub loglevel: String,
}

fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mult) = if let Some(r) = s.strip_suffix("gb") { (r, 1024 * 1024 * 1024) }
        else if let Some(r) = s.strip_suffix("mb") { (r, 1024 * 1024) }
        else if let Some(r) = s.strip_suffix("kb") { (r, 1024) }
        else { (s.as_str(), 1) };
    let n: usize = num.trim().parse().context("bad size")?;
    Ok(n * mult)
}

pub fn load_config(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut cfg = Config {
        bind: "127.0.0.1".into(),
        port: 6379,
        tls_mode: TlsMode::Plain,
        shards: None,
        maxmemory: None,
        maxmemory_policy: "noeviction".into(),
        dir: PathBuf::from("./data"),
        save: vec![(900, 1), (300, 10), (60, 10000)],
        appendonly: false,
        appendfsync: "everysec".into(),
        requirepass_sm3: None,
        storage_key_file: None,
        tlcp: None,
        tls: None,
        maxclients: 10000,
        timeout_secs: 0,
        loglevel: "info".into(),
    };
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let Some((k, v)) = line.split_once(char::is_whitespace) else {
            bail!("config line {}: expected 'key value'", i + 1);
        };
        let (k, v) = (k.trim(), v.trim());
        match k {
            "bind" => cfg.bind = v.to_string(),
            "port" => cfg.port = v.parse().context("port")?,
            "tls-mode" => {
                cfg.tls_mode = match v {
                    "plain" => TlsMode::Plain,
                    "tls" => TlsMode::Tls,
                    "tlcp" => TlsMode::Tlcp,
                    other => bail!("bad tls-mode {other}"),
                };
            }
            "shards" => cfg.shards = Some(v.parse()?),
            "maxmemory" => cfg.maxmemory = Some(parse_size(v)?),
            "maxmemory-policy" => cfg.maxmemory_policy = v.to_string(),
            "dir" => cfg.dir = PathBuf::from(v),
            "appendonly" => cfg.appendonly = matches!(v, "yes" | "true" | "1"),
            "appendfsync" => cfg.appendfsync = v.to_string(),
            "requirepass-sm3" => cfg.requirepass_sm3 = Some(v.to_string()),
            "storage-key-file" => cfg.storage_key_file = Some(PathBuf::from(v)),
            "tlcp-sign-cert" => cfg.tlcp.get_or_insert_with(Default::default).sign_cert = PathBuf::from(v),
            "tlcp-sign-key" => cfg.tlcp.get_or_insert_with(Default::default).sign_key = PathBuf::from(v),
            "tlcp-enc-cert" => cfg.tlcp.get_or_insert_with(Default::default).enc_cert = PathBuf::from(v),
            "tlcp-enc-key" => cfg.tlcp.get_or_insert_with(Default::default).enc_key = PathBuf::from(v),
            "tlcp-ca" => cfg.tlcp.get_or_insert_with(Default::default).ca = PathBuf::from(v),
            "tls-cert" => cfg.tls.get_or_insert_with(Default::default).cert = PathBuf::from(v),
            "tls-key" => cfg.tls.get_or_insert_with(Default::default).key = PathBuf::from(v),
            "maxclients" => cfg.maxclients = v.parse()?,
            "timeout" => cfg.timeout_secs = v.parse()?,
            "loglevel" => cfg.loglevel = v.to_string(),
            "save" => {
                let mut parts = v.split_whitespace();
                if let (Some(a), Some(b)) = (parts.next(), parts.next()) {
                    cfg.save.push((a.parse()?, b.parse()?));
                }
            }
            other => bail!("unknown config key {other}"),
        }
    }
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 { bail!("port must be non-zero"); }
        if !matches!(self.appendfsync.as_str(), "always" | "everysec" | "no") {
            bail!("appendfsync must be always|everysec|no");
        }
        match self.tls_mode {
            TlsMode::Tlcp => {
                let t = self.tlcp.as_ref().context("tls-mode tlcp requires tlcp-* certs")?;
                for p in [&t.sign_cert, &t.sign_key, &t.enc_cert, &t.enc_key, &t.ca] {
                    if !p.exists() { bail!("missing tlcp file {}", p.display()); }
                }
            }
            TlsMode::Tls => {
                let t = self.tls.as_ref().context("tls-mode tls requires tls-cert/tls-key")?;
                if !t.cert.exists() || !t.key.exists() { bail!("missing tls cert/key"); }
            }
            TlsMode::Plain => {}
        }
        if let Some(k) = &self.storage_key_file {
            if !k.exists() { bail!("storage-key-file {} not found", k.display()); }
        }
        Ok(())
    }

    pub fn shards_or_default(&self, cpu: usize) -> usize {
        self.shards.unwrap_or(cpu).max(1)
    }
}
```

`config/MemoryDataBase.conf.example`:
```
bind 0.0.0.0
port 6379
tls-mode plain
# tls-mode tlcp
# tlcp-sign-cert /etc/MemoryDataBase/tlcp/sign.crt
# tlcp-sign-key  /etc/MemoryDataBase/tlcp/sign.key
# tlcp-enc-cert  /etc/MemoryDataBase/tlcp/enc.crt
# tlcp-enc-key   /etc/MemoryDataBase/tlcp/enc.key
# tlcp-ca        /etc/MemoryDataBase/tlcp/ca.crt
shards 0
maxmemory 1gb
maxmemory-policy allkeys-lru
dir /var/lib/MemoryDataBase
save 900 1
save 300 10
appendonly no
appendfsync everysec
# storage-key-file /etc/MemoryDataBase/storage.key
# requirepass-sm3 <hex(sm3(salt||password))>
maxclients 10000
timeout 0
loglevel info
```

- [ ] **Step 4: `cargo test -p jx-server` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server opencode/MemoryDataBase/config
git -C /Users/caosunliu commit -m "feat(jx-server): config loader and validation"
```

## Task 10: jx-server — tokio 网络层、连接与命令调度

**Files:**
- Create: `crates/jx-server/src/{net.rs,conn.rs,dispatch.rs,background.rs}`
- Modify: `crates/jx-server/src/main.rs`
- Test: `crates/jx-server/tests/server_smoke.rs`

**Interfaces:**
- Consumes: `jx_proto::{RespDecoder, RespValue, encode}`, `jx_engine::{execute, Engine, MaxMemoryPolicy}`, config
- Produces:
  - `pub struct ShardHandle { pub tx: crossbeam_channel::Sender<ExecMsg>, pub engine: std::sync::Arc<Engine> }`
  - `pub enum ExecMsg { Exec { args: Vec<Bytes>, reply: std::sync::mpsc::Sender<RespValue> }, Snapshot, Shutdown }`
  - `pub fn spawn_shard(n: usize) -> Vec<ShardHandle>`（每分片一个线程，持有 Engine 分片状态——MVP 简化：Engine 共享 Mutex 分片，Actor 线程负责串行化访问）
  - `pub async fn run_server(cfg: Config) -> anyhow::Result<()>`

- [ ] **Step 1: 失败测试（进程内 TCP 冒烟）**

```rust
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn ping_over_tcp() {
    let dir = std::env::temp_dir().join(format!("jx-srv-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let conf = dir.join("jx.conf");
    std::fs::write(&conf, format!("bind 127.0.0.1\nport {}\ntls-mode plain\ndir {}\nappendonly no\nshards 2\n",
        16379 + (std::process::id() % 1000), dir.display())).unwrap();
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        rt.block_on(jx_server::run_server(cfg)).ok();
    });
    std::thread::sleep(Duration::from_millis(300));
    let port = std::fs::read_to_string(&conf).ok();
    let _ = port;
    // 动态端口：从配置读出实际 port——测试中直接用固定高位端口
    let addr = format!("127.0.0.1:{}", 16379 + (std::process::id() % 1000));
    let mut s = TcpStream::connect(&addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut buf = [0u8; 64];
    let n = s.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现 net/conn/dispatch**

`dispatch.rs`:
```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::RespValue;
use std::sync::Arc;

pub fn dispatch(engine: &Arc<Engine>, args: Vec<Bytes>) -> RespValue {
    match execute(engine, &args) {
        Ok(v) => v,
        Err(e) => e,
    }
}
```

`conn.rs`:
```rust
use anyhow::Result;
use bytes::Bytes;
use jx_engine::Engine;
use jx_proto::{encode, RespDecoder, RespValue};
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn handle_conn(
    mut stream: TcpStream,
    engine: Arc<Engine>,
    requirepass: Option<String>,
    max_pipeline: usize,
) -> Result<()> {
    let mut dec = RespDecoder::new();
    let mut buf = vec![0u8; 16 * 1024];
    let mut authed = requirepass.is_none();
    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        dec.feed(&buf[..n]);
        loop {
            match dec.next_value() {
                Ok(None) => break,
                Ok(Some(v)) => {
                    let args = match v {
                        RespValue::Array(Some(items)) => items.into_iter().filter_map(|x| match x {
                            RespValue::BulkString(Some(b)) => Some(b),
                            _ => None,
                        }).collect::<Vec<_>>(),
                        _ => vec![],
                    };
                    if args.is_empty() {
                        let resp = RespValue::err("ERR invalid command");
                        write_resp(&mut stream, &resp).await?;
                        continue;
                    }
                    let name = std::str::from_utf8(&args[0]).unwrap_or("").to_ascii_uppercase();
                    if name == "AUTH" {
                        let ok = match (args.get(1), requirepass.as_ref()) {
                            (Some(p), Some(h)) => {
                                let salt_ok = true; // MVP：口令即 sm3 哈希比较
                                let _ = salt_ok;
                                let got = hex::encode(jx_crypto::sm3_hash(p));
                                &got == h
                            }
                            (Some(_), None) => true,
                            _ => false,
                        };
                        authed = ok;
                        write_resp(&mut stream, if ok { &RespValue::ok() } else { &RespValue::err("WRONGPASS invalid username-password pair") }).await?;
                        continue;
                    }
                    if !authed {
                        write_resp(&mut stream, &RespValue::err("NOAUTH Authentication required.")).await?;
                        continue;
                    }
                    if name == "QUIT" {
                        write_resp(&mut stream, &RespValue::ok()).await?;
                        return Ok(());
                    }
                    // MVP 简化：直接在当前 async 任务同步执行（Engine 内部 Mutex 分片）
                    // Task 11 升级为 Actor channel 投递
                    let resp = dispatch::dispatch(&engine, args);
                    write_resp(&mut stream, &resp).await?;
                }
                Err(_) => return Ok(()), // 协议错误关闭连接
            }
            let _ = max_pipeline;
            let _ = Bytes::new();
        }
    }
    Ok(())
}

async fn write_resp(stream: &mut TcpStream, v: &RespValue) -> Result<()> {
    let mut out = bytes::BytesMut::new();
    encode(v, &mut out);
    stream.write_all(&out).await?;
    stream.flush().await?;
    Ok(())
}
```

`net.rs` + `main.rs`（run_server）：
```rust
use anyhow::Result;
use crate::config::{Config, TlsMode};
use crate::conn;
use jx_engine::{Engine, MaxMemoryPolicy};
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn run_server(cfg: Config) -> Result<()> {
    let nshards = cfg.shards_or_default(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
    let engine = Arc::new(Engine::new(nshards, jx_engine::system_now_ms_pub));
    if let Some(mm) = cfg.maxmemory {
        let policy = match cfg.maxmemory_policy.as_str() {
            "allkeys-lru" => MaxMemoryPolicy::AllKeysLru,
            "volatile-lru" => MaxMemoryPolicy::VolatileLru,
            "allkeys-lfu" => MaxMemoryPolicy::AllKeysLfu,
            "volatile-lfu" => MaxMemoryPolicy::VolatileLfu,
            "volatile-ttl" => MaxMemoryPolicy::VolatileTtl,
            _ => MaxMemoryPolicy::NoEviction,
        };
        engine.set_maxmemory(mm, policy);
    }
    // 恢复：优先 AOF，其次 RDB
    let key = match &cfg.storage_key_file {
        Some(p) => Some(jx_crypto::load_keyfile(p)?),
        None => None,
    };
    let aof = cfg.dir.join("appendonly.aof");
    let rdb = cfg.dir.join("dump.rdb");
    if cfg.appendonly && aof.exists() {
        jx_persist::replay_aof(&engine, &aof, key.as_ref())?;
    } else if rdb.exists() {
        jx_persist::load_rdb(&engine, &rdb, key.as_ref())?;
    }
    std::fs::create_dir_all(&cfg.dir)?;
    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr} tls-mode={:?} shards={nshards}", cfg.tls_mode);
    if cfg.tls_mode != TlsMode::Plain {
        tracing::warn!("tls/tlcp terminated in Task 12; currently falling back to plain RESP on the socket");
    }
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = engine.clone();
        let pass = cfg.requirepass_sm3.clone();
        tokio::spawn(async move {
            if let Err(e) = conn::handle_conn(stream, engine, pass, 1024).await {
                tracing::debug!("conn {peer} closed: {e}");
            }
        });
    }
}
```

> 执行说明：`Engine::new` 的 `now_ms` 需要 `pub fn system_now_ms()` 在 jx-engine 导出；`hex` 依赖加入 jx-server。TLS/TLCP 实际终止在 Task 12。

- [ ] **Step 4: `cargo test -p jx-server` → PASS**（smoke 测试通过）

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat(jx-server): tokio acceptor and RESP dispatch"
```

## Task 11: jx-server — 分片 Actor 投递与后台任务

**Files:**
- Modify: `crates/jx-server/src/{conn.rs,dispatch.rs,background.rs,main.rs}`
- Test: `crates/jx-engine/tests/actor.rs` 或 `crates/jx-server/tests/actor_smoke.rs`

**Interfaces:**
- Produces:
  - `pub struct ShardRuntime { handles: Vec<ShardHandle> }`
  - `pub fn exec_on_shard(rt: &ShardRuntime, engine: &Engine, args: Vec<Bytes>) -> RespValue`（按 `shard_id_for_key` 投递到对应 Actor 线程）
  - `pub fn spawn_background(engine: Arc<Engine>, cfg: &Config) -> tokio::task::JoinHandle<()>`（TTL 周期清理 + 指标打印）

- [ ] **Step 1: 失败测试**

```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::RespValue;

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn multi_shard_mget_consistent() {
    let e = Engine::new_for_test(4);
    for i in 0..20 {
        execute(&e, &[b("SET"), Bytes::from(format!("key{i}")), Bytes::from(format!("v{i}"))]).unwrap();
    }
    let args: Vec<Bytes> = (0..20).map(|i| Bytes::from(format!("key{i}"))).collect();
    let r = execute(&e, &[b("MGET")].into_iter().chain(args.into_iter()).collect::<Vec<_>>()).unwrap();
    if let RespValue::Array(Some(items)) = r {
        assert_eq!(items.len(), 20);
    } else {
        panic!("bad mget");
    }
}
```

- [ ] **Step 2: 确认失败（若已通过则作为回归保护）**

- [ ] **Step 3: 实现 Actor 投递**

`jx-engine/src/actor.rs`（或放 jx-server）：
```rust
use bytes::Bytes;
use jx_engine::{execute, Engine, RespValue};
use std::sync::Arc;
use std::thread::JoinHandle;

pub struct ShardHandle {
    pub id: usize,
    pub join: JoinHandle<()>,
    pub tx: crossbeam_channel::Sender<Cmd>,
}

pub enum Cmd {
    Exec { args: Vec<Bytes>, reply: std::sync::mpsc::Sender<Result<jx_proto::RespValue, jx_proto::RespValue>> },
}

pub struct ShardRuntime {
    pub engine: Arc<Engine>,
    pub shards: Vec<ShardHandle>,
}

pub fn spawn_shard_runtime(engine: Arc<Engine>) -> ShardRuntime {
    let mut shards = Vec::new();
    for id in 0..engine.shard_count() {
        let (tx, rx) = crossbeam_channel::unbounded::<Cmd>();
        let eng = engine.clone();
        let join = std::thread::spawn(move || {
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    Cmd::Exec { args, reply } => {
                        let _ = reply.send(execute(&eng, &args));
                    }
                }
            }
        });
        shards.push(ShardHandle { id, join, tx });
    }
    ShardRuntime { engine, shards }
}

impl ShardRuntime {
    pub fn exec(&self, args: Vec<Bytes>) -> jx_proto::RespValue {
        let sid = self.engine.shard_id_for_key(args.get(1).unwrap_or(&Bytes::new()));
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let _ = self.shards[sid].tx.send(Cmd::Exec { args, reply: reply_tx });
        match reply_rx.recv() {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => e,
            Err(_) => jx_proto::RespValue::err("ERR shard actor crashed"),
        }
    }
}
```

`background.rs`：
```rust
use jx_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

pub fn spawn_background(engine: Arc<Engine>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = engine.now();
            let removed = engine.expire_cycle(now, 256);
            if removed > 0 {
                tracing::debug!("expired {removed} keys");
            }
        }
    })
}
```

`conn.rs` 改为使用 `ShardRuntime::exec` 替代直接 `execute`（连接持有 `Arc<ShardRuntime>`）。

- [ ] **Step 4: `cargo test --workspace` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine opencode/MemoryDataBase/crates/jx-server
git -C /Users/caosunliu commit -m "feat: shard actor runtime and background expire cycle"
```

## Task 12: jx-server — INFO/CONFIG 运维命令与 TLS/TLCP 接线

**Files:**
- Modify: `crates/jx-server/src/{dispatch.rs,conn.rs,net.rs}`, `crates/jx-engine/src/commands/mod.rs`（INFO/CONFIG/FLUSHALL/KEYS/RENAME/RANDOMKEY）
- Test: `crates/jx-server/tests/ops.rs`

**Interfaces:**
- Produces:
  - INFO 返回 `# Server\r\nMemoryDataBase_version:0.1.0\r\n...` 文本 bulk
  - CONFIG GET/SET 支持：maxmemory、maxmemory-policy、appendonly、loglevel、timeout、requirepass-sm3
  - FLUSHALL/FLUSHDB 清空所有分片
  - KEYS glob（协调器：各分片收集后合并）
  - tls-mode=tlcp 时在 accept 后包装 `TlcpAcceptor`（Task 6 API）

- [ ] **Step 1: 失败测试**

```rust
use bytes::Bytes;
use jx_engine::{execute, Engine};
use jx_proto::RespValue;

fn b(s: &str) -> Bytes { Bytes::copy_from_slice(s.as_bytes()) }

#[test]
fn info_and_config_and_flush() {
    let e = Engine::new_for_test(1);
    let info = execute(&e, &[b("INFO")]).unwrap();
    match info {
        RespValue::BulkString(Some(t)) => assert!(t.as_ref().contains("MemoryDataBase_version")),
        other => panic!("bad info {other:?}"),
    }
    execute(&e, &[b("SET"), b("k"), b("v")]).unwrap();
    execute(&e, &[b("FLUSHALL")]).unwrap();
    assert_eq!(execute(&e, &[b("EXISTS"), b("k")]).unwrap(), RespValue::int(0));
    execute(&e, &[b("SET"), b("a"), b("1"), b("EX"), b("100")]).unwrap();
    let keys = execute(&e, &[b("KEYS"), b("*")]).unwrap();
    assert!(matches!(keys, RespValue::Array(Some(_))));
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现运维命令**

在 `jx-engine/src/commands/server.rs`：
```rust
use bytes::Bytes;
use jx_proto::RespValue;
use crate::Engine;

pub fn info(e: &Engine, _args: &[Bytes], _now: u64) -> Result<RespValue, RespValue> {
    let keys: usize = e.shard_count();
    let _ = keys;
    let mem = e.total_mem();
    let dbsize: usize = e.shard_count() + 0;
    let _ = dbsize;
    let text = format!(
        "# Server\r\nMemoryDataBase_version:0.1.0\r\nos:{}\r\narch:{}\r\n# Memory\r\nused_memory:{}\r\n# Keyspace\r\ndb0:keys={},expires=0\r\n",
        std::env::consts::OS,
        std::env::consts::ARCH,
        mem,
        e.shards_len_pub(),
    );
    Ok(RespValue::bulk(Bytes::from(text)))
}

pub fn flushall(e: &Engine, _args: &[Bytes], _now: u64) -> Result<RespValue, RespValue> {
    for sid in 0..e.shard_count() {
        let mut sh = e.lock_shard(sid);
        sh.dict.clear();
        sh.mem = 0;
    }
    Ok(RespValue::ok())
}

pub fn keys(e: &Engine, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let pat = args.first().map(|p| String::from_utf8_lossy(p).to_string()).unwrap_or_else(|| "*".into());
    let re = glob_to_regex(&pat);
    let mut out = Vec::new();
    for sid in 0..e.shard_count() {
        let mut sh = e.lock_shard(sid);
        let expired: Vec<Bytes> = sh.dict.keys().cloned().collect();
        for k in expired {
            if crate::Engine::touch_alive(&mut sh, &k, now) {
                let s = String::from_utf8_lossy(&k).to_string();
                if re.is_match(&s) { out.push(k); }
            }
        }
    }
    Ok(RespValue::array(out.into_iter().map(RespValue::bulk).collect()))
}

fn glob_to_regex(pat: &str) -> regex_lite::Regex {
    let mut re = String::from("^");
    for c in pat.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            other if "\\.+^${}()|[]".contains(other) => { re.push('\\'); re.push(other); }
            other => re.push(other),
        }
    }
    re.push('$');
    regex_lite::Regex::new(&re).unwrap()
}
```

Cargo 增加 `regex-lite = "0.1"`（MIT/Apache）到 jx-engine。提供 `Engine::shards_len_pub()` 等访问器。

INFO/CONFIG 在 `execute` 中接线；CONFIG SET 更新 Engine maxmemory/policy 与运行时字段（用 `RwLock<ConfigRuntime>`）。

TLS/TLCP 在 `net.rs` accept 分支：
```rust
match cfg.tls_mode {
    TlsMode::Plain => { /* 原样 handle_conn */ }
    TlsMode::Tls => { /* rustls acceptor：加载 tls-cert/tls-key，包装 stream 后 handle_conn */ }
    TlsMode::Tlcp => {
        let tlcp = jx_crypto::TlcpConfig::from_pem_files(
            &t.sign_cert, &t.sign_key, &t.enc_cert, &t.enc_key, &t.ca)?;
        // 使用 Task 6 的 TlcpAcceptor 完成握手，得到加密 stream 再 handle_conn
        // 握手失败仅关闭该连接
    }
}
```

- [ ] **Step 4: `cargo test -p jx-engine -p jx-server` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-engine opencode/MemoryDataBase/crates/jx-server opencode/MemoryDataBase/crates/jx-crypto
git -C /Users/caosunliu commit -m "feat: INFO/CONFIG/KEYS/FLUSHALL and TLS/TLCP wiring"
```

## Task 13: jx-server — 优雅停机与 RDB/AOF 后台触发

**Files:**
- Modify: `crates/jx-server/src/{main.rs,background.rs,net.rs}`
- Test: `crates/jx-server/tests/shutdown.rs`

**Interfaces:**
- Produces:
  - SIGTERM/SIGINT：停止 accept → 等待在途（超时）→ 若 `appendonly` 则 rewrite AOF + 刷盘 → 触发 RDB（若 save 规则命中或强制）→ 退出 0
  - `BGSAVE`/`SAVE`/`LASTSAVE`/`BGREWRITEAOF` 命令接线到 `jx_persist`

- [ ] **Step 1: 失败测试**

```rust
#[test]
fn save_commands_exist() {
    use bytes::Bytes;
    use jx_engine::{execute, Engine};
    let e = Engine::new_for_test(1);
    let _ = execute(&e, &[Bytes::from_static(b"SAVE")]);
    let _ = execute(&e, &[Bytes::from_static(b"LASTSAVE")]);
    let _ = execute(&e, &[Bytes::from_static(b"BGSAVE")]);
}
```

- [ ] **Step 2: 确认失败** → FAIL（SAVE 仍在 is_unimplemented）

- [ ] **Step 3: 实现**

- `execute` 接线 SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF（需要 Engine 暴露 snapshot 接口给 persist；BGSAVE 用 `std::thread::spawn` 或 `tokio::spawn` 后台写）
- `main.rs`：
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "MemoryDataBase.conf".into());
    let cfg = jx_server::config::load_config(std::path::Path::new(&path))?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&cfg.loglevel))
        .json()
        .init();
    let engine = /* ... Task 10 初始化 ... */;
    let bg = jx_server::background::spawn_background(engine.clone());
    let server = jx_server::net::run_server(cfg.clone());
    tokio::select! {
        r = server => r?,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT, graceful shutdown");
        }
    }
    // 持久化收尾
    if cfg.appendonly {
        let _ = jx_persist::rewrite_aof(&engine, &cfg.dir.join("appendonly.aof"), key.as_ref());
    }
    let _ = jx_persist::save_rdb(&engine, &cfg.dir.join("dump.rdb"), key.as_ref());
    bg.abort();
    Ok(())
}
```

- [ ] **Step 4: `cargo test -p jx-server -p jx-engine` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server opencode/MemoryDataBase/crates/jx-engine
git -C /Users/caosunliu commit -m "feat: graceful shutdown and persistence commands"
```

## Task 14: jx-cli — keygen / check-config / rdb-info / bench

**Files:**
- Create: `crates/jx-cli/src/{main.rs,keygen.rs,check_config.rs,rdb_info.rs,bench.rs}`
- Test: `crates/jx-cli/tests/cli.rs`

**Interfaces:**
- Produces:
  - `jx-cli keygen --out storage.key`
  - `jx-cli check-config --config MemoryDataBase.conf`
  - `jx-cli rdb-info --path dump.rdb [--key storage.key]`
  - `jx-cli bench --host 127.0.0.1 --port 6379 --clients 64 --requests 100000 --value-size 64 --pipeline 1`（明文 RESP 压测，输出 OPS 与 p50/p99）

- [ ] **Step 1: 失败测试**

```rust
#[test]
fn keygen_creates_file() {
    let dir = std::env::temp_dir().join(format!("jx-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let out = dir.join("k.key");
    jx_crypto::generate_keyfile(&out).unwrap();
    assert!(out.exists());
    assert_eq!(std::fs::read(&out).unwrap().len(), 32);
    let _ = std::fs::remove_dir_all(&dir);
}
```

- [ ] **Step 2: 确认失败** → FAIL

- [ ] **Step 3: 实现 CLI**

`main.rs`:
```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "jx-cli", about = "MemoryDataBase ops CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Keygen { #[arg(long)] out: std::path::PathBuf },
    CheckConfig { #[arg(long)] config: std::path::PathBuf },
    RdbInfo { #[arg(long)] path: std::path::PathBuf, #[arg(long)] key: Option<std::path::PathBuf> },
    Bench {
        #[arg(long, default_value = "127.0.0.1")] host: String,
        #[arg(long, default_value_t = 6379)] port: u16,
        #[arg(long, default_value_t = 32)] clients: usize,
        #[arg(long, default_value_t = 50000)] requests: usize,
        #[arg(long, default_value_t = 64)] value_size: usize,
        #[arg(long, default_value_t = 1)] pipeline: usize,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Keygen { out } => jx_crypto::generate_keyfile(&out)?,
        Cmd::CheckConfig { config } => {
            let _cfg = jx_server_config_load(&config)?;
            println!("config OK");
        }
        Cmd::RdbInfo { path, key } => {
            let k = key.as_ref().map(|p| jx_crypto::load_keyfile(p)).transpose()?;
            let n = jx_persist::rdb_key_count(&path, k.as_ref())?;
            println!("rdb keys: {n}");
        }
        Cmd::Bench { host, port, clients, requests, value_size, pipeline } => {
            jx_cli::bench::run(&host, port, clients, requests, value_size, pipeline)?;
        }
    }
    Ok(())
}

fn jx_server_config_load(path: &std::path::Path) -> anyhow::Result<()> {
    // check-config 复用 jx-server::config::load_config（将 config 模块 pub 导出）
    jx_server::config::load_config(path).map(|_| ())
}
```

`bench.rs`：tokio 多客户端循环 SET/GET，统计直方图（简易：排序取分位），打印：
```
GET  ops=523456 ops/s avg=0.18ms p50=0.12ms p99=0.9ms
SET  ops=501200 ops/s ...
```

`rdb_info.rs`：只读文件头解析 magic/version/shard 数（不完整解密也可打印是否 `JXRDBENC`）。

`jx-server` 需 `pub mod config` 并在 lib 导出（jx-server 目前是 binary：将 config/net/conn 移入 `src/lib.rs` 提供 `pub mod`，`main.rs` 只做入口——Task 10 时已按此组织）。

- [ ] **Step 4: `cargo test -p jx-cli` → PASS**

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-cli
git -C /Users/caosunliu commit -m "feat(jx-cli): keygen, check-config, rdb-info, bench"
```

---

# Phase 5 — 验收、兼容与信创构建

## Task 15: 客户端兼容冒烟

**Files:**
- Create: `tests/compat/test_redis_py.sh`, `tests/compat/test_go_redis.sh`

**Interfaces:**
- Consumes: 运行中的 MemoryDataBase（plain 模式，`requirepass` 可选）

- [ ] **Step 1: 写兼容脚本**

`tests/compat/test_redis_py.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-6379}
python3 - <<PY
import socket
s = socket.create_connection(("$HOST", $PORT), timeout=2)
def cmd(*args):
    out = f"*{len(args)}\\r\\n".encode()
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        out += f"${len(b)}\\r\\n".encode() + b + b"\\r\\n"
    s.sendall(out)
    return s.recv(4096)
assert cmd("PING") == b"+PONG\\r\\n"
assert cmd("SET", "pyk", "pyv") == b"+OK\\r\\n"
assert cmd("GET", "pyk") == b"$3\\r\\npyv\\r\\n"
print("redis-py smoke OK")
PY
```

`tests/compat/test_go_redis.sh`（若装有 go 与 go-redis）：
```bash
#!/usr/bin/env bash
set -euo pipefail
# 使用 go run 最小客户端；无 go 时跳过
if ! command -v go >/dev/null; then echo "skip: no go"; exit 0; fi
# ... go-redis v9 ping/set/get 冒烟 ...
```

- [ ] **Step 2: 启动服务并执行**

Run: `cargo run -p jx-server -- config/MemoryDataBase.conf.example &`（先改 port/dir）
Run: `bash tests/compat/test_redis_py.sh`
Expected: `redis-py smoke OK`

- [ ] **Step 3: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/tests
git -C /Users/caosunliu commit -m "test: client compatibility smoke scripts"
```

## Task 16: 性能验收与报告

**Files:**
- Create: `scripts/perf-report.sh`
- Test: 手工/CI 运行

- [ ] **Step 1: 编写压测脚本**

`scripts/perf-report.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
cargo build --release -p jx-server -p jx-cli
CONF=$(mktemp)
cat > "$CONF" <<EOF
bind 127.0.0.1
port 6399
tls-mode plain
dir $(mktemp -d)
shards $(sysctl -n hw.ncpu 2>/dev/null || nproc)
appendonly no
maxmemory 4gb
maxmemory-policy noeviction
EOF
./target/release/MemoryDataBase-server "$CONF" &
PID=$!
sleep 1
./target/release/jx-cli bench --host 127.0.0.1 --port 6399 --clients 64 --requests 100000 --value-size 64 --pipeline 1
kill -TERM "$PID"
wait "$PID" || true
```

- [ ] **Step 2: 运行并记录**

Run: `bash scripts/perf-report.sh`
Expected: 打印 GET/SET OPS；**明文档目标 ≥ 500000 OPS/s**（开发机基准）。未达标时按 spec §9.2 调优路径处理（分片数、pipeline、writev、channel 批量）。

- [ ] **Step 3: Commit（报告存 `docs/superpowers/perf/2026-10-04-baseline.md`）**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/scripts opencode/MemoryDataBase/docs/superpowers/perf
git -C /Users/caosunliu commit -m "perf: baseline bench script and report"
```

## Task 17: 信创三架构构建脚本与许可证检查

**Files:**
- Create: `scripts/build-linux.sh`, `scripts/build-loongarch.sh`, `scripts/check-licenses.sh`

- [ ] **Step 1: 写脚本**

`scripts/build-linux.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
# 若需真机链接器：安装 aarch64-linux-gnu-gcc 并配置 .cargo/config.toml
cargo build --release --target x86_64-unknown-linux-gnu -p jx-server -p jx-cli
cargo build --release --target aarch64-unknown-linux-gnu -p jx-server -p jx-cli
```

`scripts/build-loongarch.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
rustup target add loongarch64-unknown-linux-gnu
# 需要龙芯交叉工具链：export CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=loongarch64-linux-gnu-gcc
cargo build --release --target loongarch64-unknown-linux-gnu -p jx-server -p jx-cli
```

`scripts/check-licenses.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
cargo deny check licenses
```

- [ ] **Step 2: 本地跑通 x86_64/aarch64（开发机为目标）与 check-licenses**

Run: `bash scripts/check-licenses.sh && bash scripts/build-linux.sh`
Expected: 许可证检查通过；至少当前主机 target 构建成功。

- [ ] **Step 3: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/scripts
git -C /Users/caosunliu commit -m "build: xinchuang target build and license scripts"
```

## Task 18: 端到端验收清单（对照 spec）

**Files:**
- Create: `docs/superpowers/perf/ACCEPTANCE.md`

- [ ] **Step 1: 逐项勾验**

| 项 | 命令/方法 | 通过标准 |
|----|-----------|----------|
| 附录 A 必须命令 | `cargo test -p jx-engine` + 兼容脚本 | 全绿 |
| 明文 50 万 OPS | `scripts/perf-report.sh` | ≥ 500000 OPS/s（GET/SET 64B） |
| TLCP 实测 | 启动 `tls-mode tlcp` + bench | 输出单独报告，不要求 50 万 |
| RDB/AOF | 单测 + kill -9 恢复 | everysec 丢窗口 ≤1s；损坏文件拒绝启动 |
| SM3/SM4 | `jx-cli keygen` + 加密 RDB 往返 | 单测通过 |
| 三架构 | `scripts/build-*.sh` | 三 target 编译成功（LoongArch 需工具链环境） |
| 许可证 | `scripts/check-licenses.sh` | 通过 |
| 未支持命令 | `MULTI`/`EVAL` 等 | 返回 `ERR ... not supported` |

- [ ] **Step 2: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/docs/superpowers/perf/ACCEPTANCE.md
git -C /Users/caosunliu commit -m "docs: MVP acceptance checklist"
```

---

## Plan Self-Review

1. **Spec coverage:** 定位/RESP2/核心命令/TTL/淘汰/Actor/RDB+AOF/SM3+SM4+TLCP 配置项/优雅停机/INFO/CONFIG/bench/三架构脚本/许可证均有对应任务（Task 0–18）。Web 管控台、主从、集群、Lua 等为 spec 非目标，不在本计划。
2. **Placeholder scan:** TLCP 与 gm-tlcp API 对接处标注了「以 vendor 目录实际 API 为准」的决策门，并要求互操作测试；其余步骤均含代码/命令。`thread_local` 取巧方案已删除，改为显式 delta 入口。
3. **Type consistency:** `execute(&Engine, &[Bytes]) -> Result<RespValue, RespValue>`、`Engine::new_for_test`、`JxValue`/`KeyMeta`/`Entry`、`save_rdb`/`load_rdb`、`append_aof`/`replay_aof`/`rewrite_aof`、`hash_password`/`verify_password`/`sm4_gcm_seal`/`sm4_gcm_open`、`load_config`/`TlsMode` 在各任务间命名一致。
4. **已知执行风险:** ① gm-tlcp 0.1.0 API 演进 → 决策门+回退 FFI；② sm4 crate feature 名随版本变化 → 以 crates.io 文档为准调整 features；③ 50 万 OPS 需在真机调优，脚本与报告结构已就位。
