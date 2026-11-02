# MemoryDataBase 信创交叉编译 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 ring 交叉阻断，交付三架构 Linux 可复现构建脚本 + Docker 环境 + 工具链文档。

**Architecture:** rustls 后端从 ring 切换为 aws-lc-rs（TLCP 仍为纯 Rust gm-tlcp）；`scripts/cross-build.sh` 支持 `--target/--check-only/--docker`；Docker 镜像提供交叉 gcc；验收为 `cargo check --target` + 有工具链时 `file` 验证。

**Tech Stack:** Rust、rustls/aws-lc-rs、bash、Docker（构建用）。

**Spec:** `opencode/MemoryDataBase/docs/superpowers/specs/2026-10-05-MemoryDataBase-xinchuang-cross-build-design.md`

## Global Constraints

- 交付物：可复现构建脚本，**不含** rpm/deb、真机安装验收。
- TLS：`tls-mode=tls` 与 `tlcp` 均保留；首选 aws-lc-rs，失败则文档化 ring+交叉 gcc 回退。
- 目标：`x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu`（P0）、`loongarch64-unknown-linux-gnu`（P1）。
- 无工具链时：`cargo check --target` + 指引，不伪造二进制。
- 许可证：`cargo deny check licenses` 通过（白名单含 aws-lc-rs 所需许可）。
- 回归：`cargo test --workspace` 全绿。
- 路径相对 `/Users/caosunliu/opencode/MemoryDataBase/`；git 根 `/Users/caosunliu`。
- cargo：`export PATH="$(dirname "$(rustup which cargo)"):$PATH"`。
- 每任务 TDD 或脚本可执行验证后 commit。

## File Structure

```
crates/jx-server/Cargo.toml     # rustls features
scripts/cross-build.sh          # 三目标构建入口
scripts/build-linux.sh          # 被 cross-build 包装或标注 superseded
docker/cross/Dockerfile
.cargo/config.toml.example
docs/superpowers/xinchuang/cross-build.md
```

---

## Task 1: rustls 后端切换 aws-lc-rs

**Files:**
- Modify: `crates/jx-server/Cargo.toml`
- Test: `cargo test -p jx-server`（现有 TLS 测试）

**Interfaces:**
- Produces: jx-server 在 aws-lc-rs 后端下编译；`cargo check --target aarch64-unknown-linux-gnu -p jx-server`（安装 target 后）可执行到链接前或成功。

- [ ] **Step 1: 确认现状**

Run: `cargo tree -p jx-server -i ring 2>/dev/null | head -5`
Expected: ring 经 rustls 引入

- [ ] **Step 2: 修改 Cargo.toml**

`crates/jx-server/Cargo.toml` 将：
```toml
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "logging"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "logging"] }
```
改为：
```toml
rustls = { version = "0.23", default-features = false, features = ["aws-lc-rs", "std", "logging"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["aws-lc-rs", "logging"] }
```
（若 feature 名不对，查 docs.rs：可能是 `aws_lc_rs` 或 rustls 0.23 的正确 feature；以编译通过为准并记录。）

- [ ] **Step 3: 本机测试**

Run: `cargo test -p jx-server` 与 `cargo test --workspace`
Expected: 全绿（TLS 行为不变）

- [ ] **Step 4: 交叉 check（P0 目标）**

Run:
```bash
rustup target add aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu
cargo check --target aarch64-unknown-linux-gnu -p jx-server 2>&1 | tail -30
cargo check --target x86_64-unknown-linux-gnu -p jx-server 2>&1 | tail -30
```
Expected: 编译通过或明确记录 cc-rs/链接错误作为后续脚本输入。若 aws-lc-rs 无法 check，记录错误并改为文档化回退（Task 2/4 处理），本任务仍须保证**宿主 target** 测试全绿。

- [ ] **Step 5: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server Cargo.lock 2>/dev/null || true
git -C /Users/caosunliu add opencode/MemoryDataBase/crates/jx-server opencode/MemoryDataBase/Cargo.lock
git -C /Users/caosunliu commit -m "build(jx-server): switch rustls to aws-lc-rs backend"
```

---

## Task 2: cross-build.sh 脚本

**Files:**
- Create: `scripts/cross-build.sh`
- Modify: `scripts/build-linux.sh`（头部注释：superseded by cross-build.sh，或 exec 转发）

**Interfaces:**
- Produces:
  - `./scripts/cross-build.sh [--target <triple>|all] [--check-only] [--docker]`
  - 退出码：全部成功 0；check 失败 1；缺链接器时 `--check-only` 路径仍尝试 cargo check，链接失败且未 check-only 则警告+非零（可配 `ALLOW_LINK_FAIL=1` 时 exit 0 并打印指引）

- [ ] **Step 1: 写脚本**

```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

TARGETS_ALL="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu loongarch64-unknown-linux-gnu"
TARGET="${TARGET:-all}"
CHECK_ONLY="${CHECK_ONLY:-0}"
DOCKER="${DOCKER:-0}"

# parse args: --target --check-only --docker
# resolve TARGETS from TARGET
# for each triple: rustup target add; if DOCKER=1 && docker available: run docker/cross build
# else cargo check or cargo build --release --target
# print summary table; exit per rules
```

（完整实现由执行者按 spec 填充：summary 打印 triple、mode(check/build)、result。）

- [ ] **Step 2: 本机运行 check-only**

Run: `bash scripts/cross-build.sh --target aarch64-unknown-linux-gnu --check-only`
Expected: 打印 check 结果；成功或记录工具链缺失指引

- [ ] **Step 3: 全目标 check-only**

Run: `bash scripts/cross-build.sh --target all --check-only`
Expected: 三目标均有明确结果输出

- [ ] **Step 4: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/scripts
git -C /Users/caosunliu commit -m "build: cross-build.sh for xinchuang targets"
```

---

## Task 3: Docker 交叉镜像与 cargo 配置模板

**Files:**
- Create: `docker/cross/Dockerfile`, `.cargo/config.toml.example`

**Interfaces:**
- Produces: Dockerfile 可 `docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .`；config.toml.example 含三 target linker 与 CC_* 注释。

- [ ] **Step 1: 写 Dockerfile**

```dockerfile
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gcc g++ \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*
# LoongArch: 由构建参数 LOONGARCH_TOOLCHAIN 注入或在文档中说明
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
```

- [ ] **Step 2: config.toml.example**

```
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
# CC_aarch64_unknown_linux_gnu = "aarch64-linux-gnu-gcc"
[target.x86_64-unknown-linux-gnu]
linker = "x86_64-linux-gnu-gcc"
[target.loongarch64-unknown-linux-gnu]
linker = "loongarch64-linux-gnu-gcc"
```

- [ ] **Step 3: 可选 Docker 验证**

Run: `docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .`（若本机有 docker；无则记录 skip）

- [ ] **Step 4: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/docker opencode/MemoryDataBase/.cargo
git -C /Users/caosunliu commit -m "build: docker cross image and cargo config template"
```

---

## Task 4: 文档、许可证与回归

**Files:**
- Create: `docs/superpowers/xinchuang/cross-build.md`
- Modify: `scripts/build-linux.sh`（如未在 Task 2 处理）

**Interfaces:**
- Produces: 完整交叉构建文档；licenses + workspace 绿。

- [ ] **Step 1: 写文档**

`docs/superpowers/xinchuang/cross-build.md`：
- 目标矩阵与优先级
- 本机/Docker 两种路径
- 工具链安装（apt 包名、LoongArch 说明、CC_*/linker）
- aws-lc-rs 交叉注意与 ring 回退
- 验收清单（check-only vs file 验证）
- 常见错误（cc-rs、glibc、缺 linker）

- [ ] **Step 2: 验证**

Run:
```bash
bash scripts/check-licenses.sh
cargo test --workspace
bash scripts/cross-build.sh --target all --check-only
```
Expected: licenses ok；测试绿；三目标 check 有输出

- [ ] **Step 3: Commit**

```bash
git -C /Users/caosunliu add opencode/MemoryDataBase/docs opencode/MemoryDataBase/scripts
git -C /Users/caosunliu commit -m "docs: xinchuang cross-build guide and acceptance"
```

---

## Plan Self-Review

1. **Spec coverage:** TLS 后端（T1）、cross-build.sh（T2）、Docker+config（T3）、文档/许可/回归（T4）；非目标（安装包/真机）不在计划内。
2. **Placeholder scan:** T2 脚本给出框架与验收命令；T1 记录 aws-lc-rs feature 名以编译为准。
3. **Type consistency:** 脚本参数 `--target/--check-only/--docker` 与 spec 一致；目标三元组与 spec §3 一致。
