# MemoryDataBase 信创交叉编译指南

- **项目**：MemoryDataBase — `jx-server` / `jx-cli`
- **入口脚本**：`scripts/cross-build.sh`
- **设计**：`docs/superpowers/specs/2026-10-05-MemoryDataBase-xinchuang-cross-build-design.md`
- **状态**：已交付（脚本 + Docker + 文档）

---

## 1. 目标矩阵

| 目标 triple | 信创硬件 | 优先级 | 默认 linker | apt 包（Debian/Ubuntu） |
|-------------|----------|--------|-------------|-------------------------|
| `x86_64-unknown-linux-gnu` | 海光 / 兆芯 / 通用 | P0 | `x86_64-linux-gnu-gcc` | `gcc-x86-64-linux-gnu` `g++-x86-64-linux-gnu` |
| `aarch64-unknown-linux-gnu` | 鲲鹏 / 飞腾 | P0 | `aarch64-linux-gnu-gcc` | `gcc-aarch64-linux-gnu` `g++-aarch64-linux-gnu` |
| `loongarch64-unknown-linux-gnu` | 龙芯 | P1 | `loongarch64-linux-gnu-gcc` | 无 Debian bookworm 包；见 §3.3 |

产物目标路径：`dist/<triple>/{jx-server,jx-cli}`（release 构建时）。

---

## 2. 两种构建路径

### 2.1 本机（local）

```bash
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
cd /Users/caosunliu/opencode/MemoryDataBase

# 冒烟（无链接器也可跑）：
bash scripts/cross-build.sh --target all --check-only

# 完整 release 构建（需本机交叉 gcc）：
bash scripts/cross-build.sh --target all
bash scripts/cross-build.sh --target aarch64-unknown-linux-gnu
```

可选：复制 `.cargo/config.toml.example` → `.cargo/config.toml`，并 export `CC_*`。

### 2.2 Docker（推荐在无本机交叉工具链时使用）

```bash
# 1) 构建镜像（Debian bookworm-slim + 交叉 gcc + rustup）
docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .

# 2) 通过脚本使用镜像
bash scripts/cross-build.sh --target all --docker
bash scripts/cross-build.sh --target all --check-only --docker
```

**`--docker` 语义（Task 2 修正后）：**

- 镜像 `MemoryDataBase-cross` **缺失** → 仅此时回退本机 cargo。
- 镜像存在但镜像内 `cargo build/check` **失败** → 记为 `BUILD_FAIL` / `CHECK_FAIL`，**不回退本机**。
- 镜像内 cargo 成功 → summary 记为 `docker-build` / `docker-check`。

LoongArch：`docker/cross/Dockerfile` 默认不含龙芯工具链；需自备 tarball 并按 Dockerfile 中注释的 `LOONGARCH_TOOLCHAIN` ARG 注入，或改用本机路径。

### 2.3 常用开关

| 开关 | 作用 |
|------|------|
| `--target <triple>\|all` | 单目标或全部三目标（默认 `all`） |
| `--check-only` | 只 `cargo check --target`，不 release 链接 |
| `--docker` | 优先镜像内构建；语义见 §2.2 |
| `ALLOW_LINK_FAIL=1` | 缺 linker/cc-rs 时退出码 0，仅打印指引（适合 CI 冒烟） |

退出码：全成功 `0`；check/build 失败 `1`；缺工具链且未设 `ALLOW_LINK_FAIL=1` 时 `1`。

---

## 3. 工具链安装

### 3.1 Debian / Ubuntu apt 包名

**x86_64（两种拼写都要认）：**

- Debian/Ubuntu 官方包名（连字符）：`gcc-x86-64-linux-gnu` `g++-x86-64-linux-gnu`
- 部分文档/脚本/厂商镜像（下划线）：`gcc-x86_64-linux-gnu` `g++-x86_64-linux-gnu`

二者指向同一交叉工具链意图；以发行版 `apt-cache search` 实际可装名为准。Debian bookworm / Ubuntu 22.04+ 一般为连字符拼写。

**aarch64：**

```bash
sudo apt-get install gcc-aarch64-linux-gnu g++-aarch64-linux-gnu
```

**x86_64 示例：**

```bash
sudo apt-get install gcc-x86-64-linux-gnu g++-x86-64-linux-gnu
# 若仓库提供下划线拼写：
sudo apt-get install gcc-x86_64-linux-gnu g++-x86_64-linux-gnu
```

### 3.2 linker 与 cc-rs（aws-lc-rs）环境

```bash
# 本机 target linker（或写入 .cargo/config.toml）
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
export CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=loongarch64-linux-gnu-gcc

# aws-lc-rs / aws-lc-sys 的 C 编译器（cc-rs）
export CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc
export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
export CC_loongarch64_unknown_linux_gnu=loongarch64-linux-gnu-gcc
```

`.cargo/config.toml.example` 提供同款三 target 模板。

### 3.3 LoongArch（龙芯）

- Debian bookworm **无** `loongarch64-linux-gnu-gcc` 官方交叉包。
- 获取龙芯官方/发行版交叉工具链 tarball，安装后：

```bash
export PATH="/opt/loongarch64-linux-gnu/bin:$PATH"   # 按实际前缀调整
export CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=loongarch64-linux-gnu-gcc
export CC_loongarch64_unknown_linux_gnu=loongarch64-linux-gnu-gcc
```

- 无工具链时：`--target loongarch64-unknown-linux-gnu --check-only` 仍可做纯 Rust 冒烟；链接/`aws-lc-rs` C 编译需工具链。

### 3.4 macOS 开发机

```bash
brew install aarch64-unknown-linux-gnu-gcc   # 视可用 formula 而定
# 或使用 Docker 路径（§2.2），避免本机装交叉 gcc
```

---

## 4. TLS 后端：aws-lc-rs 与 ring 回退

### 4.1 当前（首选）：aws-lc-rs

`crates/jx-server/Cargo.toml`：

```toml
rustls = { version = "0.23", default-features = false, features = ["aws-lc-rs", "std", "logging"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["aws-lc-rs", "logging"] }
```

代码使用 `rustls::crypto::aws_lc_rs::default_provider()`。

交叉注意：

- `aws-lc-sys` 经 cc-rs 编译 C/ASM，**必须**提供目标 `CC_*` 与最终 linker。
- `--check-only` 在无工具链主机上可能在 cc-rs 阶段失败并被识别为 `TOOLCHAIN_MISS`。
- 许可证：aws-lc-rs 预期 Apache-2.0 / MIT；`scripts/check-licenses.sh`（cargo-deny）白名单校验。

### 4.2 回退：ring + 交叉 gcc

若某目标上 `aws-lc-rs` 无法编译（cc-rs/汇编器问题）：

1. 将 `rustls` / `tokio-rustls` features 改回 `ring`（对照 git 历史 `build(jx-server): switch rustls to aws-lc-rs backend`）。
2. `ring` 同样经 cc-rs 编译 C/ASM，**仍需**交叉 gcc + `CC_*` / linker，不是“无 C 依赖”路线。
3. `tls-mode=tls` 与 `tls-mode=tlcp`（gm-tlcp，纯 Rust）行为边界不变；仅构建依赖变化。
4. 回退后重跑：

```bash
bash scripts/check-licenses.sh
cargo test --workspace
bash scripts/cross-build.sh --target <triple> --check-only
```

**TLCP**：`gm-tlcp` 为纯 Rust，不引入额外 C 依赖，不受 rustls 后端切换影响。

---

## 5. 验收清单

| # | 检查项 | 命令 / 标准 |
|---|--------|-------------|
| 1 | 许可证 | `bash scripts/check-licenses.sh` → `license check: OK` |
| 2 | 工作区测试 | `cargo test --workspace` → 全绿 |
| 3 | 三目标冒烟 | `bash scripts/cross-build.sh --target all --check-only` → 三目标均有 summary 行 |
| 4 | 工具链缺失语义 | 无交叉 gcc 时 check 走 `TOOLCHAIN_MISS` + 指引；`ALLOW_LINK_FAIL=1` 可 exit 0 |
| 5 | `--docker` 语义 | 镜像缺失才回退本机；镜像内失败为 `BUILD_FAIL`/`CHECK_FAIL` |
| 6 | 有工具链时产物 | release 构建成功后 `file dist/<triple>/jx-server` ELF 架构匹配 triple |
| 7 | 文档完整 | 本文件覆盖矩阵 / 路径 / 工具链 / 回退 / 错误 / glibc / 旧脚本 |
| 8 | 旧脚本已标注 | `build-linux.sh`、`build-loongarch.sh` 头部标注 SUPERSEDED |

**无工具链 CI 冒烟推荐组合：**

```bash
bash scripts/check-licenses.sh
cargo test --workspace
ALLOW_LINK_FAIL=1 bash scripts/cross-build.sh --target all --check-only
```

**有工具链 / Docker 的完整验收：**

```bash
docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .
bash scripts/cross-build.sh --target all --docker
file dist/x86_64-unknown-linux-gnu/jx-server
file dist/aarch64-unknown-linux-gnu/jx-server
```

---

## 6. 常见错误

| 现象 | 原因 | 处理 |
|------|------|------|
| `cc-rs: failed to find tool` / `failed to find tool "..."` | aws-lc-rs/ring 的 C 编译器缺失 | 安装对应 apt 包；export `CC_<target_with_underscores>` |
| `linker \`...\` not found` / `error: linking with` | 最终链接器不在 PATH | 安装交叉 gcc；配置 `.cargo/config.toml` `linker` 或 `CARGO_TARGET_*_LINKER` |
| summary 出现 `TOOLCHAIN_MISS` | 以上两类被 `is_toolchain_error` 识别 | 按打印指引装工具链，或 `--check-only` / `ALLOW_LINK_FAIL=1` |
| summary 出现 `CHECK_FAIL` / `BUILD_FAIL` | 非工具链的编译/链接错误 | 看脚本上方完整日志；修复代码或依赖 |
| Docker 路径下 `BUILD_FAIL`/`CHECK_FAIL` | 镜像内 cargo 失败（**不**回退本机） | 修复 Dockerfile/依赖，或移除镜像后走本机路径 |
| Docker 路径下仍落到本机 | 镜像 `MemoryDataBase-cross` 不存在 | `docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .` |
| `rustup target add` 失败 | 网络/镜像源问题 | 换 rustup dist server；确认 `rustup` 可用 |
| LoongArch 仅 `check` 过、build/link 失败 | 无龙芯交叉 gcc | §3.3 安装工具链；或接受 P1 + `--check-only` |

脚本识别的**工具链**错误模式（`is_toolchain_error`）：

- `linker \`...\` not found`
- `error: linking with`
- `cc-rs: failed to find tool`
- `failed to find tool "..."`
- `error occurred in cc-rs`
- `Could not find \`...\` (linker)`
- `unable to find linker`

> 注：不再将裸 `No such file or directory` 视为工具链错误，避免误报。

---

## 7. glibc 版本注意

- 交叉产物在**目标机**上运行，动态链接器与 `libc.so.6` 版本必须 ≤ 构建机 glibc。
- 规则：**构建机 glibc ≥ 目标 OS glibc**。
- 常见信创 OS（Kylin V10、UOS、openEuler、Anolis 等）glibc 可能旧于新版 Debian/Ubuntu。
- `docker/cross/Dockerfile` 基于 `debian:bookworm-slim`（glibc 2.36）。若目标 OS glibc 更旧，应在更旧的构建基镜像上重建，或确认目标机 glibc 足够新。
- 版本核对：目标机 `ldd --version`；构建机/容器内同样命令。
- 若必须支持很旧的 glibc：考虑 musl 目标（需改目标 triple 与工具链）或在旧发行版容器内构建；当前交付物默认 glibc 目标，不包含 musl。

---

## 8. 旧脚本与入口关系

| 脚本 | 状态 | 说明 |
|------|------|------|
| `scripts/cross-build.sh` | **唯一入口** | 三目标；`--target` / `--check-only` / `--docker` |
| `scripts/build-linux.sh` | SUPERSEDED | 旧 x86_64 + aarch64；头部已标注，保留兼容 runbook |
| `scripts/build-loongarch.sh` | SUPERSEDED | 旧龙芯单目标；头部已标注，保留兼容 runbook |
| `scripts/check-licenses.sh` | 有效 | cargo-deny licenses 检查 |
| `docker/cross/Dockerfile` | 有效 | `MemoryDataBase-cross` 镜像定义 |
| `.cargo/config.toml.example` | 有效 | 三 target linker / `CC_*` 模板 |

新文档与 runbook 请只调用 `scripts/cross-build.sh`。

---

## 9. 快速参考

```bash
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
cd /Users/caosunliu/opencode/MemoryDataBase

# 许可证 + 测试 + 三目标冒烟
bash scripts/check-licenses.sh
cargo test --workspace
bash scripts/cross-build.sh --target all --check-only

# Docker 完整构建
docker build -t MemoryDataBase-cross -f docker/cross/Dockerfile .
bash scripts/cross-build.sh --target all --docker
file dist/aarch64-unknown-linux-gnu/jx-server
```
