# MemoryDataBase 信创交叉编译设计

- **项目**：MemoryDataBase 信创子项目 — 真机/CI 可复现交叉编译
- **版本**：v1 设计基线
- **日期**：2026-10-05
- **状态**：设计已评审通过，待实现
- **许可**：Apache-2.0
- **前置**：main @ `8bda0ae`（MVP + 性能 + 主从复制，测试 217 绿）

---

## 1. 目标与范围

### 1.1 目标

消除 `ring` 导致的交叉编译阻断，交付**可复现**的信创三架构 Linux 构建脚本 + Docker 构建环境 + 工具链文档，使任意 Linux CI/构建机可产出 `jx-server`/`jx-cli`。

### 1.2 交付物

- TLS 后端可交叉化（标准 TLS 功能保留）
- `scripts/cross-build.sh` + `docker/cross/Dockerfile` + linker/CC 配置模板
- `docs/superpowers/xinchuang/cross-build.md`
- 许可证检查仍通过；workspace 测试仍绿

### 1.3 非目标

- rpm/deb 安装包、真机安装验收、云 CI 流水线托管
- 信创 OS 上的性能验收（属其他子项目）

---

## 2. TLS 后端切换

`jx-server` 的 rustls/tokio-rustls features 从 `ring` 切换为可交叉后端：

1. **首选 `aws-lc-rs`**（rustls 现代后端）；交叉需 `CC_*`/linker，但文档与 cc-rs 支持成熟。
2. 若交叉失败：回退评估 `ring` + 强制交叉 gcc（文档化），或评估纯 Rust 路线。
3. **TLCP 不变**：`gm-tlcp` 纯 Rust，无额外 C 依赖。

功能边界：`tls-mode=tls` 与 `tls-mode=tlcp` 均保留；运行时行为不变，仅构建依赖变化。

许可证：aws-lc-rs 预期 Apache-2.0/MIT；`cargo deny` 白名单校验。

测试：现有 TLS/TLCP 测试保持；`cargo check --target <triple>` 作为交叉冒烟。

---

## 3. 构建矩阵与脚本

| 目标 | 信创硬件 | 优先级 |
|------|----------|--------|
| `x86_64-unknown-linux-gnu` | 海光/兆芯/通用 | P0 |
| `aarch64-unknown-linux-gnu` | 鲲鹏/飞腾 | P0 |
| `loongarch64-unknown-linux-gnu` | 龙芯 | P1（工具链文档化） |

产物：`dist/<triple>/{jx-server,jx-cli}`。

`scripts/cross-build.sh`：
- `--target <triple>|all`
- `--check-only`（无链接器时 cargo check）
- `--docker`（优先 Docker 构建）
- 检测 linker/CC_*，缺失打印安装指引

验收：
- 有工具链：`file` 架构匹配；可选 qemu 冒烟
- 无工具链：`cargo check` + 明确指引（不伪造二进制）
- `cargo deny check licenses` 通过
- 文档 `docs/superpowers/xinchuang/cross-build.md` 完整

---

## 4. Docker/工具链与风险

**Docker**（`docker/cross/Dockerfile`）：Debian bookworm/Ubuntu 22.04；`gcc-aarch64-linux-gnu`；LoongArch 工具链 ARG 可选注入。

**配置模板**：`.cargo/config.toml.example` 中各 target linker + aws-lc-rs 所需 `CC_*`。

**风险**：
| 风险 | 缓解 |
|------|------|
| aws-lc-rs 交叉失败 | 回退 ring+交叉 gcc；脚本检测 |
| glibc 版本 | 文档构建机 glibc ≥ 目标 OS |
| LoongArch 工具链 | P1 + 文档；`--check-only` |
| 许可证 | cargo deny；aws-lc-rs 入检查 |
| 与旧脚本重复 | cross-build.sh 包装或取代 build-linux.sh |

**验收清单**：三目标 check；Docker 下 x86_64+aarch64 产出+file；licenses ok；文档完整；workspace 测试绿。

---

## 5. 路线图位置

完成后可独立支撑：真机安装验证（后续）、rpm/deb 打包（可选后续）、云 CI（可选后续）。与 HA 哨兵/集群子项目无依赖。
