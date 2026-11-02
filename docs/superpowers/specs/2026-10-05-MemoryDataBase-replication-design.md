# MemoryDataBase 主从复制设计（HA 子项目 · 阶段 1）

- **项目**：MemoryDataBase HA 子项目 — 主从复制
- **版本**：v1 设计基线
- **日期**：2026-10-05
- **状态**：设计已评审通过，待实现
- **许可**：Apache-2.0
- **前置**：main @ `1d4e07a`（真分片所有权单节点，测试 180 绿）
- **路线图**：阶段 2 哨兵、阶段 3 分片集群（本 spec 仅附录，不实现）

---

## 1. 目标与范围

### 1.1 目标

在现有单节点真分片所有权架构上实现**异步主从复制**：RDB 全量 + 增量命令流、副本只读、手动提升。为信创场景提供基础高可用（人工切换）。

### 1.2 非目标（本阶段）

- 哨兵/自动 failover、分片集群、带数据降级、多主 Active-Active
- partial resync（backlog 续传）标为 stretch
- 复制通道 TLS（可后置，对齐客户端 tls-mode）

---

## 2. 角色模型与状态机

| 角色 | 行为 |
|------|------|
| **master** | 读写；写命令执行后 tee 到复制流；服务 REPLICAOF 连接 |
| **replica** | 只读；客户端写返回 `READONLY You can't write against a read only replica.`；从主全量/增量同步 |
| **standalone** | 默认；行为与现网一致（opt-in 复制） |

副本状态机：
```
offline → connecting → full_sync → streaming → (connected)
                ↑______________|  断线重连
```

配置示例：
```
role master|replica|standalone
replicaof <host> <port>
repl-port <port>          # 默认 master port+1
backlog-size 64mb
repl-timeout 60
masterauth <sm3-hash>     # 可选，复用现有口令机制
```

---

## 3. 复制协议与数据流

### 3.1 通道

独立 TCP 端口 `repl-port`（默认 master_port+1）。

### 3.2 全量同步

```
Replica                         Master
  |-- REPLICAOF sync --------->|
  |<-- FULLRESYNC <repl_id> <offset> --|
  |<-- RDB payload (Snapshot) --------|  // 复用分片 Cmd::Snapshot
  |   清空本地后加载                     |
  |<-- 从 offset 开始的增量命令流 ------|
```

全量以主为准：副本清空本地数据后应用主快照。

### 3.3 增量流格式

```
[u32 len][u8 type][payload]
type 0x01 = RESP command bytes（客户端写命令编码）
type 0x02 = heartbeat（含当前 offset）
type 0x03 = FULLRESYNC 头
```

- 主在写命令**成功执行后**追加 backlog 并 tee 到 streaming 副本。
- backlog：环形缓冲，默认 64MB，存 offset→bytes；溢出淘汰最旧。
- 副本以 replica 角色重放写命令（复制通道绕过 READONLY），维护 `master_repl_offset`。
- 心跳 1s；副本 `repl-timeout` 内无数据则断开重连。

### 3.4 客户端命令

- `REPLICAOF host port` / `REPLICAOF NO ONE`
- `INFO replication`：`role`、`master_repl_offset`、`connected_slaves`、`slaveN:...`

---

## 4. 同步流程与手动提升

**Replica 启动**：本地恢复 → 连接主 → 全量（清空后加载）→ streaming。

**Master 启动**：照常恢复 → 监听 repl-port → 写命令 tee。

**提升（`REPLICAOF NO ONE`）**：停止拉流 → 角色 master → 清空复制元数据（简化）→ 接受写入。**RPO**：已应用增量保留；主崩溃未复制写丢失（异步语义）。

**降级（master→replica）**：`REPLICAOF host port` → 清空本地 → 全量（MVP 不做带数据降级）。

**脑裂边界**：不自动选主；双主风险由运维避免；切换前停旧主（runbook 写明）。

---

## 5. 错误处理

| 场景 | 行为 |
|------|------|
| 连不上主 | 指数退避重试，状态 connecting |
| 全量中断开 | 丢弃半截快照，重连重新全量 |
| 增量断开 | 重连；backlog 不足则全量 |
| 客户端写 replica | READONLY 错误 |
| 口令错误 | 拒绝连接 |
| backlog 溢出 | 淘汰最旧；落后副本全量 |
| repl-timeout | 断开重连 |
| 提升时旧主仍写 | 不自动处理，运维保证 |

---

## 6. 测试与验收

**测试**：
1. 单元：backlog 环形缓冲、复制帧编解码、状态机
2. 集成：全量一致（含 TTL）；增量 SET/INCR 一致；客户端写 replica → READONLY；NO ONE 后可写；断线重连最终一致；复制通道重放 DEL 生效
3. 兼容：standalone 旧测试全绿；compat 冒烟通过
4. 性能：复制开启后 master 写延迟报告（异步不阻塞 ACK）

**验收**：
| 项 | 标准 |
|----|------|
| 功能 | 双节点全量+增量一致；READONLY；手动提升 |
| 回归 | cargo test --workspace 全绿 |
| 兼容 | standalone 不变；错误字符串对齐 Redis 习惯 |
| 文档 | 配置示例、切换 runbook、RPO 说明 |

**交付物**：复制模块、配置、INFO、集成测试、`docs/superpowers/ha/replication-runbook.md`。

---

## 7. 后续阶段（附录，不实现）

| 阶段 | 内容 | 依赖 |
|------|------|------|
| 2 哨兵 | 多哨兵监控 master/replica，quorum，自动 promote | 本阶段复制 + INFO |
| 3 分片集群 | slot/路由/迁移/客户端 MOVED-ASK | 现有分片 Actor + 复制（可选 per-shard 复制） |

---

## 8. 风险

| 风险 | 等级 | 缓解 |
|------|------|------|
| 复制 tee 增加 master 写路径成本 | 中 | 异步不阻塞 ACK；压测报告 |
| 双主脑裂 | 中 | 文档 + 运维 runbook；阶段 2 哨兵根治 |
| backlog 与全量边界 | 低 | 溢出即全量；stretch partial sync |
| 独立 repl-port 与信创防火墙 | 低 | 配置可改端口；文档说明 |
