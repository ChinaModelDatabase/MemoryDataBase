# MemoryDataBase 性能优化子项目设计：真分片所有权

- **项目**：MemoryDataBase 性能子项目 — 真分片所有权（True Shard Ownership）
- **版本**：v1 设计基线
- **日期**：2026-10-05
- **状态**：设计已评审通过，待实现
- **许可**：Apache-2.0
- **前置**：MVP 已合并 main（`67ea01d`），测试 121 passed

---

## 1. 背景与目标

### 1.1 问题

MVP 明文 RESP 小 value GET/SET 实测约 **17 万 OPS/s**（pipeline=1，macOS arm64），未达 spec 验收目标 **50 万 OPS/s**。根因（见 `docs/superpowers/perf/2026-10-04-baseline.md`）：

1. Actor channel hop 之上叠加 Engine 内部 `Mutex<ShardState>`（双重序列化）
2. 逐命令 RESP 编码与分配
3. 解码 probe-clone 在大 pipeline 下开销放大

### 1.2 成功标准

| 项 | 标准 |
|----|------|
| 主验收 | 开发机明文 GET/SET **pipeline=1 ≥ 500,000 OPS/s**（`scripts/perf-report.sh` 口径） |
| 完整矩阵 | pipeline=32 聚合吞吐、p99、80/20 混合、client 16/64/256 档位 |
| 延迟门禁 | pipeline=1 且 OPS 达标时 **p99 ≤ 2ms** |
| TLCP | 密文路径**单独报告**实测值，不纳入 50 万判定 |
| 兼容 | 严格兼容：RESP2 语义、附录 A 命令行为、错误字符串、RDB/AOF 格式、国密 API 不变 |
| 回归 | `cargo test --workspace` 全绿（语义等价迁移现有测试） |

### 1.3 非目标

- 不改 RESP 协议（无 RESP3）、不改持久化文件格式
- 不优化 TLS/TLCP 记录层（单独报数）
- 不做跨进程/RDMA/共享内存
- 不做 io_uring 强依赖（信创旧内核走 epoll）

---

## 2. 总体架构与所有权模型

### 2.1 变更本质

从「tokio 连接 → Actor channel → **Engine Mutex 分片**」变为「tokio 连接 → 分片 Actor **独占** `ShardState`（无 Mutex）」。

```
┌─────────────────────────────────────────────────────────┐
│  tokio Runtime（I/O）                                    │
│  Acceptor → Conn tasks: RESP 解析 → 路由 → 投递          │
│  （可选）响应合并 writev；解析缓冲 per-conn 复用           │
└──────────────────────────┬──────────────────────────────┘
                           │ bounded crossbeam channel
                           │ Exec { args, reply: oneshot }
     ┌─────────────────────┼─────────────────────┐
     ▼                     ▼                     ▼
┌─────────┐           ┌─────────┐           ┌─────────┐
│Shard 0  │           │Shard 1  │   ...     │Shard N-1│
│ 线程独占 │           │ 线程独占 │           │ 线程独占 │
│dict/TTL │           │dict/TTL │           │dict/TTL │
│evict/mem│           │evict/mem│           │evict/mem│
└────┬────┘           └────┬────┘           └────┬────┘
     │ 快照/AOF 消息          │                     │
     └──────────────────────┴──────────┬──────────┘
                                        ▼
                           后台：AOF 刷盘 / RDB / TTL 周期 / 指标
                           协调器：KEYS/FLUSHALL/MGET 跨分片
```

### 2.2 关键决策

- **数据归属**：`ShardState { dict, expires, mem, maxmemory, policy }` 仅由对应线程读写；热路径无锁。
- **Engine 变为路由表**：持有 `Vec<Sender<Cmd>>` + `shard_id_for_key`；不再持有数据。
- **命令执行**：`ShardState::execute(args, now)`，现有 `commands::*` 全部迁入，签名去掉 Engine 参数。
- **跨分片**：协调器广播/聚合；不引入分片间互锁。
- **持久化/国密**：Actor 在命令间隙处理 Snapshot/AofBatch；落盘与 SM4/TLCP 仍在后台/连接层。

---

## 3. 命令执行与跨分片协调

### 3.1 单 key 热路径

```
Conn: 解析 RESP → hash(key)%N → tx.send(Exec{args, reply: oneshot})
Shard Actor: rx.recv() → ShardState::execute(args, now) → reply.send(result)
Conn: await reply → 编码 RESP → pipeline≥2 时批量 writev
```

- **同连接顺序**：pipeline 内命令按发送序 await 回执后编码写出（内部可并行执行，对外串行响应，符合 Redis 语义）。

### 3.2 分片内

- `touch_alive` / `evict_if_needed` / `expire_cycle` 变为 `ShardState` 方法。
- `maxmemory`/`policy` 为分片字段，启动从 Config 复制；`CONFIG SET` 经协调器广播。

### 3.3 跨分片命令

| 命令 | 策略 |
|------|------|
| MGET（异分片） | 按 key 分组投递，聚合后按**输入序**回包 |
| KEYS | 广播 glob，合并去重 |
| FLUSHALL/FLUSHDB | 广播清空 |
| DBSIZE | 广播 sum |
| RENAME（异分片） | 校验源存在 → 目标 DEL+SET（带 TTL）→ 源 DEL；**非原子**（与现网一致，文档注明） |

### 3.4 背压与崩溃

- 分片 channel 有界（默认 1024，可配）；满时 `ERR shard command queue full` + 暂停该连接读。
- Actor 崩溃 = 进程 fail-fast 退出；编排拉起 + RDB/AOF 恢复。

---

## 4. I/O 与 RESP 热路径优化

### 4.1 解析

- `RespDecoder`：半包 probe-clone 改为**就地状态机**（记录已解析偏移，完整帧才消费），消除大 pipeline O(n²) clone。
- args 尽量 `Bytes` 零拷贝引用读缓冲。

### 4.2 编码与写出

- pipeline≥2：合并本批响应，一次 `writev` 或单缓冲写出。
- per-connection 输出缓冲复用，避免每命令分配。

### 4.3 可选优化（按收益，可裁剪）

1. 命令名匹配避免每次 `to_ascii_uppercase` 分配
2. INCR/GET 整数路径减少 Bytes↔String 转换
3. Linux feature `io-uring`（默认关闭，实验路径）
4. feature-gate `mimalloc`（默认系统分配器；信创可用性需验证）

### 4.4 明确不做

RESP3、TLCP 记录层优化、跨进程共享、RDMA。

---

## 5. 持久化与国密接入

### 5.1 RDB

- 协调器按分片序 `Snapshot { reply }`；Actor 在命令间隙序列化分片区间；临时文件 + 原子 rename。
- **格式与版本号不变**（`JX RDB` / `JXRDBENC` + SM4-GCM）。

### 5.2 AOF

- Actor 内编码 RESP 追加到分片 AOF 缓冲；后台合并刷盘；`appendfsync` 策略不变。
- 重写：各分片全量命令流（含 `PEXPIREAT`）；密钥模式整流一次 seal。

### 5.3 恢复顺序（不变）

1. `create_dir_all(dir)`
2. RDB 存在 → 加载到各分片
3. `appendonly && AOF 存在` → 重放
4. AOF 失败 → 拒绝启动

### 5.4 国密边界（不变）

TLCP 终止于连接层；SM3/SM4/密钥文件 API 不变；密文单独压测。

### 5.5 CONFIG 热更新

- `maxmemory`/`maxmemory-policy`：广播到分片字段。
- `requirepass-sm3`/`loglevel`/`timeout`：连接层 `ConfigRuntime` RwLock（不在分片内）。

### 5.6 兼容红线

RDB/AOF 格式、错误字符串、AUTH/CONFIG 语义不变；`INFO` 只增不改。测试允许夹具等价改写（去掉 `lock_shard` 直操作），断言语义不变。

---

## 6. 错误处理、测试与验收

### 6.1 错误处理

| 层 | 行为 |
|----|------|
| RESP 解析失败 | 关闭连接 |
| 命令语义错误 | RESP 错误，连接保持 |
| channel 满 | `ERR shard command queue full` + 背压 |
| Actor 崩溃 | 进程退出 |
| 持久化失败 | everysec 告警重试；恢复失败拒绝启动 |
| 跨分片 RENAME | 非原子，错误路径与现网对齐 |

### 6.2 测试策略

1. **回归门禁**：`cargo test --workspace` 全绿（121 项语义等价迁移）
2. **单元**：`ShardState::execute` 直测；路由稳定性；协调器 MGET 输入序
3. **集成**：TCP 冒烟、pipeline 顺序、并发、RDB/AOF 往返、恢复顺序、queue-full
4. **性能**：扩展矩阵脚本 + 新基线文档

### 6.3 验收矩阵（开发机 plain RESP）

| 场景 | 参数 | 达标线 |
|------|------|--------|
| GET/SET 主验收 | clients=64, requests≥100k, value=64B, pipeline=1 | **≥ 500,000 OPS/s** |
| pipeline 聚合 | pipeline=32 | 报告 OPS/s |
| 延迟 | pipeline=1 | p50/p99；达标时 **p99 ≤ 2ms** |
| 混合 80/20 | GET/SET | 报告 OPS/s + p99 |
| client 档位 | 16 / 64 / 256 | 吞吐曲线 |
| TLCP | 同参数 | 单独报告，不纳入判定 |

### 6.4 未达标处理

按收益调优（分片数、writev、状态机、分配器、可选 io_uring）；仍不达标则如实记录剩余瓶颈，不宣称达标。

### 6.5 交付物

- 重构后 `jx-engine`/`jx-server`
- 扩展压测脚本 + 新性能基线
- 公开 API 变化说明（`Engine` 不再暴露 `lock_shard` 等）

---

## 7. 风险

| 风险 | 等级 | 缓解 |
|------|------|------|
| 重构破坏兼容语义 | 高 | 严格回归门禁 + 兼容冒烟脚本 |
| 50 万仍达不到 | 中 | 矩阵分阶段；瓶颈如实入基线文档 |
| 测试夹具迁移遗漏 | 中 | 全量 workspace 测试 + 集成 TCP 测试 |
| io_uring/分配器在信创不可用 | 低 | 默认 feature 关闭，系统分配器 + epoll |

---

## 8. 路线图位置

本子项目完成后，后续独立子项目：

1. **信创真机交叉编译**（ring/cross-gcc、三架构交付包）
2. **HA/集群路线图**（主从复制 → 哨兵 → 分片集群，复用分片所有权与 AOF 编码）

每个子项目单独走 spec → plan → 实现。
