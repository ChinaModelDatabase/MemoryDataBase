# MemoryDataBase MVP 设计文档

- **项目**：MemoryDataBase — 面向信创的 Rust 缓存数据库/分布式数据缓存中间件
- **版本**：MVP（v0.1 设计基线）
- **日期**：2026-10-04
- **状态**：设计已评审通过，待实现
- **许可**：Apache-2.0

---

## 1. 背景与定位

### 1.1 目标

基于国外主流缓存数据库的功能谱系，用 Rust 自研满足**信创（信息技术应用创新）**要求的缓存中间件 MemoryDataBase，对标东方通 TongRDS、中创 InforSuite RDS、金蝶 AMDC、宝兰德 BES CacheServer，通过对开源 Redis 的**协议兼容 + 零改造迁移**切入党政/金融等信创市场。

### 1.2 依据的标准与竞品

| 依据 | 要点 |
|------|------|
| T/BIA 29—2025《信息技术应用创新 分布式数据缓存中间件软件技术和测试要求》 | 功能：数据分片、持久化、事务等；性能：10 万级并发毫秒级、可扩千节点；安全：**强制国密 SM2/SM3/SM4 全链路**；兼容：**全面支持 Redis 协议**、麒麟/统信 OS、飞腾/鲲鹏等国产 CPU |
| 竞品共同卖点 | 100% Redis 协议兼容、零改造迁移、国产软硬件适配、自研代码率 |
| 国外功能谱系（前 10 参考） | Redis 8/Valkey（全量数据结构、模块化、RDB+AOF、Cluster、Lua、Pub/Sub、ACL）；Dragonfly（多线程 shared-nothing、RESP+Memcached）；Memcached（纯 KV、slab、多线程）；KeyDB（多线程、Active-Active、FLASH）；Garnet（RESP 子集、分层存储）；Hazelcast/Ignite（数据网格、计算、SQL）；Aerospike（RAM 索引+SSD、XDR）等 |

### 1.3 MVP 决策摘要

| 维度 | 决策 |
|------|------|
| 定位 | 信创分布式缓存中间件（Rust），单节点优先，对标国产 Redis 替代品 |
| 兼容 | RESP2 + Redis **核心子集**（约 60–100 命令）：String/Hash/List/Set/ZSet + TTL + 管理命令；覆盖 Spring Data Redis/Jedis/Lettuce/go-redis/redis-py 主流用法 |
| 部署 | **单节点纯缓存**（主从/哨兵/集群后置为独立子项目） |
| 持久化 | **可选 RDB + AOF**（Redis 式配置开关；AOF 优先于 RDB 恢复） |
| 国密 | **全链路**：SM3 口令认证 + SM4-GCM 落盘 + SM2/TLCP（GB/T 38636-2020）传输 |
| 性能 | 明文 RESP 小 value GET/SET **≥ 50 万 OPS**（开发参考机）；TLCP 档单独实测报告 |
| 平台 | x86_64（海光/兆芯）+ ARM64（鲲鹏/飞腾）+ LoongArch（龙芯）；麒麟 V10 / 统信 UOS / openEuler / Anolis |
| 许可 | Apache-2.0；全部依赖 MIT/Apache-2.0/BSD 等宽松许可 |
| 架构 | **方案 A**：tokio I/O + 分片 Actor 存储（热路径零锁）；弃选 thread-per-core+io_uring（信创 OS 基线内核 4.19–5.4 不满足）与单线程命令执行（无法达 50 万 OPS） |

### 1.4 MVP 非目标

- 分布式分片集群、主从复制、哨兵自动切换
- MULTI/EXEC 事务、Lua 脚本、Pub/Sub、Streams、Geo、Bitmap、HLL
- RESP3、Redis 8 扩展类型（JSON/时序/概率结构/向量）
- Memcached 协议、模块系统、Web 管控台、MONITOR
- 细粒度 ACL、内置 CA、密评工具对接、三员管理、审计完备性

---

## 2. 总体架构与组件

### 2.1 Workspace 结构

```
MemoryDataBase/
├── crates/
│   ├── jx-proto/        # RESP2 编解码、协议类型、错误码映射
│   ├── jx-engine/       # 分片存储引擎：数据结构、TTL、淘汰、命令执行
│   ├── jx-persist/      # RDB 快照、AOF 日志、恢复加载、SM4 落盘加密
│   ├── jx-crypto/       # 国密适配层：SM3 口令、SM2/TLCP 传输、密钥管理
│   ├── jx-server/       # 进程入口：tokio 网络层、连接管理、Actor 调度、配置
│   └── jx-cli/          # 运维命令行：压测、配置检查、密钥生成、RDB 检查
```

组件边界：`jx-engine` 不感知网络与 TLCP；`jx-persist` 只依赖引擎导出的快照/命令流接口；`jx-crypto` 对上层暴露统一的 `Transport`/`KeyProvider` 抽象，底层可切换纯 Rust TLCP 或 GmSSL FFI。

### 2.2 运行时线程模型

```
┌─────────────────────────────────────────────────────────┐
│  tokio Runtime（I/O 线程池）                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │ Acceptor │→ │ Conn N   │→ │ RESP 解析 / 命令路由  │  │
│  └──────────┘  └────┬─────┘  └──────────┬───────────┘  │
│         TLCP 终止于 Acceptor/Conn 层     │              │
└──────────────────────────────────────────┼──────────────┘
                    crossbeam channel      │
     ┌────────────────┬───────────────────┼────────────┐
     ▼                ▼                   ▼            ▼
┌─────────┐      ┌─────────┐        ┌─────────┐  ┌─────────┐
│Shard 0  │      │Shard 1  │   ...  │Shard N-1│  │协调器    │
│Actor 线程│      │Actor 线程│        │Actor 线程│  │(跨分片)  │
│独占数据  │      │独占数据  │        │独占数据  │  │         │
└────┬────┘      └────┬────┘        └────┬────┘  └────┬────┘
     │ AOF 本地缓冲    │                  │            │
     └────────────────┴──────────┬───────┴────────────┘
                                 ▼
                    后台任务：AOF 刷盘 / RDB 快照 / TTL 清理 / 指标聚合
```

### 2.3 关键决策

- **分片数 N** = `min(CPU 核数, 配置值)`，启动后固定；`shard = hash(key) % N`（默认 SipHash-1-3，hash seed 可配置，为后续对齐 Redis 兼容预留）。
- **Actor 独占数据**：每分片一个 OS 线程，持有该分片全部 key；热路径无锁，仅 channel 投递/回执。
- **跨分片命令**（`MGET`/`KEYS`/`FLUSHALL`/`DBSIZE`）：协调器任务向各分片广播并聚合，不阻塞单分片 Actor 互等。
- **TLCP 终止位置**：tokio 连接层完成握手与记录层加解密，上层 RESP 逻辑只见明文字节流。
- **依赖约束**：tokio/crossbeam-channel/bytes/rustls 等 MIT/Apache；国密依赖 vendored 进 `jx-crypto`，许可证须为 MIT/Apache/BSD。

---

## 3. 存储引擎与数据模型

### 3.1 键空间与数据类型（MVP）

| 类型 | 内部表示 | 命令示例 |
|------|----------|----------|
| String | `Bytes` 或整数优化 | GET/SET/INCR/APPEND/STRLEN/GETRANGE/SETNX/SETEX/MSETNX |
| Hash | per-key `HashMap<Bytes, Bytes>` | HSET/HGET/HGETALL/HDEL/HLEN/HINCRBY/HEXISTS/HKEYS/HVALS |
| List | `VecDeque<Bytes>` 或块状链表 | LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET |
| Set | `HashSet<Bytes>` | SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SPOP/SRANDMEMBER |
| ZSet | 跳表 + 哈希（分数↔成员） | ZADD/ZREM/ZSCORE/ZRANGE/ZRANGEBYSCORE/ZCARD/ZINCRBY |

每个 key 元数据：类型标签、可选 TTL（绝对过期时间戳 ms）、LRU/LFU 采样字段。

### 3.2 分片内部结构

```
ShardState
├── dict: HashMap<KeyEntry, Value>     # 主字典，KeyEntry 含 type+expire
├── expires: BTreeMap<expire_ts, Key>  # 过期时间索引
├── lru/lfu 采样结构                    # 周期采样供淘汰
└── mem_usage: 近似字节统计             # maxmemory O(1) 维护
```

### 3.3 TTL 语义

- **惰性过期**：读/写命中时检查 `expire_ts`，过期视为不存在并清除。
- **主动清理**：每分片后台周期（默认 100ms 采样）从 `expires` 前沿批量删除，单轮限量避免阻塞 Actor。
- 命令：`SET ... EX/PX`、`EXPIRE/PEXPIRE`、`TTL/PTTL`、`PERSIST`（语义对齐 Redis）。

### 3.4 淘汰策略（`maxmemory` 开启时）

- 策略集：`noeviction` / `allkeys-lru` / `volatile-lru` / `allkeys-lfu` / `volatile-lfu` / `volatile-ttl`（对标 Redis）。
- 实现：每分片候选池，写入超限时本分片内随机采样 5 个取最差淘汰；**不跨分片热路径淘汰**；单分片淘汰后仍超限则写入方触发下一分片协作淘汰（轻量协作，无全局锁）。
- 淘汰仅在写路径触发。

### 3.5 值大小限制

单 value 默认上限 512MB（可配置），超限返回错误。

---

## 4. 协议层与命令执行

### 4.1 RESP2（`jx-proto`）

- 支持全部 RESP2 类型：简单字符串、错误、整数、批量字符串、数组（含嵌套，用于 HGETALL/SMEMBERS 等）。
- 基于 `bytes::Bytes` 的零拷贝解析；支持 **pipeline**（顺序解析、批量回写）。
- MVP 不实现 RESP3；`HELLO` 返回兼容错误。

### 4.2 命令表（MVP）

| 类别 | 范围 |
|------|------|
| 连接/会话 | PING、ECHO、QUIT、SELECT（仅 db0，其余错误）、AUTH（SM3）；HELLO 为 stretch（识别并拒绝） |
| 服务端 | INFO、CONFIG GET/SET（子集）、DBSIZE、FLUSHALL/FLUSHDB、TIME；COMMAND 为 stretch |
| Key 管理 | DEL、EXISTS、TYPE、EXPIRE/PEXPIRE/TTL/PTTL/PERSIST、RENAME、RANDOMKEY、KEYS（glob，经协调器） |
| 五大类型 | 见 §3.1；完整冻结清单见附录 A |
| 持久化 | SAVE、BGSAVE、LASTSAVE、BGREWRITEAOF |
| 事务/PubSub/Lua/Stream/Geo/HLL/Bitmap | 不做，返回明确错误；v1.x+ 路线图扩展 |

### 4.3 执行路径

```
Conn task                          Shard Actor
────────                           ───────────
解析 RESP → 命令名+args
  ├─ 单 key 命令：hash(key)→shard，发 ExecRequest，await 回执
  │    Actor 内同步执行（类型校验/TTL/语义/采样/AOF 缓冲）→ 回执
  ├─ 无 key 或单分片多 key：同上
  └─ 跨分片命令：协调器广播各分片 → 聚合 → 回连接
```

### 4.4 错误模型（RESP 错误字符串）

- `WRONGTYPE Operation against a key holding the wrong kind of value`
- `ERR wrong number of arguments for 'xxx' command`
- `NOAUTH Authentication required.`
- `ERR command 'MULTI' is not supported in this version`
- `ERR value is not an integer or out of range`
- OOM：`OOM command not allowed when used memory > 'maxmemory'`

### 4.5 原子性

- 单 key 命令在 Actor 内同步执行完才回执，天然原子。
- 跨分片命令：各分片内原子 + 聚合期间不保证跨 key 一致（与 Redis 非事务多 key 行为一致，文档写明）。

### 4.6 性能要点

- 热路径避免分配：复用解析缓冲、回写合并 `writev`。
- Actor 投递使用有界 channel（默认容量与并发连接相关）；满则对连接背压（暂停读），防止 OOM。

---

## 5. 持久化（RDB + AOF）

### 5.1 总体策略

持久化可选，配置开关控制。RDB 与 AOF 可同时开启；恢复时 **AOF 优先于 RDB**（AOF 完整则以 AOF 为准；AOF 校验失败回落 RDB）。

### 5.2 RDB 快照

- **Forkless 分片序贯快照**：后台任务按分片顺序向 Actor 请求 `Snapshot`；Actor 在命令间隙序列化本分片 dict 写入临时文件，完成后原子 rename。
- 快照期间分片仍可服务（请求之间允许命令插入）；牺牲严格全局一致性点，规避 fork/COW 内存峰值。
- 格式：`JX RDB\0` + version u16 + 分片数 + TLV（key/type/value/expire_ts），版本化可演进。
- 触发：定时（`save` 规则）、`BGSAVE`、优雅停机。
- 恢复：启动加载最新合法 RDB，按分片并行加载。

### 5.3 AOF 追加日志

- **分片本地缓冲 + 后台聚合刷盘**：写命令编码为 RESP 形式（与请求同构）追加到分片 AOF 缓冲；后台合并刷写 `appendonly.aof`。
- 刷盘策略：`always` / `everysec`（默认，最多丢 1s）/ `no`。
- **重写**：定时或体积阈值触发；协调器让各分片生成当前全量 RESP 命令流写入临时 AOF，成功后原子替换。
- 恢复：加载 RDB 后重放 AOF（分片并行）。

### 5.4 落盘加密（SM4-GCM）

- RDB/AOF 写盘前经 SM4-GCM 加密；密钥来自配置引用的密钥文件（`0600`），启动加载到内存。
- 文件头：魔数 + 版本 + 算法标识 + 随机 IV/nonce；恢复按文件头选择解密路径。
- 无密钥文件 = 明文模式（开发调试）；信创部署规范要求启用 SM4。

### 5.5 一致性与崩溃语义

- `everysec`：进程崩溃最多丢 1 秒写入。
- 快照/AOF 重写均写临时文件后原子 rename，互不覆盖。
- 启动恢复失败（校验和/解密失败）：**拒绝启动**并明确报错，不静默丢数据。

---

## 6. 国密与安全

### 6.1 安全目标（对齐 T/BIA 29—2025）

| 环节 | 算法 | MVP 实现 |
|------|------|----------|
| 传输加密 | SM2/SM3/SM4（TLCP，GB/T 38636-2020） | Acceptor/Conn 层终止，可选开启 |
| 身份认证 | SM3 | AUTH 口令 `SM3(salt‖password)` 存储/比对 |
| 落盘加密 | SM4-GCM | RDB/AOF 文件（§5.4） |
| 会话密钥 | SM2 ECDHE | TLCP 握手内完成，不落盘 |

### 6.2 传输层三档（`tls-mode`）

- `plain`：明文 RESP（开发/内网压测）。
- `tls`：标准 TLS 1.3（rustls），迁移过渡档。
- `tlcp`：国密 TLCP，生产推荐；双证书（签名+加密），密码套件优先 `TLS_ECDHE_SM4_GCM_SM3`（0xE051）。
- 可同时监听多端口绑定不同模式。

### 6.3 TLCP 实现选型（`jx-crypto` vendored）

- **首选**：纯 Rust `gm-tlcp`/`gm-tls`（GM-Engineers，MIT/Apache-2.0），vendor + 内部审计 + 集成测试。
- **回退**：`gmssl-rs`（GmSSL 官方 FFI，Apache-2.0，成熟但需为三架构交叉编译 GmSSL）。
- **验收门槛**：与 GmSSL `tlcp_server`/`tlcp_client` 互操作通过；Tongsuo 互操作加分项跟踪。

### 6.4 认证模型（MVP）

- 单默认用户 + 配置口令；`requirepass-sm3` 存 SM3 加盐哈希；`--requirepass` 注入明文后哈希。
- 未配置口令不强制 AUTH（开发便利）；信创部署必须配置。
- ACL 细粒度后置 v1.x。

### 6.5 密钥管理（MVP）

- SM4 落盘密钥：32 字节随机文件（`0600`），`jx-cli keygen` 生成。
- TLCP 证书：PEM 加载双证书 + CA 链；证书用 GmSSL/gm-ca 等工具链生成，MVP 不内置 CA。
- 落盘密钥轮换：先用旧密钥重写快照/AOF（v1.x 提供 `rekey`），MVP 文档写明步骤。

### 6.6 性能关系与安全边界

- TLCP 记录层 SM4-GCM 为持续 CPU 开销；验收分「明文 RESP」与「TLCP」两档报告；握手按连接摊销。
- 国产 CPU 普遍无 SM4 硬件加速，软件 SM4-GCM 吞吐需压测量化，作为分片数/连接数参数输入。
- MVP 不做：内置签发 CA、密评对接、三员管理、审计完备性；日志与 INFO 不输出口令/密钥；错误信息不泄露内部路径（debug 级别可开）。

---

## 7. 配置、运维与可观测

### 7.1 配置文件（`MemoryDataBase.conf`）

| 分组 | 关键项 |
|------|--------|
| 网络 | `bind`、`port`、`tls-mode`（plain/tls/tlcp）、`tlcp-cert`/`tlcp-key`/`tlcp-ca`、`tls-cert`/`tls-key`、`maxclients` |
| 引擎 | `shards`（默认=min(核数)）、`maxmemory`、`maxmemory-policy` |
| 持久化 | `dir`、`save`、`appendonly`、`appendfsync`、`aof-rewrite-percent`、`aof-rewrite-min-size` |
| 国密 | `requirepass-sm3`（或口令注入）、`storage-key-file` |
| 运维 | `daemonize`（可选）、`pidfile`、`logfile`、`loglevel`、`slowlog-log-slower-than`、`timeout` |

- 启动校验：证书/密钥存在性与权限、端口冲突、`maxmemory` 合理性；失败退出并打印原因。
- 热更新子集：`maxmemory`、`maxmemory-policy`、`loglevel`、`timeout`、`requirepass-sm3`；网络/TLS/分片数重启生效。

### 7.2 运维命令（MVP）

- `INFO [section]`：server/memory/persistence/stats/cpu/security 等子段。
- `CONFIG GET/SET/RESETSTAT`（热更新子集）。
- `SLOWLOG GET/LEN/RESET`；`CLIENT LIST/KILL`；`LASTSAVE`；`TIME`。
- **不提供** `MONITOR`（生产危险，与性能目标冲突）。

### 7.3 可观测

- 结构化日志（JSON 可选）：配置摘要、TLCP 握手结果、持久化任务、淘汰事件、限流错误聚合。
- Prometheus `/metrics`（可选，建议绑定本机/管理口）：QPS（按命令类别）、延迟直方图 p50/p99、连接数、channel 积压、每分片内存/key 数、淘汰与过期计数、AOF 刷盘延迟、RDB 耗时、TLCP 握手计数。

### 7.4 部署与升级

- 单二进制 `MemoryDataBase-server` + 配置 + 可选 systemd unit（麒麟/统信/openEuler 可用）；不强制 root。
- `jx-cli`：`keygen`、`check-config`、`bench`、`rdb-info`。
- 容器镜像提供；信创主交付为 tar/rpm/deb 三架构原生包。
- 升级：优雅停机（RDB+AOF 刷盘）后替换二进制；配置与数据文件带版本号，不支持则明确报错。

---

## 8. 错误处理与故障语义

### 8.1 分层错误模型

| 层 | 错误类型 | 处理原则 |
|----|----------|----------|
| 协议层 | RESP 解析失败、非法帧 | 关闭该连接（流状态不可恢复），计数日志 |
| 命令层 | 语法/类型/范围错误 | RESP 错误字符串，连接保持（对齐 Redis） |
| 引擎层 | Actor 崩溃、channel 断开 | **进程退出（fail-fast）**：Actor 独占数据，状态不可信；由编排拉起并从 RDB/AOF 恢复 |
| 持久化层 | 磁盘满、fsync 失败 | `everysec` 失败：告警 + 持续重试，写入仍进内存（文档说明数据风险）；启动恢复失败：拒绝启动 |
| 国密层 | 握手失败、证书过期 | 仅关闭该连接；落盘解密失败拒绝启动 |
| 资源层 | OOM 且 noeviction | 返回 OOM 错误，连接保持 |
| 背压 | channel 满 | 暂停该连接读（TCP 背压），超时断开 |

### 8.2 进程级故障语义

- 单分片 Actor 异常 = 进程退出；MVP 不做热重启分片，正确性优先；HA 属后续主从/哨兵子项目。
- 优雅停机：停 accept → 等待在途命令（超时可配）→ RDB/AOF 刷盘 → 退出码 0。
- `SIGTERM` 优雅停机；`SIGKILL` 依赖持久化恢复语义。

### 8.3 错误可观测与非目标

- 每类错误计数进指标；连接关闭原因结构化日志（限流）；`INFO stats` 输出 `rejected_connections`、`expired_keys`、`evicted_keys`、`aof_last_write_status` 等。
- 非目标：跨进程分片冗余、自动 failover、在线分片迁移；所有降级路径均有告警指标与日志，不静默丢数据。

---

## 9. 测试、性能验收与信创构建

### 9.1 测试金字塔

| 层级 | 内容 |
|------|------|
| 单元测试 | RESP 编解码、五大类型命令语义、TTL/淘汰、RDB/AOF 编解码、SM3/SM4/密钥文件、配置解析 |
| 引擎集成 | 每分片全命令矩阵；跨分片 MGET/KEYS；pipeline；并发下 TTL/淘汰正确性 |
| 协议兼容 | redis-py、go-redis、Jedis/Lettuce（可选 JVM CI）、hiredis 冒烟；差异用例表（SELECT db>0、MULTI 等明确报错） |
| 持久化 | kill -9 后恢复：everysec 丢窗口 ≤1s；RDB+AOF 优先级；SM4 加密恢复；损坏文件拒绝启动 |
| 国密 | SM3 口令校验；TLCP 与 GmSSL 互操作；`tls-mode` 三档切换；明文/密文吞吐对比 |
| 稳定性 | 24h 长稳（内存不涨、channel 不泄）；连接风暴；慢客户端背压 |

### 9.2 性能验收基准

| 场景 | 目标 | 说明 |
|------|------|------|
| 明文 RESP，小 value GET/SET | **≥ 50 万 OPS** | 开发 x86_64 参考机；分片数=核数；pipeline=1 与 32 两档 |
| TLCP GET/SET | 单独报告实测值 | 不预设 50 万；出对比报告供架构参数输入 |
| 混合读写 80/20 | 报告 OPS + p99 | 典型缓存场景 |
| TTL 活跃写入 + maxmemory | 报告 OPS + 淘汰正确性 | 压力场景 |
| 三架构对比 | 同配置相对性能比 | x86_64 / ARM64 / LoongArch |

- 数据集：1M key，value 64B/512B 两档；记录硬件/OS/内核版本；结果存档。
- 工具：`jx-cli bench` + memtier_benchmark/redis-benchmark 兼容口径。
- 未达标调优路径：channel 批量、writev 合并、分片数、SM4 实现替换（均可局部替换）。

### 9.3 信创构建矩阵

| 目标 | Rust target | 要点 |
|------|-------------|------|
| x86_64（海光/兆芯/通用） | `x86_64-unknown-linux-gnu` | 主开发/CI 基线 |
| ARM64（鲲鹏/飞腾） | `aarch64-unknown-linux-gnu` | 交叉编译，glibc sysroot 对齐麒麟/UOS |
| LoongArch（龙芯） | `loongarch64-unknown-linux-gnu` | Rust Tier 2（1.71+）；龙芯工具链链接 |

- OS 验证：银河麒麟 V10 SP2+、统信 UOS V20+、openEuler 22.03 LTS（4.19 内核基线也需冒烟）、Anolis 8.9/23。
- 依赖许可证：CI `cargo deny`/`cargo license`，仅 MIT/Apache-2.0/BSD/ISC/Unlicense；`jx-crypto` vendored 代码许可证与 NOTICE 干净。
- C 依赖策略：默认纯 Rust TLCP；GmSSL FFI 回退时三架构分别编译静态库（打包脚本风格可参考同仓库 kmd 的麒麟/统信构建脚本）。
- 交付物：tar.gz + rpm/deb + Dockerfile + systemd unit + 中文安装/运维手册骨架。

### 9.4 后续子项目接口预留

- 分片模型、Actor 消息协议、持久化文件格式均版本化，为 v1.x 主从复制（可复用 AOF 编码）、集群分片、MULTI/EXEC、Lua 留出演进空间。
- 指标与配置命名对齐 Redis 习惯，降低客户运维迁移成本。

---

## 10. 风险与缓解

| 风险 | 等级 | 缓解 |
|------|------|------|
| `gm-tlcp` 仅 0.1.0，成熟度不足 | 高 | vendor + 审计 + GmSSL 互操作门槛；准备 `gmssl-rs` FFI 回退；TLCP 与引擎解耦可替换 |
| TLCP 记录层拖垮 50 万 OPS 目标 | 高 | 验收分明文/密文两档；明文档作为架构达标口径；密文档出实测；SM4 实现可替换 |
| 信创 OS 旧内核（4.19）限制 I/O 特性 | 中 | 方案 A 基于 epoll/tokio，不依赖 io_uring；CI 含旧内核冒烟 |
| Redis 命令语义细节偏差导致迁移失败 | 中 | 兼容测试矩阵 + 差异用例表；优先覆盖主流客户端高频路径 |
| 三架构交叉编译与依赖许可证 | 中 | cargo deny 进 CI；LoongArch Tier 2 官方工具链；C 依赖默认关闭 |
| 单节点故障即停机 | 中（产品层） | MVP 明确非目标；v1.x 主从/哨兵子项目承接 HA |

---

## 11. 路线图（子项目分解）

按依赖排序，每个子项目独立走「设计 → 计划 → 实现」：

1. **MVP（本设计）**：核心引擎 + RESP2 核心子集 + 单节点 + 可选 RDB/AOF + 全链路国密 + 三架构
2. **内存与淘汰深化**：slab/arena 分配、碎片控制、更精细 LFU
3. **高可用**：主从复制、故障切换、哨兵式协调
4. **集群与分片**：hash-slot、gossip、slot 迁移、客户端路由
5. **脚本与高级特性**：Lua/WASM、Pub/Sub、Streams、MULTI/EXEC、Bitmap、HLL、Geo
6. **安全与运维深化**：ACL、审计、Web 管控台、密评对接
7. **扩展数据类型**：JSON、时序、概率结构、向量检索（Redis 8 方向）
8. **协议兼容扩展**：Memcached 协议、RESP3
9. **扩展机制**：模块/插件系统

---

## 附录 A：MVP 命令集（实现时冻结）

> **必须实现（MVP 冻结边界，验收以此为准）**：
> - 连接：PING ECHO QUIT SELECT AUTH
> - 服务端：INFO CONFIG GET CONFIG SET CONFIG RESETSTAT DBSIZE FLUSHALL FLUSHDB TIME
> - Key：DEL EXISTS TYPE EXPIRE PEXPIRE TTL PTTL PERSIST RENAME RANDOMKEY KEYS
> - String：GET SET SETNX SETEX MGET MSET MSETNX INCR DECR INCRBY DECRBY APPEND STRLEN GETRANGE
> - Hash：HSET HGET HMGET HDEL HEXISTS HLEN HKEYS HVALS HGETALL HINCRBY
> - List：LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX LSET
> - Set：SADD SREM SMEMBERS SISMEMBER SCARD SPOP SRANDMEMBER
> - ZSet：ZADD ZREM ZSCORE ZRANGE ZRANGEBYSCORE ZCARD ZINCRBY
> - 持久化：SAVE BGSAVE LASTSAVE BGREWRITEAOF
>
> **可延后（stretch，实现阶段评审决定，若不做则返回明确错误）**：
> - HELLO（识别并拒绝）、COMMAND、MSETNX、SETEX/PSETEX、GETRANGE 之外的 SETRANGE
> - 位图：GETBIT SETBIT BITCOUNT
> - HMSET（可用 HSET 兼容替代则仍建议实现）、LSET、SRANDMEMBER 多 count 形态
>
> **明确不做（返回固定错误）**：MULTI/EXEC/WATCH、PUB/SUB、EVAL、STREAM/GEO/HLL 等。

新增命令须更新本节并说明兼容性影响；错误实现（WRONGTYPE/参数错误）必须对齐 Redis 语义。

## 附录 B：依赖白名单（初始）

| Crate | 用途 | 许可 |
|-------|------|------|
| tokio | 异步运行时 | MIT |
| crossbeam-channel | Actor channel | MIT/Apache-2.0 |
| bytes | 零拷贝缓冲 | MIT |
| rustls | 标准 TLS 1.3 | MIT/Apache-2.0 |
| sm2/sm3/sm4（RustCrypto） | 国密原语 | MIT/Apache-2.0 |
| gm-tlcp / gm-tls（vendored） | TLCP 协议 | MIT/Apache-2.0 |
| serde/serde_json | 配置与指标 | MIT/Apache-2.0 |
| clap | CLI | MIT/Apache-2.0 |
| thiserror/anyhow | 错误 | MIT/Apache-2.0 |
| prometheus 或自研编码 | 指标 | MIT/Apache-2.0 |

（完整清单在实现阶段由 `cargo deny` 固化。）

---

## 附录 C：设计决策记录（ADR 摘要）

| 决策 | 选择 | 原因 |
|------|------|------|
| 架构 | tokio + 分片 Actor | 50 万 OPS + 信创旧内核可用 + 单 key 原子 |
| 淘汰 | 分片内淘汰 + 轻量协作 | 热路径无锁 |
| 快照 | Forkless 分片序贯 | 规避大内存 COW，适配容器/国产环境 |
| TLCP | 纯 Rust 优先 + FFI 回退 | 许可证与三架构交叉编译友好；成熟度风险可控 |
| 性能口径 | 明文档 50 万 OPS | 密码学税不可控，避免目标不可验收 |
| Actor 崩溃 | 进程 fail-fast | 数据独占，热重启复杂度不值得 |
