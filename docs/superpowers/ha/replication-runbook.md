# Replication Runbook (HA)

双节点主从复制运维手册。覆盖角色、配置、切换（promote）、RPO、双主风险与 repl-port 约定。

## 角色

| 角色 | 配置 | 行为 |
|------|------|------|
| `master` | `role master` | 监听 client port + repl-port；写命令 tee 到 backlog 并推给已连接 replica；FULLRESYNC 发送真实 RDB 快照 |
| `replica` | `role replica` + `replicaof <host> <port>` | 连接 master 的 repl-port；全量（RDB）+ 增量同步；客户端写返回 `READONLY` |
| `standalone` | `role standalone`（默认） | 无复制；`REPLICAOF` 可在线切换角色（内存态） |

## repl-port 约定（重要）

- **`replicaof` 里填的是 master 的 client port**，不是 repl-port。
- 代码自动连接 **`master_client_port + 1`**（`Config::repl_port` 默认 `port + 1`）。
- **repl 监听绑定地址 = master 的 `bind` 配置**（与 client 监听同一 host），不是硬编码 `0.0.0.0`。若 `bind 127.0.0.1`，replica 只能从本机连 repl-port。
- master 侧可通过 `INFO` replication 段的 **`master_repl_port:`** 查看实际绑定的 repl 端口。
- 若 master 显式配置了 `repl-port`（不等于 client port + 1），则 **replica 无法自动发现**——当前版本不做端口探测，replicaof 必须保证 `port+1` 与 master repl-port 一致，或使用下面的手工步骤。
- 动态 `REPLICAOF host port`（conn 层）同样使用 `port + 1`。
- **降级（demote）：** 在曾为 master 的节点上执行 `REPLICAOF host port` 会**停止 repl 监听 + heartbeat 任务**并清空 `repl.master`（不再有 FULLRESYNC 源，写入不再 tee）。随后该节点以 replica 身份向新 master 做全量同步。

## 配置示例

Master（`/etc/MemoryDataBase/master.conf`）：

```conf
bind 0.0.0.0
port 6379
role master
# repl-port 6380          # 可选；不配置则为 port+1
backlog-size 64mb
# repl-timeout 60         # replica 侧空闲读超时（秒）；master 每 ~1s 发 Heartbeat
# requirepass-sm3 <hex>   # 配置后 replica 需对应 masterauth
dir /var/lib/MemoryDataBase-master
```

Replica（`/etc/MemoryDataBase/replica.conf`）：

```conf
bind 0.0.0.0
port 6381
role replica
replicaof 127.0.0.1 6379   # 填 master 的 client port（连接 6379+1=6380）
backlog-size 64mb
# repl-timeout 60         # 复制流空闲超时；超时后断开并重连/重新 FULLRESYNC
# masterauth <password>    # 与 master requirepass-sm3 对应的明文口令
dir /var/lib/MemoryDataBase-replica
shards 2                   # 必须与 master 相同（RDB 按分片分区）
```

**约束：** master 与 replica 的 `shards` 必须一致，否则 FULLRESYNC 的 RDB 加载会失败（`load_rdb` 拒绝分片数不匹配）。改分片数需重新做全量。

**明文复制通道：** repl-port 上的复制帧（FULLRESYNC RDB、Command、Heartbeat）当前为**明文**传输（与 `tls-mode plain` 的 client 口一致；TLS/TLCP 尚未覆盖 repl 通道）。生产环境请通过网络隔离 / 私网 / 防火墙限制 repl-port 访问；需要口令时配置 `requirepass-sm3` + `masterauth`（AUTH 本身也走明文 repl 口）。

## 监控

```
INFO replication
```

- `role:master|replica`
- `master_repl_offset:`（master backlog 偏移 / replica 跟随的 master 偏移）
- `connected_slaves:`（master 上已连接 replica 数）
- `master_repl_port:`（master 实际绑定的 repl 端口；便于确认 port+1 约定）

replica 后台同步状态（连接中/流式）见服务端日志。

**replica 偏移语义：** replica 侧 `master_repl_offset` 在收到 master 的 **Heartbeat**（约每 1 秒，携带 master 当前 offset）或应用 **Command** 帧（按 teed 帧长推进）后更新。空闲期间靠 Heartbeat 保持偏移有意义；`repl-timeout` 超时前 Heartbeat 会持续到达，正常链路不会误超时。

## 切换（Promote）步骤

Master 故障时，将 replica 提升为新 master：

1. **确认旧 master 已不可写**（进程退出或网络隔离），避免双主。
2. 在 **replica 客户端端口** 执行：

   ```
   REPLICAOF NO ONE
   ```

   - 角色切换为 master：客户端写立即可用（READONLY 解除）。
   - 若该节点从未有 repl 监听（动态提升），会**惰性启动** repl 监听，端口 = 本节点 `repl-port`（默认 client port + 1），绑定地址 = 本节点 `bind`，供后续新 replica 接入。
3. 验证：

   ```
   SET probe promoted
   GET probe          # 应返回 promoted
   INFO replication   # role:master
   ```

4. 如有其他 replica，将它们 `REPLICAOF <新master client port>` 指向提升后的节点（仍按 port+1 约定连 repl）。
5. 旧 master 恢复后**不要直接重启为 master**（否则双主）。建议以 replica 身份重新加入：`REPLICAOF <新master client port>`，或清空数据目录后按 replica 配置重启。旧 master 以 master 身份被 `REPLICAOF` 降级时，会自动停止其 repl 监听（见上文 demote）。

## RPO（数据丢失量）

- 增量复制为**流式 tee**：写命令成功返回客户端后进入 backlog 并推给 replica。网络中断时，中断窗口内已提交到 master 但未送达 replica 的写可能丢失。
- **FULLRESYNC 丢失窗口已关闭：** backlog offset 在**快照之前**采集，快照 + 从该更早 offset 起的 backlog 一起发送。快照采集期间并发写入的命令仍保留在 backlog 中，不会因 FULLRESYNC 顺序而丢失。
- **残余风险（double-apply）：** 快照采集窗口内 tee 的写可能同时出现在 RDB 快照与 backlog 切片中，replica 会各应用一次。幂等 `SET` 无影响；`INCR` 等非幂等命令建议在切换/全量期间停止写入。
- master 在 `snapshot_to_bytes` 失败时**关闭 replica 连接且不发送空 FULLRESYNC**（空快照会被当作成功并 flush replica 数据）；replica 侧重连后重试全量。
- 大量写超出 `backlog-size` 环形缓冲时，迟到的 replica 只能 FULLRESYNC，不能部分续传——全量窗口内的数据仍依赖快照，不额外丢。
- **实践 RPO：** 约等于「master 到 replica 的网络/应用延迟」（通常毫秒级）；故障切换时以旧 master 最后确认写为准。无法保证零 RPO（无同步复制）。

## 双主风险（警告）

- `REPLICAOF NO ONE` 后节点即为 master。若**旧 master 同时恢复并仍以 master 身份服务**，将出现**双主**：两边可写、数据分叉、后续任一方 FULLRESYNC 都会覆盖对方。
- 缓解：
  - 切换前确认旧 master 已宕机/下线；
  - 旧 master 恢复后必须先降级（`REPLICAOF 新master`）或清数据重做 replica；
  - 单活写入：任一时刻只允许一个节点接受写。
- 当前版本无哨兵/集群自动选主（见设计 spec 附录），切换为**人工**流程，双主窗口由操作纪律约束。

## 故障排查

| 现象 | 排查 |
|------|------|
| replica 一直 `connecting` | master repl-port 是否监听；`replicaof` 是否填了 client port（自动 +1）；防火墙是否放行 port+1；master `bind` 是否允许 replica 访问 |
| FULLRESYNC 后无数据 | 两边 `shards` 是否一致；master 是否有数据；看 replica 日志 snapshot 加载条数；master snapshot 失败时会关连接（日志有 `snapshot failed`） |
| replica 写报 READONLY | 预期行为；切换用 `REPLICAOF NO ONE` |
| AUTH/WRONGPASS | master `requirepass-sm3` 与 replica `masterauth` 是否对应；repl-port 与 client 口令同一 secret |
| 切换后新 replica 连不上提升节点 | 提升节点 repl 监听在 **其 client port + 1**，绑定地址 = 其 `bind`；确认端口未被占用（惰性启动失败时见错误日志） |
| replica 反复 reconnect | 检查 `repl-timeout` 是否过小；master 是否在发 Heartbeat（约 1s）；网络是否中断复制流 |

