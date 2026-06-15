# Raft Distributed KV Store

这是一个教学型、基于 Raft 的分布式 KV 数据库项目。完整要求见
[`SPEC.md`](SPEC.md)。

## 当前进度

当前已完成：

- 从 JSON 文件读取节点和集群配置。
- 缺少配置文件时生成本地 3 节点默认配置。
- 单节点内存 KV 状态机。
- 使用 JSON 文件保存节点状态和 KV 数据。
- 使用临时文件和 rename 原子替换持久化文件。
- 单节点命令行工具，可验证启动恢复和操作后持久化。
- Raft RequestVote 选举和空 AppendEntries Leader 心跳。
- AppendEntries 日志一致性检查、冲突日志删除和多数提交。
- committed 日志按顺序应用到 KV 状态机，并持久化每个节点的 KV 数据。
- Leader 提供基于 Raft 日志的 HTTP PUT、GET 和 DELETE 接口。
- 使用 Python 标准库提供 Raft HTTP JSON RPC 和状态接口。

当前尚未实现快照、成员变更和线性一致性读取协议。GET 暂时只允许访问
Leader 的本地状态机。

## 配置格式

```json
{
  "nodes": [
    {
      "node_id": "node1",
      "host": "127.0.0.1",
      "port": 8001,
      "data_dir": "data/node1",
      "peers": ["127.0.0.1:8002", "127.0.0.1:8003"]
    }
  ]
}
```

## 运行测试

```powershell
python -m compileall -q src scripts
python -m unittest discover -s tests -v
```

## 清理节点数据

运行演示前可删除默认三个节点的本地数据：

```powershell
python scripts/clean_data.py
```

请先停止正在运行的节点。所有自动演示脚本也会在启动节点前自动清理数据。

## 单节点 CLI 演示

以下命令每次启动时都会从 `data/node1` 恢复 KV 数据：

```powershell
python src/node.py --node-id node1 --data-dir data/node1 put a 1
python src/node.py --node-id node1 --data-dir data/node1 get a
python src/node.py --node-id node1 --data-dir data/node1 status
python src/node.py --node-id node1 --data-dir data/node1 delete a
python src/node.py --node-id node1 --data-dir data/node1 get a
```

`put` 和 `delete` 输出 `OK`。`get` 输出保存的 value，不存在时输出
`NOT_FOUND`。此工具只用于本地验证，尚未接入 Raft 或网络通信。

## 启动 3 节点 Raft 集群

打开 3 个 PowerShell 窗口，分别运行：

```powershell
python src/node.py --node-id node1 --config config.json serve
```

```powershell
python src/node.py --node-id node2 --config config.json serve
```

```powershell
python src/node.py --node-id node3 --config config.json serve
```

当 `config.json` 不存在时，会使用默认地址：

- `node1`: `127.0.0.1:8001`
- `node2`: `127.0.0.1:8002`
- `node3`: `127.0.0.1:8003`

等待约 3 秒后查看各节点状态。PowerShell 下推荐使用
`Invoke-RestMethod`，它能直接处理 JSON：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/status
Invoke-RestMethod http://127.0.0.1:8002/status
Invoke-RestMethod http://127.0.0.1:8003/status
```

其中一个节点的 `role` 应为 `leader`，其他节点应为 `follower`，并显示相同
的 `leader_id`。在每个节点窗口按 `Ctrl+C` 可停止节点。

## 测试日志复制

先通过 `/status` 找到 Leader，然后向 Leader 调用调试接口。例如 Leader 是
`node1`：

```powershell
curl.exe -X POST http://127.0.0.1:8001/debug/append_log `
  -H "Content-Type: application/json" `
  -d '{"type":"put","key":"a","value":"1"}'
```

Leader 会返回当前 `log_length`、`commit_index`、`replicated_to` 和提交结果。
等待一次心跳后，再查看三个节点：

```powershell
curl.exe http://127.0.0.1:8001/status
curl.exe http://127.0.0.1:8002/status
curl.exe http://127.0.0.1:8003/status
```

每个节点的 `log_length` 和 `commit_index` 应一致。`last_applied` 当前保持为
与 `commit_index` 一致。向 follower 调用 `/debug/append_log` 会返回
`not leader` 和当前 `leader_id`。

## 测试 Raft KV 接口

先通过 `/status` 找到 Leader。假设 Leader 是 `node1`：

```powershell
# PUT a=1
Invoke-RestMethod -Method Put http://127.0.0.1:8001/kv/a `
  -ContentType "application/json" `
  -Body '{"value":"1"}'

# GET a
Invoke-RestMethod http://127.0.0.1:8001/kv/a

# DELETE a
Invoke-RestMethod -Method Delete http://127.0.0.1:8001/kv/a

# 再次 GET，返回 404 和 NOT_FOUND
Invoke-RestMethod http://127.0.0.1:8001/kv/a
```

PowerShell 下不推荐使用 `curl.exe -d` 直接传 JSON，因为引号和转义规则容易导致
请求体格式错误。

向 follower 发送 KV 请求时会返回 `not leader`、`leader_id` 和可用于重试的
`leader_hint`。操作后查看三个节点状态：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/status
Invoke-RestMethod http://127.0.0.1:8002/status
Invoke-RestMethod http://127.0.0.1:8003/status
```

三个节点的 `log_length`、`commit_index` 和 `last_applied` 应保持一致。

## 自动演示脚本

基础演示会自动启动三个节点、选举 Leader、执行 PUT/GET/DELETE、打印状态并
停止节点：

```powershell
python scripts/demo.py
```

Leader 宕机容错演示会写入数据、关闭当前 Leader、等待新 Leader，并验证剩余
两个节点仍可读写：

```powershell
python scripts/test_failover.py
```

Follower 重启恢复演示会关闭一个 follower，在它宕机期间继续提交日志，然后
重启该 follower 并等待它通过 AppendEntries 追赶：

```powershell
python scripts/test_restart_recovery.py
```

该脚本验证重启 follower 最终能够恢复到与集群一致的 `log_length`、
`commit_index` 和 `last_applied`，并确认故障前后的 KV 数据均可读取。

多数派与少数派演示包含两个场景：原 Leader 宕机后剩余多数派只选出一个新
Leader 并继续写入；仅保留一个节点时，该少数派无法提交写入：

```powershell
python scripts/test_no_split_brain.py
```

该脚本用于验证不会出现两个可提交写入的 Leader，并验证少数派的
`commit_index` 不会推进、未提交写入不会应用到 KV 状态机。

脚本每一步都会打印 `[OK]` 或 `[FAIL]`，适合截图放入课程报告。运行脚本前请
确认默认端口 `8001`、`8002`、`8003` 没有被其他进程占用。自动脚本固定使用
默认三节点配置，不读取项目根目录中的自定义 `config.json`。

## 后续运行目标

- 持久化 Raft term、投票和日志，使节点重启后完整恢复。
- 增加故障恢复和网络分区测试。
