# 大数据存储技术期末大作业：基于 Raft 的分布式 KV 数据库

目标：实现一个教学型基于 Raft 的分布式 KV 数据库。

必须功能：

1. 至少支持 3 个节点集群。
2. 容忍任意 1 个节点宕机后系统仍可用。
3. 支持 Put/Get/Delete。
4. 实现 Raft Leader 选举：
   - follower/candidate/leader 三种状态
   - term 任期递增
   - 随机选举超时
   - RequestVote RPC
   - 多数投票
   - Leader 心跳
5. 实现日志复制：
   - AppendEntries RPC
   - prevLogIndex / prevLogTerm 日志匹配检查
   - 冲突日志删除
   - leader 推进 commitIndex
   - follower 应用 committed 日志
6. 网络通信：
   - HTTP JSON RPC
   - 客户端 HTTP 接口：PUT /kv/{key}, GET /kv/{key}, DELETE /kv/{key}
7. 故障恢复：
   - 节点重启后从本地文件恢复 currentTerm、votedFor、log、kv 状态
   - Leader 宕机后自动选举新 Leader
8. 测试：
   - 3 节点启动测试
   - Put/Get/Delete 测试
   - Leader 宕机测试
   - Follower 宕机恢复测试
   - 脑裂/少数派不可提交测试

实现语言：Python 3，尽量只使用标准库。
项目风格：代码清晰，适合课程作业展示，不追求生产级性能。
