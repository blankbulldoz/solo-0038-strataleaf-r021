# 恢复与观测

备份必须调用 archives.backup，不能只拷贝活动主文件。backup 与 restore 都不覆盖存在目标；恢复使用新路径，先校验来源 SHA-256，再打开新句柄。主库恢复生成新身份，副本恢复绑定来源。恢复清理原机保留点与消费者；消费方重新注册时须根据已恢复的 floor/head 判断是否需要重建。

diagnostics.inspect(deep=True) 检查 SQLite 完整性、外键、提交连续性、当前索引与文档投影；fingerprint 比较指定读视图的键、文档版本、内容及到期事实。前者不能证明所有业务数据正确，后者相同也不能代替模式与索引定义比对。checkpoint 返回 SQLite 的 busy、log、checkpointed 信息，并不承诺截断 WAL。

examples.observations 的 contention 会让独立连接暂持 IMMEDIATE 锁，记录第二句柄的 busy 超时及释放后的成功；retention_pressure 记录读保留点和消费者分别阻止水位推进的现象。它们用独立临时文件，不接触生产路径。观察包含真实修订和异常，适合复核运行边界。

在线索引构建是长操作而非长事务。调用方反复 `advance`（默认每块等于 `begin` 时的 `chunk_size`）直到 `phase` 变成 `catching_up`，再反复 `catch_up` 直到 `lag` 为 0，最后 `finish`。进程中断后重新打开数据库并再次 `begin` 同一名称与定义即恢复，`progress` 给出当前阶段、已扫描键数、扫描边界、待重放起点与暂存条目数。构建期间可以正常提交；若写入量持续超过追赶速度，`finish` 会在切换事务中重放剩余提交，此时短暂占用写者，但范围只是尚未追赶的部分。不要直接删除 index_builds 行或用文件复制绕过 `abort`；`abort` 会同时清掉暂存投影。构建未完成时 `diagnostics.inspect` 会检查其快照与重放起点是否仍落在保留历史内。

关闭所有调用方句柄再移动数据目录；不支持共享网络盘或多主写入。在空间紧张时先核实 pin/消费者，推进已完成的确认后再回收；回收不自动缩小文件，物理容量由 SQLite 空闲页复用。到期清理只在显式调用时执行，不能把墙钟到期等同于读视图变动。
