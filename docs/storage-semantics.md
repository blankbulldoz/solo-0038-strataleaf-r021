# 数据与兼容边界

物理表包括 meta、collections、versions、commits、receipts、index_defs、index_values、index_builds、index_build_values、pins、consumers。versions 的主键为集合、键、修订；body 的 SQL NULL 表示删除，JSON null 只能作为文档字段。collections 保存当前模式；index_values 只包含 ready 索引对当前 head 的投影；index_builds 持久化未完成构建的进度，index_build_values 是它的暂存投影，两者在切换事务中被提升和清空。commits 保存完整的操作序列；receipts 保存不受历史回收影响的请求摘要与结果。幂等键不能被不同操作复用，回放不重新执行前置条件。

floor 是仍可读取的最早修订，不代表每条物理版本的最小修订；每个键在 floor 之前的最后版本可能是构造 floor 读视图的基准，必须保留。commits 仅需保存大于 floor 的提交，消费者从自己的 ack 之后读取。pin 与消费者位置不能低于现存 floor。

在线索引构建把快照固定在 `scan_revision`（开始时的 head），逐块读取该读视图并记录 `scan_after` 与 `scanned`；`pending_from` 指向第一条尚未重放进暂存投影的提交，初值为 `scan_revision + 1`。扫描不覆盖开始之后的新写入，这些写入与删除由 `catching_up` 按提交顺序补上，因此构建期间的数据不会遗漏。写入路径只维护 `index_defs` 中的 ready 索引，不触碰暂存投影。回收水位额外受 `scan_revision` 与 `pending_from - 1` 约束，构建所需的历史或被提前回收时诊断会报告。切换在单个 `BEGIN IMMEDIATE` 中补齐尾部分并写入 `index_defs`，其后第一个写批次即维护该索引；切换失败（如唯一重复）整个事务回滚并随后删除暂存投影与构建记录，不留部分索引，修复数据后必须重新 `begin`。

模式包含 fields、required、additional，支持点路径类型；additional=false 限制顶层未知根字段，不递归禁止已声明对象中的子字段。字段不允许 null，除非该字段未声明类型；缺失可通过 required 控制。integer 排除 bool，number 接受整数及有限浮点。JSON 对象不允许非字符串键；浮点必须有限。索引忽略缺失字段与对象/数组，空值作为可索引标量，数值 1 和 1.0 共用索引值，true 与 1 不共用。

游标携带数据库身份、谓词摘要、读版本和最后键，并由数据库私有密钥签名。它不是权限凭据，不冻结模式，也不是保留租约。需要稳定的长时间读取，调用者自行管理具名保留点。

副本包是内部可信通道数据，校验和只用于检测传输损坏。正常备份恢复新主库保留逻辑内容而更换身份；副本初始化保留上游身份。分段追赶未完成时暂不提供当前索引，避免最新唯一约束错误约束中间历史视图。目录 epoch 可在没有文档提交时前进，最终空提交包仍能传递新的模式与索引。
