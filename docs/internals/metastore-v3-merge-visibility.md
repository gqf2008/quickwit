> Status: the mechanism that closes the merge-visibility window. Internal while it is under development.
>
> History: two reader-side designs were tried and rejected — see "为什么不是读者侧调和" below.

# 合并窗口的可见性（manifest 布局）

## 要解决的问题

manifest 布局下一次合并发布会同时做两件事：发布合并产物、把被替换的源 split 标记为删除。
`publish_ops` 按 stripe 分组逐 stripe 提交，而两者 hash 到不同 stripe 时就是两次提交，会留下一个读者
无法自行化解的中间态：只看得到产物时，同一批文档被产物与源 split 各返回一次；只看得到标记时，
那批文档在视图里没有承载者。

真桶实测（同 harness、同 10 s 探针，五节点）：

| 版本 | 机制 | 命中超额采样（有效采样） | split 文档和超额采样 | 峰值 |
|---|---|---|---|---|
| 修复前 R4 | 无 | 8 / 150 | 7 / 150 | 命中 59 500（acked 54 460） |
| 读者侧 R6（`6a8a3b0`，已否） | 读者调和 | 0 / 127（150 行中 23 行无效） | 0 / 127 | 55 140（= acked） |
| 读者侧 R7（`3a70011c5`，已否） | 读者调和 | 0 / 123（145 行中 22 行无效） | 0 / 123 | 56 020（= acked） |
| 副本 R8（`feb04cbea`，已否） | WAL 副本 | 0 / 88（105 行中 17 行无效） | 0 / 88 | 55 960（= acked） |
| 待标记 R9（`51126e3ae`） | pending marks | 0 / 119（140 行中 21 行无效） | 0 / 119 | 53 600（= acked） |
| 待标记 R10（`e37aa4d5e`，最终） | pending marks + scoped clear | 0 / 102（120 行中 18 行无效） | 0 / 102 | 53 020（= acked） |

## 机制：标记随产物写在同一次 compare-and-swap 里

1. `publish_ops` 先按 `stripe_of(split_id)` 分组，再按"本次发布了几个 split"分两种走法：
   - **一个产物**（日志/链路合并的常态）：本次 mutation 里所有 `MarkedForDeletion` 的 op，只要不属于产物那条 stripe，
     就以**待标记（pending mark）**的形式写进**产物 manifest 的同一次 compare-and-swap**——
     可见性与产物严格同批，且 manifest 不受查询窗口裁剪。待标记只是"把读者已经找到的 split 改成
     `MarkedForDeletion`"，不会凭空造出记录，因此永远不会把已删的 split 带回来。
     产物提交后，其余 stripe 再提交各自的标记（幂等）；随后该 mutation 清掉这些待标记（best effort）。
   - **多个产物**（parquet 合并路径）：没有哪一次 CAS 能同时带上所有产物，改**两遍提交**——
     先提交全部非标记 op，再提交全部标记 op。中间态是"产物可见 + 源仍 Published"，即**重复命中而不是丢文档**。
2. 读者（`list_splits`）因此不需要任何调和：读完所有 stripe 的 segment 与 WAL 之后，把各 manifest 的
   待标记作用到"已经找到且仍是 `Published`"的记录上；同一 split 若同时出现在两条 stripe，
   `apply_op` 按**状态在生命周期里的位置**取靠后的一条（不用 `update_timestamp`——它是秒级、平局常见），
   且**删除是最终态**（split id 不复用，任何记录都不得把已删 split 插回）。
3. 点查 `get_splits_by_id`（mutation 路径）只读 split 自己的 stripe，因此要等它自己那次提交落地才看到标记；
   中间态是"源仍是 Published"，对 mutation 是安全的（再标记一次幂等）。

### 为什么不用"把标记副本放进产物 stripe 的 WAL"

先前版本把 `MarkedForDeletion` 的**记录副本**写进产物 stripe 的 WAL。它有两个洞，都在评审里被实测出来：
副本只在"产物 stripe 被 fold"时才被丢弃，于是**只 fold owner stripe**（把 owner 顶过 32 次提交阈值）之后，
已删 split 会被副本永久带回 `list_splits`（ghost，探针 P2）；而且副本与 owner 侧的真实记录在秒级时间戳上
常常平局，读者只能靠条带读序决定谁赢（R3-1）。改成"待标记"后，这两个洞都不存在：待标记不是记录、
不参与 fold，也不会把缺席的 split 变成存在。

## 隐藏契约（每条都来自实测或评审）

- **待标记只改状态、不造记录**：它只作用于读者已经找到且仍为 `Published` 的 split，因此
  "源已删"永远压过"待标记还没清"，ghost 不可能出现；待标记清理失败也只是留下一条冗余标记。
- **待标记不参与 fold**：它在 manifest 上，fold 动的是 WAL 与 segment；产物的 fold 不会把它带进 segment。
- **多产物合并不保证原子**：n>1 产物用两遍提交（产物先、标记后），中间态是重复命中而不是丢文档。
- **清理是 mutation 作用域的**：`clear_pending_marks` 只删除本次 mutation 写进去的 id，另一个 mutation 在同一 stripe
  上写的 mark 不受影响。清理失败（或该 stripe 长时间没有下一次变更）只会让 mark 多留一会儿，
  最坏退化为"重复命中"，不会丢文档。
- **点查与搜索视图的差异**：owner 侧提交落地前，`get_splits_by_id` 仍可能看到源是 `Published`
  （搜索视图已经按待标记把它当 `MarkedForDeletion`）。这是有意的：点查是 mutation 读原始状态的路径。

## 为什么不是读者侧调和（两次尝试都在真桶/评审上被打回）

- 第一版：读者"只要还有源是 Published 就挡住产物" + "从产物缺席推断复活源" → 真桶 R5 把历史已删除的源
  复活，命中被抬到 87 860（acked 55 740）。
- 第二版：只做受限恢复（产物在视图里且被挡住时才恢复源） → 评审的三条探针各有反例：
  ① 窗口裁剪被当成"源已删除"，跨 bucket 的窗口读重新出现重叠；
  ② 源被 janitor 删掉后产物与仍 Published 的源长期重叠、无自愈；
  ③ 恢复把 `MarkedForDeletion` 谎报成 `Published`，外溢到 GC/retention/merge planner 的 `list_splits`。
  根因是同一个：**调和的状态输入来自被窗口裁剪过的视图，而输出改写了 `list_splits` 的公共语义**。

## 测试（manifest_layout.rs）

- `test_a_half_committed_merge_is_visible_consistently`：failpoint 让**源所在 stripe** 的提交失败，
  读者仍看到"产物 Published + 两个源 MarkedForDeletion"。
- `test_a_windowed_read_of_a_half_committed_merge_sees_one_copy`：源跨两个 bucket、窗口只覆盖其中一个、
  产物在**另一条 stripe**；窗口读的 Published 只有产物、窗口内的源是 `MarkedForDeletion`。
  该用例在读者侧实现（`3a70011c5`）上**失败**（实测：`visible=["source-a-0"]`、`marked=[]`），在最终实现上通过。
- `test_a_deleted_split_does_not_come_back_when_only_the_owner_stripe_folds`：只把 owner stripe 顶过
  fold 阈值并 fold，已删 split 不得复现。该用例在"副本"实现（`2e165a827`）上**失败**（ghost 复现），在最终实现上通过。
- `test_the_owner_stripe_records_the_marking_and_the_marks_are_cleared`：点查看到标记，且待标记在 mutation 结束后被清。
- `test_apply_op_lets_the_furthest_state_win` / `test_apply_op_never_restores_a_removed_split`：状态排在生命周期
  后面的记录赢、删除对本次读取是最终态。
- `test_a_multi_product_merge_never_hides_documents`：n>1 时中间态是重复而不是缺文档。

## 已知取舍

- **多产物合并的残余窗口**：n>1 产物时用两遍提交（产物先、标记后），中间态是重复命中而非丢文档。
  日志/链路合并是单产物、走原子路径；parquet 合并路径需要单独收口（例如让该路径只发布一个产物）。
- **未清理的 mark 的成本**：mark 留在产物 manifest 上直到该 stripe 的下一次变更（本次 mutation 会尽力清掉、且只清
  自己写进去的 id）。残留的 mark 只会把"本来就存在"的 split 标成 `MarkedForDeletion`，不占 WAL/segment 空间。
- **版本**：本次新增了一个 manifest 字段 `pending_marks`，但**不递增** `MANIFEST_LAYOUT_FORMAT_VERSION`：
  字段带 `#[serde(default)]`，旧对象读出来是空集合；旧读者读到新对象会忽略该字段（滚动升级期退化为"重复命中"，
  不会丢文档）。读者对版本是等值校验，递增反而会让已有数据不可读。
