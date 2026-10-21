# Distributed KV 改动审计（按 code lesson 顺序）

本文审计 `4886453..cf56bfd` 的全部 distributed-KV 改动，并按
`.code-lessons/walkthroughs/distributed-kv-implementation.yaml` 的 14 章、94 步顺序说明（原实现
92 步，加上本轮两个外部 oracle 验证步骤）：
改动解决什么问题、为什么当前实现满足其局部不变量，以及证据边界在哪里。

## 先澄清 issue 编号

- `ServingStudioSim#18` 是已经合并的 “Improve alignment analysis and kernel workflow guidance”，
  不包含 KV store、router 或 PD relay 要求，因此不能用它判定本实现是否合规。
- distributed-KV 的需求来源是 `ServingStudioSim#16`：独立 DRAM store worker、L6a
  placement/directory、receiver-submitted push/read，以及 unified/PD 的远端 KV 流程。
- 下文的“RFC 符合性”均指 #16。若任务字面要求必须符合 #18，则结论是“不适用”，而不是
  “已满足 #18”。

## 总结结论

代码审计未发现破坏 #16 核心协议的不一致：目录只发布已经落地的数据，store 在传输前做权威
预留，relay 在 decode 读完前保持 pin，接收方只提交一次传输，取消按 request identity 清理，
前台完成时间不被后台 writeback 拖长。四次提交还补上了单调 landing、两份 GPU KV 的独立计费、
权威重试、失败证据落盘和负向审计。

这不是“所有验收均已完成”。CPU 协议和差分测试可复现；物理 CUDA、HP/AFD profile 与真实硬件
链路校准仍是环境门槛。模拟器也刻意只建模 `session -> token count`，没有 Mooncake 的对象字节、
副本、租约和分布式一致性，也没有 KVCacheD 的 CUDA VMM/page geometry。

## 01 · Unified flow：前台完成与后台写入

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `unified-state` | 单一 compute pool 之外需要独立推进 store 协议，且 push ack 前不能丢失写入所有权。 | flow 同时持有 pool、router 和 `writes`；两者共用同一 transfer calendar，写入直到 ack 才从 `writes` 删除。 |
| `unified-entry` | arrival 必须先有全局 request record；前台完成后仍可能有后台 push。 | `on_arrival` 先登记再 admit；`background_pending` 合并 write、router 和 worker wakeup，L7 可继续 drain。 |
| `unified-cancel` | request 可能同时存在于 admission、active、held source、router queue 和 store reservation。 | 取消先清 store 路径，再清 flow write，并广播给所有 compute worker；不会只释放其中一份所有权。 |
| `unified-replies` | store reply 是 compute 下一步的前置条件，capacity exhaustion 不能变成永久等待。 | 每 tick 先推进 router，再推进 compute；ack 定向返回 producer；候选耗尽写入 health error 并取消 request。 |
| `unified-events` | 用户完成与共享 KV writeback 是两个时间点，串行等待会虚增 E2E 并消除真实重叠。 | push offer 先加入 `writes`，同一 tick 可发出前台 completion；后台 drain 保证写入最终仍被推进。 |

## 02 · PD flow：prefill → store → decode

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `pd-state` | remote PD 要把 prefill、decode 和 store 解耦，又要把 ack 送回真正的 producer。 | flow 分别保存两个 pool 和两张 producer map；direct GPU link 与 host link 分开配置。 |
| `pd-entry` | request 首先属于 prefill，但 flow 空闲判定必须覆盖 prefill、decode 和 store。 | arrival 只进入 prefill；pending/active/background 统计覆盖三个阶段，避免过早结束。 |
| `pd-cancel` | 取消可能发生在 lookup、prefill、pinned relay、decode read 或 final push 任一阶段。 | 同一 request 的 router/prefill/decode/source-map 都被清理；identity guard 防止迟到消息伤害新 owner。 |
| `pd-prefill-events` | 长输出需要 pinned relay；单 token 已无 decode 计算；无 session 必须保留 direct control。 | 三个分支显式分开：relay、shared write、原 direct handoff，不把 direct worker 的语义偷偷改变。 |
| `pd-ack-dispatch` | prefill 和 decode 都能产生 push，公共 ack 类型必须回到正确来源。 | ack 以 request 在两张互斥 producer map 中的成员关系定向；未知/重复 ack 成为协议错误。 |
| `pd-decode-events` | relay 只有在 decode 收到字节后才可解除 pin，decode 完成后还要写回增长后的上下文。 | read landing 后发送 `Consumed`；最终 KV 以 shared retention push，并保持前台完成与后台写入分离。 |

## 03 · Router：目录、放置和重试

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `router-state` | store 是有状态的：写可选位置，读只能去实际落地的位置。 | router 独立持有 `session -> (store,tokens)` 目录和 pending push；store 只管数据，worker 只管计算。 |
| `directory-lookup` | 冷 session 不应唤醒任何 store；陈旧目录必须安全退化。 | 目录 miss 直接合成 0-hit；目录 hit 只发一个目标 store，store 的 0-hit reply 可修正陈旧项。 |
| `placement-candidates` | 负载策略只能提出顺序，不能在 router 中猜测瞬时可用容量。 | least-loaded/round-robin/session-hash 只排列候选；最终是否容纳由 store reservation 判断。 |
| `authoritative-retry` | 排队期间容量可能变化；一次预测失败不能直接让请求失败。 | 每个 pending request 独立持有“尚未尝试”的 `VecDeque`，拒绝后每个候选至多尝试一次，耗尽才报错。 |
| `directory-receipts` | 传输开始不等于数据可读；旧 store 的延迟 eviction 不能删除新 store 的目录。 | 仅 `PushComplete` 发布目录；eviction 只有 source store 与当前目录匹配时才删除。 |
| `router-cancel-wakeup` | 已取消 reply 不应复活请求，但 store 自身的 reservation/pin 仍需推进清理。 | router 继续 tick 所有 store，同时过滤 canceled request 的外发 reply，并保留必要 wakeup。 |

“目录和候选列表由 router 独立持有”的具体含义是：目录是已确认的读路由事实；候选列表是某次
尚未成功 push 的重试状态。二者都不是 KV 内容，也不属于任一 compute worker。把候选表放在
store 会让一个 store 不知道其他候选；放在 request worker 又会把 L6 placement 泄漏进 L5。

## 04 · SessionStore / PrefixCache：容量与 pin

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `store-ledger` | resident、incoming reservation 和 relay pin 都消耗同一 DRAM 容量。 | `SessionStore` 统一计算三类占用；router 看到的 load 包含尚未落地的 reservation。 |
| `cache-copy-pin` | 共享 hit 必须可重复读取，relay 又必须防止 decode 前被淘汰。 | `copy` 不移除 entry 并按策略刷新元数据；pinned entry 从 victim 集合排除。 |
| `store-reserve` | 先占网络再发现没空间会制造无效传输和错误容量轨迹。 | receiver 只有 reservation 成功后才 submit transfer；pinned reservation 全有或全无。 |
| `store-land` | 到达时必须原子地把 reservation 转成 resident，不能双计费或出现可见空窗。 | land 消耗对应 incoming reservation 后插入 cache；shared 可按实际空间截断，pinned 不允许截断。 |
| `store-consume-cancel` | session ID 会复用，迟到 consume/cancel 不能解除另一个 request 的 pin。 | reservation 与 pin 都带 request identity；只释放匹配 owner 的资源。 |

## 05 · Store worker：reserve、receive、land、ack

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `storage-shell-state` | storage 节点没有模型/admission，但有消息队列和异步 transfer timeline。 | 独立 `KvStoreWorker` 只组合协议 shell 与 `SessionStore`；active/request queue 恒为零。 |
| `storage-tick` | 同 tick 的取消、到达和新消息顺序会决定是否泄漏 reservation。 | 先处理取消与已到达 transfer，再启动普通消息；回归测试覆盖同 tick 边界。 |
| `storage-start-push` | store 是接收方，也是容量的最终权威。 | 成功 reserve 后由 store 提交 GPU→DRAM transfer；失败立即回 `CapacityExceeded`，没有网络双计。 |
| `storage-waiting` | 同 session 的并发写会造成旧数据覆盖新数据或 pin owner 混乱。 | in-flight/pinned 时后续同-session write 排队；其他 session 仍可并行推进。 |
| `storage-land-ack` | ack 若早于落地，目录和 producer 会认为尚未到达的数据可用。 | 到达后先 land，再发 `PushComplete`；ack 携带实际 token 数和 store send group。 |
| `storage-receipts` | router 目录和审计器都需要来自真实 mutation 的事实，而非推测。 | retain/evict/reservation/pin 行记录 before/after，占用变化直接来自 store receipt。 |
| `storage-cancellation` | queue、incoming 和 pinned 三处都可能持有取消请求。 | cancel 从每处按 identity 删除；已花掉的网络时间不回滚，符合离散事件时间线。 |

这里的 “DRAM worker pool” 是**存储 worker 池**，不是另一个 disaggregated compute pool。
它之所以仍叫 worker，是因为模拟器 L5 的可调度/可 tick 端点统一使用 worker 接口。它没有模型、
没有 decoder KV、没有 request admission，也不占 GPU ID；独立池用来表达 store replica、DRAM
容量、网络端点、排队和故障域。PD 的 prefill/decode pool 才是 disaggregated compute pools。

## 06 · Remote admission：选择一次，等待后恢复

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `selected-state` | 等远端 lookup/read 的请求不能重新进入 selection，避免重排和重复查找。 | pending 与 selected ownership 分离；waiting request 保留唯一归属，取消可从对应状态删除。 |
| `local-first` | 本地已有足够 prefix 时远端查找只会增加延迟和带宽。 | admission 先走可用 local hit，再对不足部分发 lookup；仍受原 budget/fit gates。 |
| `lookup-to-read` | miss 应立即回到普通 admission；hit 需要一个可追踪的 read plan。 | 0-hit 变 ready，正 hit 保存 location/tokens；不会把 directory reply 当作字节已经到达。 |
| `skip-obsolete-read` | lookup 排队期间可能出现更长的本地 prefix。 | submit 前重新选择最佳 partition；local 已不短于 remote 时跳过传输。 |
| `read-arrival` | read 飞行期间 local cache 仍可能增长或改变最佳 partition。 | landing 再比较并选最佳目的地；较短 remote 绝不覆盖较长 local。 |
| `resume-normal-gates` | 远端等待不能绕开既有 token budget、fit 和 reserve 规则。 | landing/miss 后从 preview/budget/fit/reserve 恢复，而非直接强行 admit。 |

## 07 · Prefill 生命周期与共享 iteration shell

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `unified-admission-messages` | lookup result、read arrival、cancel 都要进入同一 admission FSM。 | unified lifecycle 只定义 terminal 行为；公共 remote admission 处理中间状态。 |
| `unified-terminal-push` | unified 请求可以先完成，再让 shared KV 后台写入。 | terminal 先 hold/offer，flow 记录 write；ack 后才释放 held source。 |
| `pd-admission-messages` | PD prefill 与 unified 共享查找机制，但终点不同。 | 复用同一状态机，terminal lifecycle 独立，避免复制或运行时模式分支。 |
| `pd-prefill-terminal` | output=1、relay、standalone 的所有权和后继不同。 | 三条明确路径分别 shared push、pinned push、direct handoff，控制组保持不变。 |
| `remote-iteration-shell` | 远端 read 是新的异步 timeline，但不应复制 batching/execution FSM。 | shell 组合已有 `IterBatchWorker`，只接管消息与 transfer landing。 |
| `receiver-read` | 同一传输由两端提交会重复计费。 | GPU receiver 依据 `store_send_gid` 唯一提交 DRAM→GPU transfer；store 不再发 pull。 |

## 08 · DecodeCore 与 direct/remote PD shell

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `decode-state` | direct 和 remote decode 的 compute/budget/active 行为必须相同。 | 公共 `DecodeCore` 拥有 pull-ready 与 compute 状态，两种 shell 只适配输入和完成输出。 |
| `decode-pull-arrival` | transfer 未到达前 KV 不可用于计算。 | pull 保存在按 end time 推进的 timeline；到达后才进入 ready/admit。 |
| `decode-admit` | landed context 也必须经过 HBM reserve 和标准执行。 | core reserve 成功才 commit resident 并执行；不足时保持 backlog。 |
| `direct-decode-adapter` | store-disabled 是实验控制组，重构不能改变既有结果。 | direct shell 围绕同一 core 保留原 handoff/release 协议；action-equivalence 测试锁定行为。 |
| `remote-decode-input` | 有 session 的 remote relay 与无 session 的 direct handoff 都需要支持。 | typed 消息区分 store relay/standalone，不用模糊字段猜测来源。 |
| `remote-decode-consume` | 太早 unpin 会丢 relay；decode 完成后的新上下文应成为 shared entry。 | landing 后才 `KvConsumed`，完成时 hold 并发 final shared push。 |
| `remote-decode-cancel` | decode 可能处于 backlog、in-flight、active 或 held-write。 | shell、core 和 store pin 都按 request 清理，测试覆盖阶段性取消。 |

## 09 · GPU KV ownership：单调 landing 与恢复

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `monotonic-landing` | 慢 remote response 可能短于更新后的 local prefix。 | install 只在 remote 更长时发生，因此异步返回不会让 cache 倒退。 |
| `held-and-retained` | push 时 source 必须存活，同时后续 turn 仍可本地复用。 | held source 与独立 retained cache 都计入 HBM；没有用一个 token ledger 冒充两份物理数据。 |
| `request-owned-restore` | 容量压力可截短 retained copy，ack 释放 hold 后应尝试恢复，但不能覆盖新 owner。 | restoration right 绑定 request；激活或替换 session 会撤销它。 |
| `restore-regression` | 即使新 entry 更短，它也代表更新的所有权事件。 | stale ack 不按“长度更长”覆盖新 entry；测试覆盖 shorter replacement。 |
| `remote-hit-attribution` | 远端命中不等于真正被当前 request 使用。 | 只记录相对 local 的可用增量，且要求 fetched entry 仍是本 request 消费的版本。 |
| `kv-cancel` | active、held、retained restoration 分属不同 owner。 | 通过 KV trait 的 owner-aware cleanup 释放正确对象，避免只改 shell 状态。 |

## 10 · 配置、builder 和 deployment assembly

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `store-config` | DRAM topology、容量、带宽和 replica 不应伪装成 GPU worker 参数。 | 独立 `kv_store` block；校验容量至少容纳一个 token，store 不分配 GPU/model。 |
| `worker-selectors` | remote 与 direct 必须是明确 composition，便于实验配对。 | 新 selector 只在显式配置时启用；原 unified/pd selector 保持控制路径。 |
| `unified-recipe` | unified 需要 remote-aware ownership、admission、execution 三者一致装配。 | builder 从同一 KV geometry 构造组件并共享 transfer endpoint。 |
| `prefill-recipe` | PD prefill 复用 admission，却有 pinned relay terminal。 | builder 注入 prefill lifecycle，而非在 shared shell 内判断 PD 模式。 |
| `decode-recipe` | remote decode 不做 prefix lookup，也不保留 local prefix cache。 | builder 只配置 pull backlog、core 与 held source，避免不存在的 cache 命中。 |
| `unified-deployment` | `kv_store` 的存在必须整体切换 flow，不能混搭半套协议。 | store block 与 remote selector 互相要求；缺省保持旧 flow。 |
| `pd-deployment` | prefill/decode 两侧必须同时 remote 且 KV layout 相容。 | paired selector 与 bytes/token 校验阻止 direct/remote 错配。 |
| `reject-disabled-cache` | prefill 的 remote bytes 需要可安装目的地。 | remote unified/prefill 拒绝 disabled local prefix cache，避免“传完即丢”。 |
| `schema-export` | Launcher 需要发现 optional store block 并生成 Rust config。 | schema 把 block 暴露为 optional；省略不改变旧 preset。 |
| `launcher-optional-block` | optional/null/sweep traversal 容易误把缺省当错误或漏掉 leaf。 | loader/validator 测试覆盖 omission、null、defaults、sweeps 与 invalid block。 |

## 11 · 共享网络日历与 L7 完成

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `host-endpoint` | DRAM 是网络端点但不是 GPU，若复用 GPU ID 会污染设备统计。 | host endpoint 与 GPU 共用 calendar，却有独立 device kind；GPU count 保持兼容。 |
| `transfer-calendar` | 网络竞争必须同时约束 sender 和 receiver。 | 一次 submit 更新两个 endpoint 的 free time，并按独立 latency+bandwidth 计算 cost。 |
| `foreground-horizon` | 最后一条后台 push 可晚于用户请求完成。 | L7 冻结 foreground horizon 计算 SLO/throughput，继续 tick 直到 background 清空。 |
| `failure-evidence` | 协议错误若直接 return，会丢失定位 capacity/pin 的唯一证据。 | 返回原错误前 finalize/flush request 与 store logs；负向测试验证日志存在。 |

## 12 · 日志与 Analyzer

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `store-event-shape` | placement、transfer、pin、evict 字段不同，但需按同一时间线关联。 | sparse row schema 允许操作专属字段为空，同时保留 request/session/source。 |
| `event-sequence` | 相同 timestamp 下 router/store/cluster 事件仅靠时间无法恢复因果。 | run-wide sequence 给所有 KV 事件稳定总序；logger drop 只负责最终 flush。 |
| `transport-events` | 在意图处记 transfer 会漏掉 reserve failure，也可能双记。 | 只在真实 `submit_transfer` 点记录 bytes、queue、endpoints 与 end time。 |
| `device-metadata` | host memory 若混入旧 `gpus` 会破坏 Analyzer 兼容。 | 新 `devices` 标明 kind；旧 `gpus/gpu_ids` 仍只含 GPU。 |
| `independent-peaks` | held 和 non-cache 峰值可能发生在不同瞬间。 | 每个窗口分别取最大值，不从一个 total-peak snapshot 推导另一个峰值。 |
| `snapshot-wire` | 新峰值必须贯穿 snapshot、row 和 parquet schema。 | 字段端到端序列化；老日志显示 unavailable 而非错误地补 0。 |
| `peak-regression` | 只测试同一时刻峰值会掩盖采样器错误。 | 回归用错开的峰值时刻验证两个独立 maxima。 |
| `analyzer-raw-peaks` | 对 worker/time 求均值会冲淡短时 HBM 峰值。 | Analyzer 暴露 raw-window max，并保留 legacy-column compatibility。 |

## 13 · 回归、证据审计和等价性

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `always-miss-protocol` | 只测 standalone bypass 不能证明 remote shell 的 cadence 等价。 | stub 回应每个真实 lookup 为 miss，并把 request/SLO/summary 与 barebone 精确比较。 |
| `copy-interleaving` | shared read 飞行时源 entry 可被 eviction。 | 测试证明 copy 已独立成立，源淘汰不使 wire data 失效。 |
| `protocol-audit` | 单元测试不能证明一次完整 run 没有超容或 premature unpin。 | validator 按 sequence 重放 occupancy receipts，并检查 read landing 与 unpin 因果。 |
| `audit-attribution` | prefix-reuse 和 foreground completion 容易被日志口径误算。 | audit 交叉检查实际 admitted tokens、completion horizon 和 background count。 |
| `require-evidence` | 缺日志不能被当成“没有错误”。 | metadata 声明 store worker 时，缺 store/comm/request 流会明确失败。 |
| `audit-negative-tests` | 只给正确输入的 auditor 可能从未执行关键 reject branch。 | 删除事件、超容和 premature unpin 等故意损坏样例都必须被拒绝。 |
| `store-off-equivalence` | 新 schema/重构可能无意改变 direct baseline。 | 按 request ID 精确比较 modeled result；只排除明确的 additive metadata 字段。 |
| `pair-comparison` | 不匹配 workload/profile/tick 的性能差不是 storage 因果。 | 工具先检查 pairing，再输出 Analyzer reports 与 peak comparisons。 |
| `cpu-gate` | 协议回归需要在无 CUDA 环境可重复运行。 | `just test-distributed-kv` 汇总 Rust、Python、审计和外部 oracle 子集。 |
| `external-oracle-generation` | 自写期望值只能证明实现符合自己，不能证明与参考实现一致。 | 生成器固定 commit/source hash，直接执行 Mooncake policy 和 KVCacheD manager，再输出规范化 fixture。 |
| `external-oracle-parity` | 上游输出还必须作用于 simulator，而不是只保存成文档。 | Rust 对每个 victim/容量字段精确比较；Python 可重新执行两个 pinned checkout 并精确比较完整 JSON。 |

本轮新增两个可重复外部 oracle：Mooncake 的 LRU/FIFO victim order，以及 KVCacheD 的固定容量
allocate/reject/release accounting。它们是第 13 章证据的一部分，细节见
`doc/kv_storage_reference_parity.md`。

## 14 · Preset、输入与验收边界

| Lesson step | 为什么改 | 为什么当前实现是对的 |
| --- | --- | --- |
| `smoke-matrix` | 单一路径无法覆盖 replica 和 placement 分支。 | smoke preset 交叉 store count 与三种 placement，并使用多轮 session reuse。 |
| `pd-controls` | remote PD 的变化必须对 matched direct control 才能解释。 | preset 保持 model/GPU/workload 相同，仅改变 store 相关因素。 |
| `unified-controls` | unified 同样需要 store-on/off 成对。 | 结构化两个 preset，case label 可配对，避免把额外 workload 当收益。 |
| `reuse-fixtures` | “禁用 reuse”若同时缩短输入，会改变计算量。 | fixture 固定总 prompt context，只改变已声明的可复用前缀。 |
| `load-generator` | 小样例不足以观察 contention，手写大 trace 又不可复现。 | deterministic generator 产生 64 session × 8 turn 的稳定输入。 |
| `accepted-design-deltas` | RFC 与实现之间有明确取舍，隐瞒会让验收口径漂移。 | 文档列出 receive-backlog、两份 KV、whole-hit read 等选择及其测量方式。 |
| `acceptance-boundary` | CPU 协议正确不等于硬件性能结论。 | 文档区分已记录证据和 CUDA/HP/AFD/profile/link calibration 未完成门槛。 |

## 对 #16 的需求映射

| #16 要求 | 实现位置/证据 | 结论 |
| --- | --- | --- |
| 无模型、无 admission 的独立 DRAM KV worker | 第 04、05、10、11 章 | 满足 |
| L6a pluggable placement + `session -> store` directory | 第 03 章 | 满足 |
| shared prefix copy，远端 entry 保留 | `cache-copy-pin`、`copy-interleaving` | 满足 |
| unified completion 后写、PD prefill/decode relay | 第 01、02、07、08 章 | 满足 |
| receiver 提交 transfer；read 无独立 pull 消息 | `storage-start-push`、`receiver-read` | 满足 |
| pinned relay 到 decode 完成读取 | `store-reserve`、`remote-decode-consume` | 满足 |
| unified/HP 与 PD first scope，保留 direct controls | 第 10、14 章；HP 是 full-attention composition | 核心路径满足；硬件/profile 验收未完成 |
| store-disabled 行为保持 | `direct-decode-adapter`、`store-off-equivalence` | CPU modeled-result 证据满足 |
| 全部 acceptance/performance 结论 | CUDA、HP/AFD profile 与真实链路仍缺环境 | 尚不能宣称全部完成 |

## 工业系统相似点与差异

| 维度 | ServingStudioSim 本设计 | Mooncake 类 disaggregated KV store | KVCacheD |
| --- | --- | --- | --- |
| 角色 | DRAM store replicas + L6a router + compute pools | 独立 KVCache/对象存储、元数据与数据传输组件 | 单机/节点内 GPU KV 的 CUDA VMM 弹性分配 |
| key/value 抽象 | session ID + token count | object/key + byte ranges/segments | GPU block/page 与虚拟地址映射 |
| 数据移动 | 离散事件中的 α+β cost 与共享端点 calendar | 真实 RDMA/TCP 等传输 | 物理 GPU page map/unmap；不是远端 DRAM router |
| 放置/发现 | 单 router 目录，落地后发布，eventual invalidation | 分布式 metadata/master 与多节点生命周期 | allocator/page manager，无 session directory |
| 淘汰/容量 | LRU/LFU/FIFO；reservation；pinned relay | 容量与 eviction policy，真实对象生命周期 | all-or-nothing GPU block allocation/release |
| 一致性/可靠性 | 确定性模拟；无复制、租约、持久化、故障恢复 | 工业实现通常包含分片、复制/高可用和故障处理 | 关注本地 VMM/engine 生命周期，不提供分布式存储一致性 |

因此它与 Mooncake 的相似性主要在“计算存储解耦、远端共享 KV、元数据定位、容量/淘汰和网络
成本”；与 KVCacheD 的相似性主要在“容量守恒和原子分配”。它不是 Mooncake 的逐对象仿真，
也不应把 `KvStoreWorker` 误称为 KVCacheD 那种 GPU memory worker。
