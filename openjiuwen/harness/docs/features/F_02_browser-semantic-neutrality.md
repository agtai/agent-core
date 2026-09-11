# F_02 浏览器语义中立工具与 replan 门禁

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-11 |
| 范围 | `openjiuwen/harness/tools/browser_move/playwright_runtime/` |
| 测试基线 | `uv run pytest tests/unit_tests/harness/tools/browser_move -q` → 616 passed, 4 skipped |
| 关联 spec | 无（本子系统当前无对应 spec，见「已知遗留」） |

## 背景

健康的多步浏览器任务会被 `semantic_replan_budget_exhausted` 提前判死。实验会话 bu-lab-20
的复现序列是 `snapshot → find → hover → evaluate(读取) → handle_dialog → drop`：前四步全部
返回 ok，`handle_dialog` 与 `drop` 从未执行，任务直接进入 blocked 终态。

链路如下：

1. `semantic_state.build_semantic_state()` 的 digest 只包含 url / form_values /
   selected_filters / result_count / field_coverage。
2. `SemanticStateTracker.observe()` 发现 digest 与上一次相同即判 `no_progress` 并累加
   `_consecutive_no_progress`；到 3 次写入 `_replan_required`（reason
   `three_consecutive_no_progress_states`）。
3. `BrowserWorkingContextStore._apply_replan_observation` 把它复制进 phase state，并记一笔
   failed strategy。
4. `BrowserRuntimeRail._consume_phase_budget` → `_consume_replan_gate` 拒绝后续调用；
   `replan_count >= 2` 时置 blocked 终态。

根因是**两个不同的问题被同一个信号回答了**：

- (a)「这组动作需不需要重新抓页面？」——由 `_BROWSER_STATE_REFRESH_TOOL_NAMES` /
  `_BROWSER_STATE_OBSERVATION_TOOL_NAMES` 回答；
- (b)「这组动作该不该计入 no-progress / loop 评分？」——此前**没有**任何概念回答它，
  两条 capture 路径都无差别地调用 `semantic_tracker.observe(...)`。

指针级（hover）、查询级（find / evaluate 读取）、对话框待命级（handle_dialog）的工具在结构上
就不可能改变那个 digest。把它们的 digest 不变判成「没有进展」是**范畴错误**，不是死循环。

## 决策

### 1. 单一真相源：`tool_semantics.py`

新建 `playwright_runtime/tool_semantics.py`，明确回答问题 (b)：

- `SEMANTICALLY_NEUTRAL_TOOL_NAMES`：无条件中立的工具名集合。
- `tool_is_semantically_neutral(tool_name, tool_args)`：含参数条件判断（脚本、tabs、batch）。
- `NEUTRAL_PROGRESS_NAME = "observation"` + `progress_is_semantically_neutral(...)`。

顺带把此前重复 / 散落的三处语义判断收敛进来，避免 token 列表出现第二份拷贝：

- `coerce_tool_args`——原先 `semantic_state.py` 与 `runtime.py` 各有一份，现两处委托；
- `operation_intent`——从 `runtime._operation_intent` 原样搬移（**含既有的大小写怪癖**，
  行为逐字不变），`runtime` 侧退化为薄委托；
- `BATCH_READ_ONLY_OPS` / `batch_steps_are_read_only`——batch 步骤只读判定。
  `runtime` 侧的 `_BATCH_SAFE_READ_SELECTOR_OPS` / `_BATCH_EXPLICIT_SELECTOR_OPS`
  **保持不动**（它们同时服务 batch 执行路径，不只服务评分），由单测断言
  `BATCH_READ_ONLY_OPS` 是两者的超集，防止日后漂移。

中立判定 = 该工具**自身无法**改变 url / form_values / selected_filters / result_count：

| 工具 | 中立 | 理由 |
|---|---|---|
| `browser_snapshot` | ✅ | 只读当前页快照 |
| `browser_find` | ✅ | 在已有快照里查询，不触碰页面 |
| `browser_probe_cards` / `browser_probe_interactives` | ✅ | 只读探测 |
| `browser_take_screenshot` | ✅ | 只读渲染 |
| `browser_hover` | ✅ | 指针悬停只能**揭示**内容，不提交、不导航、不改筛选 |
| `browser_handle_dialog` | ✅ | 为下一次弹窗**预置**响应，本次调用不动页面 |
| `browser_evaluate` / `browser_run_code(_unsafe)` | 条件 | `operation_intent(...) != "script_mutation"` 才中立 |
| `browser_tabs` | 条件 | 仅 `action=list` 中立；切换/新建/关闭会换 url |
| `browser_batch_interact` | 条件 | 仅当既有只读步骤检测认定所有 step 只读 |
| navigate / navigate_back / click / type / press_key / fill_form / select_option / drag / **drop** / file_upload / close / custom_action | ❌ | 直接改变 url / 表单 / 筛选 / 结果集 |

**`browser_drop` 仍然算 mutating**：drop 是真实提交——落下的文件或元素会改变表单值、触发上传
或改变结果集。它的 digest 不变是「这一步没生效」的**真实信号**，抹掉它就会把一个真实失败的
拖放循环放行。为此它只拿到独立的 action class（见决策 4），不进中立集、也不进只读恢复集。

### 2. `SemanticStateTracker.observe(..., mutating=True)`

新增 keyword 参数，默认 `True`（向后兼容）。`mutating=False` 且 digest 重复时：

- progress 标记为 `observation`，`observable_progress=False`；
- **不**累加 `_consecutive_no_progress`、**不**累加 `_state_revisit_count`；
- **不**把重复 digest 追加进 `_history` / `_filter_history`（保住 ABA 检测的意义）；
- 仍然递增 `revision`，仍然返回完整 `latest` 负载（所有既有 key 一个不少——
  `browser_working_context` 按名投影它们）。

`mutating=False` 但 digest **确实变了**时，走与今天完全一致的 `progress` 路径：hover 揭示出新
字段是货真价实的进展，不该因为工具中立就被降级。

### 3. 从 capture 路径把 flag 穿下去

`BrowserStateContextProcessor._completed_state_action_group` 现在返回 4 元组，多出
`mutating`：一组动作里**只要有一个**已执行工具非中立，整组算 mutating。hover /
handle_dialog 因此仍走 **FULL capture**（问题 a 的答案不变），但被评为 neutral（问题 b 的答案
改变）——这正是本次修复的核心分离点。

`capture_browser_state` / `_capture_browser_state_via_driver` /
`capture_compact_browser_state` 三个入口各加 `mutating: bool = True`，签名向后兼容。processor
侧新增 `_call_capture` 统一承载既有的防御式 `TypeError` 回退：先带
`(action_group_id, mutating)` 调用，provider 不认 `mutating` 就退到只带 `action_group_id`，
再不认就裸调——旧 provider 签名不会让 processor 崩掉。

### 4. 中立观测不得被当作失败的 replan trial

`BrowserWorkingContextStore._apply_replan_observation` 此前把任何非
`progress` / `observable_progress=True` 的观测都记 `record_failed_strategy` +
`replan_required=True`。现在遇到中立 progress 直接早退：既不算「恢复」也不算「失败策略」，
`replan_trial_pending` 原样保留。一次只读探测既不该消耗 trial，也不该伪造恢复。

### 5. action class 碰撞

`_classify_action_class` 此前把 hover / drop 和 click 一起归进 `"interaction"`，于是
「hover 某元素 → click 同一元素」产生**相同的 strategy fingerprint**，被读成「Semantic loop
detected」。现在给出独立 class，并把这三项判断**移到 phase 分类之前**（否则 hover 会先被
form / filtering / extraction 阶段吸走）：

- `hover` → `pointer_reveal`
- `handle_dialog` → `dialog_control`
- `drop` → `file_drop`（`find` 既有的 `target_discovery` 不变）

已核对全部消费方（`last_action_class` / `next_action_class` / `_strategy_fingerprint` /
`service.py` 的 `_recent_action_summaries` / `_BROWSER_PHASE_DEFINITIONS`）：这些值只被拼进
自由文本或做相等比较，未知值安全降级。

### 6. 只读恢复

`_is_read_only_recovery` 增加 `hover` 与 `handle_dialog`：指针揭示和弹窗预置是合法的恢复尝试。
**不加 `drop`**（真实变更）。`_BROWSER_READ_ONLY_RECOVERY_LIMIT` 不动。

### 7. 可诊断性

门禁真正该拦时，拦截理由要能指导下一步。新增 `_replan_blocker_detail`，在
`semantic_replan_budget_exhausted` 的报错文本与 `state["blockers"]` 负载里附上 replan reasons
与最近几条 `(tool, action_class, progress)`；`_build_action_record` 增记 `tool` 字段。
首个 blocker token 仍逐字是 `semantic_replan_budget_exhausted`，措辞保留 "semantic" /
"budget exhausted"，`_denial_code` 的映射不受影响。

## 拒绝的方案

1. **抬高阈值**（`_consecutive_no_progress >= 3`、`_STATE_REVISIT_REPLAN_THRESHOLD`、
   `replan_count >= 2`）。这些数字不是本次的病因：中立工具的 digest **永远**不变，抬阈值只是
   把同一个错误判决往后推几步，代价是真实死循环也跟着晚判。
2. **把 hover / handle_dialog 加进 `_is_replan_exempt_tool`**。该路径从
   `_consume_phase_budget` 提前返回，**连 phase attempt 记账都跳过**，中立工具会变成可以无限
   调用——一个纯 hover 的死循环将永远不终止。现在中立工具照常消耗 phase attempt，纯中立循环
   仍由 phase budget 终结。
3. **把 hover / dialog 的效果塞进 semantic digest**（例如把「揭示出的 caption」计入）。digest
   是跨步骤稳定的任务级语义（我在哪、填了什么、筛了什么、有多少结果），把瞬时的指针态塞进去
   会让它对鼠标位置敏感，从而把每一次 hover 都变成「进展」——这会同时**摧毁**真实循环检测。
4. **在 capture 侧直接跳过中立组的 `observe()`**。那样 `revision` 不再推进、`latest` 负载
   停更，下游按名投影这些字段的代码会读到过期状态。中立组仍然观测，只是不计分。

## 验证

- `uv run pytest tests/unit_tests/harness/tools/browser_move -q` → **616 passed, 4 skipped**
  （此前 567，净增 49 条用例 = 27 个新测试函数按参数化展开；无任何既有断言被削弱或删除，
  `test_browser_deferred_core_tools.py` 未改动）。
- 新增 `test_browser_tool_semantics.py`：中立判定矩阵、脚本按 `operation_intent` 分流、
  tabs 仅 list 中立、batch 全只读才中立、`BATCH_READ_ONLY_OPS` 超集守卫、JSON 字符串参数、
  `runtime` 委托。
- 新增 `test_browser_replan_gate.py`：lab-20 序列全程不被门禁拦；中立工具仍消耗 phase
  attempt；纯中立循环仍由 phase budget 终结；同目标重复点击仍走到
  `semantic_replan_denial_budget_exhausted`；两次「实质不同但都无进展」的 mutating 策略仍耗尽
  `semantic_replan_budget_exhausted`；hover→click 指纹不再碰撞；drop 不是只读恢复。
- 扩充 `test_browser_semantic_state.py`（中立不累计、mutating 控制用例仍红→绿、中立但 digest
  变化仍算进展、`latest` key 完整）与 `test_browser_state_context_processor.py`（中立组走
  FULL capture 且标 non-mutating、混合组标 mutating、旧 provider 签名优雅降级）。
- `ruff check` 改动文件全绿；`ruff format --check` 对本次改动文件无新增偏差（仓库存在大量
  既有格式偏差，未顺手重排无关文件）。
- `mypy` 对改动文件未新增报错（仓库既有 3099 条历史报错）。

## 已知遗留

1. **【已由 [[F_03_script-intent-casing-and-classifier-guards]] 解决】**
   `operation_intent` 的正则先把表达式 `lower()` 再匹配 `dispatchEvent` / `setAttribute`，
   这两个分支实际上永远匹配不到（只有 `.click(` 与 `.value =` 生效）。这是**既有缺陷**，本次
   按「复用既有检查、不改行为」原则原样搬移，未修。修它会让一部分此前被判
   `script_extraction` 的脚本变成 `script_mutation`，属于独立的行为变更，应单独立项。
2. `browser_move` 子系统目前在 `docs/specs/` 下没有对应 spec（`S_05` 只把它列为一个工具组，
   不描述语义门禁契约），因此本次无 spec 需要修订。若后续要固化「语义中立 / phase budget /
   replan 门禁」这套契约，应新开一份 spec 并把本文的不变量搬进去。
3. `_BATCH_SAFE_READ_SELECTOR_OPS` / `_BATCH_EXPLICIT_SELECTOR_OPS` 仍留在 `runtime.py`，与
   `BATCH_READ_ONLY_OPS` 靠单测维持超集关系而非类型约束。三者合一需要先厘清 batch 执行路径
   对前两者的依赖，不在本次范围内。
