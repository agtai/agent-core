# F_03 脚本意图大小写修复与分类器不变量守卫

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-11 |
| 范围 | `openjiuwen/harness/tools/browser_move/runtime/tool_semantics.py`、`.../runtime.py`、`openjiuwen/harness/tools/browser_move/lab/run_bu_prompt.py`、`tests/unit_tests/harness/tools/browser_move/` |
| 测试基线 | `pytest tests/unit_tests/harness/tools/browser_move -q` → 改动前 616 passed / 4 skipped，改动后 **678 passed / 4 skipped** |
| 关联 feature | [[F_02_browser-semantic-neutrality]]（本文解决其「已知遗留 1」） |
| 关联 spec | 无（`browser_move` 子系统仍无对应 spec，见 F_02 已知遗留 2） |

## 背景

### 缺陷本体

`operation_intent` 先把脚本表达式 `lower()` 再用**大小写敏感**的正则匹配：

```python
_SCRIPT_MUTATION_RE   = re.compile(r"\.click\s*\(|dispatchEvent|\.value\s*=|setAttribute\s*\(")
_SCRIPT_EXTRACTION_RE = re.compile(r"textContent|innerText|return|querySelector|document\.title")
expression = str(args.get("function") or ...).lower()
```

于是两个正则里所有混合大小写的分支**永远匹配不到**：`dispatchEvent`、`setAttribute`、
`textContent`、`innerText`、`querySelector` 全是死分支，实际生效的只有 `.click(`、
`.value =`、`return`、`document.title`。

### 为什么它在 F_02 之后才从"化妆品"变成"安全问题"

F_02 把它记为**已知遗留 1**，并按「复用既有检查、不改行为」的原则原样搬移。当时这个判断是
成立的：`operation_intent` 只喂 `_strategy_fingerprint` 和 `_is_read_only_recovery`，误分类
最多让指纹粗一点、多放行一次只读恢复。

F_02 同时新增了 `tool_semantics.py:tool_is_semantically_neutral`，并让脚本工具的中立性**直接
挂在这个分类器上**：

```python
if _matches_tool_name(normalized_name, _SCRIPT_TOOL_NAMES):
    return operation_intent(normalized_name, args) != "script_mutation"
```

中立 = **被排除在 no-progress 与 loop 评分之外**。两件事一叠加，后果就变了：一个用
`dispatchEvent` / `setAttribute` 改页面的脚本被判成 `script_extraction` → 中立 → 它反复执行
而 digest 纹丝不动时，门禁**根本看不见**。这正是 F_02 在**拒绝的方案 3** 里立下的红线——
「中立判定绝不能掩盖一个真实的死循环」——只不过那条红线当时是被大小写 bug 从背后捅穿的，
而不是被某个设计选择捅穿的。

结论：F_02 对遗留 1 的定性（「独立的行为变更，应单独立项」）没错，但它的优先级从 cosmetic
升到 safety-critical。本文就是那个单独立项。

## 决策

### 1. 大小写修复：`re.IGNORECASE` + 去掉 `.lower()`

两条候选路线：

- **(A) 两个正则加 `re.IGNORECASE`，`expression` 不再 `lower()`** ← **选中**
- (B) 保留 `.lower()`，把正则里的字面量全部改成小写

选 A 的理由：标记串保持**它们在真实页面脚本里的拼写**（`dispatchEvent` 而不是
`dispatchevent`），所以在仓库里 grep `dispatchEvent` 能同时命中标记与用例；而 B 会让正则变成
一串肉眼难以校对的小写串，并且**下一个往里加分支的人会再犯同一个错**——他会自然地写
`scrollIntoView`，然后它又是死分支。A 把「大小写不敏感」编码进匹配语义本身，而不是编码进一条
必须靠人记住的约定。

**两个正则的分支集合一字未动**（不增、不删、不改写），三个返回值
`script_mutation` / `script_extraction` / `script_inspection` 也一字未动——下游有三处按字符串
相等比较（`tool_semantics.tool_is_semantically_neutral`、`runtime._classify_tool_phase` 的
`filter_intent`、`runtime._is_read_only_recovery`）。

正则上方留了一条注释，说明**不得**把这些标记拿去匹配一个预先小写化的表达式，以及这样做过一次
的后果。

### 2. 波及面（blast radius）

| # | 消费点 | 改动前 | 改动后 | 判定 |
|---|---|---|---|---|
| 1 | `runtime._classify_tool_phase` 的 `filter_intent` | 提到 filter 词且用 `dispatchEvent` 的脚本 → `filter_intent=False` → 落 `extraction` 阶段 | → `filter_intent=True` → 落 **`filtering`** 阶段 | 正确。用脚本去改筛选器就是筛选动作，该记在 filtering 的预算上 |
| 2 | `runtime._strategy_fingerprint` 的 `intent` | 用 `querySelector` / `textContent` 的读取脚本全部塌成 `script_inspection` | 拆成 `script_extraction` vs `script_inspection` | 指纹**更有分辨力** → 误报的 loop 更少，方向安全 |
| 3 | `runtime._is_read_only_recovery` | `dispatchEvent` / `setAttribute` 脚本算只读恢复 | 不再算 | 正确——它们是真实变更 |
| 4 | `tool_semantics.tool_is_semantically_neutral` | 同上脚本被判中立、不计分 | 不再中立、照常计分 | 正确，即本次修复的目的 |

`_classify_action_class` **不受影响**：`evaluate` / `run_code` 的判断早于阶段分类，两种情况都
返回 `script_exploration`。

**没有任何既有断言依赖旧（错误）分类**——全量 616 条改动前用例改动后一条不改、全绿。逐条核对
过的三处高危用例：

- `test_replan_fingerprint_allows_a_different_requested_field`：第二次调用的意图确实从
  `script_inspection` 变成 `script_extraction`，但指纹里的 `fields` 是 `title` vs `price`，
  两者本就不同，替代策略仍被放行；
- `test_repeated_replan_denials_become_terminal_after_finite_budget`：`.click(` 两侧都是
  `script_mutation`，逐字不变；
- `test_replan_allows_one_bounded_read_only_evidence_recovery`：`document.title` 两侧都是
  `script_extraction`，逐字不变。

### 3. 不变量守卫：中立 ⊆ 只读恢复（FIX 2）

`runtime._is_read_only_recovery` 有自己的一份 token 列表（`probe` / `find` / `snapshot` /
`screenshot` / `hover` / `handle_dialog` + tabs / evaluate / batch 规则），与
`SEMANTICALLY_NEUTRAL_TOOL_NAMES` 分居两个模块。今天前者是后者的超集，但没有任何东西保证它。

**两个概念不合并**，因为它们本来就不同：

- `browser_tabs(action=select)` **是**只读恢复（不改页面内容），**不**中立（换 url）；
- `browser_drop` **两者都不是**。

只锁住必须成立的那一个方向：`SEMANTICALLY_NEUTRAL_TOOL_NAMES` 里的每个名字都必须让
`BrowserRuntimeRail._is_read_only_recovery(name, {})` 为真。守卫测试
`test_every_semantically_neutral_tool_is_a_read_only_recovery` 参数化跑完整个集合，风格对齐既有的
`test_batch_read_only_ops_stay_a_superset_of_the_runtime_selector_op_sets`；另有
`test_read_only_recovery_is_strictly_wider_than_neutrality` 钉住上面两个反例，防止有人日后把两个
集合"顺手合并"。`_is_read_only_recovery` 的 docstring 点名了这个不变量与守卫测试。

### 4. 不变量守卫：目录驱动的 action-class 表（FIX 3）

`tool_semantics._matches_tool_name` 做的是**精确名 / `.name` 后缀 / `_name` 后缀**匹配，而
`runtime._classify_action_class` 与 `_is_read_only_recovery` 做的是**裸子串包含**。风格不一致会
在未来的命名上炸：`browser_dropdown_select` 会命中 `"drop"` → `file_drop`，
`browser_find_and_click` 会命中 `"find"` → `target_discovery` **并且**被当成只读恢复。

**不整体重写这些子串检查**——其中几处是刻意为之，用来匹配带 vendor 前缀的名字
（`mcp_playwright-official_browser_hover`）。改为加一张守卫表：从 `browser_capabilities` 导入
`CORE_BROWSER_TOOL_NAMES` + `ADVANCED_CODE_BROWSER_TOOL_NAMES` + `UNSAFE_DEV_BROWSER_TOOL_NAMES`，
对每个真实工具名断言 `_classify_action_class(name, {}, _build_phase_state("t"))` 等于表里写死的
期望值，并对 hover / find / handle_dialog / drop 追加 vendor 前缀形式。另有一条
`test_action_class_table_covers_the_whole_catalog` 保证目录新增工具时这张表必须同步扩充。

这样任何未来的命名碰撞都会变成一条失败的断言，而不是一次静默的误分类。

**表里没有发现今天就分类错误的工具**，但记录两条观察（未改动，见「已知遗留」）：
`browser_take_screenshot` 与 `browser_close` 都落到兜底的 `"other"`。

### 5. lab runner 的取值链路（FIX 4，无运行时行为变化）

`run_bu_prompt.py` 此前只从 `BU_QUERY` 环境变量取 prompt。Windows cmd.exe 会在进程启动前就把
含 `& | ^ < >` 或引号的值截断 / 改写——lab-19 与 lab-20 的 prompt 就是这样到达时已经缺了一截。
同时 `conversation_id` 硬编码成 `"bu-lab-20"`，于是每次实验都复用同一个 session id，
session 级 phase state 会跨实验串味。

改为 argparse（无第三方依赖），按优先级取第一个非空来源：

1. `--query-file <path>`（argv）
2. `--query "<text>"`（argv）
3. `BU_QUERY_FILE`（env，路径）
4. `BU_QUERY`（env，正文）
5. 既有默认串

文件按 UTF-8 读、保留换行；路径不存在时 `SystemExit` 且报错文本点名**具体路径与来源**
（`--query-file` 还是 `BU_QUERY_FILE`）。`conversation_id` 取
`--conversation-id` / `BU_CONVERSATION_ID`，缺省为 `bu-lab-<YYYYmmdd-HHMMSS>-<6 位随机>`。
随机尾巴不是装饰：Windows 上的墙钟粒度不足以把连续两次运行分开，而撞了 id 就意味着共享
session 级 phase state（这正是要消除的东西）。既有 banner 保留，新增一行
`query_source=<argv-file|argv|env-file|env|default> query_chars=<n> conversation_id=<id>`——
prompt 被截断在开跑之前就看得见。

agent 构造、`init_model`、`Runner` 用法一字未动。

## 拒绝的方案

1. **保留 `.lower()`、把正则字面量改小写。** 见决策 1：可读性差，且把「必须小写」变成一条
   靠人记忆维持的约定，下一个加分支的人会再犯。
2. **顺手扩充两个正则的分支**（例如补 `scrollIntoView` / `getAttribute`）。修 bug 和改语义是
   两件事。让死分支复活本身就已经改变了一批脚本的分类；同一次改动里再动分支集合，出问题时
   无从二分。
3. **把 `_is_read_only_recovery` 与 `tool_is_semantically_neutral` 合并成一个概念。** 它们回答
   不同的问题（「这次尝试安全吗」 vs 「这次调用能否改变语义 digest」），`browser_tabs(select)`
   与 `browser_drop` 是现成的反例。合并会连带把 tabs(select) 变成中立，把切换标签页造成的
   url 变化从评分里抹掉。
4. **把 `_classify_action_class` / `_is_read_only_recovery` 的子串检查整体换成
   `_matches_tool_name`。** 那会一次性打掉 vendor 前缀匹配（`mcp_playwright-official_browser_hover`
   的后缀是 `_browser_hover`，不是 `_hover`），风险远大于它防住的假想碰撞。守卫表用零行为
   风险换到了同样的回归保护。
5. **给 `run_bu_prompt.py` 加 `python-dotenv` 之外的 CLI 库**（click / typer）。一个实验室脚本
   不值得新增依赖，argparse 够用。

## 验证

- `pytest tests/unit_tests/harness/tools/browser_move -q` → **678 passed, 4 skipped**
  （改动前 616 passed / 4 skipped，净增 62 条 = 参数化展开后的新用例；**无任何既有断言被修改
  或删除**）。
- 新增回归证明：把 `operation_intent` 临时替换回「小写表达式 + 大小写敏感正则」的旧实现后，
  `test_repeated_dispatch_event_script_with_an_unchanged_digest_ends_blocked`、
  `test_dispatch_event_script_is_not_a_read_only_recovery`、
  `test_mixed_case_mutation_markers_classify_as_script_mutation`（dispatchEvent / setAttribute）
  四项**确实失败**；`test_unmarked_script_still_reads_as_a_neutral_inspection` 保持通过。
- `test_browser_replan_gate._mutate_until_blocked` 的 `mutating` 参数改为由
  `tool_is_semantically_neutral` 现算（此前硬编码 `True`）。对既有用例（click / type /
  select_option / press_key，全部非中立）行为逐字不变，但让驱动器**诚实**：一个被误判中立的
  调用在这个驱动器里永远到不了 blocked 终态，回归测试因此才真正有效。
- `ruff check` 六个改动文件全绿。

### 未回归项（逐条确认）

- 中立语义与 F_02 完全一致：hover / find / handle_dialog / snapshot / screenshot / probes
  无条件中立；drop 仍为 mutating；tabs 仅 `action=list` 中立；batch 仅全步只读时中立。
- 阈值一个未动：`_consecutive_no_progress >= 3`、`_STATE_REVISIT_REPLAN_THRESHOLD`、
  `replan_count >= 2`、`_BROWSER_READ_ONLY_RECOVERY_LIMIT`。
- `_is_replan_exempt_tool` 未新增任何条目。
- 首个 blocker token 仍逐字为 `semantic_replan_budget_exhausted`。

## 已知遗留

1. `browser_take_screenshot` 与 `browser_close` 在 `_classify_action_class` 里都落到兜底的
   `"other"`，因而共享同一个 action class（截图之后关闭页面会产生同类指纹）。这是**既有行为**，
   本次只在守卫表里如实记录，未改分类器——改它属于独立的行为变更，需要先确认
   `last_action_class` / `next_action_class` 的下游文本消费方。
2. `_classify_action_class` / `_is_read_only_recovery` 的子串匹配与
   `tool_semantics._matches_tool_name` 的后缀匹配仍是两套风格，靠守卫表而非类型约束维持一致。
   统一它们需要先把 vendor 前缀规范化收敛到一个入口（`canonicalize_playwright_tool_name` 是候选
   落点），不在本次范围内。
3. F_02 已知遗留 2（`browser_move` 缺 spec）与 3（`_BATCH_*_SELECTOR_OPS` 三份 op 集合靠单测维持
   超集关系）本次未处理，仍然成立。
