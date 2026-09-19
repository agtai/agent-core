# F_04 浏览器子代理的 Jev 决策策略（模型槽位）

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-19 |
| 范围 | `openjiuwen/harness/tools/browser_move/policy/`（新增）、`.../runtime/runtime.py`（`probe_for_policy` / `activate_page`）、`openjiuwen/harness/subagents/browser_agent.py`、`openjiuwen/harness/schema/decision_policy.py`（新增，review 修订）、`.../backends/browser_use/sidecar/session_adapter.py`（`type_text` 清空目标）、`.../lab/run_jiuwen_jev.py`、`tests/unit_tests/harness/tools/browser_move/` |
| 测试基线 | `pytest tests/unit_tests/harness/tools/browser_move -q` → 改动后 **748 passed, 26 xfailed, 3 warnings in 3.04s**（含本文新增 8 个用例）；`fix/jev-policy-review` 审查修复后 **761 passed, 26 xfailed**（另增 14 个用例，覆盖 B1-B6，见下方决策 8-10）；WAIT 就地稳定优化后 **770 passed, 26 xfailed**（另增 9 个用例，覆盖决策 11-12） |
| 关联 feature | [[F_02_browser-semantic-neutrality]]（同一子系统；本文不改语义中立判定） |
| 关联 spec | [[S_18_subagents-and-lifecycle]]（不变量 6 增补：策略模型直通） |

## 背景

浏览器子代理的每一步都由一次聊天模型回合决定：模型读取 12k 字符的 `<browser_state>`，再以自由文本生成
工具调用 JSON。实测每步数秒，且模型可能产出未注册的目标。TypeSafe 的 Jev 是结构化决策模型：一次请求
带 `state` 与若干 `choice` 问题，返回每题的选项、概率分布与置信度，不生成文本，OpenRouter 上
（`typesafe/jev-1.13`，`POST /api/alpha/decisions`）实测每次约 0.45～0.6 s，且只能选给出的编号。

目标：**jiuwen 是 agent（手），Jev 是脑，browser-use 是 driver。** 参照物是 Browser Use 自家的
jev-ultrafast（同一 Google Flights 目标：其文档 7.1 s / 17 次 Jev 请求 / 10 次交互；在本仓库 driver 上
用其原始循环复现为 25.5 s / 33 次请求，含绕路）。

## 决策

### 1. Jev 接在 `Model` 槽位，而不是新增工具或 rail

`policy/jev_decision_model.py:JevDecisionModel(Model)`：浏览器回合（工具表含 `browser_click`）时
探测页面、问一次 Jev、返回**恰好一个** `browser_*` 工具调用；其它调用（摘要、字段取值）转给被包装的
聊天模型。`DeepAgent` / `ReActAgent` / rails / checkpoint / 权限引擎全部不动；Jev 与 LLM 按配置互换。

### 2. 策略自有的页内探测，元素以 stamp 选择器注册为 PageState 目标

`policy/probe_js.py`：一次 `evaluate` 完成"等待稳定 + 快照"。每个可见控件打上
`data-openjiuwen-jev=<id>` 属性，作为 `selector_hint_validated=True, match_count=1` 的唯一选择器交给
`PageState.register_interactives`，因此 `browser_click(target_id)` 走既有的目标校验与 `IndexRef`/
`SelectorRef` 解析。不用 `driver.observe()`：实测 85～156 ms 且 `<input>` 的 `value` 为空；不复用
`INTERACTIVE_PROBE_JS`：40 条上限，且其选择器唯一性启发式会丢掉 Google 的大部分控件。

### 3. 先稳定、后决策

上游循环在页面变动时立即决策再重试，实测 33 次请求中 12 次被丢弃。本探测在 `readyState`、
DOM 静默窗口（60 ms，上限 500 ms）与自动补全选项渲染（最多 900 ms，全文档 `[role=option]`）之后才
返回。全部用 `setTimeout`，不用 `requestAnimationFrame`（后台标签页不触发），并带兜底 resolver
（否则未决 Promise 会撞上 driver 30 s 超时）。

### 4. 隐藏标签页先前置

browser-use 以后台标签页驱动页面，`document.visibilityState == "hidden"` 时定时器节流到约 1 s、
下拉菜单不渲染，策略会反复点击同一控件。`runtime.activate_page(url)` 通过 `driver.switch_tab`
前置该标签页，探测报告 hidden 时调用一次。

### 5. 输入值：按字段从页面上下文后台预取，目标值缓存可选

`TYPE_TEXT` 的字符串由聊天模型根据目标、字段、页面文本与历史生成（与上游 `field_text` 同源），
但在探测到可编辑字段时即在后台发起，实测两处输入等待 0 ms。`goal_value_cache=True` 时另加一个
`text_value` 选择题，让 Jev 从目标中抽取的候选值里挑；默认关闭，避免"值事先已知"的争议。

### 6. driver 缺陷修复：清空的是获得焦点的输入框

`type_text(clear=True)` 先点击再清空**原节点**；Google Flights 的点击把焦点移进对话框里预填了
"Tokyo" 的副本输入框，结果输入成 "TokyoZurich"，无补全项。改为清空 `document.activeElement`
（可编辑时），这是所有绕路的根因，LLM 路径同样受益。

### 7. 策略模型直通 `create_browser_agent`

`model` 满足 `openjiuwen/harness/schema/decision_policy.py:DecisionPolicyModel` 结构化协议
（暴露 `bind_runtime(runtime)`）时：不做温度副本、不注入三个 LLM 专用 ContextProcessor、
`enable_model_anomaly_detection_rail=False`、`bind_runtime(browser_backend)`。装配层按结构
而非具体类判定（决策 8），`JevDecisionModel` 只是当前唯一实现。声明式路径
（`build_browser_agent_config`）暂不支持，见已知遗留 2。

### 8. 策略模型的类型契约是结构化 Protocol，不是具体类（review, `fix/jev-policy-review`）

`harness/subagents` 是通用装配层，不应为了识别一个模型槽位而 `import` 某个具体策略实现——
那样会把 `httpx` 依赖的策略模块拖进每一次浏览器子代理构造的导入图，也让新增第二种决策策略
必须回来改 `browser_agent.py`。`openjiuwen/harness/schema/decision_policy.py` 定义
`@runtime_checkable class DecisionPolicyModel(Protocol)`，只要求一个 `bind_runtime(runtime)`
方法；`browser_agent.py` 改用 `isinstance(model, DecisionPolicyModel)` 判定，不再 import
`JevDecisionModel`。`JevDecisionModel` 无需显式继承该协议，结构上已经满足。

### 9. 每个任务是一个隔离的 `_Run`，绝不跨任务共享可变状态（review, `fix/jev-policy-review`）

`create_browser_agent` 按 agent 构造一次 `JevDecisionModel`，但一个 agent 会服务多个任务。
早期实现把 goal / history / pending / prefetched 等状态直接放在模型实例上：第二个任务会静默
继承第一个任务的目标与历史，`_consecutive_waits`/`_tick` 跨任务累积耗尽等待预算，陈旧的
`_pending` 与新页面的 `page_key` 比对出虚假的 `page_changed`。`policy/jev_decision_model.py:_Run`
把这些字段收进一个 dataclass；模型至多持有一个当前 run。新 run 的判定是"没有 run，或这次
`_goal_from(messages)` 算出的目标和当前 run 不同，或当前 run 已经被 `_final()` 标记为
`finished`"。起新 run 时先取消上一个 run 的 `values_task` 与全部 `prefetched` 任务再丢弃它，
不让后台工作跨任务泄漏成"Task exception was never retrieved"噪音。`prefetched` 的 key 额外
并入探测快照的 `page_key`，使一个页面生成的字段值不会被下一页的同名字段复用，页面切换时
上一页遗留的 prefetch 任务同样被取消。`ticks` / `started_at` 保留为公开只读 property，委托给
当前 run；`report()` 的键形状不变（`lab/run_jiuwen_jev.py` 消费它）。

同批把决策客户端从 `self._client` 改名为 `self._decisions`：`Model.__init__` 已经把
`self._client` 建成一个真正挂了遥测的 `BaseModelClient`，旧代码在 `super().__init__()` 之后
立刻把它整个换成 `JevDecisionsClient`，任何继承来的、触碰 `self._client` 的方法（图像/语音/
视频生成、KV-cache 亲和性探测）都会在类型不对的对象上操作。

### 10. 探针与决策端点的失败必须降级为 `BLOCKED`，不得让整个回合崩溃（review, `fix/jev-policy-review`）

`BrowserAgentRuntime.probe_for_policy` 原先对 `_evaluate_page_js` 的异常没有任何防护，一次
页面脚本报错（探测帧被卸载、driver 抖动、导航中途求值）会直接从 `Model.invoke` 里抛出去，
杀掉整个 agent 回合——即便调用方 `_probe()` 早就按"探针可能报 error"的信封形状写好了消费
逻辑。现在它用与同类 `INTERACTIVE_PROBE_JS` 路径一致的失败信封包住：
`{"ok": False, "error": ..., "elements": [], "page_state": ...}`，`asyncio.CancelledError`
照常传播、不被吞掉。`build_action_space` 对空 `elements` 天然只产出 `WAIT`/`DONE`/`BLOCKED`
三个控制操作，tick 循环因此能正常收敛到 `BLOCKED` 而不是抛异常或原地打转。

决策端点一侧同理：`decide` + `interpret` 现在包在 `try/except BaseError` 里，失败（连接错误、
HTTP 错误、重试耗尽、概率分布校验失败）时记 warning 并返回 `_final(run, "BLOCKED", ...)`，
调用方仍能拿到 url/title/steps/page_text 等可用的终态摘要，而不是收到一个模型层崩溃。
`JevDecisionsClient` 里把 HTTP 错误消息中截断的 `response.text[:300]` 整段删掉，只保留状态码
——避免把响应体（可能带请求回显或账号信息）写进日志。

### 11. WAIT 就地稳定，不再为它多付一次决策请求（`fix/jev-policy-review`）

`_decide_message` 每轮都要探测一次页面、再问一次 Jev。分类器答 `WAIT` 时，旧代码原地
`continue`——立即重新探测（仍旧封顶 500 ms 的默认稳定窗口）、再付一次决策请求（经 OpenRouter
代理实测中位 ~484 ms）。但 `WAIT` 的语义就是"页面还没就绪"，而**分类器在页面变化之前不可能
给出不同的答案**——在同一张未变的页面上再问一次，纯粹是丢进水里的延迟；页内再探测一次几乎
不花钱（`POLICY_PROBE_JS` 本就有一条 DOM 静默窗口 + 硬上限的稳定逻辑，`settle_ms` 参数
`probe_js.py:20` 早就读了，只是 Python 侧从未真正传过更大的值）。

`_probe` 因此加一个关键字参数 `settle_ms`（决策 2b），`_decide_message` 把首次探测挪到循环外，
`WAIT` 分支不再 `continue` 去重新决策，而是调用新的 `_settle_wait` 辅助方法：用**加倍**的
`settle_ms`（从 `PROBE_SETTLE_MS` 起，封顶 `MAX_PROBE_SETTLE_MS`）反复原地探测，直到探测到的
`page_key` 与分类器刚看到的那次不同，或整条 WAIT 连击的页内等待预算 `WAIT_SETTLE_BUDGET_MS`
用尽。只有前者把控制权交回外层循环让分类器**再问一次**；后者是终态（决策 12）。

比较新鲜度用 `snapshot["page_key"]`，不用 `generation_id` 或 `marker`——`page_key` 已经是
`_probe`（决策 9 的 `page_changed` 判定）与 `_field_key`（prefetch 隔离）在用的窄语义键，
三处共享同一个"页面变没变"的定义，不再另发明一套。

### 12. 两个独立的界限：决策请求数与页内等待的墙钟预算（`fix/jev-policy-review`）

`MAX_CONSECUTIVE_WAITS`（决策请求计数）与 `WAIT_SETTLE_BUDGET_MS`（页内墙钟预算）互不替代，
都必须保留：一个带 CSS 动画或走秒表的页面永远不会 DOM 静默，`awaitQuietDom` 的静默窗口永远
不会触发，每次 settle 探测都会烧光它的 `settle_ms` 硬上限——如果只有前者，`_settle_wait` 会在
一次 `WAIT` 判决内无限重探测，`run.consecutive_waits` 却因为内层循环不递增它而完全看不到这
个情况。`_settle_wait` 内层循环因此**不**触碰 `run.consecutive_waits`：它只被外层每次真正问过
分类器后递增，继续充当"总共能忍受几次 WAIT 判决"的天花板。

**墙钟预算按整条 WAIT 连击计，不按单次判决计**，记在 `_Run.settle_spent_ms` 上，与
`consecutive_waits` 同时清零（真正动作之后）。初版把它做成 `_settle_wait` 的局部变量，于是每
次 `WAIT` 判决都重新发一份完整预算：一个永不 DOM 静默的页面会把 3 s 预算乘上
`MAX_CONSECUTIVE_WAITS + 1` 次判决，到 BLOCKED 的时间从 ~3.8 s 涨到 ~21 s——这条路径正是优化
前最快的那条（卡死页面原本只花几次决策请求就放弃），反而被"优化"成了最慢的。

预算耗尽因此是**终态**（`_settle_wait` 返回 `progressed=False`，调用方直接 `BLOCKED`），不再
交回分类器多问一次：被问的是一张 `page_key` 与上次逐字相同的快照，历史也没变，换来的只会是
同一个 `WAIT`，代价却是一整次决策请求。区分"还在加载"与"真的卡住了"的判据因此从"再问一次
分类器"换成"给足 3 s 页内等待后交互表面是否动过"——后者不花钱，且对这个问题是更直接的证据。

三个新常量（`policy/jev_decision_model.py`，紧邻 `MAX_CONSECUTIVE_WAITS`）：

| 常量 | 值 | 编码的约束 |
|---|---|---|
| `PROBE_SETTLE_MS` | 500 | 必须等于 `probe_js.py` 的 JS 默认值，不升级时探测耗时不变 |
| `MAX_PROBE_SETTLE_MS` | 1500 | `load(3s) + settle + 1s` 的 JS 兜底 resolver 必须比 driver 请求超时至少低 1s |
| `WAIT_SETTLE_BUDGET_MS` | 3000 | 整条 WAIT 连击允许花费的页内等待总量（`_Run.settle_spent_ms`），用完即 BLOCKED |

`MAX_PROBE_SETTLE_MS` 的推导：`backends/browser_use/transport.py:30` 的 `_REQUEST_TIMEOUT_S = 30.0`
是 `evaluate()` 走 sidecar wire 调用时的有效超时（`transport.py:193`，`_evaluate_page_js`
（`runtime/runtime.py:2293-2297`）没有传自己的 timeout，用的就是这个默认值）。JS 侧的
`lastResort` 兜底 resolver 在 `load_timeout_ms + settle_ms + 1000`（`probe_js.py:214`）后必然
resolve；用默认参数算出的 4500 ms 已经在正常工作，说明 30 s 的余量远超需要。理论上
`settle_ms` 可以升到 `30000 - 3000(load) - 1000 - 1000(安全边界) = 25000 ms` 而不撞超时，
但选 1500 ms 是为了让 `WAIT_SETTLE_BUDGET_MS=3000ms` 的加倍序列（500 → 1000 → 1500 封顶）
在正常测试和实跑中就能触发封顶行为，同时仍然留了远超所需的安全边际——不是把余量榨干。

### 13. B4/B5 两个残留缺口的补完（`fix/jev-policy-review`，同批 WAIT 优化触及了同一批函数时发现）

`JevDecisionsClient.decide` 以 `return response.json()` 收尾：一个 200 状态码但非 JSON 的响应体
会抛 `json.JSONDecodeError`（继承自 `ValueError`），既不是 `BaseError` 也不是 `httpx.HTTPError`，
逃出决策 10 加的 `except BaseError`，直接崩掉整个回合——这正是 B4 想关掉的失败类别。现在
`response.json()` 包进 `try/except json.JSONDecodeError`，转成与 HTTP 错误路径同样的
`MODEL_CALL_FAILED`，且不把原始响应体带进错误消息（呼应决策 10 去掉 `response.text[:300]`
的理由：避免请求回显或账号信息进日志）。

`run.values_task.result()` 在 `values_task.done()` 为真时被无保护调用；`done()` 对"以异常结束"的
任务同样为真，`.result()` 会把那个异常重新抛出到一条没有 handler 的路径上。B5 给 `_value_for`
里 `await task` 的 prefetch 分支加了 `try/except`，但漏了这一处；只有 `--goal-value-cache`（即
variant-B 实验配置）才会触达。现在同样降级为空值列表并 `logger.warning`，与 `_value_for` 的
prefetch 失败路径行为一致。

两处都是 B4/B5 已关闭功能内部的完成度补丁，不是新范围；改动各一行，随 WAIT 优化一起被发现。

## 拒绝的方案

1. **`browser_jev_run` 运行时工具**：LLM 仍决定何时委托，每次委托多一回合 LLM。
2. **Jev 作为 `BrowserDriver` 后端**：driver 只有眼与手，等于丢掉 Jev 的决策。
3. **复用 `driver.observe()` / `INTERACTIVE_PROBE_JS`**：见决策 2。
4. **`requestAnimationFrame` 等待**：隐藏标签页永不触发，探测挂到超时。
5. **`inception/mercury-2.5` 作为取值模型**：经本仓库 OpenAI 客户端返回空内容；保留
   `JEV_VALUE_MODEL` 显式开关。
6. **WAIT 只在下一轮循环里升级 `settle_ms`，但仍照常付决策请求**（`fix/jev-policy-review`）：
   评估过一个改动更小的方案——保留 `WAIT` 触发 `continue` 重新决策的循环结构不变，只是把每次
   探测的 `settle_ms` 逐轮加倍，让探测本身多等一会儿，但每轮仍然照常再付一次决策请求。拒绝
   的理由：**它只挽回了浪费的一部分**——被浪费的是那次请求本身（~484 ms 中位），不是探测
   多等的那一小段稳定窗口。这个变体仍然在同一张未变的页面上反复问分类器，只是把探测窗口挪
   大了一点，对"决策请求次数随等待轮数线性增长"这个核心问题毫无改善；本文选择的方案（决策
   11-12）把等待完全挪进探测（零决策请求），只在页面真的变化或预算耗尽时才交还给分类器。

## 验证

- 单测：`test_policy_jev.py`（动作空间/请求形状、非给出编号拒绝、首回合 LLM 取值、缓存取值、
  缓存关闭时预取、非浏览器回合转发；`fix/jev-policy-review` 新增 run 隔离、`_client`/`_decisions`
  分离、探针失败降级、决策失败降级、prefetch 跨页隔离、`DecisionPolicyModel` 协议满足性；
  WAIT 就地稳定优化新增 `TestJevWaitCollapsesIntoInPageSettling`——WAIT 被探测吸收而非走
  决策请求线、`settle_ms` 逐次加倍并封顶、永不变化的页面在两个预算内收敛到 `BLOCKED`、
  非-WAIT 路径逐字不变、`page_changed` 记账在重构后仍然正确；`TestJevDecisionsClientMalformedBody`
  与 `TestJevGoalValueExtractionDegradesGracefully`——决策 13 补完的两个 B4/B5 残留缺口）、
  `test_browser_policy_probe.py`（`probe_for_policy` 注册目标并可解析为 `SelectorRef`、
  `activate_page` 切换标签页、探针求值异常时返回失败信封）。
- lab：`.venv/bin/python -m openjiuwen.harness.tools.browser_move.lab.run_jiuwen_jev`，需 `.env` 中
  OpenRouter key（`TYPESAFE_API_KEY` / `TYPESAFE_API_URL` / `TYPESAFE_MODEL`）、`BROWSER_CDP_URL`、
  `.venvs/browser-use` sidecar，以及以 `--remote-debugging-port=9222` 启动的 Chrome（每次计时前换新
  profile，Google 会记住上次搜索）。

| 运行（Zurich→London 单程，2026-09-20） | 总时长 | Jev 请求 | 交互 | 等待 | Jev 中位 | 结果 |
|---|---|---|---|---|---|---|
| jev-ultrafast 原始循环 + 本仓库 driver | 25.5 s | 33 | 21 | 0 | ~450 ms | done，绕路 9 步 |
| jev-ultrafast 文档（TypeSafe 直连） | 7.1 s | 17 | 10 | 1 | 178 ms | done |
| 本文，目标值缓存开（run 5） | 11.6 s | 14 | 10 | 3 | 543 ms | completed |
| 本文，缓存关 + 预取（run 10） | 12.2 s | 14 | 10 | 3 | 484 ms | completed |
| 本文，WAIT 就地稳定（`fix/jev-policy-review`） | pending | pending | pending | pending | pending | 未获得可用凭据，见已知遗留 7 |

总时长含约 2.6 s 的导航与页面加载；每步约 0.69 s = Jev 0.48 s + 探测 0.15 s + 循环与动作 0.06 s。

## 已知遗留

审查关闭（`fix/jev-policy-review`）：每任务状态复用（B1）、决策客户端覆盖 `Model._client`
（B2）、`probe_for_policy` 无失败信封（B3）、决策端点失败直接抛出且日志回显响应体（B4，
决策 10 + 决策 13 补完的 `response.json()` 分支）、预取值跨页泄漏且失败会崩溃（B5，决策 10 +
决策 13 补完的 `values_task.result()` 分支）、子代理装配层硬依赖具体策略类（B6）——机制见
决策 8-10 与 13，接口约束见 `S_18` 不变量 6。WAIT 就地稳定优化见决策 11-12。以下为仍然打开
的遗留项：

1. 指令文本在 `policy/prompts.py` 常量中，按 `S_06` 应迁到 `harness/prompts/sections/`。
2. 声明式路径（`SubAgentConfig` / `TaskTool` 派生的 browser_agent）没有 `decision_backend`，仍用父模型。
3. 决策客户端直接用 `httpx.AsyncClient`，未走 `core/common/clients` 的连接池。
4. 每次 sidecar `connect()` 在 Chrome 里留下额外标签页。
5. OpenRouter 转发使每次 Jev 约 0.45 s，是当前下限；TypeSafe 直连需邀请。
6. sidecar `type_text` 清空目标的改动无单测（sidecar 在独立 venv 中运行）。
7. WAIT 就地稳定优化未做实跑 A/B：`TYPESAFE_API_KEY` 在本地存在，但决策端点对当前凭据返回
   401，判断是基础设施/凭据问题而非代码缺陷，不在本轮范围内排查。上表的 pending 行只由单测
   验证过（`TestJevWaitCollapsesIntoInPageSettling` 断言探测调用次数多于决策调用次数），实际
   墙钟收益需要凭据可用后补测，不得用现有行数做算术推出。
8. `_decide_message` 循环体后仍留着一条 `return self._final(run, "BLOCKED", snapshot, "waited
   without progress")`（紧跟 `for` 循环之后）。这段代码不可达：循环唯一的非 `return` 出口是
   `WAIT` 分支的 `continue`，而该分支自身已经在 `run.consecutive_waits > MAX_CONSECUTIVE_WAITS`
   时提前 `return`，所以最后一次迭代必然从循环内部返回。发现于本轮但未清理——它先于 WAIT 优化
   存在，清理它是与本次改动无关的独立小重构，不在本轮范围内。
9. 预算耗尽即 BLOCKED（决策 12）放弃了"分类器第二次可能改口"这一分支。论据是同一张
   `page_key` 未变的快照 + 未变的历史只会换来同一个 `WAIT`；若实跑中观察到分类器对逐字相同的
   输入给出不同判决（采样温度、服务端版本漂移），这个假设就不成立，届时应把终态改回"再问一
   次，仅当第二次仍是 `WAIT` 才 BLOCKED"。当前无凭据可实测（同遗留 7），故按确定性假设实现。
