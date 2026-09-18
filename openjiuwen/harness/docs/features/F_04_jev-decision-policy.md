# F_04 浏览器子代理的 Jev 决策策略（模型槽位）

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-19 |
| 范围 | `openjiuwen/harness/tools/browser_move/policy/`（新增）、`.../runtime/runtime.py`（`probe_for_policy` / `activate_page`）、`openjiuwen/harness/subagents/browser_agent.py`、`openjiuwen/harness/schema/decision_policy.py`（新增，review 修订）、`.../backends/browser_use/sidecar/session_adapter.py`（`type_text` 清空目标）、`.../lab/run_jiuwen_jev.py`、`tests/unit_tests/harness/tools/browser_move/` |
| 测试基线 | `pytest tests/unit_tests/harness/tools/browser_move -q` → 改动后 **748 passed, 26 xfailed, 3 warnings in 3.04s**（含本文新增 8 个用例）；`fix/jev-policy-review` 审查修复后 **761 passed, 26 xfailed**（另增 14 个用例，覆盖 B1-B6，见下方决策 8-10） |
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

## 拒绝的方案

1. **`browser_jev_run` 运行时工具**：LLM 仍决定何时委托，每次委托多一回合 LLM。
2. **Jev 作为 `BrowserDriver` 后端**：driver 只有眼与手，等于丢掉 Jev 的决策。
3. **复用 `driver.observe()` / `INTERACTIVE_PROBE_JS`**：见决策 2。
4. **`requestAnimationFrame` 等待**：隐藏标签页永不触发，探测挂到超时。
5. **`inception/mercury-2.5` 作为取值模型**：经本仓库 OpenAI 客户端返回空内容；保留
   `JEV_VALUE_MODEL` 显式开关。

## 验证

- 单测：`test_policy_jev.py`（动作空间/请求形状、非给出编号拒绝、首回合 LLM 取值、缓存取值、
  缓存关闭时预取、非浏览器回合转发；`fix/jev-policy-review` 新增 run 隔离、`_client`/`_decisions`
  分离、探针失败降级、决策失败降级、prefetch 跨页隔离、`DecisionPolicyModel` 协议满足性）、
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

总时长含约 2.6 s 的导航与页面加载；每步约 0.69 s = Jev 0.48 s + 探测 0.15 s + 循环与动作 0.06 s。

## 已知遗留

审查关闭（`fix/jev-policy-review`）：每任务状态复用（B1）、决策客户端覆盖 `Model._client`
（B2）、`probe_for_policy` 无失败信封（B3）、决策端点失败直接抛出且日志回显响应体（B4）、
预取值跨页泄漏且失败会崩溃（B5）、子代理装配层硬依赖具体策略类（B6）——机制见决策 8-10，
接口约束见 `S_18` 不变量 6。以下为仍然打开的遗留项：

1. 指令文本在 `policy/prompts.py` 常量中，按 `S_06` 应迁到 `harness/prompts/sections/`。
2. 声明式路径（`SubAgentConfig` / `TaskTool` 派生的 browser_agent）没有 `decision_backend`，仍用父模型。
3. 决策客户端直接用 `httpx.AsyncClient`，未走 `core/common/clients` 的连接池。
4. 每次 sidecar `connect()` 在 Chrome 里留下额外标签页。
5. OpenRouter 转发使每次 Jev 约 0.45 s，是当前下限；TypeSafe 直连需邀请。
6. sidecar `type_text` 清空目标的改动无单测（sidecar 在独立 venv 中运行）。
