# F_04 浏览器子代理的 Jev 决策策略（模型槽位）

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-19 |
| 范围 | `openjiuwen/harness/tools/browser_move/policy/`（新增）、`.../runtime/runtime.py`（`probe_for_policy` / `activate_page`）、`openjiuwen/harness/subagents/browser_agent.py`、`.../backends/browser_use/sidecar/session_adapter.py`（`type_text` 清空目标）、`.../lab/run_jiuwen_jev.py`、`tests/unit_tests/harness/tools/browser_move/` |
| 测试基线 | `pytest tests/unit_tests/harness/tools/browser_move -q` → 改动后 **748 passed, 26 xfailed, 3 warnings in 3.04s**（含本文新增 8 个用例） |
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

`model` 为 `JevDecisionModel` 时：不做温度副本、不注入三个 LLM 专用 ContextProcessor、
`enable_model_anomaly_detection_rail=False`、`bind_runtime(browser_backend)`。声明式路径
（`build_browser_agent_config`）暂不支持，见已知遗留 2。

## 拒绝的方案

1. **`browser_jev_run` 运行时工具**：LLM 仍决定何时委托，每次委托多一回合 LLM。
2. **Jev 作为 `BrowserDriver` 后端**：driver 只有眼与手，等于丢掉 Jev 的决策。
3. **复用 `driver.observe()` / `INTERACTIVE_PROBE_JS`**：见决策 2。
4. **`requestAnimationFrame` 等待**：隐藏标签页永不触发，探测挂到超时。
5. **`inception/mercury-2.5` 作为取值模型**：经本仓库 OpenAI 客户端返回空内容；保留
   `JEV_VALUE_MODEL` 显式开关。

## 验证

- 单测：`test_policy_jev.py`（动作空间/请求形状、非给出编号拒绝、首回合 LLM 取值、缓存取值、
  缓存关闭时预取、非浏览器回合转发）、`test_browser_policy_probe.py`（`probe_for_policy`
  注册目标并可解析为 `SelectorRef`、`activate_page` 切换标签页）。
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

1. 指令文本在 `policy/prompts.py` 常量中，按 `S_06` 应迁到 `harness/prompts/sections/`。
2. 声明式路径（`SubAgentConfig` / `TaskTool` 派生的 browser_agent）没有 `decision_backend`，仍用父模型。
3. 决策客户端直接用 `httpx.AsyncClient`，未走 `core/common/clients` 的连接池。
4. 每次 sidecar `connect()` 在 Chrome 里留下额外标签页。
5. OpenRouter 转发使每次 Jev 约 0.45 s，是当前下限；TypeSafe 直连需邀请。
6. sidecar `type_text` 清空目标的改动无单测（sidecar 在独立 venv 中运行）。
