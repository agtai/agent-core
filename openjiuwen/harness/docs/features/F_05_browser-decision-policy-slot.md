# F_05 浏览器子代理的决策策略槽位

## 元信息

| 项 | 值 |
|---|---|
| 类型 | feature |
| 日期 | 2026-09-19 |
| 范围 | `openjiuwen/harness/schema/decision_policy.py`（新增）、`openjiuwen/harness/subagents/browser_agent.py`（`create_browser_agent` 直通）、`openjiuwen/harness/tools/browser_move/playwright_runtime/runtime.py`（`probe_for_policy` / `activate_page`）、`tests/unit_tests/harness/tools/browser_move/` |
| 测试基线 | 见「验证」 |
| 关联 spec | [[S_18_subagents-and-lifecycle]]（不变量 6 增补） |
| Refs | 首个仓库外实现：agtai/jiuwen-jev（结构化决策模型接入 `model` 槽位） |

## 背景

浏览器子代理的每一步由一次聊天模型回合决定：模型读取 `<browser_state>` 文本，再以自由文本生成
工具调用 JSON，且可能命名未注册的目标。结构化决策模型换一种方式：一次请求带页面状态与若干选择题，
返回选项编号，几百毫秒内给出下一步，且只能选给出的编号。要让这类模型接到 `model` 槽位，装配层需要
三件事：识别它、把运行时交给它、不再把 LLM 专用的处理器套在它身上。本次只落地这三个槽位；具体策略
实现（HTTP 客户端、页面探测脚本、提示文本）在仓库外。

## 数据结构

- `DecisionPolicyModel`：`@runtime_checkable` Protocol，仅要求
  `bind_runtime(runtime: BrowserAgentRuntime) -> None`。
- `BrowserAgentRuntime.probe_for_policy(source, params) -> dict`：`source` 为 `(params) => ...` 页面
  函数，`params` 为其唯一实参。成功时返回脚本结果，`elements` 每项获得 `target_id`，字典获得
  `generation_id`；失败返回 `{"ok": False, "error": <str>, "elements": [], "page_state": <dict>}`。
- `BrowserAgentRuntime.activate_page(url) -> bool`。

## 决策

1. **协议而非基类**。装配层按结构判定（`isinstance(model, DecisionPolicyModel)`），不 import 具体
   实现：否则策略的 HTTP 依赖会进入每次浏览器子代理构造的导入图，且新增第二种策略必须回来改
   `browser_agent.py`。
2. **策略模型原样使用**。不做温度副本（策略没有采样温度）；不注入三个 LLM 专用的
   ContextProcessor（它们生成面向聊天模型的 `<browser_state>` 文本与工具结果窗口）；
   `enable_model_anomaly_detection_rail=False`（该 rail 按输出文本重复判定异常，对结构化决策无
   意义）。调用方仍可通过 `rails=` 自带 rail。
3. **探测复用 `_execute_probe_json`**。策略函数包成
   `async (page) => await page.evaluate(source, params)` 交给既有代码执行器；结果经
   `_annotate_probe_generation` 与 `PageState.register_interactives` 注册，因此 `browser_click(target_id)`
   等既有工具的目标校验与代际检查不变。元素的 `selector_hint_validated=True, match_count=1` 由策略
   自己保证（它给每个控件打唯一属性）。
4. **`activate_page` 用 Playwright 的 `bringToFront()`**。后台标签页把定时器节流到约 1 s 且不渲染
   下拉菜单，靠新探测决策的策略会反复点同一控件。`url` 用于在同一 context 内挑选目标页，否则前置
   当前页。
5. **失败信封而非异常**。探测脚本错误、代码执行器未就绪都返回结构化失败，策略据此降级（例如判
   BLOCKED），子代理回合不崩溃。`asyncio.CancelledError` 照常上抛。

## 拒绝的方案

- **新增工具或 rail 承载策略**：策略要替代的是"决定下一步"本身，只有 `model` 槽位在那个位置；工具
  与 rail 只能在回合前后旁路。
- **装配层 import 具体策略类判定**：见决策 1。
- **同步支持声明式路径**（`build_browser_agent_config` / `SubAgentConfig`）：该路径没有放置策略实例
  的位置，留作已知遗留。

## 验证

- `tests/unit_tests/harness/tools/browser_move/test_browser_policy_probe.py`：探测元素注册为目标并可解析
  为 CSS 选择器、脚本收到 `source` 与 `params`；脚本异常与执行器未就绪的失败信封；`activate_page` 的
  前置脚本与失败返回。
- `tests/unit_tests/harness/tools/browser_move/test_create_browser_agent.py`：结构化判定；策略模型直通
  （同一对象、`bind_runtime` 收到运行时、异常检测 rail 关闭、不注入 ContextProcessorRail）。
- 基线：`pytest tests/unit_tests/harness/tools/browser_move -q` → 改动后 **539 passed, 2 warnings in 1.95s**（含本文新增 7 个用例）。

## 已知遗留

1. 声明式路径（`SubAgentConfig` / `TaskTool` 派生的 browser_agent）没有放置策略实例的位置，仍用父模型。
2. `probe_for_policy` 每次调用与 `probe_interactives` 一样写一份 audit artifact；高频策略探测下需要
   评估目录增长。
