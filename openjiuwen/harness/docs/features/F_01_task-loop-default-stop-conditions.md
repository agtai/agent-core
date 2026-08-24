# 外层任务循环的默认停止条件

## 元信息

| 项 | 值 |
|---|---|
| 日期 | 2026-08-24 |
| 范围 | 改 `openjiuwen/harness/schema/config.py`（新增 `DeepAgentConfig.task_loop_max_rounds` / `task_loop_timeout_seconds`）、`openjiuwen/harness/deep_agent.py`（`_queue_pending_rails` 自动注入带上边界；新增 `_stop_condition_evaluators()` 与 `_default_task_completion_rail()`；`start()` 兜底 rail 同样带边界）；新增 `tests/unit_tests/harness/test_task_loop_default_bounds.py`；修订 `docs/specs/S_03_task-loop.md` |
| 测试基线 | `tests/unit_tests/harness/test_task_loop_default_bounds.py` 11 passed；连同 `test_loop_coordinator.py` / `test_task_completion_extensions.py` / `test_deep_agent_interaction.py` / `test_deep_agent_rail_event_routing.py` 合计 75 passed |
| Refs | `#1583` |

## 背景

`enable_task_loop` 为真时，内层 `ReActAgent` 拿到 `max_iterations = sys.maxsize`
（`deep_agent.py::_create_react_agent`，热重载路径 `_hot_reload_model` 同）。这是**有意的
分层**：终止权上交外层任务循环，内层不再自行封顶。

分层的另一半没有落地。`_queue_pending_rails` 在启用任务循环时自动注入
`TaskCompletionRail()`——**不带任何参数**。而：

```python
# rails/task_completion_rail.py::build_evaluators
if self.max_rounds is not None:
    result.append(MaxRoundsEvaluator(self.max_rounds))
if self.timeout_seconds is not None:
    result.append(TimeoutEvaluator(self.timeout_seconds))
if self.completion_promise is not None:
    result.append(CompletionPromiseEvaluator(...))
```

三个参数默认全是 `None`，`build_evaluators()` 因此返回**空列表**。再看消费侧：

```python
# task_loop/loop_coordinator.py::should_continue
ctx = self._build_eval_context()
for ev in self._evaluators:      # 空
    ...
return True                       # 恒真
```

于是默认形态下，**内层不封顶（`sys.maxsize`），外层也不封顶（求值器链为空）**。任务循环唯一
的出口是模型自己发出完成信号，或调用方主动 `abort()`。

外部实测（jiuwenswarm）：一次会话连续运行十三分钟未结束，其间 9500 个推理块；另一次同一
工具带同一参数连调四次后模型改走错误路径，写坏了目标文档。

**第二条路径**：交互循环入口 `DeepAgent.start()` 不经过 `_ensure_initialized()`，因此它调用
`prepare_interaction_task_loop()` → `_setup_task_loop()` 构造 `LoopCoordinator` 时，
`_queue_pending_rails` 排队的那份 rail **尚未注册**，`_task_completion_rail` 仍是 `None`。
原实现在这里直接取 `[]`，于是即便自动注入带了边界也拿不到。同一段代码路径也让调用方经
`rails=[...]` 传入的 `TaskCompletionRail` 在该入口上被整个忽略。协调器只构造一次
（后续 `prepare_interaction_task_loop` 复用），空链因此是永久的。

## 数据结构

新增两个配置字段，落在 `DeepAgentConfig`：

| 字段 | 类型 | 默认 | 语义 |
|---|---|---|---|
| `task_loop_max_rounds` | `int \| None` | `100` | 外层轮次上限；`None` 解除 |
| `task_loop_timeout_seconds` | `float \| None` | `3600.0` | 整个任务循环的墙钟上限；`None` 解除 |

作用域限定于**自动注入**的那一份 `TaskCompletionRail`。调用方经 `add_rail()` 或
`create_deep_agent(rails=[...])` 显式传入的 rail 保持自身设置不受影响。

求值时机为**轮次之间**（`should_continue()` 在每轮前后被调用），因此二者都不约束单次模型
调用内部。

## 决策

1. **不动 `sys.maxsize`**。它表达的是"内层不负责终止"这一分层意图，改成有限值等于把终止
   责任重新分给内层，与任务循环的设计相悖。缺陷不在这一行。
2. **在自动注入点补默认边界**。缺陷的准确位置是 `TaskCompletionRail()` 的零参构造：它让
   一个"由外层负责终止"的系统失去了外层。改动落在 `_queue_pending_rails`。
3. **默认值有限而宽松**：轮次 100、墙钟 3600 秒。这两个数不是调优目标，是**兜底**——正常
   任务不该接近它们，触达即意味着已经失控。取值偏大是有意的：过早掐断一个正在推进的任务，
   比让一个卡住的任务多跑一会儿更糟。
4. **保留解除边界的能力**。显式传 `None` 仍得到无限循环，这是调用方的选择；改变的只有
   默认形态。
5. **新配置项走 `DeepAgentConfig`，不加到 `create_deep_agent` 参数表**。遵循 `S_01`
   「新增配置项一律走 `DeepAgentConfig` / Spec 拆分」。工厂以显式 kwargs 构造 config，未传的
   字段自然取 dataclass 默认值，因此工厂无需改动。
6. **求值器解析要看 pending rails**。新增 `DeepAgent._stop_condition_evaluators()`，
   `_task_completion_rail` 为空时回退到 `find_pending_rails_by_type(TaskCompletionRail)`。
   两处 `LoopCoordinator(...)` 构造点都改走它。修在这里而不是在 `start()` 里调整注册顺序：
   决定协调器拿到什么的是这一处，任何入口都要经过它；改 `start()` 的生命周期顺序只覆盖一个
   入口，且要动一段有既有测试锁定的时序。
7. **`start()` 的兜底 rail 也带上同样的边界**。它在 `_task_completion_rail` 为空时注册，之后
   就是该字段的值；留成零参构造会让下一次 session 切换重建协调器时再拿到空链。构造走
   `_default_task_completion_rail()`，其中 `self._deep_config or DeepAgentConfig()`——直接 new
   出来的 `DeepAgent`（不经 `create_deep_agent`）`_deep_config` 为 `None`，在 `start()` 里裸读
   它会在持有 `_interaction_start_lock` 时抛异常，等待方随即永久挂起。

## 拒绝的方案

**A. 把 `sys.maxsize` 改成有限值（如 200）**

最直觉的改法，也是最初的提议。拒绝：`sys.maxsize` 是分层契约的表达而非疏漏。改它会让内层与
外层同时持有轮次上限，语义重叠且互相干扰——两个上限里较小的那个实际生效，而调用方配置的是
另一个。真正失效的是外层，修外层。

**B. 改 `TaskCompletionRail.__init__` 的参数默认值**

把 `max_rounds` / `timeout_seconds` 的默认从 `None` 改成有限值，自动注入处不动。拒绝：该 rail
是公开可构造的类，显式 `TaskCompletionRail()` 是合法用法，调用方可能正是想要一个只靠
completion promise 停止的循环。改类默认值会波及所有显式构造点，超出缺陷范围。

**C. 在模型调用层加墙钟或输出 token 上限**

能覆盖"单次生成不返回"这一形态——本次两个求值器都覆盖不到它。拒绝：不属于任务循环的职责
边界（`S_03` 范围外），且与模型侧既有的 `ModelConfig.max_tokens` 重叠。列入「已知遗留」。

**D. 只加轮次上限，不加墙钟**

diff 更小。拒绝：轮次与时间约束的是不同失效形态。一个每轮都很快但永远不收敛的循环会撞上轮次
上限；一个轮次不多但每轮极慢的循环只会撞上墙钟。两者互不替代。

## 验证

```
tests/unit_tests/harness/test_task_loop_default_bounds.py    10 passed
tests/unit_tests/harness/test_loop_coordinator.py            \
tests/unit_tests/harness/test_task_completion_extensions.py  / 37 passed
```

前六项覆盖注入路径：零参构造的 rail 产出空求值器链（缺陷形状本身）、空链的
`LoopCoordinator` 千轮不停、配置默认值有限且为正、默认值确实到达求值器链（而非只停在 rail
属性上）、轮次上限端到端生效并记录 `stop_reason`、显式 `None` 仍可解除。

后五项覆盖交互路径：`start()` 时机上 rail 确实还在 pending、pending rail 仍能给出边界、
调用方经 `rails=` 传入的 rail 优先于默认值、关闭任务循环时空链是正确结果（内层此时自带
`max_iterations=15`）、未配置的 `DeepAgent` 也能构造出带边界的兜底 rail。

回归面：`test_deep_agent_interaction.py`（34）与 `test_deep_agent_rail_event_routing.py`（3）
一并跑绿。前者是这次改动的实际把关者——`start()` 里裸读 `_deep_config` 的中间版本让
`test_stop_waits_for_in_progress_start_setup[register]` 永久挂起，而不是报错。

变异验证两轮：把两个配置默认值改回 `None`，10 项中 2 项失败；把
`_stop_condition_evaluators()` 退回只读 `_task_completion_rail`，另 2 项失败。恢复后全绿。

## 已知遗留

1. **单次生成内的失控不在本次覆盖内**。两个求值器都在轮次之间求值，一次不返回的模型调用不会
   让循环获得求值机会。该形态需要模型侧输出 token 上限（`ModelConfig.max_tokens`，框架已支持
   但默认为 `None`），或流式层的墙钟。属于另一层的问题，建议单独评估。
2. **重复工具调用的检测未涉及**。identity 提示词（`prompts/sections/identity.py`）已含「切勿
   使用相同参数重复调用同一工具」，实测未能阻止；机制层的检测能力存在于 `enterprise-dev` 的
   `agent_ras`（`RepeatToolCallDetector`），但 `develop` 上没有。两条线如何合并需维护者定夺。
3. **`TokenBudgetEvaluator` 两端都是断的**。`schema/stop_condition.py:109` 已实现该求值器，但
   `TaskCompletionRail.build_evaluators()` 从不构造它，调用方只能经 `evaluators=[...]` 逃生口
   传入；更关键的是 `LoopCoordinator.add_token_usage()`（`loop_coordinator.py:86`）**没有生产
   调用方**，`ctx.token_usage` 恒为 0。只补配置面会得到一个恒不触发的上限。本次未做：要先定
   记账口径（是否含推理 token、子代理是否计入），且 `goal/evaluation.py:299` 已有一份作用域限
   于 goal 轮的 token 预算，两者是并存还是收敛需维护者定夺。
