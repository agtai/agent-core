# 后端任务增强迁移记录

本分支按“先迁移、再分析”建立，不是完成整合或上线的声明。

## 固定版本

- 官方 develop 起点：`cd1dbb6673179562d9eed3d4b9b43710421de4bb`。
- 迁移来源：`e57563a308c8b55f27673e7e04e33de3f9254471`。
- 配对分支：agtai/jiuwenswarm 与 agtai/agent-core 的 `codex/backend-task-enhancement`。

## 本次范围

迁入任务受理、权限和确认、持久命令与派发、Task/Attempt、Work检查点、项目执行及文件效果恢复，以及当前实现需要的类型和适配。

本次不迁入 LiveVoice 前端、Gateway、Realtime Provider、语音总控或媒体连接；基线多模态代码不改。没有部署、启用后台任务入口、调用真实模型或迁移用户数据库。

迁移以保留行为和存储格式为准。兼容类型仍可能包含 voice/P2/P3 名称、响应代次或任务呈现字段；这不代表它们已被认定为最终必要实现。JiuwenSwarm 的 presentation、语音命名的公共契约和受理装配暂保留为任务服务的导入与调用依赖，下一轮需要拆分。本次没有借迁移之机改数据库协议或删兼容数据。

## 验证边界与风险

本次按机械迁移及必要上游合并验证，不引入新的业务分类策略、权限规则或恢复合同。现有目标任务身份、取消竞争、文件效果阶段和旧数据格式保持不变。主要风险是上游接口变化和遗漏依赖。

验收包括：Python解析与实际导入、任务/SQLite/恢复/项目效果相关测试、最新TaskManager回归、冲突部分检查、未修改前端/Gateway/Provider，以及两个源工作区不变。验证结果见下文。

基线语音如何调用公共任务受理、如何只保留一个job/task业务权威、如何替代语音兼容合同，留待下一轮分析。本次不声称这些接缝已经产品化完成。

## 本次验证结果

- `tests/integration_tests/application_tasks`、`tests/unit_tests/core/application/tasks` 和现有 `tests/unit_tests/core/common/test_task_manager.py`：215 项通过，7 条警告。覆盖 SQLite、恢复、文件效果、执行归属与原生 TaskManager 回归。
- 迁移范围的 43 个 Python 文件通过语法解析；Ruff 致命错误检查与 import 顺序检查通过；Git diff 空白检查通过。
- 完整格式和静态检查未全部通过：25 个文件存在格式差异，完整 Ruff 有 10 项提示，另有 codespell 和 Pylint 提示。包含原有 timeout 接口、平台条件导入和分支分析提示。本次未为消除这些提示批量改写迁移代码，后续需单独处理，不能据此宣称完整质量门禁已通过。
- 验证使用现有 Python 3.12 测试环境，没有调用真实模型、部署服务或执行用户数据库升级。

本次共新增应用任务包并修改 5 个公共执行文件。文件清单见 `source-manifest.json`；新增代码不等于最终保留规模。
