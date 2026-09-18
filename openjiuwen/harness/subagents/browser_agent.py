# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Factory helpers for the browser subagent."""

from __future__ import annotations

import copy
import dataclasses
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from openjiuwen.core.common.logging import logger
from openjiuwen.core.context_engine import ToolResultWindowProcessorConfig
from openjiuwen.core.foundation.llm.model import Model
from openjiuwen.core.foundation.llm.schema.config import ModelRequestConfig
from openjiuwen.core.foundation.tool import McpServerConfig, Tool, ToolCard
from openjiuwen.core.single_agent.rail.base import AgentRail
from openjiuwen.core.single_agent.schema.agent_card import AgentCard
from openjiuwen.core.sys_operation import SysOperation
from openjiuwen.harness.deep_agent import DeepAgent
from openjiuwen.harness.factory import create_deep_agent
from openjiuwen.harness.rails.context_engineer import ContextProcessorRail
from openjiuwen.harness.schema.config import SubAgentConfig
from openjiuwen.harness.tools.browser_move.offload_recall import BrowserOffloadRecallTool
from openjiuwen.harness.tools.browser_move.policy.jev_decision_model import JevDecisionModel
from openjiuwen.harness.tools.browser_move.runtime.browser_capabilities import (
    DEFAULT_BROWSER_CAPABILITIES,
    narrow_allowed_tools_for_browser_driver,
    resolve_browser_capabilities,
)
from openjiuwen.harness.tools.browser_move.runtime.browser_state_context_processor import (
    BrowserStateContextProcessorConfig,
)
from openjiuwen.harness.tools.browser_move.runtime.browser_working_context_processor import (
    BrowserWorkingContextProcessorConfig,
)
from openjiuwen.harness.tools.browser_move.runtime.config import (
    BrowserInstanceConfig,
    RuntimeSettings,
    build_browser_guardrails,
    build_playwright_mcp_config,
    build_runtime_settings,
    resolve_browser_driver_backend,
)
from openjiuwen.harness.tools.browser_move.runtime.runtime import (
    BROWSER_CATALOG_RUNTIME_TOOL_NAMES,
    BrowserAgentRuntime,
    BrowserRuntimeRail,
)
from openjiuwen.harness.tools.browser_move.runtime.runtime_tools import (
    build_browser_runtime_tools,
)

try:
    from openjiuwen.harness.prompts import resolve_language
except ImportError:

    def resolve_language(language: Optional[str] = None) -> str:  # type: ignore[misc]
        return language if language in {"cn", "en"} else "cn"


if TYPE_CHECKING:
    from openjiuwen.harness.workspace.workspace import Workspace


BROWSER_AGENT_FACTORY_NAME = "browser_agent"
# Agent checkpoints are namespaced by AgentCard.id inside a conversation.
# Keep the default stable so reconstructing this subagent can restore its
# Session-backed working context on a same-conversation follow-up.
BROWSER_AGENT_CARD_ID = "openjiuwen.browser_agent"
DEFAULT_BROWSER_AGENT_TEMPERATURE = 0.4
DEFAULT_BROWSER_AGENT_MAX_ITERATIONS = 100
_BROWSER_MODEL_TEMPERATURE_MARKER = "_browser_agent_temperature"
_BROWSER_PARENT_MODEL_MARKER = "_browser_agent_parent_model"

DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT_EN = (
    "You are a browser automation agent. Complete the user's web task by planning and using "
    "the available browser tools to navigate, inspect, interact, and extract evidence. "
    "Every model call includes one runtime-maintained <browser_working_context> immediately before the "
    "latest complete <browser_state> observation. Treat working context as authoritative for phase "
    "status, field coverage, blockers, structured evidence, recent action deltas, and the runtime "
    "directive. A fresh browser capture occurs initially and after a recognized state-invalidating "
    "action; otherwise the cached observation is reused. When the runtime directive requires "
    "replanning or a recent action reports no semantic progress, do not repeat it; use a materially "
    "different approach or finish with the available evidence. "
    "Before acting, classify the task as a simple lookup or a complex workflow and keep a compact "
    "phase plan. Prefer direct navigation when the task already contains a known URL. Prefer "
    "observable page conditions over fixed sleeps. Keep actions targeted, avoid redundant "
    "verification, and only claim completion when the requested outcome is evidenced on the page."
)

DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT_CN = (
    "你是浏览器自动化代理。请规划并使用可用的浏览器工具完成用户的网页任务："
    "导航、检查、交互和提取证据。"
    "每次模型调用都会在最新完整的 <browser_state> 观察之前注入一份由 runtime 维护的 "
    "<browser_working_context>。其中的阶段状态、字段覆盖率、阻断项、结构化证据、最近动作变化和 "
    "runtime 指令均为权威信息。系统仅在初始调用和已识别的浏览器状态变更操作完成后重新捕获页面；"
    "其他调用复用缓存观察。当 runtime 要求重新规划或最近动作没有语义进展时，不得重复该操作；"
    "应改用实质不同的策略，或基于现有证据结束任务。"
    "执行前先将任务判断为简单查询或复杂流程，并维护紧凑的阶段计划。"
    "任务已包含已知 URL 时优先直接导航。优先等待可观察页面条件，不要固定 sleep。"
    "操作应保持目标明确，避免重复验证；只有当网页上的具体证据证明任务已完成时，才声明完成。"
)

DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT: Dict[str, str] = {
    "cn": DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT_CN,
    "en": DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT_EN,
}

DEFAULT_BROWSER_AGENT_DESCRIPTION_EN = (
    "Dedicated browser subagent that controls the browser to complete web tasks."
)
DEFAULT_BROWSER_AGENT_DESCRIPTION_CN = "专用浏览器子代理，控制浏览器完成网页任务。"
DEFAULT_BROWSER_AGENT_DESCRIPTION: Dict[str, str] = {
    "cn": DEFAULT_BROWSER_AGENT_DESCRIPTION_CN,
    "en": DEFAULT_BROWSER_AGENT_DESCRIPTION_EN,
}


def _coerce_browser_instance(
    browser_instance: Optional[BrowserInstanceConfig | Dict[str, Any]],
    browser_key: Optional[str],
) -> Optional[BrowserInstanceConfig]:
    """Normalize the per-instance browser config from a model, dict, or bare key.

    A dict form is accepted so the teams manifest can carry browser identity as
    serializable ``factory_kwargs`` across the spawn wire boundary.
    """
    if isinstance(browser_instance, BrowserInstanceConfig):
        return browser_instance
    if isinstance(browser_instance, dict):
        allowed = {f.name for f in dataclasses.fields(BrowserInstanceConfig)}
        return BrowserInstanceConfig(**{k: v for k, v in browser_instance.items() if k in allowed})
    if browser_key:
        return BrowserInstanceConfig(key=str(browser_key))
    return None


def _resolve_runtime_settings(
    model: Model,
    settings: Optional[RuntimeSettings],
    instance: Optional[BrowserInstanceConfig] = None,
) -> RuntimeSettings:
    if settings is not None:
        return settings
    if model.model_client_config is not None:
        cc = model.model_client_config
        request_model_name = ""
        if model.model_config is not None:
            request_model_name = (
                getattr(model.model_config, "model", None) or getattr(model.model_config, "model_name", None) or ""
            )
        return RuntimeSettings(
            provider=cc.client_provider,
            api_key=cc.api_key,
            api_base=cc.api_base or "",
            model_name=request_model_name,
            mcp_cfg=build_playwright_mcp_config(instance),
            guardrails=build_browser_guardrails(),
            instance=instance,
        )
    return build_runtime_settings(instance)


def _browser_model_with_temperature(model: Model, temperature: float) -> Model:
    """Copy a parent model descriptor and override sampling for browser work."""
    resolved_temperature = max(0.0, min(float(temperature), 2.0))
    if getattr(model, _BROWSER_MODEL_TEMPERATURE_MARKER, None) == resolved_temperature:
        return model
    parent_model = getattr(model, _BROWSER_PARENT_MODEL_MARKER, model)

    model_config = getattr(model, "model_config", None)
    if isinstance(model_config, ModelRequestConfig):
        browser_model_config = model_config.model_copy(
            deep=True,
            update={"temperature": resolved_temperature},
        )
    elif model_config is None:
        browser_model_config = ModelRequestConfig(temperature=resolved_temperature)
    else:
        browser_model_config = copy.copy(model_config)
        setattr(browser_model_config, "temperature", resolved_temperature)

    model_client_config = getattr(model, "model_client_config", None)
    if issubclass(type(model), Model) and model_client_config is not None:
        browser_model = Model(
            model_client_config=model_client_config,
            model_config=browser_model_config,
        )
        setattr(browser_model, _BROWSER_MODEL_TEMPERATURE_MARKER, resolved_temperature)
        setattr(browser_model, _BROWSER_PARENT_MODEL_MARKER, parent_model)
        return browser_model

    # Lightweight test doubles and compatibility model descriptors may not be
    # constructible as a concrete Model. Preserve their shape without mutating
    # the parent object.
    browser_model = copy.copy(model)
    browser_model.model_config = browser_model_config
    setattr(browser_model, _BROWSER_MODEL_TEMPERATURE_MARKER, resolved_temperature)
    setattr(browser_model, _BROWSER_PARENT_MODEL_MARKER, parent_model)
    return browser_model


def build_browser_agent_config(
    model: Model,
    *,
    card: Optional[AgentCard] = None,
    system_prompt: Optional[str] = None,
    tools: Optional[List[Tool | ToolCard]] = None,
    mcps: Optional[List[McpServerConfig]] = None,
    rails: Optional[List[AgentRail]] = None,
    enable_task_loop: bool = False,
    max_iterations: int = DEFAULT_BROWSER_AGENT_MAX_ITERATIONS,
    temperature: float = DEFAULT_BROWSER_AGENT_TEMPERATURE,
    workspace: Optional[str | "Workspace"] = None,
    skills: Optional[List[str]] = None,
    backend: Optional[Any] = None,
    sys_operation: Optional[SysOperation] = None,
    language: Optional[str] = None,
    prompt_mode: Optional[str] = None,
    settings: Optional[RuntimeSettings] = None,
    browser_key: Optional[str] = None,
    browser_instance: Optional[BrowserInstanceConfig | Dict[str, Any]] = None,
) -> SubAgentConfig:
    """Build a SubAgentConfig that materializes as create_browser_agent()."""
    resolved_language = resolve_language(language)
    instance = _coerce_browser_instance(browser_instance, browser_key)
    browser_model = _browser_model_with_temperature(model, temperature)
    resolved_settings = _resolve_runtime_settings(browser_model, settings, instance)
    return SubAgentConfig(
        agent_card=card
        or AgentCard(
            id=BROWSER_AGENT_CARD_ID,
            name="browser_agent",
            description=DEFAULT_BROWSER_AGENT_DESCRIPTION.get(
                resolved_language,
                DEFAULT_BROWSER_AGENT_DESCRIPTION["cn"],
            ),
        ),
        system_prompt=system_prompt
        or DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT.get(
            resolved_language,
            DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT["cn"],
        ),
        tools=list(tools or []),
        mcps=list(mcps or []),
        model=browser_model,
        rails=rails,
        skills=skills,
        backend=backend,
        workspace=workspace,
        sys_operation=sys_operation,
        language=resolved_language,
        prompt_mode=prompt_mode,
        enable_task_loop=enable_task_loop,
        max_iterations=max_iterations,
        factory_name=BROWSER_AGENT_FACTORY_NAME,
        factory_kwargs={"settings": resolved_settings},
    )


def create_browser_agent(
    model: Model,
    *,
    card: Optional[AgentCard] = None,
    system_prompt: Optional[str] = None,
    tools: Optional[List[Tool | ToolCard]] = None,
    mcps: Optional[List[McpServerConfig]] = None,
    subagents: Optional[List[SubAgentConfig | DeepAgent]] = None,
    rails: Optional[List[AgentRail]] = None,
    enable_task_loop: bool = False,
    max_iterations: int = DEFAULT_BROWSER_AGENT_MAX_ITERATIONS,
    temperature: float = DEFAULT_BROWSER_AGENT_TEMPERATURE,
    workspace: Optional[str | "Workspace"] = None,
    skills: Optional[List[str]] = None,
    backend: Optional[Any] = None,
    sys_operation: Optional[SysOperation] = None,
    language: Optional[str] = None,
    prompt_mode: Optional[str] = None,
    settings: Optional[RuntimeSettings] = None,
    browser_key: Optional[str] = None,
    browser_instance: Optional[BrowserInstanceConfig | Dict[str, Any]] = None,
    browser_capabilities: Optional[List[str]] = None,
    **config_kwargs: Any,
) -> DeepAgent:
    """Create the browser subagent with task-scoped capability context.

    ``browser_capabilities`` is resolved against the trusted capability
    catalog here. Core is always applied, including when the caller omits the
    optional capability list.
    """
    if browser_capabilities is not None and (
        not isinstance(browser_capabilities, list)
        or not all(isinstance(capability, str) for capability in browser_capabilities)
    ):
        raise ValueError("browser_capabilities must be a list of strings")

    resolved_capabilities = resolve_browser_capabilities(browser_capabilities)
    if resolved_capabilities.rejected_names:
        rejected = ", ".join(resolved_capabilities.rejected_names)
        available = ", ".join(capability.name for capability in DEFAULT_BROWSER_CAPABILITIES)
        raise ValueError(f"Unsupported browser capabilities: {rejected}. Available capabilities: {available}")

    resolved_language = resolve_language(language)
    instance = _coerce_browser_instance(browser_instance, browser_key)
    # A decision policy in the model slot is used as is: no temperature copy, no LLM-only context rails.
    policy_model = model if isinstance(model, JevDecisionModel) else None
    browser_model = model if policy_model is not None else _browser_model_with_temperature(model, temperature)
    resolved_settings = _resolve_runtime_settings(browser_model, settings, instance)

    allowed_tool_names = resolved_capabilities.allowed_tool_names
    # Default backend is browser_use: only advertise tools that are actually registered.
    if resolve_browser_driver_backend(resolved_settings.instance) == "browser_use":
        allowed_tool_names = narrow_allowed_tools_for_browser_driver(
            allowed_tool_names,
            registered_catalog_tool_names=BROWSER_CATALOG_RUNTIME_TOOL_NAMES,
        )

    logger.info(
        "Resolved browser capabilities: requested=%s, selected=%s, allowed_tools=%s",
        resolved_capabilities.requested_names,
        resolved_capabilities.selected_names,
        allowed_tool_names,
    )

    final_card = card or AgentCard(
        id=BROWSER_AGENT_CARD_ID,
        name="browser_agent",
        description=DEFAULT_BROWSER_AGENT_DESCRIPTION.get(
            resolved_language,
            DEFAULT_BROWSER_AGENT_DESCRIPTION["cn"],
        ),
    )
    final_prompt = system_prompt or DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT.get(
        resolved_language,
        DEFAULT_BROWSER_AGENT_SYSTEM_PROMPT["cn"],
    )

    runtime_kwargs: Dict[str, Any] = {
        "provider": resolved_settings.provider,
        "api_key": resolved_settings.api_key,
        "api_base": resolved_settings.api_base,
        "model_name": resolved_settings.model_name,
        "mcp_cfg": resolved_settings.mcp_cfg,
        "guardrails": resolved_settings.guardrails,
        "instance": resolved_settings.instance,
        "allowed_tool_names": allowed_tool_names,
    }
    browser_backend = BrowserAgentRuntime(**runtime_kwargs)
    if policy_model is not None:
        policy_model.bind_runtime(browser_backend)
    injected_tools = build_browser_runtime_tools(browser_backend, language=resolved_language)
    working_context_config = BrowserWorkingContextProcessorConfig(
        language=resolved_language,
        runtime_projection_only=True,
    )
    injected_rails: List[AgentRail] = [
        BrowserRuntimeRail(browser_backend),
    ]
    # Non-CORE exception: recall offloaded browser tool results from working context.
    injected_tools.append(BrowserOffloadRecallTool(workspace, language=resolved_language))

    browser_state_processor = (
        "BrowserStateContextProcessor",
        BrowserStateContextProcessorConfig(provider=browser_backend),
    )
    browser_working_context_processor = (
        "BrowserWorkingContextProcessor",
        working_context_config,
    )
    browser_windowed_tool_names = [
        "browser_snapshot",
        "browser_find",
        "browser_evaluate",
    ]
    browser_tool_result_window_processor = (
        "ToolResultWindowProcessor",
        ToolResultWindowProcessorConfig(
            tool_names=browser_windowed_tool_names,
            keep_last_k=1,
            trim_size=1000,
            min_offload_chars=4096,
            small_result_trim_size=800,
        ),
    )
    caller_context_rails = [rail for rail in (rails or []) if isinstance(rail, ContextProcessorRail)]
    if policy_model is not None:
        config_kwargs.setdefault("enable_model_anomaly_detection_rail", False)
    elif caller_context_rails:
        for context_rail in caller_context_rails:
            context_rail.add_processors(
                [
                    browser_tool_result_window_processor,
                    browser_state_processor,
                    browser_working_context_processor,
                ]
            )
    else:
        injected_rails.append(
            ContextProcessorRail(
                processors=[
                    browser_tool_result_window_processor,
                    browser_state_processor,
                    browser_working_context_processor,
                ],
                preset=False,
            )
        )

    final_tools = list(tools or []) + injected_tools
    final_mcps = list(mcps or [])
    final_rails = list(rails or []) + injected_rails

    agent = create_deep_agent(
        model=browser_model,
        card=final_card,
        system_prompt=final_prompt,
        tools=final_tools,
        mcps=final_mcps,
        subagents=subagents,
        rails=final_rails,
        enable_task_loop=enable_task_loop,
        max_iterations=max_iterations,
        workspace=workspace,
        skills=skills,
        backend=backend,
        sys_operation=sys_operation,
        language=resolved_language,
        prompt_mode=prompt_mode,
        **config_kwargs,
    )
    agent.register_task_resource_cleanup(
        browser_backend.release_task_resources,
        prepare=browser_backend.acquire_task_resources,
    )
    return agent


__all__ = [
    "BROWSER_AGENT_CARD_ID",
    "BROWSER_AGENT_FACTORY_NAME",
    "DEFAULT_BROWSER_AGENT_MAX_ITERATIONS",
    "DEFAULT_BROWSER_AGENT_TEMPERATURE",
    "build_browser_agent_config",
    "create_browser_agent",
]
