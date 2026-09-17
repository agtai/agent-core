# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Playwright runtime package bootstrap."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any

# SRC_ROOT is the browser_move/ directory that contains runtime/ (with its
# nested controllers/), shared/, etc. Adding it to sys.path makes bare module
# imports (e.g. `from runtime.controllers.action import ...`) work both when
# running standalone (MCP server) and when loaded as part of the openjiuwen
# package.
_HERE = Path(__file__).resolve().parent
SRC_ROOT = _HERE.parent
# Walk up four levels (browser_move -> tools -> harness -> openjiuwen -> repo root)
REPO_ROOT = SRC_ROOT.parent.parent.parent.parent
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))
sys.modules.setdefault("runtime", sys.modules[__name__])

__all__ = [
    "REPO_ROOT",
    "SRC_ROOT",
    "BrowserStateContextProcessor",
    "BrowserStateContextProcessorConfig",
    "BrowserWorkingContextProcessor",
    "BrowserWorkingContextProcessorConfig",
    "BrowserWorkingContextRail",
    "build_browser_runtime_mcp_config",
    "browser_tools",
    "controller",
    "register_browser_runtime_mcp_server",
    "reset_active_browser_runtimes",
    "reset_managed_browser_runtime",
    "restart_local_browser_runtime_server",
    "service",
    "stop_local_browser_runtime_server",
]


def __getattr__(name: str) -> Any:
    if name in {"BrowserStateContextProcessor", "BrowserStateContextProcessorConfig"}:
        module = import_module(
            "openjiuwen.harness.tools.browser_move.runtime.browser_state_context_processor"
        )
        return getattr(module, name)
    if name in {
        "BrowserWorkingContextProcessor",
        "BrowserWorkingContextProcessorConfig",
    }:
        module = import_module(
            "openjiuwen.harness.tools.browser_move.runtime.browser_working_context_processor"
        )
        return getattr(module, name)
    if name == "BrowserWorkingContextRail":
        module = import_module("openjiuwen.harness.tools.browser_move.runtime.browser_working_context_rail")
        return getattr(module, name)
    if name == "controller":
        return import_module("openjiuwen.harness.tools.browser_move.runtime.controllers")
    if name == "browser_tools":
        return import_module("openjiuwen.harness.tools.browser_move.backends.playwright_mcp.browser_tools")
    if name == "service":
        return import_module("openjiuwen.harness.tools.browser_move.runtime.service")
    if name in {"reset_active_browser_runtimes", "reset_managed_browser_runtime"}:
        module = import_module("openjiuwen.harness.tools.browser_move.runtime.runtime")
        return getattr(module, name)
    if name in {
        "build_browser_runtime_mcp_config",
        "register_browser_runtime_mcp_server",
        "restart_local_browser_runtime_server",
        "stop_local_browser_runtime_server",
    }:
        module = import_module("openjiuwen.harness.tools.browser_move.backends.playwright_mcp.browser_tools")
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
