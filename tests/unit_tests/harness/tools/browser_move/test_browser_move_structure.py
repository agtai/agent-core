#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Architecture-invariant regression tests for the browser_move layout.

The browser subsystem was restructured from a driver-centric layout
(``playwright_runtime/`` + ``drivers/`` + ``clients/`` + ``utils/``) into a
self-describing one: a backend-agnostic ``runtime/`` plus a ``backends/``
tree that splits the shared ``contract/`` from each concrete backend
(``browser_use/`` and ``playwright_mcp/``), with ``chrome/``, ``shared/`` and
``lab/`` peers.

That move was purely structural -- no behavior changed. These tests lock the
new import boundaries in place so a future edit cannot silently resurrect a
legacy path or move a module back across a backend boundary. They are
deterministic and do no I/O: every assertion is about module resolution and a
couple of registry constants.
"""

from __future__ import annotations

import importlib
import importlib.util

import pytest

_PKG = "openjiuwen.harness.tools.browser_move"

# Modules that MUST resolve at their canonical post-refactor path. One entry per
# architectural zone (runtime core, backend contract, each backend, chrome,
# shared) so a regression in any zone fails a named case.
CANONICAL_MODULES = [
    # backend-agnostic runtime core
    f"{_PKG}.runtime.runtime",
    f"{_PKG}.runtime.batch_executor",
    f"{_PKG}.runtime.page_state",
    f"{_PKG}.runtime.browser_capabilities",
    f"{_PKG}.runtime.controllers.action",
    f"{_PKG}.runtime.controllers.base",
    # shared backend contract (protocol / errors / registry)
    f"{_PKG}.backends.contract.base",
    f"{_PKG}.backends.contract.errors",
    f"{_PKG}.backends.contract.registry",
    # browser_use backend + its sidecar
    f"{_PKG}.backends.browser_use.driver",
    f"{_PKG}.backends.browser_use.transport",
    f"{_PKG}.backends.browser_use.sidecar.main",
    # playwright_mcp backend + its stdio/http clients
    f"{_PKG}.backends.playwright_mcp.mcp_server",
    f"{_PKG}.backends.playwright_mcp.browser_tools",
    f"{_PKG}.backends.playwright_mcp.clients.stdio_client",
    f"{_PKG}.backends.playwright_mcp.clients.streamable_http_client",
    # local chrome process management
    f"{_PKG}.chrome.managed_browser",
    # shared utilities
    f"{_PKG}.shared.env",
    f"{_PKG}.shared.parsing",
    f"{_PKG}.shared.upload_paths",
]

# Legacy import paths that MUST NOT resolve after the refactor. Their parent
# package (browser_move) still exists, so ``find_spec`` returns None rather than
# raising -- that None is the assertion.
LEGACY_MODULES_GONE = [
    f"{_PKG}.playwright_runtime",
    f"{_PKG}.playwright_runtime_mcp_server",
    f"{_PKG}.drivers",
    f"{_PKG}.clients",
    f"{_PKG}.utils",
    f"{_PKG}.controllers",
]

# Public attributes re-exported from the package root via lazy ``__getattr__``.
PUBLIC_CALLABLES = [
    "build_browser_runtime_mcp_config",
    "register_browser_runtime_mcp_server",
    "reset_active_browser_runtimes",
    "reset_managed_browser_runtime",
    "restart_local_browser_runtime_server",
    "stop_local_browser_runtime_server",
]


@pytest.mark.parametrize("module_path", CANONICAL_MODULES)
def test_canonical_modules_resolve(module_path: str) -> None:
    module = importlib.import_module(module_path)
    assert module is not None
    assert module.__name__ == module_path


@pytest.mark.parametrize("module_path", LEGACY_MODULES_GONE)
def test_legacy_paths_are_removed(module_path: str) -> None:
    assert importlib.util.find_spec(module_path) is None, (
        f"legacy module path '{module_path}' still resolves; the refactor "
        "boundary regressed"
    )


def test_lab_runner_lives_under_lab() -> None:
    assert importlib.util.find_spec(f"{_PKG}.lab.run_bu_prompt") is not None


@pytest.mark.parametrize("attr", PUBLIC_CALLABLES)
def test_package_root_reexports_resolve(attr: str) -> None:
    pkg = importlib.import_module(_PKG)
    resolved = getattr(pkg, attr)
    assert callable(resolved)


def test_browser_use_is_the_only_backend_and_mcp_name_reserved() -> None:
    registry = importlib.import_module(f"{_PKG}.backends.contract.registry")
    assert registry.registered_backends() == ("browser_use",)
    assert "playwright_mcp" in registry.RESERVED_BACKEND_NAMES
    assert "playwright_mcp" not in registry.registered_backends()


def test_browser_agent_importer_uses_new_paths() -> None:
    # The most direct cross-package consumer of browser_move. Importing it
    # exercises the updated dotted paths end-to-end (a stale import here would
    # raise ModuleNotFoundError at collection).
    module = importlib.import_module("openjiuwen.harness.subagents.browser_agent")
    assert hasattr(module, "create_browser_agent")


@pytest.mark.parametrize(
    "importer",
    [
        "openjiuwen.core.single_agent.agents.react_agent",
        "openjiuwen.harness.rails.subagent.subagent_rail",
        "openjiuwen.harness.tools.subagent.task_tool",
    ],
)
def test_cross_package_importers_are_locatable(importer: str) -> None:
    assert importlib.util.find_spec(importer) is not None
