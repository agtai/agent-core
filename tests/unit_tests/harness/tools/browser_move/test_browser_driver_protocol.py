#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B1 protocol conformance tests for BrowserDriver."""

from __future__ import annotations

import inspect
import sys

import pytest

from openjiuwen.harness.tools.browser_move.drivers.base import BrowserDriver
from openjiuwen.harness.tools.browser_move.drivers.browser_use.driver import BrowserUseDriver
from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import wire

from tests.unit_tests.harness.tools.browser_move.fakes.fake_driver import FakeDriver, assert_is_browser_driver


def _protocol_method_names() -> tuple[str, ...]:
    return wire.DRIVER_METHOD_NAMES


def _public_callables(cls: type) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(cls)
        if callable(member) and not name.startswith("_")
    }


@pytest.mark.parametrize("driver_cls", [FakeDriver, BrowserUseDriver])
def test_driver_satisfies_runtime_checkable_protocol(driver_cls: type) -> None:
    driver = driver_cls()
    assert_is_browser_driver(driver)


def test_fake_driver_implements_every_protocol_method() -> None:
    missing = set(_protocol_method_names()) - _public_callables(FakeDriver)
    assert not missing, f"FakeDriver missing protocol methods: {sorted(missing)}"


def test_browser_use_driver_implements_every_protocol_method() -> None:
    missing = set(_protocol_method_names()) - _public_callables(BrowserUseDriver)
    assert not missing, f"BrowserUseDriver missing protocol methods: {sorted(missing)}"


def test_importing_browser_use_driver_does_not_load_browser_use_package() -> None:
    snapshot = set(sys.modules)
    import importlib

    importlib.reload(
        importlib.import_module("openjiuwen.harness.tools.browser_move.drivers.browser_use.driver")
    )
    added = set(sys.modules) - snapshot
    browser_use_modules = [name for name in added if name == "browser_use" or name.startswith("browser_use.")]
    assert not browser_use_modules, f"unexpected browser_use imports: {browser_use_modules}"
