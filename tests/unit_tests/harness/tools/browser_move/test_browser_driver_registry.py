#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B1 registry tests for BrowserDriver backends."""

from __future__ import annotations

import pytest

from openjiuwen.harness.tools.browser_move.drivers.errors import DriverUnsupported
from openjiuwen.harness.tools.browser_move.drivers.registry import (
    create_browser_driver,
    register_browser_driver,
    registered_backends,
)
from tests.unit_tests.harness.tools.browser_move.fakes.fake_driver import FakeDriver


def test_browser_use_backend_is_registered() -> None:
    assert "browser_use" in registered_backends()
    driver = create_browser_driver("browser_use")
    assert driver is not None


def test_unknown_backend_raises_driver_unsupported_with_registered_list() -> None:
    with pytest.raises(DriverUnsupported, match="Unknown browser driver backend 'missing'") as exc_info:
        create_browser_driver("missing")
    message = str(exc_info.value)
    assert "Registered:" in message
    for backend in registered_backends():
        assert backend in message


def test_playwright_mcp_is_not_registered() -> None:
    assert "playwright_mcp" not in registered_backends()


def test_registering_playwright_mcp_raises_value_error() -> None:
    with pytest.raises(ValueError, match="playwright_mcp.*reserved"):
        register_browser_driver("playwright_mcp", FakeDriver)


def test_registering_empty_backend_name_raises_value_error() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        register_browser_driver("", FakeDriver)
