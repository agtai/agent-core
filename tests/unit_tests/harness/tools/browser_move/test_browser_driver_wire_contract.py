#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B1 wire contract tests for BrowserDriver sidecar protocol."""

from __future__ import annotations

import inspect

from openjiuwen.harness.tools.browser_move.drivers import errors as driver_errors
from openjiuwen.harness.tools.browser_move.drivers.base import BrowserDriver
from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import wire


def _browser_driver_public_method_names() -> set[str]:
    names: set[str] = set()
    for name, member in inspect.getmembers(BrowserDriver):
        if name.startswith("_"):
            continue
        if callable(member):
            names.add(name)
    return names


def test_wire_method_names_match_protocol_plus_control() -> None:
    expected = _browser_driver_public_method_names() | set(wire.CONTROL_METHOD_NAMES)
    assert wire.WIRE_METHOD_NAMES == frozenset(expected)


def test_driver_method_names_are_subset_of_wire_methods() -> None:
    assert set(wire.DRIVER_METHOD_NAMES) <= wire.WIRE_METHOD_NAMES


def test_every_wire_error_code_maps_to_driver_error_subclass() -> None:
    for code in wire.ERROR_CODES:
        cls = getattr(driver_errors, code, None)
        assert cls is not None, f"missing errors.{code}"
        assert issubclass(cls, driver_errors.DriverError), f"errors.{code} is not a DriverError subclass"
