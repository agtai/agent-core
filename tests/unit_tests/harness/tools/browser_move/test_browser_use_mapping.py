#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B1 mapping tests for browser-use BrowserStateSummary duck-typing."""

from __future__ import annotations

from types import SimpleNamespace

from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import mapping


def _node(
    *,
    index: int,
    backend_node_id: int = 100,
    absolute_position: object | None = None,
    ax_node: object | None = None,
) -> tuple[int, SimpleNamespace]:
    return (
        index,
        SimpleNamespace(
            backend_node_id=backend_node_id,
            frame_id="frame-1",
            node_name="BUTTON",
            node_value=None,
            attributes={"id": "submit"},
            absolute_position=absolute_position,
            ax_node=ax_node,
            is_visible=True,
            is_scrollable=False,
        ),
    )


def test_map_browser_state_summary_empty_selector_map() -> None:
    summary = SimpleNamespace(
        url="https://example.test/",
        title="Empty",
        tabs=[],
        dom_state=SimpleNamespace(selector_map={}, llm_representation=None),
        browser_errors=[],
        state_error=None,
        screenshot=None,
        pixels_above=0,
        pixels_below=0,
        is_pdf_viewer=False,
    )
    result = mapping.map_browser_state_summary(summary, driver_generation=3, captured_at=1.5)
    assert result["elements"] == ()
    assert result["driver_generation"] == 3
    assert result["captured_at"] == 1.5
    assert result["errors"] == ()


def test_map_enhanced_node_missing_absolute_position() -> None:
    _, node = _node(index=1, absolute_position=None)
    mapped = mapping.map_enhanced_node(1, node)
    assert mapped["box"] is None
    assert mapped["backend_node_id"] == 100


def test_map_enhanced_node_missing_ax_node() -> None:
    _, node = _node(index=2, ax_node=None)
    mapped = mapping.map_enhanced_node(2, node)
    assert mapped["role"] is None
    assert mapped["name"] is None


def test_map_browser_state_summary_includes_state_error() -> None:
    summary = SimpleNamespace(
        url="https://example.test/err",
        title="Err",
        tabs=[SimpleNamespace(target_id="t1", url="https://example.test/err", title="Err")],
        dom_state=SimpleNamespace(
            selector_map={1: _node(index=1)[1]},
            llm_representation=lambda: "ax snapshot text",
        ),
        browser_errors=["network glitch"],
        state_error="dom capture failed",
        screenshot="b64data",
        pixels_above=10,
        pixels_below=20,
        is_pdf_viewer=True,
    )
    result = mapping.map_browser_state_summary(
        summary,
        driver_generation=7,
        captured_at=99.0,
        include_screenshot=True,
    )
    assert result["ax_text"] == "ax snapshot text"
    assert result["screenshot_b64"] == "b64data"
    assert "network glitch" in result["errors"]
    assert "dom capture failed" in result["errors"]
    assert len(result["elements"]) == 1
    assert result["elements"][0]["index"] == 1
    assert result["tabs"][0]["active"] is True


def test_map_dom_rect_from_simple_namespace() -> None:
    rect = SimpleNamespace(x=1.0, y=2.0, width=3.0, height=4.0)
    assert mapping.map_dom_rect(rect) == {"x": 1.0, "y": 2.0, "width": 3.0, "height": 4.0}
