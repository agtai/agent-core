#!/usr/bin/env python
# coding: utf-8
# pylint: disable=protected-access

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from openjiuwen.core.foundation.tool import McpServerConfig, Tool, ToolCard
from openjiuwen.harness.tools.browser_move.runtime.config import BrowserRunGuardrails
from openjiuwen.harness.tools.browser_move.runtime.runtime import BrowserAgentRuntime
from openjiuwen.harness.tools.browser_move.runtime.runtime_tools import (
    BrowserBatchInteractTool,
    BrowserCancelTool,
    BrowserClearCancelTool,
    BrowserClickTool,
    BrowserCloseTool,
    BrowserCustomActionTool,
    BrowserDragTool,
    BrowserDropTool,
    BrowserEvaluateTool,
    BrowserFileUploadTool,
    BrowserFillFormTool,
    BrowserFindTool,
    BrowserHandleDialogTool,
    BrowserHoverTool,
    BrowserListActionsTool,
    BrowserNavigateBackTool,
    BrowserNavigateTool,
    BrowserPressKeyTool,
    BrowserProbeCardsTool,
    BrowserProbeInteractivesTool,
    BrowserRuntimeHealthTool,
    BrowserSelectOptionTool,
    BrowserSnapshotTool,
    BrowserTabsTool,
    BrowserTakeScreenshotTool,
    BrowserTypeTool,
    build_browser_runtime_tools,
)


def _run(coro):
    return asyncio.run(coro)


def _make_runtime() -> BrowserAgentRuntime:
    mcp_cfg = McpServerConfig(
        server_id="test",
        server_name="test",
        server_path="stdio://playwright",
        client_type="stdio",
        params={"cwd": "."},
    )
    return BrowserAgentRuntime(
        provider="openai",
        api_key="test-key",
        api_base="https://example.invalid/v1",
        model_name="test-model",
        mcp_cfg=mcp_cfg,
        guardrails=BrowserRunGuardrails(max_steps=3, max_failures=1, timeout_s=30, retry_once=False),
    )


def test_build_browser_runtime_tools_returns_catalog_and_cancel_tools() -> None:
    tools = build_browser_runtime_tools(_make_runtime())
    assert len(tools) == 20


def test_each_tool_is_tool_subclass() -> None:
    for tool in build_browser_runtime_tools(_make_runtime()):
        assert isinstance(tool, Tool)


def test_each_tool_has_tool_card() -> None:
    for tool in build_browser_runtime_tools(_make_runtime()):
        assert isinstance(tool.card, ToolCard)


def test_default_catalog_tool_names() -> None:
    names = [tool.card.name for tool in build_browser_runtime_tools(_make_runtime())]
    assert names == [
        "browser_navigate",
        "browser_navigate_back",
        "browser_click",
        "browser_type",
        "browser_press_key",
        "browser_take_screenshot",
        "browser_tabs",
        "browser_close",
        "browser_select_option",
        "browser_evaluate",
        "browser_drag",
        "browser_file_upload",
        "browser_hover",
        "browser_find",
        "browser_handle_dialog",
        "browser_drop",
        "browser_fill_form",
        "browser_snapshot",
        "browser_cancel_run",
        "browser_clear_cancel",
    ]
    # Phase C: helpers are demoted from model injection.
    for demoted in (
        "browser_probe_interactives",
        "browser_probe_cards",
        "browser_batch_interact",
        "browser_custom_action",
        "browser_list_custom_actions",
        "browser_runtime_health",
    ):
        assert demoted not in names


def test_helper_tool_classes() -> None:
    (
        navigate,
        navigate_back,
        click,
        type_tool,
        press_key,
        screenshot,
        tabs,
        close_tool,
        select_option,
        evaluate,
        drag,
        file_upload,
        hover,
        find_tool,
        handle_dialog,
        drop,
        fill_form,
        snapshot,
        cancel,
        clear_cancel,
    ) = build_browser_runtime_tools(_make_runtime())
    assert isinstance(navigate, BrowserNavigateTool)
    assert isinstance(navigate_back, BrowserNavigateBackTool)
    assert isinstance(click, BrowserClickTool)
    assert isinstance(type_tool, BrowserTypeTool)
    assert isinstance(press_key, BrowserPressKeyTool)
    assert isinstance(screenshot, BrowserTakeScreenshotTool)
    assert isinstance(tabs, BrowserTabsTool)
    assert isinstance(close_tool, BrowserCloseTool)
    assert isinstance(select_option, BrowserSelectOptionTool)
    assert isinstance(evaluate, BrowserEvaluateTool)
    assert isinstance(drag, BrowserDragTool)
    assert isinstance(file_upload, BrowserFileUploadTool)
    assert isinstance(hover, BrowserHoverTool)
    assert isinstance(find_tool, BrowserFindTool)
    assert isinstance(handle_dialog, BrowserHandleDialogTool)
    assert isinstance(drop, BrowserDropTool)
    assert isinstance(fill_form, BrowserFillFormTool)
    assert isinstance(snapshot, BrowserSnapshotTool)
    assert isinstance(cancel, BrowserCancelTool)
    assert isinstance(clear_cancel, BrowserClearCancelTool)


def test_language_en_uses_non_empty_descriptions() -> None:
    tools = build_browser_runtime_tools(_make_runtime(), language="en")
    for tool in tools:
        assert tool.card.description
        assert any(ch.isascii() and ch.isalpha() for ch in tool.card.description)


def test_tool_ids_are_non_empty() -> None:
    for tool in build_browser_runtime_tools(_make_runtime()):
        assert tool.card.id


def test_cancel_tool_calls_cancel_run() -> None:
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()
    runtime.cancel_run = AsyncMock(return_value={"ok": True, "session_id": "s1", "request_id": None, "error": None})
    tool = BrowserCancelTool(runtime)
    result = _run(tool.invoke({"session_id": "s1"}))
    runtime.ensure_runtime_ready.assert_called_once()
    runtime.cancel_run.assert_called_once_with(session_id="s1", request_id=None)
    assert result.success is True


def test_navigate_tool_calls_runtime_navigate() -> None:
    runtime = _make_runtime()
    runtime.navigate = AsyncMock(
        return_value={
            "ok": True,
            "url": "https://example.com/",
            "title": "Example Domain",
            "changed_document": True,
            "page_state": {"url": "https://example.com/", "generation_id": "g1"},
        }
    )
    tool = BrowserNavigateTool(runtime)
    result = _run(
        tool.invoke(
            {
                "url": "https://example.com",
                "wait_until": "domcontentloaded",
                "timeout_ms": 5000,
            }
        )
    )
    runtime.navigate.assert_called_once_with(
        url="https://example.com",
        wait_until="domcontentloaded",
        timeout_ms=5000,
    )
    assert result.success is True
    assert result.data["url"] == "https://example.com/"


def test_navigate_tool_is_registered_in_build_browser_runtime_tools() -> None:
    tools = build_browser_runtime_tools(_make_runtime())
    navigate = next(tool for tool in tools if tool.card.name == "browser_navigate")
    assert isinstance(navigate, BrowserNavigateTool)
    assert "url" in navigate.card.input_params["required"]


def test_navigate_tool_rejects_non_integer_timeout() -> None:
    runtime = _make_runtime()
    runtime.navigate = AsyncMock()
    tool = BrowserNavigateTool(runtime)
    result = _run(tool.invoke({"url": "https://example.com", "timeout_ms": "slow"}))
    assert result.success is False
    assert "timeout_ms" in (result.error or "")
    runtime.navigate.assert_not_called()


def test_runtime_navigate_calls_driver_navigate() -> None:
    from openjiuwen.harness.tools.browser_move.backends.contract.base import NavResult

    runtime = _make_runtime()
    driver = AsyncMock()
    driver.navigate = AsyncMock(
        return_value=NavResult(
            url="https://example.com/",
            title="Example Domain",
            changed_document=True,
            driver_generation=2,
        )
    )
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]
    runtime._apply_document_changed = MagicMock()  # type: ignore[method-assign]

    result = _run(
        runtime.navigate(
            url="https://example.com",
            wait_until="domcontentloaded",
            timeout_ms=2500,
        )
    )

    driver.navigate.assert_called_once_with(
        "https://example.com",
        wait_until="domcontentloaded",
        timeout_ms=2500,
    )
    runtime._apply_document_changed.assert_called_once_with(
        changed=True,
        url="https://example.com/",
        title="Example Domain",
    )
    assert result["ok"] is True
    assert result["url"] == "https://example.com/"
    assert result["title"] == "Example Domain"


def test_click_tool_calls_runtime_click() -> None:
    runtime = _make_runtime()
    runtime.click = AsyncMock(return_value={"ok": True, "detail": "clicked", "page_state": {}})
    tool = BrowserClickTool(runtime)
    result = _run(
        tool.invoke(
            {
                "generation_id": "g1",
                "target_id": "t_g1_1",
                "button": "left",
                "click_count": 1,
            }
        )
    )
    runtime.click.assert_called_once_with(
        generation_id="g1",
        target_id="t_g1_1",
        ref="",
        selector="",
        button="left",
        click_count=1,
    )
    assert result.success is True


def test_type_tool_calls_runtime_type_text() -> None:
    runtime = _make_runtime()
    runtime.type_text = AsyncMock(return_value={"ok": True, "detail": "typed", "page_state": {}})
    tool = BrowserTypeTool(runtime)
    result = _run(
        tool.invoke(
            {
                "generation_id": "g2",
                "selector": "#q",
                "text": "hello",
                "clear": True,
                "press_enter": True,
            }
        )
    )
    runtime.type_text.assert_called_once_with(
        generation_id="g2",
        text="hello",
        target_id="",
        ref="",
        selector="#q",
        clear=True,
        press_enter=True,
        sensitive=False,
    )
    assert result.success is True


def test_press_key_tool_calls_runtime_press_key() -> None:
    runtime = _make_runtime()
    runtime.press_key = AsyncMock(return_value={"ok": True, "detail": "pressed", "page_state": {}})
    tool = BrowserPressKeyTool(runtime)
    result = _run(tool.invoke({"key": "Enter"}))
    runtime.press_key.assert_called_once_with(keys="Enter")
    assert result.success is True


def test_navigate_back_tool_calls_runtime_navigate_back() -> None:
    runtime = _make_runtime()
    runtime.navigate_back = AsyncMock(
        return_value={"ok": True, "url": "https://example.com/", "title": "Example", "page_state": {}}
    )
    tool = BrowserNavigateBackTool(runtime)
    result = _run(tool.invoke({}))
    runtime.navigate_back.assert_called_once_with()
    assert result.success is True


def test_screenshot_tool_calls_runtime_take_screenshot() -> None:
    runtime = _make_runtime()
    runtime.take_screenshot = AsyncMock(
        return_value={"ok": True, "screenshot_b64": "abc", "full_page": False, "page_state": {}}
    )
    tool = BrowserTakeScreenshotTool(runtime)
    result = _run(tool.invoke({"full_page": False}))
    runtime.take_screenshot.assert_called_once_with(full_page=False)
    assert result.success is True


def test_tabs_tool_calls_runtime_tabs() -> None:
    runtime = _make_runtime()
    runtime.tabs = AsyncMock(return_value={"ok": True, "action": "list", "tabs": [], "page_state": {}})
    tool = BrowserTabsTool(runtime)
    result = _run(tool.invoke({"action": "list"}))
    runtime.tabs.assert_called_once_with(action="list", index=None)
    assert result.success is True


def test_select_option_tool_calls_runtime() -> None:
    runtime = _make_runtime()
    runtime.select_option = AsyncMock(return_value={"ok": True, "detail": "selected", "page_state": {}})
    tool = BrowserSelectOptionTool(runtime)
    result = _run(
        tool.invoke(
            {
                "generation_id": "g1",
                "selector": "#country",
                "label": "Singapore",
            }
        )
    )
    runtime.select_option.assert_called_once_with(
        generation_id="g1",
        target_id="",
        ref="",
        selector="#country",
        value=None,
        label="Singapore",
    )
    assert result.success is True


def test_evaluate_tool_calls_runtime() -> None:
    runtime = _make_runtime()
    runtime.evaluate = AsyncMock(return_value={"ok": True, "value": 2, "page_state": {}})
    tool = BrowserEvaluateTool(runtime)
    result = _run(tool.invoke({"function": "() => 1 + 1"}))
    runtime.evaluate.assert_called_once_with(source="() => 1 + 1", args=None)
    assert result.success is True


def test_runtime_evaluate_rejects_document_dumps() -> None:
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()
    result = _run(runtime.evaluate(source="() => document.body.innerHTML"))
    assert result["ok"] is False
    assert "full-document" in (result["error"] or "")


def test_runtime_select_option_calls_driver() -> None:
    from openjiuwen.harness.tools.browser_move.backends.contract.base import ActResult, SelectorRef

    runtime = _make_runtime()
    driver = AsyncMock()
    driver.select_option = AsyncMock(
        return_value=ActResult(ok=True, detail="selected", document_changed=False, driver_generation=4)
    )
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]
    runtime._resolve_catalog_element_ref = AsyncMock(return_value=SelectorRef(css="#country"))  # type: ignore[method-assign]
    runtime._apply_document_changed = MagicMock()  # type: ignore[method-assign]

    result = _run(
        runtime.select_option(
            generation_id="g1",
            selector="#country",
            label="SG",
        )
    )
    driver.select_option.assert_called_once_with(
        SelectorRef(css="#country"),
        value=None,
        label="SG",
    )
    assert result["ok"] is True


def test_close_snapshot_drag_upload_tools_call_runtime() -> None:
    runtime = _make_runtime()
    runtime.close_page = AsyncMock(return_value={"ok": True, "page_state": {}})
    runtime.snapshot = AsyncMock(return_value={"ok": True, "ax_text": "Heading", "page_state": {}})
    runtime.drag = AsyncMock(return_value={"ok": True, "page_state": {}})
    runtime.file_upload = AsyncMock(return_value={"ok": True, "page_state": {}})
    runtime.fill_form = AsyncMock(return_value={"ok": True, "results": [], "page_state": {}})

    assert _run(BrowserCloseTool(runtime).invoke({})).success is True
    assert _run(BrowserSnapshotTool(runtime).invoke({})).success is True
    assert _run(
        BrowserDragTool(runtime).invoke(
            {
                "generation_id": "g1",
                "source_selector": "#a",
                "target_selector": "#b",
            }
        )
    ).success is True
    assert _run(
        BrowserFileUploadTool(runtime).invoke(
            {
                "generation_id": "g1",
                "selector": "input[type=file]",
                "paths": ["/tmp/a.txt"],
            }
        )
    ).success is True
    assert _run(
        BrowserFillFormTool(runtime).invoke(
            {
                "generation_id": "g1",
                "fields": [{"type": "textbox", "selector": "#name", "value": "Ada"}],
            }
        )
    ).success is True

    runtime.close_page.assert_called_once()
    runtime.snapshot.assert_called_once_with(include_screenshot=False)
    runtime.drag.assert_called_once()
    runtime.file_upload.assert_called_once()
    runtime.fill_form.assert_called_once()


def test_runtime_click_calls_driver_click() -> None:
    from openjiuwen.harness.tools.browser_move.backends.contract.base import ActResult, SelectorRef

    runtime = _make_runtime()
    driver = AsyncMock()
    driver.click = AsyncMock(
        return_value=ActResult(ok=True, detail="clicked", document_changed=False, driver_generation=3)
    )
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]
    runtime._resolve_catalog_element_ref = AsyncMock(return_value=SelectorRef(css="#go"))  # type: ignore[method-assign]
    runtime._apply_document_changed = MagicMock()  # type: ignore[method-assign]

    result = _run(
        runtime.click(
            generation_id="g1",
            selector="#go",
        )
    )

    runtime._resolve_catalog_element_ref.assert_called_once()
    driver.click.assert_called_once()
    assert result["ok"] is True
    assert result["detail"] == "clicked"


def test_runtime_navigate_back_calls_driver_go_back() -> None:
    from openjiuwen.harness.tools.browser_move.backends.contract.base import NavResult

    runtime = _make_runtime()
    driver = AsyncMock()
    driver.go_back = AsyncMock(
        return_value=NavResult(
            url="https://example.com/",
            title="Example Domain",
            changed_document=True,
            driver_generation=4,
        )
    )
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]
    runtime._apply_document_changed = MagicMock()  # type: ignore[method-assign]

    result = _run(runtime.navigate_back())
    driver.go_back.assert_called_once_with()
    assert result["ok"] is True
    assert result["url"] == "https://example.com/"


def test_clear_cancel_tool_calls_runtime_clear_cancel() -> None:
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()
    runtime.clear_cancel = AsyncMock(return_value={"ok": True, "session_id": "s1", "request_id": "r1", "error": None})
    tool = BrowserClearCancelTool(runtime)
    result = _run(tool.invoke({"session_id": "s1", "request_id": "r1"}))
    runtime.ensure_runtime_ready.assert_called_once()
    runtime.clear_cancel.assert_called_once_with(session_id="s1", request_id="r1")
    assert result.success is True


def test_list_actions_tool_uses_runtime_api() -> None:
    runtime = _make_runtime()
    runtime.list_actions = AsyncMock(return_value={"ok": True, "actions": ["echo"], "details": {"echo": {}}})
    tool = BrowserListActionsTool(runtime)
    result = _run(tool.invoke({}))
    runtime.list_actions.assert_called_once()
    assert result.success is True
    assert result.data["actions"] == ["echo"]


def test_custom_action_tool_uses_runtime_api() -> None:
    runtime = _make_runtime()
    runtime.run_custom_action = AsyncMock(return_value={"ok": True, "session_id": "s1"})
    tool = BrowserCustomActionTool(runtime)
    result = _run(
        tool.invoke(
            {
                "action": "echo",
                "session_id": "s1",
                "request_id": "r1",
                "params": {"text": "hello"},
            }
        )
    )
    runtime.run_custom_action.assert_called_once_with(
        action="echo",
        session_id="s1",
        request_id="r1",
        params={"text": "hello"},
    )
    assert result.success is True


def test_probe_interactives_tool_uses_runtime_api() -> None:
    runtime = _make_runtime()
    runtime.probe_interactives = AsyncMock(
        return_value={
            "ok": True,
            "elements": [
                {
                    "id": "e1",
                    "role": "button",
                    "text": "Next",
                    "selector_hint": "button:nth-of-type(1)",
                }
            ],
            "error": None,
        }
    )

    tool = BrowserProbeInteractivesTool(runtime)

    result = _run(
        tool.invoke(
            {
                "max_items": 20,
                "viewport_only": True,
                "query": "next",
            }
        )
    )

    runtime.probe_interactives.assert_called_once_with(
        max_items=20,
        viewport_only=True,
        query="next",
    )
    assert result.success is True
    assert result.data["elements"][0]["text"] == "Next"


def test_probe_cards_tool_uses_runtime_api() -> None:
    runtime = _make_runtime()
    runtime.probe_cards = AsyncMock(
        return_value={
            "ok": True,
            "cards": [
                {
                    "id": "card_1",
                    "title": "Book",
                    "price": "£10.00",
                    "selector_hint": "article.product_pod",
                }
            ],
            "error": None,
        }
    )

    tool = BrowserProbeCardsTool(runtime)

    result = _run(
        tool.invoke(
            {
                "max_cards": 20,
                "viewport_only": True,
                "include_buttons": True,
                "query": "book",
            }
        )
    )

    runtime.probe_cards.assert_called_once_with(
        max_cards=20,
        viewport_only=True,
        include_buttons=True,
        query="book",
    )
    assert result.success is True
    assert result.data["cards"][0]["title"] == "Book"


def test_runtime_health_tool_uses_runtime_api() -> None:
    runtime = _make_runtime()
    runtime.runtime_health = AsyncMock(
        return_value={
            "ok": False,
            "started": False,
            "last_heartbeat_ok": None,
            "provider": "openai",
            "api_base": "https://example.invalid/v1",
            "model_name": "test-model",
        }
    )
    tool = BrowserRuntimeHealthTool(runtime)
    result = _run(tool.invoke({}))
    runtime.runtime_health.assert_called_once()
    assert result.success is True
    assert result.data["started"] is False


def test_batch_interact_tool_uses_runtime_api_for_realistic_form_flow() -> None:
    runtime = _make_runtime()
    runtime.batch_interact = AsyncMock(
        return_value={
            "ok": True,
            "steps": [
                {"index": 0, "op": "fill", "ok": True},
                {"index": 1, "op": "autocomplete", "ok": True},
                {"index": 2, "op": "select_option", "ok": True},
            ],
            "error": None,
        }
    )
    tool = BrowserBatchInteractTool(runtime)
    steps = [
        {"op": "fill", "label": "First name", "value": "John"},
        {
            "op": "autocomplete",
            "placeholder": "From",
            "value": "Singapore",
            "choose_text": "Singapore (SIN)",
        },
        {"op": "select_option", "label": "Nationality", "option_text": "Singapore"},
    ]

    result = _run(
        tool.invoke(
            {
                "steps": steps,
                "timeout_ms": 3000,
                "wait_after_each_ms": 50,
                "continue_on_error": False,
                "global_timeout_ms": 15000,
                "generation_id": "g3",
                "session_id": "sess-batch",
                "request_id": "req-batch",
            }
        )
    )

    runtime.batch_interact.assert_called_once_with(
        steps=steps,
        generation_id="g3",
        timeout_ms=3000,
        condition_timeout_ms=None,
        wait_after_each_ms=50,
        continue_on_error=False,
        global_timeout_ms=15000,
        session_id="sess-batch",
        request_id="req-batch",
    )
    assert result.success is True
    assert result.data["steps"][1]["op"] == "autocomplete"


def test_batch_interact_tool_reports_runtime_error() -> None:
    runtime = _make_runtime()
    runtime.batch_interact = AsyncMock(
        return_value={
            "ok": False,
            "error": "browser_code_executor_not_ready",
            "steps_requested": 1,
        }
    )
    tool = BrowserBatchInteractTool(runtime)

    result = _run(
        tool.invoke(
            {
                "steps": [{"op": "click", "text": "Search"}],
                "generation_id": "g0",
            }
        )
    )

    assert result.success is False
    assert result.error == "browser_code_executor_not_ready"
    assert result.data["steps_requested"] == 1


def test_batch_interact_schema_supports_single_action_and_exposes_condition_waits() -> None:
    tool = BrowserBatchInteractTool(_make_runtime())
    steps_schema = tool.card.input_params["properties"]["steps"]
    operations = steps_schema["items"]["properties"]["op"]["enum"]

    assert steps_schema["minItems"] == 1
    assert steps_schema["maxItems"] == 25
    assert "generation_id" in tool.card.input_params["required"]
    assert "target_id" in steps_schema["items"]["properties"]
    assert "ref" in steps_schema["items"]["properties"]
    assert "option_target_id" in steps_schema["items"]["properties"]
    assert "wait_for_url" in operations
    assert "wait_for_first_card_title" in operations
    assert "wait_for_sort_state" in operations
    assert "wait_for_result_count" in operations
    assert "wait_for_dom_text_change" in operations
    assert "wait_for_stable" in operations
    assert "wait_for_tab" in operations
    assert "sleep" not in operations
    step_properties = steps_schema["items"]["properties"]
    assert "min_tabs" in step_properties
    assert "title_contains" in step_properties
    assert "activate" in step_properties
    assert "ms" not in step_properties
    assert "wait_after_ms" not in step_properties
    assert "wait_after_type_ms" not in step_properties
    assert "wait_after_each_ms" not in tool.card.input_params["properties"]
    assert "default 2500" in (
        tool.card.input_params["properties"]["timeout_ms"]["description"].lower()
    )
