# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Raw CDP helpers for operations browser-use has no high-level API for.

STDLIB ONLY. browser-use's ``ExecuteJavaScriptEvent`` is commented out in
0.13.10, so ``evaluate``, ``stamp``, ``resolve(SelectorRef)``, and
``drag`` go through ``cdp_session.cdp_client.send.Runtime.evaluate`` /
``DOM.querySelector`` / ``Runtime.callFunctionOn`` /
``Input.dispatchMouseEvent`` directly.

Every function here takes a duck-typed ``cdp_client`` (whatever object
exposes ``.send.<Domain>.<method>(params, session_id=...)``, matching
cdp-use's ``CDPClient``) plus a ``session_id``. This module never imports
``browser_use`` or ``cdp_use`` itself, so it stays importable and testable
from the main agent-core environment where neither package is installed.

Runs as a flat sibling module inside the sidecar process (``import cdp``),
mirroring every other file in this subtree except ``wire.py``.
"""

from __future__ import annotations

import json
from typing import Any

import exceptions


async def get_document_root_node_id(cdp_client: Any, session_id: str) -> int:
    """Return the root node id of the current document."""
    result = await cdp_client.send.DOM.getDocument(params={"depth": 0}, session_id=session_id)
    return int(result["root"]["nodeId"])


async def query_selector_node_id(cdp_client: Any, session_id: str, selector: str) -> int | None:
    """Return the CDP nodeId matching ``selector``, or None if not found."""
    root_node_id = await get_document_root_node_id(cdp_client, session_id)
    result = await cdp_client.send.DOM.querySelector(
        params={"nodeId": root_node_id, "selector": selector}, session_id=session_id
    )
    node_id = int(result.get("nodeId", 0) or 0)
    return node_id or None


async def describe_node_backend_id(cdp_client: Any, session_id: str, node_id: int) -> int:
    """Resolve a CDP nodeId to its stable backendNodeId."""
    result = await cdp_client.send.DOM.describeNode(params={"nodeId": node_id}, session_id=session_id)
    return int(result["node"]["backendNodeId"])


async def resolve_object_id_by_backend_node(cdp_client: Any, session_id: str, backend_node_id: int) -> str:
    """Resolve a backendNodeId to a Runtime remote objectId.

    Raises ``exceptions.StaleNodeError`` when the backend node no longer
    resolves (page navigated, node detached, etc.).
    """
    result = await cdp_client.send.DOM.resolveNode(params={"backendNodeId": backend_node_id}, session_id=session_id)
    object_id = (result.get("object") or {}).get("objectId")
    if not object_id:
        raise exceptions.StaleNodeError(f"backend node {backend_node_id} no longer resolves")
    return str(object_id)


async def call_function_on_backend_node(
    cdp_client: Any,
    session_id: str,
    backend_node_id: int,
    function_declaration: str,
    *,
    arguments: list[dict[str, Any]] | None = None,
    await_promise: bool = True,
    return_by_value: bool = True,
) -> Any:
    """Call ``function_declaration`` bound to ``this`` = the resolved node.

    ``function_declaration`` is a JS function expression whose ``this`` is
    the target DOM node, e.g. ``"function(v){ this.value = v; }"``.
    Raises ``exceptions.EvaluateError`` on a JS-side exception.
    """
    object_id = await resolve_object_id_by_backend_node(cdp_client, session_id, backend_node_id)
    result = await cdp_client.send.Runtime.callFunctionOn(
        params={
            "functionDeclaration": function_declaration,
            "objectId": object_id,
            "arguments": arguments or [],
            "awaitPromise": await_promise,
            "returnByValue": return_by_value,
        },
        session_id=session_id,
    )
    return _unwrap_js_result(result)


async def evaluate_expression(
    cdp_client: Any,
    session_id: str,
    function_source: str,
    args: Any = None,
    *,
    await_promise: bool = True,
    return_by_value: bool = True,
) -> Any:
    """Evaluate a page-scoped JS function expression invoked with ``args``.

    ``function_source`` must be a JS function expression taking one
    argument, e.g. ``"(x) => x + 1"``. Raises ``exceptions.EvaluateError``
    on a JS-side exception.
    """
    expression = f"({function_source})({json.dumps(args)})"
    result = await cdp_client.send.Runtime.evaluate(
        params={
            "expression": expression,
            "awaitPromise": await_promise,
            "returnByValue": return_by_value,
        },
        session_id=session_id,
    )
    return _unwrap_js_result(result)


async def dispatch_mouse_click(
    cdp_client: Any,
    session_id: str,
    *,
    x: float,
    y: float,
    button: str = "left",
    click_count: int = 1,
) -> None:
    """Dispatch a synthetic mouse press+release at page coordinates."""
    base = {"x": x, "y": y, "button": button, "clickCount": click_count, "modifiers": 0}
    await cdp_client.send.Input.dispatchMouseEvent(params={**base, "type": "mousePressed"}, session_id=session_id)
    await cdp_client.send.Input.dispatchMouseEvent(params={**base, "type": "mouseReleased"}, session_id=session_id)


async def dispatch_mouse_move(cdp_client: Any, session_id: str, *, x: float, y: float) -> None:
    """Dispatch a synthetic mouse move to page coordinates (used by drag)."""
    await cdp_client.send.Input.dispatchMouseEvent(
        params={"type": "mouseMoved", "x": x, "y": y, "button": "none", "clickCount": 0, "modifiers": 0},
        session_id=session_id,
    )


async def insert_text(cdp_client: Any, session_id: str, text: str) -> None:
    """Insert text at the current focus/caret position."""
    await cdp_client.send.Input.insertText(params={"text": text}, session_id=session_id)


async def dispatch_key_press(cdp_client: Any, session_id: str, key: str) -> None:
    """Dispatch a synthetic keyDown+keyUp for a single named key (e.g. ``"Enter"``)."""
    await cdp_client.send.Input.dispatchKeyEvent(params={"type": "keyDown", "key": key}, session_id=session_id)
    await cdp_client.send.Input.dispatchKeyEvent(params={"type": "keyUp", "key": key}, session_id=session_id)


async def request_node_id_from_object(cdp_client: Any, session_id: str, object_id: str) -> int:
    """Resolve a Runtime remote objectId back to a CDP nodeId."""
    result = await cdp_client.send.DOM.requestNode(params={"objectId": object_id}, session_id=session_id)
    return int(result["nodeId"])


def _unwrap_js_result(result: dict[str, Any]) -> Any:
    """Raise ``exceptions.EvaluateError`` on a JS exception, else return the value."""
    exception_details = result.get("exceptionDetails")
    if exception_details:
        js_message = str(exception_details.get("text", "unknown JS error"))
        exception = exception_details.get("exception") or {}
        js_description = str(exception.get("description", ""))
        raise exceptions.EvaluateError(js_message, js_message=js_message, js_stack=js_description)
    return (result.get("result") or {}).get("value")


__all__ = [
    "call_function_on_backend_node",
    "describe_node_backend_id",
    "dispatch_key_press",
    "dispatch_mouse_click",
    "dispatch_mouse_move",
    "evaluate_expression",
    "get_document_root_node_id",
    "insert_text",
    "query_selector_node_id",
    "request_node_id_from_object",
    "resolve_object_id_by_backend_node",
]
