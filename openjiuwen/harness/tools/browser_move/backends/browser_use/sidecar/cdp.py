# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Raw CDP helpers for operations browser-use has no high-level API for.

STDLIB ONLY. browser-use's ``ExecuteJavaScriptEvent`` is commented out in
0.13.10, so ``evaluate``, ``stamp``, ``resolve(SelectorRef)``, ``drag``,
and ``upload_files`` go through ``cdp_session.cdp_client.send.Runtime.evaluate`` /
``DOM.querySelector`` / ``Runtime.callFunctionOn`` /
``Input.dispatchMouseEvent`` / ``Input.setInterceptDrags`` /
``Input.dispatchDragEvent`` / ``DOM.setFileInputFiles`` directly.

Every function here takes a duck-typed ``cdp_client`` (whatever object
exposes ``.send.<Domain>.<method>(params, session_id=...)``, matching
cdp-use's ``CDPClient``) plus a ``session_id``. This module never imports
``browser_use`` or ``cdp_use`` itself, so it stays importable and testable
from the main agent-core environment where neither package is installed.

Runs as a flat sibling module inside the sidecar process (``import cdp``),
mirroring every other file in this subtree except ``wire.py``.
"""

from __future__ import annotations

import asyncio
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


async def dispatch_mouse_move(
    cdp_client: Any,
    session_id: str,
    *,
    x: float,
    y: float,
    buttons: int = 0,
) -> None:
    """Dispatch a synthetic mouse move to page coordinates (used by drag).

    Pass ``buttons=1`` while the primary button is held so Chromium treats the
    move as part of an active drag gesture (required for HTML5 ``dragstart``).
    """
    button = "left" if buttons & 1 else "none"
    await cdp_client.send.Input.dispatchMouseEvent(
        params={
            "type": "mouseMoved",
            "x": x,
            "y": y,
            "button": button,
            "buttons": buttons,
            "clickCount": 0,
            "modifiers": 0,
        },
        session_id=session_id,
    )


async def set_intercept_drags(cdp_client: Any, session_id: str, enabled: bool) -> None:
    """Enable/disable CDP drag interception (``Input.setInterceptDrags``)."""
    await cdp_client.send.Input.setInterceptDrags(params={"enabled": bool(enabled)}, session_id=session_id)


async def dispatch_drag_event(
    cdp_client: Any,
    session_id: str,
    *,
    event_type: str,
    x: float,
    y: float,
    data: dict[str, Any],
) -> None:
    """Dispatch one ``Input.dispatchDragEvent`` (``dragEnter`` / ``dragOver`` / ``drop`` / ``dragCancel``)."""
    await cdp_client.send.Input.dispatchDragEvent(
        params={"type": event_type, "x": x, "y": y, "data": data, "modifiers": 0},
        session_id=session_id,
    )


def register_drag_intercepted(cdp_client: Any, callback: Any) -> Any:
    """Register an ``Input.dragIntercepted`` handler; return an unregister callable."""

    def _unregister_registry() -> None:
        registry = getattr(cdp_client, "_event_registry", None)
        if registry is not None and hasattr(registry, "unregister"):
            registry.unregister("Input.dragIntercepted")

    register = getattr(cdp_client, "register", None)
    input_reg = getattr(register, "Input", None) if register is not None else None
    if input_reg is not None and hasattr(input_reg, "dragIntercepted"):
        input_reg.dragIntercepted(callback)
        return _unregister_registry

    registry = getattr(cdp_client, "_event_registry", None)
    if registry is not None and hasattr(registry, "register"):
        registry.register("Input.dragIntercepted", callback)
        return _unregister_registry

    custom = getattr(cdp_client, "register_event", None)
    if callable(custom):
        custom("Input.dragIntercepted", callback)
        unregister = getattr(cdp_client, "unregister_event", None)
        if callable(unregister):
            return lambda: unregister("Input.dragIntercepted")
        return lambda: None

    raise exceptions.DriverUnsupported("CDP client cannot register Input.dragIntercepted handlers")


def _drag_data_from_intercept_event(event: Any) -> dict[str, Any]:
    """Normalize ``Input.dragIntercepted`` payload to a CDP ``DragData`` dict."""
    if isinstance(event, dict):
        data = event.get("data", event)
    else:
        data = getattr(event, "data", event)
    if not isinstance(data, dict):
        return {"items": [], "files": [], "dragOperationsMask": 1}
    items = data.get("items") or []
    files = data.get("files") or []
    mask = data.get("dragOperationsMask", 1)
    return {
        "items": list(items) if isinstance(items, (list, tuple)) else [],
        "files": list(files) if isinstance(files, (list, tuple)) else [],
        "dragOperationsMask": int(mask if mask is not None else 1),
    }


_HTML5_DND_JS = (
    "function(target) {"
    "  if (!target) { return { ok: false, error: 'missing drop target' }; }"
    "  const source = this;"
    "  const dt = new DataTransfer();"
    "  const opts = { bubbles: true, cancelable: true, dataTransfer: dt };"
    "  source.dispatchEvent(new DragEvent('dragstart', opts));"
    "  target.dispatchEvent(new DragEvent('dragenter', opts));"
    "  target.dispatchEvent(new DragEvent('dragover', opts));"
    "  const dropped = target.dispatchEvent(new DragEvent('drop', opts));"
    "  source.dispatchEvent(new DragEvent('dragend', opts));"
    "  return { ok: true, dropped: !!dropped };"
    "}"
)


async def html5_drag_drop_via_js(
    cdp_client: Any,
    session_id: str,
    *,
    source_backend_node_id: int,
    target_backend_node_id: int,
) -> dict[str, Any]:
    """Fire HTML5 ``dragstart``/``dragenter``/``dragover``/``drop``/``dragend`` in-page.

    Used when CDP intercept did not yield ``dragIntercepted`` but the source is
    HTML5-draggable. Returns the JS result dict (``ok`` / ``dropped`` / ``error``).
    """
    target_object_id = await resolve_object_id_by_backend_node(cdp_client, session_id, target_backend_node_id)
    result = await call_function_on_backend_node(
        cdp_client,
        session_id,
        source_backend_node_id,
        _HTML5_DND_JS,
        arguments=[{"objectId": target_object_id}],
    )
    if not isinstance(result, dict):
        return {"ok": False, "error": "html5 js drag returned non-object"}
    return result


async def perform_drag(
    cdp_client: Any,
    session_id: str,
    *,
    sx: float,
    sy: float,
    tx: float,
    ty: float,
    steps: int = 10,
    delay_ms: int = 0,
    source_is_html5: bool = False,
    source_backend_node_id: int | None = None,
    target_backend_node_id: int | None = None,
) -> dict[str, Any]:
    """Drag from ``(sx,sy)`` to ``(tx,ty)`` with HTML5 DnD support when needed.

    Sequence (Playwright / Puppeteer style):
      1. ``Input.setInterceptDrags(true)`` + listen for ``Input.dragIntercepted``
      2. mousePressed at source, mouseMoved (with ``buttons=1``) toward target
      3. if intercept fires → ``dragEnter`` / ``dragOver`` / ``drop`` + mouseReleased
      4. else if source is HTML5-draggable → JS ``DragEvent`` fallback between nodes
      5. else plain pointer mouseReleased (non-HTML5 UIs)

    Never reports success for an HTML5-draggable source when neither CDP intercept
    nor the JS fallback could complete a drop.
    """
    steps = max(1, int(steps))
    intercepted: asyncio.Future = asyncio.get_running_loop().create_future()

    def _on_intercepted(event: Any, _sid: Any = None) -> None:
        if not intercepted.done():
            intercepted.set_result(_drag_data_from_intercept_event(event))

    unregister = register_drag_intercepted(cdp_client, _on_intercepted)
    try:
        await set_intercept_drags(cdp_client, session_id, True)
        await cdp_client.send.Input.dispatchMouseEvent(
            params={
                "type": "mouseMoved",
                "x": sx,
                "y": sy,
                "button": "none",
                "buttons": 0,
                "clickCount": 0,
                "modifiers": 0,
            },
            session_id=session_id,
        )
        await cdp_client.send.Input.dispatchMouseEvent(
            params={
                "type": "mousePressed",
                "x": sx,
                "y": sy,
                "button": "left",
                "buttons": 1,
                "clickCount": 1,
                "modifiers": 0,
            },
            session_id=session_id,
        )
        for step in range(1, steps + 1):
            frac = step / steps
            await dispatch_mouse_move(
                cdp_client,
                session_id,
                x=sx + (tx - sx) * frac,
                y=sy + (ty - sy) * frac,
                buttons=1,
            )
            if intercepted.done():
                break
            if delay_ms:
                await asyncio.sleep(delay_ms / 1000.0)

        if intercepted.done():
            data = intercepted.result()
            await dispatch_drag_event(cdp_client, session_id, event_type="dragEnter", x=tx, y=ty, data=data)
            await dispatch_drag_event(cdp_client, session_id, event_type="dragOver", x=tx, y=ty, data=data)
            await dispatch_drag_event(cdp_client, session_id, event_type="drop", x=tx, y=ty, data=data)
            await cdp_client.send.Input.dispatchMouseEvent(
                params={
                    "type": "mouseReleased",
                    "x": tx,
                    "y": ty,
                    "button": "left",
                    "buttons": 0,
                    "clickCount": 1,
                    "modifiers": 0,
                },
                session_id=session_id,
            )
            return {"ok": True, "detail": "dragged (html5 cdp)", "mode": "html5_cdp"}

        await cdp_client.send.Input.dispatchMouseEvent(
            params={
                "type": "mouseReleased",
                "x": tx,
                "y": ty,
                "button": "left",
                "buttons": 0,
                "clickCount": 1,
                "modifiers": 0,
            },
            session_id=session_id,
        )

        if source_is_html5:
            if source_backend_node_id is None or target_backend_node_id is None:
                return {
                    "ok": False,
                    "detail": (
                        "HTML5 drag did not start (no Input.dragIntercepted); "
                        "cannot run JS fallback without source/target backend node ids"
                    ),
                    "mode": "html5_failed",
                }
            js_result = await html5_drag_drop_via_js(
                cdp_client,
                session_id,
                source_backend_node_id=source_backend_node_id,
                target_backend_node_id=target_backend_node_id,
            )
            if js_result.get("ok"):
                return {"ok": True, "detail": "dragged (html5 js)", "mode": "html5_js"}
            error = js_result.get("error") or "HTML5 JS drag/drop produced no effect"
            return {"ok": False, "detail": str(error), "mode": "html5_failed"}

        return {"ok": True, "detail": "dragged", "mode": "pointer"}
    finally:
        try:
            await set_intercept_drags(cdp_client, session_id, False)
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        try:
            unregister()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


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


async def set_file_input_files(
    cdp_client: Any,
    session_id: str,
    backend_node_id: int,
    files: list[str],
) -> None:
    """Attach local file path(s) to an ``<input type=file>`` via CDP.

    Uses ``backendNodeId`` (not ``nodeId``): ``nodeId`` can fail to grant the
    renderer file access and leave a fakepath that later submits as
    ``ERR_FILE_NOT_FOUND``. Paths must be absolute and readable by the Chrome
    process that owns the CDP session.
    """
    await cdp_client.send.DOM.setFileInputFiles(
        params={"files": list(files), "backendNodeId": int(backend_node_id)},
        session_id=session_id,
    )


SUPPORTED_DROP_MIME_TYPES: frozenset[str] = frozenset(
    {
        "text/plain",
        "text/uri-list",
        "text/html",
    }
)


def build_external_drag_data(
    *,
    paths: list[str] | None = None,
    items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a CDP ``DragData`` payload for an external (out-of-page) drop.

    ``paths`` become ``files`` (absolute paths Chrome can open). ``items`` are
    MIME payloads; only :data:`SUPPORTED_DROP_MIME_TYPES` are accepted — any
    other mime raises ``DriverUnsupported``.
    """
    file_paths = [str(path) for path in (paths or []) if str(path or "").strip()]
    mime_items: list[dict[str, str]] = []
    for raw in items or []:
        if not isinstance(raw, dict):
            raise exceptions.DriverUnsupported("drop data items must be objects with mimeType/data")
        mime = str(raw.get("mimeType") or raw.get("mime_type") or "").strip().lower()
        if not mime:
            raise exceptions.DriverUnsupported("drop data item missing mimeType")
        if mime not in SUPPORTED_DROP_MIME_TYPES:
            supported = ", ".join(sorted(SUPPORTED_DROP_MIME_TYPES))
            raise exceptions.DriverUnsupported(
                f"drop MIME type {mime!r} is not supported (supported: {supported})"
            )
        mime_items.append({"mimeType": mime, "data": str(raw.get("data") or "")})
    if not file_paths and not mime_items:
        raise exceptions.DriverError("drop requires at least one path or data item")
    return {
        "items": mime_items,
        "files": file_paths,
        "dragOperationsMask": 1,  # copy
    }


async def perform_external_drop(
    cdp_client: Any,
    session_id: str,
    *,
    x: float,
    y: float,
    paths: list[str] | None = None,
    items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Drop external files and/or MIME data onto page coordinates via CDP.

    Distinct from element→element ``perform_drag``: this synthesizes an
    out-of-page DataTransfer using ``Input.dispatchDragEvent`` only.
    """
    data = build_external_drag_data(paths=paths, items=items)
    await dispatch_drag_event(cdp_client, session_id, event_type="dragEnter", x=x, y=y, data=data)
    await dispatch_drag_event(cdp_client, session_id, event_type="dragOver", x=x, y=y, data=data)
    await dispatch_drag_event(cdp_client, session_id, event_type="drop", x=x, y=y, data=data)
    file_count = len(data.get("files") or [])
    item_count = len(data.get("items") or [])
    parts: list[str] = []
    if file_count:
        parts.append(f"{file_count} file(s)")
    if item_count:
        parts.append(f"{item_count} mime item(s)")
    return {
        "ok": True,
        "detail": f"dropped {' + '.join(parts)} via CDP Input.dispatchDragEvent",
        "files": list(data.get("files") or []),
        "items": list(data.get("items") or []),
    }


async def enable_page_domain(cdp_client: Any, session_id: str) -> None:
    """Enable the Page domain (required for JS dialog events/handling)."""
    await cdp_client.send.Page.enable(params={}, session_id=session_id)


async def handle_javascript_dialog(
    cdp_client: Any,
    session_id: str,
    *,
    accept: bool,
    prompt_text: str | None = None,
) -> None:
    """Accept or dismiss the currently open JS dialog via CDP."""
    params: dict[str, Any] = {"accept": bool(accept)}
    if prompt_text is not None:
        params["promptText"] = str(prompt_text)
    await cdp_client.send.Page.handleJavaScriptDialog(params=params, session_id=session_id)


def register_javascript_dialog_opening(cdp_client: Any, callback: Any) -> Any:
    """Register a ``Page.javascriptDialogOpening`` handler; return unregister callable."""

    def _unregister_registry() -> None:
        registry = getattr(cdp_client, "_event_registry", None)
        if registry is not None and hasattr(registry, "unregister"):
            registry.unregister("Page.javascriptDialogOpening")

    register = getattr(cdp_client, "register", None)
    page_reg = getattr(register, "Page", None) if register is not None else None
    if page_reg is not None and hasattr(page_reg, "javascriptDialogOpening"):
        page_reg.javascriptDialogOpening(callback)
        return _unregister_registry

    registry = getattr(cdp_client, "_event_registry", None)
    if registry is not None and hasattr(registry, "register"):
        registry.register("Page.javascriptDialogOpening", callback)
        return _unregister_registry

    custom = getattr(cdp_client, "register_event", None)
    if callable(custom):
        custom("Page.javascriptDialogOpening", callback)
        unregister = getattr(cdp_client, "unregister_event", None)
        if callable(unregister):
            return lambda: unregister("Page.javascriptDialogOpening")
        return lambda: None

    raise exceptions.DriverUnsupported("CDP client cannot register Page.javascriptDialogOpening handlers")


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
    "SUPPORTED_DROP_MIME_TYPES",
    "build_external_drag_data",
    "call_function_on_backend_node",
    "describe_node_backend_id",
    "dispatch_drag_event",
    "dispatch_key_press",
    "dispatch_mouse_click",
    "dispatch_mouse_move",
    "enable_page_domain",
    "evaluate_expression",
    "get_document_root_node_id",
    "handle_javascript_dialog",
    "html5_drag_drop_via_js",
    "insert_text",
    "perform_drag",
    "perform_external_drop",
    "query_selector_node_id",
    "register_drag_intercepted",
    "register_javascript_dialog_opening",
    "request_node_id_from_object",
    "resolve_object_id_by_backend_node",
    "set_file_input_files",
    "set_intercept_drags",
]
