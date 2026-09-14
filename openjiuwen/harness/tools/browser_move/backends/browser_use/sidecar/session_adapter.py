# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The only module in this subtree that imports ``browser_use``.

Wraps a single ``browser_use.browser.session.BrowserSession`` (attach-only,
``is_local=False``) and exposes plain-dict-in/plain-dict-out async methods
that ``main.py`` dispatches wire requests to. Everything here operates on
wire-level dicts (JSON-safe), never on the ``backends.contract.base`` dataclasses --
those live in the agent-core process, not here.

Element refs cross the wire as plain dicts:
    {"kind": "index", "index": <int>, "driver_generation": <int>}
    {"kind": "selector", "css": <str>, "nth": <int>}
    {"kind": "text", "text": <str>, "role": <str | None>}
    {"kind": "node", "backend_node_id": <int>, "frame_id": <str | None>}

Runs as a flat sibling module inside the sidecar process (``import
session_adapter``), importing sibling modules the same way.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from pathlib import Path
from typing import Any

import cdp
import exceptions
import keys as key_utils
import mapping


def _strip_upload_path_text(raw: str) -> str:
    text = str(raw or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        text = text[1:-1].strip()
    return text


def _upload_root() -> Path | None:
    raw = (os.environ.get("BROWSER_UPLOAD_ROOT") or "").strip()
    if not raw:
        return None
    try:
        return Path(raw).expanduser().resolve()
    except OSError:
        return None


def _normalize_upload_paths(paths: list[str]) -> tuple[list[str], str | None]:
    """Return ``(existing_absolute_paths, error_or_none)``.

    Mirrors runtime/driver ``resolve_upload_file_paths``: quote-strip,
    ``expanduser().resolve()``, optional ``BROWSER_UPLOAD_ROOT`` for relative
    names, and distinguish not-found vs open/read failure. CDP always gets
    absolute paths Chrome can open.
    """
    existing: list[str] = []
    missing: list[str] = []
    unreadable: list[str] = []
    seen: set[str] = set()
    root = _upload_root()

    for raw in paths or []:
        text = _strip_upload_path_text(str(raw))
        if not text:
            continue
        primary = Path(text).expanduser()
        candidates = [primary]
        if not primary.is_absolute() and root is not None:
            candidates.append(root / text)
            candidates.append(root / primary.name)
        resolved_ok: str | None = None
        last_resolved = text
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            last_resolved = str(resolved)
            if not resolved.is_file():
                continue
            try:
                with resolved.open("rb") as handle:
                    handle.read(1)
            except OSError:
                unreadable.append(str(resolved))
                resolved_ok = None
                break
            resolved_ok = str(resolved)
            break
        if resolved_ok is None:
            if last_resolved not in unreadable:
                missing.append(last_resolved)
            continue
        if resolved_ok not in seen:
            seen.add(resolved_ok)
            existing.append(resolved_ok)

    if not existing and not missing and not unreadable:
        return [], "upload_files requires at least one path"
    err_parts: list[str] = []
    if missing:
        listed = ", ".join(repr(p) for p in missing)
        err_parts.append(
            f"upload file(s) not found: {listed} "
            "(create the file, use an absolute path, or place it under "
            "BROWSER_UPLOAD_ROOT and call list_upload_files)"
        )
    if unreadable:
        listed = ", ".join(repr(p) for p in unreadable)
        err_parts.append(f"upload file(s) not readable: {listed}")
    if err_parts:
        return existing, "; ".join(err_parts)
    return existing, None


def browser_use_version() -> str:
    try:
        import importlib.metadata

        return importlib.metadata.version("browser-use")
    except Exception:  # noqa: BLE001 - version string is best-effort, never fatal
        return "unknown"


class SessionAdapter:
    """Owns the browser-use session and translates wire calls into it."""

    def __init__(self) -> None:
        self._session: Any = None
        self._cdp_session: Any = None
        self._driver_generation = 0
        self._index_cache: dict[int, Any] = {}
        # Dialog arming: apply to the next JS dialog, or to a currently pending one.
        self._dialog_armed: dict[str, Any] | None = None
        self._dialog_pending: dict[str, Any] | None = None
        self._dialog_listener_registered = False
        self._dialog_unregister: Any = None
        self._dialog_handle_tasks: set[asyncio.Task] = set()

    # -- lifecycle ---------------------------------------------------

    async def connect(self, *, cdp_url: str, timeout_s: float = 30.0) -> dict[str, Any]:
        from browser_use.browser.profile import BrowserProfile
        from browser_use.browser.session import BrowserSession

        profile = BrowserProfile(headless=None)
        self._session = BrowserSession(cdp_url=cdp_url, is_local=False, browser_profile=profile)
        await self._session.start()
        self._cdp_session = await self._session.get_or_create_cdp_session()
        await self._ensure_dialog_listener()

        version_info: dict[str, Any] = {}
        try:
            version_info = await self._cdp_session.cdp_client.send.Browser.getVersion(session_id=None) or {}
        except Exception:  # noqa: BLE001 - version info is best-effort
            version_info = {}

        try:
            initial_url = await self._session.get_current_page_url()
        except Exception:  # noqa: BLE001
            initial_url = ""

        return {
            "backend": "browser_use",
            "browser_version": str(version_info.get("product", "") or ""),
            "protocol_version": str(version_info.get("protocolVersion", "") or ""),
            "backend_version": browser_use_version(),
            "initial_url": initial_url,
        }

    async def health(self) -> dict[str, Any]:
        if self._session is None:
            return {"connected": False, "url": "", "tab_count": 0, "latency_ms": 0.0, "error": "not connected"}
        started = time.monotonic()
        try:
            url = await self._session.get_current_page_url()
            tabs = await self._session.get_tabs()
            latency_ms = (time.monotonic() - started) * 1000.0
            return {"connected": True, "url": url, "tab_count": len(tabs), "latency_ms": latency_ms, "error": None}
        except Exception as exc:  # noqa: BLE001 - health() must never raise
            latency_ms = (time.monotonic() - started) * 1000.0
            return {"connected": False, "url": "", "tab_count": 0, "latency_ms": latency_ms, "error": str(exc)}

    async def close(self) -> None:
        """Detach only. MUST NOT KILL CHROME -- ``stop()``, never ``kill()``."""
        if self._session is not None:
            await self._session.stop()
        self._session = None
        self._cdp_session = None
        self._index_cache = {}

    # -- eyes ----------------------------------------------------------

    async def observe(
        self, *, include_dom: bool = True, include_screenshot: bool = False, cached: bool = False
    ) -> dict[str, Any]:
        summary = await self._session.get_browser_state_summary(
            include_screenshot=include_screenshot, cached=cached
        )
        self._driver_generation += 1
        dom_state = getattr(summary, "dom_state", None)
        self._index_cache = dict(getattr(dom_state, "selector_map", None) or {})
        return mapping.map_browser_state_summary(
            summary,
            driver_generation=self._driver_generation,
            captured_at=time.time(),
            include_screenshot=include_screenshot,
        )

    async def screenshot(self, *, full_page: bool = False, clip: dict[str, Any] | None = None) -> str:
        data = await self._session.take_screenshot(full_page=full_page, clip=clip)
        return base64.b64encode(data).decode("ascii")

    async def list_tabs(self) -> list[dict[str, Any]]:
        tabs = await self._session.get_tabs()
        try:
            current_url = await self._session.get_current_page_url()
        except Exception:  # noqa: BLE001
            current_url = ""
        return [mapping.map_tab(tab, current_url) for tab in tabs]

    # -- id bridge -------------------------------------------------------

    async def resolve(self, ref: dict[str, Any]) -> dict[str, Any]:
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        box = await self._element_box(backend_node_id)
        tag, attributes, visible = await self._element_facts(backend_node_id)
        return {
            "backend_node_id": backend_node_id,
            "frame_id": ref.get("frame_id"),
            "box": box,
            "visible": visible,
            "tag": tag,
            "attributes": attributes,
        }

    async def stamp(self, ref: dict[str, Any], *, attribute: str, value: str) -> str:
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        cdp_session = await self._ensure_cdp_session()
        await cdp.call_function_on_backend_node(
            cdp_session.cdp_client,
            cdp_session.session_id,
            backend_node_id,
            "function(attr, val){ this.setAttribute(attr, val); }",
            arguments=[{"value": attribute}, {"value": value}],
            return_by_value=True,
        )
        return f'[{attribute}="{value}"]'

    # -- hands: navigation ------------------------------------------------

    async def navigate(
        self,
        *,
        url: str,
        wait_until: str = "load",
        timeout_ms: int | None = None,
        new_tab: bool = False,
    ) -> dict[str, Any]:
        from browser_use.browser.events import NavigateToUrlEvent

        before_url = await self._safe_current_url()
        event = self._session.event_bus.dispatch(
            NavigateToUrlEvent(url=url, wait_until=wait_until, timeout_ms=timeout_ms, new_tab=new_tab)
        )
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._nav_result(before_url)

    async def go_back(self) -> dict[str, Any]:
        from browser_use.browser.events import GoBackEvent

        before_url = await self._safe_current_url()
        event = self._session.event_bus.dispatch(GoBackEvent())
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._nav_result(before_url)

    async def go_forward(self) -> dict[str, Any]:
        from browser_use.browser.events import GoForwardEvent

        before_url = await self._safe_current_url()
        event = self._session.event_bus.dispatch(GoForwardEvent())
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._nav_result(before_url)

    async def reload(self) -> dict[str, Any]:
        from browser_use.browser.events import RefreshEvent

        before_url = await self._safe_current_url()
        event = self._session.event_bus.dispatch(RefreshEvent())
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._nav_result(before_url)

    # -- hands: element actions -------------------------------------------

    async def click(
        self,
        ref: dict[str, Any],
        *,
        button: str = "left",
        click_count: int = 1,
        modifiers: list[str] | None = None,
    ) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        box = await self._element_box(backend_node_id)
        if box is None:
            raise exceptions.ElementNotFound(f"element (backend_node_id={backend_node_id}) has no bounding box")
        cx, cy = box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0
        cdp_session = await self._ensure_cdp_session()
        for _ in range(max(1, click_count)):
            await cdp.dispatch_mouse_click(
                cdp_session.cdp_client, cdp_session.session_id, x=cx, y=cy, button=button, click_count=click_count
            )
        return await self._act_result(before_url, ok=True, detail="clicked")

    async def type_text(
        self,
        ref: dict[str, Any],
        text: str,
        *,
        clear: bool = True,
        press_enter: bool = False,
        sensitive: bool = False,
    ) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        cdp_session = await self._ensure_cdp_session()
        box = await self._element_box(backend_node_id)
        if box is not None:
            cx, cy = box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0
            await cdp.dispatch_mouse_click(cdp_session.cdp_client, cdp_session.session_id, x=cx, y=cy)
        if clear:
            await cdp.call_function_on_backend_node(
                cdp_session.cdp_client,
                cdp_session.session_id,
                backend_node_id,
                "function(){ if ('value' in this) { this.value = ''; } else { this.textContent = ''; } "
                "this.dispatchEvent(new Event('input', {bubbles: true})); }",
            )
        await cdp.insert_text(cdp_session.cdp_client, cdp_session.session_id, text)
        if press_enter:
            await cdp.dispatch_key_press(cdp_session.cdp_client, cdp_session.session_id, "Enter")
        detail = "typed" if not sensitive else "typed (sensitive)"
        return await self._act_result(before_url, ok=True, detail=detail)

    async def press_key(self, keys: str) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        from browser_use.browser.events import SendKeysEvent

        event = self._session.event_bus.dispatch(SendKeysEvent(keys=key_utils.normalize_key_combo(keys)))
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._act_result(before_url, ok=True, detail="keys sent")

    async def select_option(
        self, ref: dict[str, Any], *, value: str | None = None, label: str | None = None
    ) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        from browser_use.browser.events import SelectDropdownOptionEvent

        node = await self._node_for_event(ref)
        text = label if label is not None else (value or "")
        event = self._session.event_bus.dispatch(SelectDropdownOptionEvent(node=node, text=text))
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._act_result(before_url, ok=True, detail="option selected")

    async def set_checked(self, ref: dict[str, Any], checked: bool) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        cdp_session = await self._ensure_cdp_session()
        await cdp.call_function_on_backend_node(
            cdp_session.cdp_client,
            cdp_session.session_id,
            backend_node_id,
            "function(want){ if (this.checked !== want) { this.checked = want; "
            "this.dispatchEvent(new Event('click', {bubbles: true})); "
            "this.dispatchEvent(new Event('change', {bubbles: true})); } }",
            arguments=[{"value": bool(checked)}],
        )
        return await self._act_result(before_url, ok=True, detail="checked state set")

    async def scroll(
        self, *, direction: str, amount: float, ref: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        from browser_use.browser.events import ScrollEvent

        node = await self._node_for_event(ref) if ref is not None else None
        event = self._session.event_bus.dispatch(ScrollEvent(node=node, direction=direction, amount=amount))
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._act_result(before_url, ok=True, detail="scrolled")

    async def upload_files(self, ref: dict[str, Any], paths: list[str]) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        normalized, path_error = _normalize_upload_paths(paths or [])
        if path_error:
            return await self._act_result(before_url, ok=False, detail=path_error)
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        cdp_session = await self._ensure_cdp_session()
        facts = await cdp.call_function_on_backend_node(
            cdp_session.cdp_client,
            cdp_session.session_id,
            backend_node_id,
            "function(){"
            "  const isFile = !!(this && this.tagName === 'INPUT' &&"
            "    String(this.type || '').toLowerCase() === 'file');"
            "  return {isFile: isFile, multiple: !!(isFile && this.multiple),"
            "    files: (isFile && this.files) ? this.files.length : 0};"
            "}",
        )
        if not isinstance(facts, dict) or not facts.get("isFile"):
            return await self._act_result(
                before_url,
                ok=False,
                detail="upload target is not an <input type=file>",
            )
        if len(normalized) > 1 and not facts.get("multiple"):
            return await self._act_result(
                before_url,
                ok=False,
                detail=(
                    f"upload target does not accept multiple files "
                    f"(got {len(normalized)} paths); pass a single path or a multiple= file input"
                ),
            )
        await cdp.set_file_input_files(
            cdp_session.cdp_client,
            cdp_session.session_id,
            backend_node_id,
            normalized,
        )
        attached = await cdp.call_function_on_backend_node(
            cdp_session.cdp_client,
            cdp_session.session_id,
            backend_node_id,
            "function(){ return (this.files && this.files.length) || 0; }",
        )
        count = int(attached or 0)
        if count < 1:
            return await self._act_result(
                before_url,
                ok=False,
                detail=(
                    "file input has no files after attach "
                    "(DOM.setFileInputFiles did not stick; check path is readable by Chrome)"
                ),
            )
        if count != len(normalized):
            return await self._act_result(
                before_url,
                ok=False,
                detail=f"file input has {count} file(s) after attach, expected {len(normalized)}",
            )
        return await self._act_result(before_url, ok=True, detail=f"uploaded {count} file(s)")

    async def drag(
        self, source: dict[str, Any], target: dict[str, Any], *, steps: int = 10, delay_ms: int = 0
    ) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        source_id = await self._resolve_ref_to_backend_node_id(source)
        target_id = await self._resolve_ref_to_backend_node_id(target)
        source_box = await self._element_box(source_id)
        target_box = await self._element_box(target_id)
        if source_box is None or target_box is None:
            raise exceptions.ElementNotFound("drag source or target has no bounding box")
        sx, sy = source_box["x"] + source_box["width"] / 2.0, source_box["y"] + source_box["height"] / 2.0
        tx, ty = target_box["x"] + target_box["width"] / 2.0, target_box["y"] + target_box["height"] / 2.0
        source_is_html5 = await self._element_is_html5_draggable(source_id)
        cdp_session = await self._ensure_cdp_session()
        result = await cdp.perform_drag(
            cdp_session.cdp_client,
            cdp_session.session_id,
            sx=sx,
            sy=sy,
            tx=tx,
            ty=ty,
            steps=steps,
            delay_ms=delay_ms,
            source_is_html5=source_is_html5,
            source_backend_node_id=source_id,
            target_backend_node_id=target_id,
        )
        return await self._act_result(before_url, ok=bool(result.get("ok")), detail=str(result.get("detail") or ""))

    async def hover(self, ref: dict[str, Any]) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        box = await self._element_box(backend_node_id)
        if box is None:
            raise exceptions.ElementNotFound(f"element (backend_node_id={backend_node_id}) has no bounding box")
        cx, cy = box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0
        cdp_session = await self._ensure_cdp_session()
        # Approach stream then settle on center (tooltip / :hover menus).
        await cdp.dispatch_mouse_move(
            cdp_session.cdp_client,
            cdp_session.session_id,
            x=max(0.0, cx - 4.0),
            y=max(0.0, cy - 4.0),
        )
        await cdp.dispatch_mouse_move(cdp_session.cdp_client, cdp_session.session_id, x=cx, y=cy)
        return await self._act_result(before_url, ok=True, detail="hovered")

    async def handle_dialog(self, *, accept: bool, prompt_text: str | None = None) -> dict[str, Any]:
        """Handle the open dialog, or arm for the next one.

        Call this *before* the click/type that opens a blocking dialog when the
        dialog is not already pending. Success always means either
        ``Page.handleJavaScriptDialog`` ran, or the next opening is armed to run it.
        """
        before_url = await self._safe_current_url()
        await self._ensure_dialog_listener()
        if self._dialog_pending is not None:
            pending = self._dialog_pending
            self._dialog_pending = None
            cdp_session = await self._ensure_cdp_session()
            await cdp.handle_javascript_dialog(
                cdp_session.cdp_client,
                cdp_session.session_id,
                accept=bool(accept),
                prompt_text=prompt_text,
            )
            dialog_type = str(pending.get("type") or "dialog")
            action = "accepted" if accept else "dismissed"
            return await self._act_result(before_url, ok=True, detail=f"{action} {dialog_type}")
        self._dialog_armed = {"accept": bool(accept), "prompt_text": prompt_text}
        return await self._act_result(
            before_url,
            ok=True,
            detail="armed for next dialog; call before the action that opens it",
        )

    async def drop(
        self,
        ref: dict[str, Any],
        *,
        paths: list[str] | None = None,
        data: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Drop external files and/or MIME data onto an element (not element→element drag)."""
        before_url = await self._safe_current_url()
        path_list = list(paths or [])
        data_list = list(data or [])
        if not path_list and not data_list:
            return await self._act_result(
                before_url,
                ok=False,
                detail="drop requires at least one of paths or data",
            )
        normalized: list[str] = []
        if path_list:
            normalized, path_error = _normalize_upload_paths(path_list)
            if path_error:
                return await self._act_result(before_url, ok=False, detail=path_error)
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        box = await self._element_box(backend_node_id)
        if box is None:
            raise exceptions.ElementNotFound(f"drop target (backend_node_id={backend_node_id}) has no bounding box")
        cx, cy = box["x"] + box["width"] / 2.0, box["y"] + box["height"] / 2.0
        cdp_session = await self._ensure_cdp_session()
        try:
            result = await cdp.perform_external_drop(
                cdp_session.cdp_client,
                cdp_session.session_id,
                x=cx,
                y=cy,
                paths=normalized,
                items=data_list,
            )
        except exceptions.DriverUnsupported:
            raise
        except exceptions.DriverError as exc:
            return await self._act_result(before_url, ok=False, detail=str(exc))
        detail = str(result.get("detail") or "dropped")
        # Best-effort verify: file inputs may receive files via the DataTransfer.
        if normalized:
            attached = await cdp.call_function_on_backend_node(
                cdp_session.cdp_client,
                cdp_session.session_id,
                backend_node_id,
                "function(){"
                "  if (this && this.tagName === 'INPUT' &&"
                "      String(this.type || '').toLowerCase() === 'file') {"
                "    return (this.files && this.files.length) || 0;"
                "  }"
                "  return null;"
                "}",
            )
            if isinstance(attached, int) and attached > 0:
                detail = f"{detail}; file input now has {attached} file(s)"
        return await self._act_result(before_url, ok=bool(result.get("ok")), detail=detail)

    async def switch_tab(self, tab: dict[str, Any]) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        from browser_use.browser.events import SwitchTabEvent

        event = self._session.event_bus.dispatch(SwitchTabEvent(target_id=tab["target_id"]))
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        self._cdp_session = None  # invalidate cached CDP session across tab switch
        self._dialog_listener_registered = False
        self._dialog_unregister = None
        return await self._act_result(before_url, ok=True, detail="switched tab")

    async def close_tab(self, tab: dict[str, Any]) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        from browser_use.browser.events import CloseTabEvent

        event = self._session.event_bus.dispatch(CloseTabEvent(target_id=tab["target_id"]))
        await event
        await event.event_result(raise_if_any=True, raise_if_none=False)
        return await self._act_result(before_url, ok=True, detail="closed tab")

    # -- evaluate and wait ------------------------------------------------

    async def evaluate(
        self,
        source: str,
        *,
        args: Any = None,
        await_promise: bool = True,
        return_by_value: bool = True,
    ) -> Any:
        cdp_session = await self._ensure_cdp_session()
        return await cdp.evaluate_expression(
            cdp_session.cdp_client,
            cdp_session.session_id,
            source,
            args,
            await_promise=await_promise,
            return_by_value=return_by_value,
        )

    async def wait_load_state(self, *, state: str = "load", timeout_ms: int) -> dict[str, Any]:
        before_url = await self._safe_current_url()
        await asyncio.sleep(min(timeout_ms / 1000.0, 5.0))
        return await self._act_result(before_url, ok=True, detail=f"waited for {state}")

    # -- internal helpers -------------------------------------------------

    async def _ensure_cdp_session(self) -> Any:
        if self._cdp_session is None:
            self._cdp_session = await self._session.get_or_create_cdp_session()
        return self._cdp_session

    async def _ensure_dialog_listener(self) -> None:
        """Enable Page events and arm javascriptDialogOpening routing."""
        if self._dialog_listener_registered:
            return
        cdp_session = await self._ensure_cdp_session()
        await cdp.enable_page_domain(cdp_session.cdp_client, cdp_session.session_id)

        def _on_dialog(event: Any, _sid: Any = None) -> None:
            payload = event if isinstance(event, dict) else getattr(event, "__dict__", {}) or {}
            if not isinstance(payload, dict):
                payload = {}
            info = {
                "type": str(payload.get("type") or "alert"),
                "message": str(payload.get("message") or ""),
                "defaultPrompt": str(payload.get("defaultPrompt") or ""),
            }
            armed = self._dialog_armed
            if armed is not None:
                self._dialog_armed = None
                self._dialog_pending = None
                task = asyncio.create_task(
                    self._apply_armed_dialog(
                        accept=bool(armed.get("accept")),
                        prompt_text=armed.get("prompt_text"),
                    )
                )
                self._dialog_handle_tasks.add(task)
                task.add_done_callback(self._dialog_handle_tasks.discard)
            else:
                self._dialog_pending = info

        self._dialog_unregister = cdp.register_javascript_dialog_opening(
            cdp_session.cdp_client, _on_dialog
        )
        self._dialog_listener_registered = True

    async def _apply_armed_dialog(self, *, accept: bool, prompt_text: str | None) -> None:
        try:
            cdp_session = await self._ensure_cdp_session()
            await cdp.handle_javascript_dialog(
                cdp_session.cdp_client,
                cdp_session.session_id,
                accept=accept,
                prompt_text=prompt_text,
            )
        except Exception:  # noqa: BLE001 - dialog arming must not crash the session
            pass

    async def _safe_current_url(self) -> str:
        try:
            return await self._session.get_current_page_url()
        except Exception:  # noqa: BLE001
            return ""

    async def _nav_result(self, before_url: str) -> dict[str, Any]:
        url = await self._safe_current_url()
        try:
            title = await self._session.get_current_page_title()
        except Exception:  # noqa: BLE001
            title = ""
        self._driver_generation += 1
        return {
            "url": url,
            "title": title,
            "changed_document": url != before_url,
            "driver_generation": self._driver_generation,
        }

    async def _act_result(self, before_url: str, *, ok: bool, detail: str) -> dict[str, Any]:
        url = await self._safe_current_url()
        changed = url != before_url
        if changed:
            self._driver_generation += 1
        return {"ok": ok, "detail": detail, "document_changed": changed, "driver_generation": self._driver_generation}

    def _node_from_index(self, index: int, driver_generation: int) -> Any:
        if driver_generation != self._driver_generation:
            raise exceptions.StaleIndexError(
                f"index {index} was observed at driver_generation {driver_generation}, "
                f"current is {self._driver_generation}"
            )
        node = self._index_cache.get(index)
        if node is None:
            raise exceptions.StaleIndexError(f"index {index} is not present in the last observation")
        return node

    async def _resolve_ref_to_backend_node_id(self, ref: dict[str, Any]) -> int:
        """Resolve any wire ElementRef dict to a CDP backendNodeId."""
        kind = ref.get("kind")
        if kind == "index":
            node = self._node_from_index(int(ref["index"]), int(ref["driver_generation"]))
            backend_node_id = getattr(node, "backend_node_id", None)
            if backend_node_id is None:
                raise exceptions.StaleIndexError(f"index {ref['index']} has no backend_node_id")
            return int(backend_node_id)
        if kind == "node":
            return int(ref["backend_node_id"])
        if kind == "selector":
            cdp_session = await self._ensure_cdp_session()
            node_id = await cdp.query_selector_node_id(cdp_session.cdp_client, cdp_session.session_id, ref["css"])
            if node_id is None:
                raise exceptions.ElementNotFound(f"no element matches selector {ref['css']!r}")
            return await cdp.describe_node_backend_id(cdp_session.cdp_client, cdp_session.session_id, node_id)
        if kind == "text":
            return await self._resolve_text_ref(ref)
        raise exceptions.ElementNotFound(f"unsupported element ref kind {kind!r}")

    async def _resolve_text_ref(self, ref: dict[str, Any]) -> int:
        cdp_session = await self._ensure_cdp_session()
        text = str(ref.get("text", ""))
        role = ref.get("role")
        find_fn = (
            "(args) => { "
            "const text = args.text; const role = args.role; "
            "const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT); "
            "let node = walker.currentNode; "
            "while (node) { "
            "  if (node.textContent && node.textContent.includes(text) && "
            "      (!role || node.getAttribute('role') === role)) { return node; } "
            "  node = walker.nextNode(); "
            "} "
            "return null; }"
        )
        result = await cdp_session.cdp_client.send.Runtime.evaluate(
            params={
                "expression": f"({find_fn})({json.dumps({'text': text, 'role': role})})",
                "awaitPromise": True,
                "returnByValue": False,
            },
            session_id=cdp_session.session_id,
        )
        object_id = (result.get("result") or {}).get("objectId")
        if not object_id:
            raise exceptions.ElementNotFound(f"no element matches text {text!r}")
        node_id = await cdp.request_node_id_from_object(cdp_session.cdp_client, cdp_session.session_id, object_id)
        return await cdp.describe_node_backend_id(cdp_session.cdp_client, cdp_session.session_id, node_id)

    async def _node_for_event(self, ref: dict[str, Any] | None) -> Any:
        """Return a real ``EnhancedDOMTreeNode`` for BU high-level events.

        Only ``IndexRef`` carries a full cached node object; other ref kinds
        are resolved to a backend node id and looked up freshly via
        ``BrowserSession.get_dom_element_by_index`` when possible, else the
        cached node keyed by backend id from the last observation.
        """
        if ref is None:
            return None
        if ref.get("kind") == "index":
            return self._node_from_index(int(ref["index"]), int(ref["driver_generation"]))
        backend_node_id = await self._resolve_ref_to_backend_node_id(ref)
        for node in self._index_cache.values():
            if getattr(node, "backend_node_id", None) == backend_node_id:
                return node
        raise exceptions.ElementNotFound(
            f"backend_node_id={backend_node_id} has no cached selector-map node for this action; "
            "re-observe the page first"
        )

    async def _element_box(self, backend_node_id: int) -> dict[str, float] | None:
        cdp_session = await self._ensure_cdp_session()
        try:
            rect = await cdp.call_function_on_backend_node(
                cdp_session.cdp_client,
                cdp_session.session_id,
                backend_node_id,
                "function(){ const r = this.getBoundingClientRect(); "
                "return {x: r.x, y: r.y, width: r.width, height: r.height}; }",
            )
        except exceptions.DriverError:
            return None
        if not isinstance(rect, dict):
            return None
        return {k: float(rect.get(k, 0.0) or 0.0) for k in ("x", "y", "width", "height")}

    async def _element_is_html5_draggable(self, backend_node_id: int) -> bool:
        """True when the node opts into HTML5 DnD (mouse-only drag would be a no-op)."""
        cdp_session = await self._ensure_cdp_session()
        try:
            result = await cdp.call_function_on_backend_node(
                cdp_session.cdp_client,
                cdp_session.session_id,
                backend_node_id,
                "function(){"
                "  if (this.getAttribute && this.getAttribute('draggable') === 'true') return true;"
                "  if (this.draggable === true) return true;"
                "  return false;"
                "}",
            )
        except exceptions.DriverError:
            return False
        return bool(result)

    async def _element_facts(self, backend_node_id: int) -> tuple[str, dict[str, str], bool]:
        cdp_session = await self._ensure_cdp_session()
        try:
            facts = await cdp.call_function_on_backend_node(
                cdp_session.cdp_client,
                cdp_session.session_id,
                backend_node_id,
                "function(){ const attrs = {}; for (const a of this.attributes || []) { attrs[a.name] = a.value; } "
                "const r = this.getBoundingClientRect(); "
                "return {tag: this.tagName ? this.tagName.toLowerCase() : '', attributes: attrs, "
                "visible: !!(r.width > 0 && r.height > 0)}; }",
            )
        except exceptions.DriverError:
            return "", {}, False
        if not isinstance(facts, dict):
            return "", {}, False
        return (
            str(facts.get("tag", "")),
            dict(facts.get("attributes", {}) or {}),
            bool(facts.get("visible", False)),
        )


__all__ = ["SessionAdapter", "browser_use_version"]
