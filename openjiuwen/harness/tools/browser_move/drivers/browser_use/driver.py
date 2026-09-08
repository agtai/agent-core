# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""``BrowserDriver`` implementation backed by the browser-use sidecar.

This module imports NO ``browser_use``. All browser-use usage lives in
``drivers/browser_use/sidecar/session_adapter.py``, running in a separate
process with its own dependency closure (see ``drivers/browser_use/AGENTS``
notes / STAGE A handoff D1). Every method here serializes a
``drivers.base`` dataclass to a wire-safe dict, sends it to the sidecar via
``SidecarTransport``, and deserializes the reply back into the matching
``drivers.base`` dataclass.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Sequence

from openjiuwen.harness.tools.browser_move.drivers.base import (
    ActResult,
    Box,
    DriverHealth,
    DriverInfo,
    ElementRef,
    IndexRef,
    NavResult,
    NodeRef,
    Observation,
    ObservedElement,
    ResolvedElement,
    SelectorRef,
    TabRef,
    TextRef,
)
from openjiuwen.harness.tools.browser_move.drivers.browser_use.transport import SidecarTransport
from openjiuwen.harness.tools.browser_move.drivers.errors import DriverNotConnected


def _ref_to_wire(ref: ElementRef) -> dict[str, Any]:
    if isinstance(ref, IndexRef):
        return {"kind": "index", "index": ref.index, "driver_generation": ref.driver_generation}
    if isinstance(ref, SelectorRef):
        return {"kind": "selector", "css": ref.css, "nth": ref.nth}
    if isinstance(ref, TextRef):
        return {"kind": "text", "text": ref.text, "role": ref.role}
    if isinstance(ref, NodeRef):
        return {"kind": "node", "backend_node_id": ref.backend_node_id, "frame_id": ref.frame_id}
    raise TypeError(f"unsupported ElementRef type: {type(ref)!r}")


def _tab_to_wire(tab: TabRef) -> dict[str, Any]:
    return {"target_id": tab.target_id, "url": tab.url, "title": tab.title, "active": tab.active}


def _box_from_wire(data: dict[str, Any] | None) -> Box | None:
    if data is None:
        return None
    return Box(x=float(data["x"]), y=float(data["y"]), width=float(data["width"]), height=float(data["height"]))


def _tab_from_wire(data: dict[str, Any]) -> TabRef:
    return TabRef(
        target_id=str(data["target_id"]), url=str(data["url"]), title=str(data["title"]), active=bool(data["active"])
    )


def _element_from_wire(data: dict[str, Any]) -> ObservedElement:
    return ObservedElement(
        index=int(data["index"]),
        backend_node_id=int(data["backend_node_id"]),
        frame_id=data.get("frame_id"),
        tag=str(data["tag"]),
        role=data.get("role"),
        name=data.get("name"),
        value=data.get("value"),
        attributes=dict(data.get("attributes") or {}),
        box=_box_from_wire(data.get("box")),
        visible=bool(data.get("visible", False)),
        scrollable=bool(data.get("scrollable", False)),
    )


def _observation_from_wire(data: dict[str, Any]) -> Observation:
    return Observation(
        url=str(data["url"]),
        title=str(data["title"]),
        tabs=tuple(_tab_from_wire(t) for t in data.get("tabs") or ()),
        elements=tuple(_element_from_wire(e) for e in data.get("elements") or ()),
        ax_text=data.get("ax_text"),
        screenshot_b64=data.get("screenshot_b64"),
        pixels_above=int(data.get("pixels_above", 0)),
        pixels_below=int(data.get("pixels_below", 0)),
        errors=tuple(data.get("errors") or ()),
        is_pdf_viewer=bool(data.get("is_pdf_viewer", False)),
        captured_at=float(data.get("captured_at", time.time())),
        driver_generation=int(data.get("driver_generation", 0)),
    )


def _act_result_from_wire(data: dict[str, Any]) -> ActResult:
    return ActResult(
        ok=bool(data["ok"]),
        detail=str(data.get("detail", "")),
        document_changed=bool(data.get("document_changed", False)),
        driver_generation=int(data.get("driver_generation", 0)),
    )


def _nav_result_from_wire(data: dict[str, Any]) -> NavResult:
    return NavResult(
        url=str(data["url"]),
        title=str(data.get("title", "")),
        changed_document=bool(data.get("changed_document", False)),
        driver_generation=int(data.get("driver_generation", 0)),
    )


class BrowserUseDriver:
    """``BrowserDriver`` that delegates every call to the browser-use sidecar process."""

    def __init__(self, **_cfg: Any) -> None:
        self._transport: SidecarTransport | None = None
        self._driver_generation = 0
        self._last_observation: Observation | None = None
        self._cdp_url: str | None = None
        self._connected_at: float | None = None
        self._info: DriverInfo | None = None
        self._lock = asyncio.Lock()

    def _require_transport(self) -> SidecarTransport:
        if self._transport is None:
            raise DriverNotConnected("BrowserUseDriver.connect() has not been called")
        return self._transport

    # -- lifecycle ---------------------------------------------------

    async def connect(self, *, cdp_url: str, timeout_s: float = 30.0) -> DriverInfo:
        async with self._lock:
            transport = SidecarTransport()
            await transport.start(timeout_s=timeout_s)
            result = await transport.request(
                "connect", {"cdp_url": cdp_url, "timeout_s": timeout_s}, timeout_s=timeout_s
            )
            self._transport = transport
            self._cdp_url = cdp_url
            self._connected_at = time.time()
            self._info = DriverInfo(
                backend=str(result["backend"]),
                browser_version=str(result.get("browser_version", "")),
                protocol_version=str(result.get("protocol_version", "")),
                backend_version=str(result.get("backend_version", "")),
                initial_url=str(result.get("initial_url", "")),
            )
            return self._info

    async def health(self) -> DriverHealth:
        if self._transport is None:
            return DriverHealth(connected=False, url="", tab_count=0, latency_ms=0.0, error="not connected")
        try:
            result = await self._transport.request("health", {})
        except Exception as exc:  # noqa: BLE001 - health() contract: never raises
            return DriverHealth(connected=False, url="", tab_count=0, latency_ms=0.0, error=str(exc))
        return DriverHealth(
            connected=bool(result.get("connected", False)),
            url=str(result.get("url", "")),
            tab_count=int(result.get("tab_count", 0)),
            latency_ms=float(result.get("latency_ms", 0.0)),
            error=result.get("error"),
        )

    async def close(self) -> None:
        if self._transport is not None:
            await self._transport.close()
        self._transport = None
        self._last_observation = None

    # -- eyes ----------------------------------------------------------

    async def observe(
        self, *, include_dom: bool = True, include_screenshot: bool = False, cached: bool = False
    ) -> Observation:
        if cached and self._last_observation is not None:
            return self._last_observation
        async with self._lock:
            transport = self._require_transport()
            result = await transport.request(
                "observe",
                {"include_dom": include_dom, "include_screenshot": include_screenshot, "cached": False},
            )
            observation = _observation_from_wire(result)
            self._driver_generation = observation.driver_generation
            self._last_observation = observation
            return observation

    async def screenshot(self, *, full_page: bool = False, clip: Box | None = None) -> str:
        transport = self._require_transport()
        clip_data = None if clip is None else {"x": clip.x, "y": clip.y, "width": clip.width, "height": clip.height}
        return await transport.request("screenshot", {"full_page": full_page, "clip": clip_data})

    async def list_tabs(self) -> tuple[TabRef, ...]:
        transport = self._require_transport()
        result = await transport.request("list_tabs", {})
        return tuple(_tab_from_wire(t) for t in result)

    # -- id bridge -------------------------------------------------------

    async def resolve(self, ref: ElementRef) -> ResolvedElement:
        transport = self._require_transport()
        result = await transport.request("resolve", {"ref": _ref_to_wire(ref)})
        return ResolvedElement(
            backend_node_id=int(result["backend_node_id"]),
            frame_id=result.get("frame_id"),
            box=_box_from_wire(result.get("box")),
            visible=bool(result.get("visible", False)),
            tag=str(result.get("tag", "")),
            attributes=dict(result.get("attributes") or {}),
        )

    async def stamp(self, ref: ElementRef, *, attribute: str, value: str) -> str:
        transport = self._require_transport()
        result = await transport.request("stamp", {"ref": _ref_to_wire(ref), "attribute": attribute, "value": value})
        return str(result)

    # -- hands: navigation ------------------------------------------------

    async def navigate(
        self, url: str, *, wait_until: str = "load", timeout_ms: int | None = None, new_tab: bool = False
    ) -> NavResult:
        transport = self._require_transport()
        result = await transport.request(
            "navigate", {"url": url, "wait_until": wait_until, "timeout_ms": timeout_ms, "new_tab": new_tab}
        )
        nav_result = _nav_result_from_wire(result)
        self._driver_generation = nav_result.driver_generation
        return nav_result

    async def go_back(self) -> NavResult:
        transport = self._require_transport()
        result = await transport.request("go_back", {})
        nav_result = _nav_result_from_wire(result)
        self._driver_generation = nav_result.driver_generation
        return nav_result

    async def go_forward(self) -> NavResult:
        transport = self._require_transport()
        result = await transport.request("go_forward", {})
        nav_result = _nav_result_from_wire(result)
        self._driver_generation = nav_result.driver_generation
        return nav_result

    async def reload(self) -> NavResult:
        transport = self._require_transport()
        result = await transport.request("reload", {})
        nav_result = _nav_result_from_wire(result)
        self._driver_generation = nav_result.driver_generation
        return nav_result

    # -- hands: element actions -------------------------------------------

    async def click(
        self, ref: ElementRef, *, button: str = "left", click_count: int = 1, modifiers: Sequence[str] = ()
    ) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "click",
            {"ref": _ref_to_wire(ref), "button": button, "click_count": click_count, "modifiers": list(modifiers)},
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def type_text(
        self, ref: ElementRef, text: str, *, clear: bool = True, press_enter: bool = False, sensitive: bool = False
    ) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "type_text",
            {"ref": _ref_to_wire(ref), "text": text, "clear": clear, "press_enter": press_enter, "sensitive": sensitive},
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def press_key(self, keys: str) -> ActResult:
        transport = self._require_transport()
        result = await transport.request("press_key", {"keys": keys})
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def select_option(
        self, ref: ElementRef, *, value: str | None = None, label: str | None = None
    ) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "select_option", {"ref": _ref_to_wire(ref), "value": value, "label": label}
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def set_checked(self, ref: ElementRef, checked: bool) -> ActResult:
        transport = self._require_transport()
        result = await transport.request("set_checked", {"ref": _ref_to_wire(ref), "checked": checked})
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def scroll(self, *, direction: str, amount: float, ref: ElementRef | None = None) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "scroll", {"direction": direction, "amount": amount, "ref": None if ref is None else _ref_to_wire(ref)}
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def upload_files(self, ref: ElementRef, paths: Sequence[str]) -> ActResult:
        transport = self._require_transport()
        result = await transport.request("upload_files", {"ref": _ref_to_wire(ref), "paths": list(paths)})
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def drag(
        self, source: ElementRef, target: ElementRef, *, steps: int = 10, delay_ms: int = 0
    ) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "drag",
            {"source": _ref_to_wire(source), "target": _ref_to_wire(target), "steps": steps, "delay_ms": delay_ms},
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def switch_tab(self, tab: TabRef) -> ActResult:
        transport = self._require_transport()
        result = await transport.request("switch_tab", {"tab": _tab_to_wire(tab)})
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    async def close_tab(self, tab: TabRef) -> ActResult:
        transport = self._require_transport()
        result = await transport.request("close_tab", {"tab": _tab_to_wire(tab)})
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result

    # -- evaluate and wait ------------------------------------------------

    async def evaluate(
        self, source: str, *, args: Any = None, await_promise: bool = True, return_by_value: bool = True
    ) -> Any:
        transport = self._require_transport()
        return await transport.request(
            "evaluate",
            {"source": source, "args": args, "await_promise": await_promise, "return_by_value": return_by_value},
        )

    async def wait_load_state(self, state: str = "load", *, timeout_ms: int) -> ActResult:
        transport = self._require_transport()
        result = await transport.request(
            "wait_load_state", {"state": state, "timeout_ms": timeout_ms}, timeout_s=(timeout_ms / 1000.0) + 5.0
        )
        act_result = _act_result_from_wire(result)
        self._driver_generation = act_result.driver_generation
        return act_result


__all__ = ["BrowserUseDriver"]
