# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Scripted BrowserDriver fake for B1 unit tests — never spawns a sidecar."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Sequence

from openjiuwen.harness.tools.browser_move.drivers.base import (
    ActResult,
    Box,
    BrowserDriver,
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
from openjiuwen.harness.tools.browser_move.drivers.errors import (
    DriverNotConnected,
    StaleIndexError,
    StaleNodeError,
)


@dataclass
class ActCall:
    """One recorded hands/bridge call on a FakeDriver."""

    method: str
    kwargs: dict[str, Any] = field(default_factory=dict)


def _default_observation(*, driver_generation: int = 1) -> Observation:
    tab = TabRef(target_id="tab-1", url="https://example.test/", title="Example", active=True)
    element = ObservedElement(
        index=1,
        backend_node_id=42,
        frame_id=None,
        tag="button",
        role="button",
        name="Go",
        value=None,
        visible=True,
    )
    return Observation(
        url="https://example.test/",
        title="Example",
        tabs=(tab,),
        elements=(element,),
        ax_text=None,
        screenshot_b64=None,
        pixels_above=0,
        pixels_below=0,
        errors=(),
        is_pdf_viewer=False,
        captured_at=time.time(),
        driver_generation=driver_generation,
    )


class FakeDriver:
    """BrowserDriver backed by a scripted observation queue and in-memory state."""

    def __init__(
        self,
        observations: Sequence[Observation] | None = None,
        *,
        driver_generation: int = 1,
        vanished_backend_node_ids: frozenset[int] | None = None,
        evaluate_handler: Any = None,
    ) -> None:
        self._observations = list(observations or [_default_observation(driver_generation=driver_generation)])
        self._obs_index = 0
        self._driver_generation = int(driver_generation)
        self._connected = False
        self._vanished = vanished_backend_node_ids or frozenset()
        self._evaluate_handler = evaluate_handler
        self.act_calls: list[ActCall] = []
        self._info = DriverInfo(
            backend="fake",
            browser_version="fake/1.0",
            protocol_version="1",
            backend_version="fake/1.0",
            initial_url="https://example.test/",
        )

    @property
    def driver_generation(self) -> int:
        return self._driver_generation

    def bump_driver_generation(self, *, delta: int = 1) -> None:
        self._driver_generation += int(delta)

    def _require_connected(self) -> None:
        if not self._connected:
            raise DriverNotConnected("FakeDriver.connect() has not been called")

    def _record(self, method: str, **kwargs: Any) -> None:
        self.act_calls.append(ActCall(method=method, kwargs=dict(kwargs)))

    def _act_result(self, *, document_changed: bool = False) -> ActResult:
        return ActResult(
            ok=True,
            detail="ok",
            document_changed=document_changed,
            driver_generation=self._driver_generation,
        )

    def _nav_result(self, *, changed_document: bool = False) -> NavResult:
        obs = self._observations[min(self._obs_index, len(self._observations) - 1)]
        return NavResult(
            url=obs.url,
            title=obs.title,
            changed_document=changed_document,
            driver_generation=self._driver_generation,
        )

    async def connect(self, *, cdp_url: str, timeout_s: float = 30.0) -> DriverInfo:
        _ = (cdp_url, timeout_s)
        self._connected = True
        return self._info

    async def health(self) -> DriverHealth:
        if not self._connected:
            return DriverHealth(connected=False, url="", tab_count=0, latency_ms=0.0, error="not connected")
        obs = self._observations[min(self._obs_index, len(self._observations) - 1)]
        return DriverHealth(connected=True, url=obs.url, tab_count=len(obs.tabs), latency_ms=1.0)

    async def close(self) -> None:
        self._connected = False

    async def observe(
        self,
        *,
        include_dom: bool = True,
        include_screenshot: bool = False,
        cached: bool = False,
    ) -> Observation:
        _ = (include_dom, include_screenshot, cached)
        self._require_connected()
        if self._obs_index >= len(self._observations):
            return self._observations[-1]
        observation = self._observations[self._obs_index]
        self._obs_index += 1
        self._driver_generation = observation.driver_generation
        return observation

    async def screenshot(self, *, full_page: bool = False, clip: Box | None = None) -> str:
        _ = (full_page, clip)
        self._require_connected()
        self._record("screenshot", full_page=full_page, clip=clip)
        return "ZmFrZS1zY3JlZW5zaG90"

    async def list_tabs(self) -> tuple[TabRef, ...]:
        self._require_connected()
        obs = self._observations[min(self._obs_index, len(self._observations) - 1)]
        return obs.tabs

    async def resolve(self, ref: ElementRef) -> ResolvedElement:
        self._require_connected()
        self._record("resolve", ref=ref)
        if isinstance(ref, IndexRef):
            if ref.driver_generation != self._driver_generation:
                raise StaleIndexError(f"stale index {ref.index} for generation {ref.driver_generation}")
            obs = self._observations[min(self._obs_index, len(self._observations) - 1)]
            element = next((e for e in obs.elements if e.index == ref.index), obs.elements[0])
            return ResolvedElement(
                backend_node_id=element.backend_node_id,
                frame_id=element.frame_id,
                box=element.box,
                visible=element.visible,
                tag=element.tag,
                attributes=dict(element.attributes),
            )
        if isinstance(ref, NodeRef):
            if ref.backend_node_id in self._vanished:
                raise StaleNodeError(f"backend node {ref.backend_node_id} vanished")
            return ResolvedElement(
                backend_node_id=ref.backend_node_id,
                frame_id=ref.frame_id,
                box=None,
                visible=True,
                tag="button",
            )
        if isinstance(ref, SelectorRef):
            return ResolvedElement(
                backend_node_id=99,
                frame_id=None,
                box=None,
                visible=True,
                tag="div",
            )
        if isinstance(ref, TextRef):
            return ResolvedElement(
                backend_node_id=100,
                frame_id=None,
                box=None,
                visible=True,
                tag="span",
            )
        raise TypeError(f"unsupported ref type: {type(ref)!r}")

    async def stamp(self, ref: ElementRef, *, attribute: str, value: str) -> str:
        self._require_connected()
        self._record("stamp", ref=ref, attribute=attribute, value=value)
        if isinstance(ref, IndexRef) and ref.driver_generation != self._driver_generation:
            raise StaleIndexError(
                f"stale index {ref.index} for generation {ref.driver_generation}; "
                f"current={self._driver_generation}"
            )
        if isinstance(ref, NodeRef) and ref.backend_node_id in self._vanished:
            raise StaleNodeError(f"backend node {ref.backend_node_id} vanished")
        if isinstance(ref, SelectorRef):
            return ref.css
        return f'[{attribute}="{value}"]'

    async def navigate(
        self,
        url: str,
        *,
        wait_until: str = "load",
        timeout_ms: int | None = None,
        new_tab: bool = False,
    ) -> NavResult:
        _ = (wait_until, timeout_ms, new_tab)
        self._require_connected()
        self._record("navigate", url=url)
        return self._nav_result(changed_document=True)

    async def go_back(self) -> NavResult:
        self._require_connected()
        self._record("go_back")
        return self._nav_result(changed_document=True)

    async def go_forward(self) -> NavResult:
        self._require_connected()
        self._record("go_forward")
        return self._nav_result(changed_document=True)

    async def reload(self) -> NavResult:
        self._require_connected()
        self._record("reload")
        return self._nav_result(changed_document=True)

    async def click(
        self,
        ref: ElementRef,
        *,
        button: str = "left",
        click_count: int = 1,
        modifiers: Sequence[str] = (),
    ) -> ActResult:
        self._require_connected()
        self._record("click", ref=ref, button=button, click_count=click_count, modifiers=tuple(modifiers))
        return self._act_result(document_changed=False)

    async def type_text(
        self,
        ref: ElementRef,
        text: str,
        *,
        clear: bool = True,
        press_enter: bool = False,
        sensitive: bool = False,
    ) -> ActResult:
        self._require_connected()
        self._record(
            "type_text",
            ref=ref,
            text=text,
            clear=clear,
            press_enter=press_enter,
            sensitive=sensitive,
        )
        return self._act_result(document_changed=False)

    async def press_key(self, keys: str) -> ActResult:
        self._require_connected()
        self._record("press_key", keys=keys)
        return self._act_result(document_changed=False)

    async def select_option(
        self,
        ref: ElementRef,
        *,
        value: str | None = None,
        label: str | None = None,
    ) -> ActResult:
        self._require_connected()
        self._record("select_option", ref=ref, value=value, label=label)
        return self._act_result(document_changed=False)

    async def set_checked(self, ref: ElementRef, checked: bool) -> ActResult:
        self._require_connected()
        self._record("set_checked", ref=ref, checked=checked)
        return self._act_result(document_changed=False)

    async def scroll(
        self,
        *,
        direction: str,
        amount: float,
        ref: ElementRef | None = None,
    ) -> ActResult:
        self._require_connected()
        self._record("scroll", direction=direction, amount=amount, ref=ref)
        return self._act_result(document_changed=False)

    async def upload_files(self, ref: ElementRef, paths: Sequence[str]) -> ActResult:
        self._require_connected()
        self._record("upload_files", ref=ref, paths=list(paths))
        return self._act_result(document_changed=False)

    async def drag(
        self,
        source: ElementRef,
        target: ElementRef,
        *,
        steps: int = 10,
        delay_ms: int = 0,
    ) -> ActResult:
        self._require_connected()
        self._record("drag", source=source, target=target, steps=steps, delay_ms=delay_ms)
        return self._act_result(document_changed=False)

    async def switch_tab(self, tab: TabRef) -> ActResult:
        self._require_connected()
        self._record("switch_tab", tab=tab)
        return self._act_result(document_changed=False)

    async def close_tab(self, tab: TabRef) -> ActResult:
        self._require_connected()
        self._record("close_tab", tab=tab)
        return self._act_result(document_changed=False)

    async def evaluate(
        self,
        source: str,
        *,
        args: Any = None,
        await_promise: bool = True,
        return_by_value: bool = True,
    ) -> Any:
        _ = (await_promise, return_by_value)
        self._require_connected()
        self._record("evaluate", source=source, args=args)
        if self._evaluate_handler is not None:
            return await self._evaluate_handler(source, args)
        # Default: satisfy batch_executor locate / selector-state / page-meta polls.
        if isinstance(args, dict):
            if "data-openjiuwen-batch-tmp" in source:
                selector = str(
                    args.get("selector")
                    or (f"[data-testid='{args['testid']}']" if args.get("testid") else "")
                    or "#fake-located"
                )
                return {
                    "ok": True,
                    "match_count": 1,
                    "visible": True,
                    "enabled": True,
                    "selector": selector,
                    "tag": "button",
                    "text": "ok",
                    "value": "ok",
                }
            if "selector" in args or "attribute" in args or "max_chars" in args:
                return {
                    "ok": True,
                    "match_count": 1,
                    "visible": True,
                    "enabled": True,
                    "text": str(args.get("text") or "ok"),
                    "value": str(args.get("value") or "ok"),
                    "attr": "ok",
                }
            if "sx" in args and "tx" in args:
                return {"source": "#__openjiuwen_drag_source__", "target": "#__openjiuwen_drag_target__"}
        if args is not None and isinstance(args, str):
            # wait_for_text / has-text probes
            return True
        if "location.href" in source or "document.title" in source:
            obs = self._observations[min(self._obs_index, len(self._observations) - 1)]
            return {"url": obs.url, "title": obs.title}
        if "innerText" in source and "body" in source:
            return "fake page text"
        return True

    async def wait_load_state(self, state: str = "load", *, timeout_ms: int) -> ActResult:
        self._require_connected()
        self._record("wait_load_state", state=state, timeout_ms=timeout_ms)
        return self._act_result(document_changed=False)


def assert_is_browser_driver(driver: object) -> None:
    """Runtime-checkable protocol assertion used by protocol tests."""
    assert isinstance(driver, BrowserDriver)


__all__ = ["ActCall", "FakeDriver", "assert_is_browser_driver", "_default_observation"]
