# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""BrowserDriver protocol and value types.

The driver is eyes and hands only: it observes the page and performs
low-level actions. Orchestration (PageState, generation accounting, probe
scoring, semantic state, batch sequencing, wait_for_* polling) stays in
``BrowserAgentRuntime``. The driver never sees a ``target_id`` or a
PageState ``generation_id``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class IndexRef:
    """Reference an element by its last-observed selector-map index.

    ``driver_generation`` pins the observation this index was minted in; a
    driver must raise ``StaleIndexError`` if the current driver generation
    has moved on and the index no longer safely identifies the same node.
    """

    index: int
    driver_generation: int


@dataclass(frozen=True)
class SelectorRef:
    """Reference an element by a CSS selector, optionally the Nth match."""

    css: str
    nth: int = 0


@dataclass(frozen=True)
class TextRef:
    """Reference an element by visible text, optionally scoped by role."""

    text: str
    role: str | None = None


@dataclass(frozen=True)
class NodeRef:
    """Reference an element by CDP backend node id."""

    backend_node_id: int
    frame_id: str | None = None


ElementRef = IndexRef | SelectorRef | TextRef | NodeRef


@dataclass(frozen=True)
class Box:
    """Element bounding box in CSS pixels, viewport-relative."""

    x: float
    y: float
    width: float
    height: float


@dataclass(frozen=True)
class TabRef:
    """One browser tab as reported by the driver.

    ``target_id`` is a CDP target id and is UNRELATED to PageState
    ``target_id``; it is named for CDP because that is what browser-use
    returns.
    """

    target_id: str
    url: str
    title: str
    active: bool


@dataclass(frozen=True)
class ObservedElement:
    """One interactive/observable DOM node from an ``observe()`` call."""

    index: int
    backend_node_id: int
    frame_id: str | None
    tag: str
    role: str | None
    name: str | None
    value: str | None
    attributes: dict[str, str] = field(default_factory=dict)
    box: Box | None = None
    visible: bool = False
    scrollable: bool = False


@dataclass(frozen=True)
class Observation:
    """A full page observation.

    ``ax_text`` fills the slot today held by Playwright MCP's
    ``browser_snapshot`` text. It is sourced from the backend's own
    LLM-oriented page representation and is not expected to be
    byte-identical to that AX snapshot format.
    """

    url: str
    title: str
    tabs: tuple[TabRef, ...]
    elements: tuple[ObservedElement, ...]
    ax_text: str | None
    screenshot_b64: str | None
    pixels_above: int
    pixels_below: int
    errors: tuple[str, ...]
    is_pdf_viewer: bool
    captured_at: float
    driver_generation: int


@dataclass(frozen=True)
class ResolvedElement:
    """The concrete DOM node an ``ElementRef`` resolved to."""

    backend_node_id: int
    frame_id: str | None
    box: Box | None
    visible: bool
    tag: str
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ActResult:
    """Result of one hands action.

    ``document_changed`` is the observed replacement for the tool-name
    sniffing the runtime used to do to detect page-identity changes.
    """

    ok: bool
    detail: str
    document_changed: bool
    driver_generation: int


@dataclass(frozen=True)
class NavResult:
    """Result of a navigation action (navigate/back/forward/reload)."""

    url: str
    title: str
    changed_document: bool
    driver_generation: int


@dataclass(frozen=True)
class DriverInfo:
    """Static facts about a connected driver, returned by ``connect()``."""

    backend: str
    browser_version: str
    protocol_version: str
    backend_version: str
    initial_url: str


@dataclass(frozen=True)
class DriverHealth:
    """Liveness snapshot returned by ``health()``. Never raises."""

    connected: bool
    url: str
    tab_count: int
    latency_ms: float
    error: str | None = None


@runtime_checkable
class BrowserDriver(Protocol):
    """Eyes-and-hands browser automation surface.

    Every method is async and maps one-to-one to a wire method name (see
    ``drivers/browser_use/sidecar/wire.py``). Implementations own no
    PageState/generation concepts; they only know ``driver_generation`` and
    DOM node identity.
    """

    async def connect(self, *, cdp_url: str, timeout_s: float = 30.0) -> DriverInfo: ...

    async def health(self) -> DriverHealth: ...

    async def close(self) -> None: ...

    async def observe(
        self,
        *,
        include_dom: bool = True,
        include_screenshot: bool = False,
        cached: bool = False,
    ) -> Observation: ...

    async def screenshot(self, *, full_page: bool = False, clip: Box | None = None) -> str: ...

    async def list_tabs(self) -> tuple[TabRef, ...]: ...

    async def resolve(self, ref: ElementRef) -> ResolvedElement: ...

    async def stamp(self, ref: ElementRef, *, attribute: str, value: str) -> str: ...

    async def navigate(
        self,
        url: str,
        *,
        wait_until: str = "load",
        timeout_ms: int | None = None,
        new_tab: bool = False,
    ) -> NavResult: ...

    async def go_back(self) -> NavResult: ...

    async def go_forward(self) -> NavResult: ...

    async def reload(self) -> NavResult: ...

    async def click(
        self,
        ref: ElementRef,
        *,
        button: str = "left",
        click_count: int = 1,
        modifiers: Sequence[str] = (),
    ) -> ActResult: ...

    async def type_text(
        self,
        ref: ElementRef,
        text: str,
        *,
        clear: bool = True,
        press_enter: bool = False,
        sensitive: bool = False,
    ) -> ActResult: ...

    async def press_key(self, keys: str) -> ActResult: ...

    async def select_option(
        self,
        ref: ElementRef,
        *,
        value: str | None = None,
        label: str | None = None,
    ) -> ActResult: ...

    async def set_checked(self, ref: ElementRef, checked: bool) -> ActResult: ...

    async def scroll(
        self,
        *,
        direction: str,
        amount: float,
        ref: ElementRef | None = None,
    ) -> ActResult: ...

    async def upload_files(self, ref: ElementRef, paths: Sequence[str]) -> ActResult: ...

    async def drag(
        self,
        source: ElementRef,
        target: ElementRef,
        *,
        steps: int = 10,
        delay_ms: int = 0,
    ) -> ActResult: ...

    async def switch_tab(self, tab: TabRef) -> ActResult: ...

    async def close_tab(self, tab: TabRef) -> ActResult: ...

    async def evaluate(
        self,
        source: str,
        *,
        args: Any = None,
        await_promise: bool = True,
        return_by_value: bool = True,
    ) -> Any: ...

    async def wait_load_state(self, state: str = "load", *, timeout_ms: int) -> ActResult: ...


__all__ = [
    "ActResult",
    "Box",
    "BrowserDriver",
    "DriverHealth",
    "DriverInfo",
    "ElementRef",
    "IndexRef",
    "NavResult",
    "NodeRef",
    "Observation",
    "ObservedElement",
    "ResolvedElement",
    "SelectorRef",
    "TabRef",
    "TextRef",
]
