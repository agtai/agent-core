# coding: utf-8
"""Opt-in local smoke for BrowserUseDriver (B3).

Gated on ``BROWSER_CDP_URL`` and a discoverable sidecar interpreter so CI's
default configuration skips automatically (browser-use is never in the main env).

Seven steps from the B3 plan:
1. connect to an existing CDP endpoint; assert health + backend_version
2. navigate example.com; assert changed_document
3. observe; assert elements + ax_text
4. click by IndexRef + type_text on a form page
5. stamp then act via returned selector
6. evaluate("(a) => a.x + 1", args={"x": 1}) == 2
7. close(); Chrome CDP still alive; sidecar process gone
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from urllib.request import urlopen

import pytest

from openjiuwen.harness.tools.browser_move.drivers.base import IndexRef, NodeRef, SelectorRef
from openjiuwen.harness.tools.browser_move.drivers.browser_use.driver import BrowserUseDriver
from openjiuwen.harness.tools.browser_move.drivers.browser_use.transport import discover_sidecar_python
from openjiuwen.harness.tools.browser_move.drivers.errors import DriverUnsupported


def _sidecar_available() -> bool:
    try:
        discover_sidecar_python()
        return True
    except DriverUnsupported:
        return False


def _cdp_ready(endpoint: str) -> bool:
    try:
        with urlopen(f"{endpoint.rstrip('/')}/json/version", timeout=2.0) as response:  # nosec B310
            payload = response.read().decode("utf-8", errors="ignore")
            return "webSocketDebuggerUrl" in payload or "Browser" in payload
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not (os.getenv("BROWSER_CDP_URL") or "").strip() or not _sidecar_available(),
    reason="Set BROWSER_CDP_URL and create .venvs/browser-use to run BrowserUseDriver smoke.",
)


async def _run_smoke(cdp_url: str) -> dict[str, Any]:
    driver = BrowserUseDriver()
    info = await driver.connect(cdp_url=cdp_url, timeout_s=45.0)
    health = await driver.health()
    assert health.connected is True
    assert info.backend_version == "0.13.10"

    nav = await driver.navigate("https://example.com")
    assert nav.changed_document is True

    observation = await driver.observe(include_dom=True, include_screenshot=False)
    assert observation.elements
    assert observation.ax_text

    # Prefer a form page for type_text; fall back to example.com click-only.
    form_nav = await driver.navigate("https://httpbin.org/forms/post")
    assert form_nav.url
    form_obs = await driver.observe(include_dom=True)
    typed = False
    clicked = False
    for element in form_obs.elements:
        tag = (element.tag or "").lower()
        if not typed and tag in {"input", "textarea"}:
            await driver.type_text(
                IndexRef(index=element.index, driver_generation=form_obs.driver_generation),
                "openjiuwen-smoke",
                clear=True,
            )
            typed = True
        if not clicked and element.visible:
            await driver.click(IndexRef(index=element.index, driver_generation=form_obs.driver_generation))
            clicked = True
        if typed and clicked:
            break
    if not clicked and form_obs.elements:
        first = form_obs.elements[0]
        await driver.click(IndexRef(index=first.index, driver_generation=form_obs.driver_generation))

    stamp_target = form_obs.elements[0] if form_obs.elements else observation.elements[0]
    stamp_gen = form_obs.driver_generation if form_obs.elements else observation.driver_generation
    selector = await driver.stamp(
        IndexRef(index=stamp_target.index, driver_generation=stamp_gen),
        attribute="data-openjiuwen-target-id",
        value="smoke_t1",
    )
    assert selector.startswith("[data-openjiuwen-target-id=")
    await driver.click(SelectorRef(css=selector))

    # Also prove NodeRef stamp path when backend_node_id is present.
    if stamp_target.backend_node_id:
        selector2 = await driver.stamp(
            NodeRef(backend_node_id=stamp_target.backend_node_id, frame_id=stamp_target.frame_id),
            attribute="data-openjiuwen-target-id",
            value="smoke_t2",
        )
        assert "smoke_t2" in selector2

    evaluated = await driver.evaluate("(a) => a.x + 1", args={"x": 1})
    assert evaluated == 2

    transport = driver._transport  # noqa: SLF001 — smoke asserts process teardown
    sidecar_proc = getattr(transport, "_process", None) if transport is not None else None
    sidecar_pid = getattr(sidecar_proc, "pid", None)

    await driver.close()

    assert _cdp_ready(cdp_url), "managed/attached Chrome must survive driver.close()"
    if sidecar_proc is not None:
        assert sidecar_proc.poll() is not None, f"sidecar pid={sidecar_pid} still running after close"

    return {
        "ok": True,
        "backend_version": info.backend_version,
        "url": observation.url,
        "element_count": len(observation.elements),
    }


def test_browser_use_driver_smoke() -> None:
    cdp_url = (os.getenv("BROWSER_CDP_URL") or "").strip()
    assert cdp_url, "BROWSER_CDP_URL required"
    assert _cdp_ready(cdp_url), f"CDP endpoint not ready: {cdp_url}"
    result = asyncio.run(_run_smoke(cdp_url))
    assert result["ok"] is True
    assert result["backend_version"] == "0.13.10"
    assert result["element_count"] > 0
