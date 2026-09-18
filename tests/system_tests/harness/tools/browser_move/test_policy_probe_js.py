# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Opt-in check of ``POLICY_PROBE_JS`` against a fixture page in the attached Chrome, through BrowserUseDriver.

Gated on ``BROWSER_CDP_URL`` and a discoverable sidecar interpreter, like the driver smoke test.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest

from openjiuwen.harness.tools.browser_move.backends.browser_use.driver import BrowserUseDriver
from openjiuwen.harness.tools.browser_move.backends.browser_use.transport import discover_sidecar_python
from openjiuwen.harness.tools.browser_move.backends.contract.base import SelectorRef
from openjiuwen.harness.tools.browser_move.backends.contract.errors import DriverUnsupported
from openjiuwen.harness.tools.browser_move.policy.probe_js import POLICY_PROBE_JS, STAMP_ATTRIBUTE

FIXTURE = """<!doctype html><html><head><title>Probe fixture</title></head><body>
<h1>Trip</h1>
<label for="from">Where from?</label><input id="from" role="combobox" value="Tokyo" aria-controls="list">
<ul id="list" role="listbox" hidden></ul>
<span id="to-label">Where to?</span><input aria-labelledby="to-label" type="text">
<label>Class <select id="class"><option value="e" selected>Economy</option><option value="b">Business</option>
<option value="f" disabled>First</option></select></label>
<input type="checkbox" id="nonstop" aria-label="Nonstop only" checked>
<button type="submit">Search</button>
<button disabled>Disabled</button>
<input type="password" aria-label="Secret">
<input type="hidden" value="h">
<a href="/help">Help</a>
<div role="dialog" aria-label="Passengers"><button>Add adult</button></div>
<p>Visible paragraph text.</p>
<p hidden>Hidden text.</p>
<button style="position:absolute; left:-9999px">Offscreen</button>
</body></html>"""
WRITE_FIXTURE = "(html) => { document.open(); document.write(html); document.close(); return document.title; }"
COUNT_MATCHES = "(hints) => hints.map((hint) => document.querySelectorAll(hint).length)"


def _sidecar_available() -> bool:
    try:
        discover_sidecar_python()
        return True
    except DriverUnsupported:
        return False


pytestmark = pytest.mark.skipif(
    not (os.getenv("BROWSER_CDP_URL") or "").strip() or not _sidecar_available(),
    reason="Set BROWSER_CDP_URL and create .venvs/browser-use to run the policy probe check.",
)


def _by_label(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["label"]: item for item in snapshot["elements"]}


async def _probe(driver: BrowserUseDriver, after: dict[str, Any] | None) -> dict[str, Any]:
    params = {"stamp_attribute": STAMP_ATTRIBUTE, "after": after, "max_items": 250}
    snapshot = await driver.evaluate(POLICY_PROBE_JS, args=params)
    assert isinstance(snapshot, dict) and not snapshot.get("error"), snapshot
    return snapshot


async def _exercise(cdp_url: str) -> None:
    driver = BrowserUseDriver()
    await driver.connect(cdp_url=cdp_url, timeout_s=45.0)
    try:
        await driver.navigate("about:blank")
        assert await driver.evaluate(WRITE_FIXTURE, args=FIXTURE) == "Probe fixture"

        first = await _probe(driver, None)
        items = _by_label(first)
        assert set(items) == {"Where from?", "Where to?", "Class", "Nonstop only", "Search", "Help", "Add adult"}, items
        assert items["Where from?"]["role"] == "combobox" and items["Where from?"]["editable"] is True
        assert items["Where from?"]["value"] == "Tokyo"
        assert items["Where to?"]["role"] == "textbox" and items["Where to?"]["editable"] is True
        assert items["Class"]["editable"] is False and items["Class"]["value"] == "Economy"
        assert items["Class"]["options"] == [{"label": "Business", "value": "b"}]
        assert items["Nonstop only"]["checked"] == "true"
        assert items["Search"]["role"] == "button" and items["Help"]["role"] == "link"
        assert items["Add adult"]["region"] == "dialog:Passengers"
        assert "Visible paragraph text." in first["text"] and "Hidden text." not in first["text"]
        assert first["can_scroll_down"] is False and first["omitted"] == 0
        hints = [item["selector_hint"] for item in first["elements"]]
        assert await driver.evaluate(COUNT_MATCHES, args=hints) == [1] * len(hints), "stamped selectors must be unique"

        second = await _probe(driver, None)
        assert {k: v["node"] for k, v in _by_label(second).items()} == {k: v["node"] for k, v in items.items()}
        assert second["page_key"] == first["page_key"]

        field = items["Where from?"]
        await driver.type_text(SelectorRef(css=field["selector_hint"]), "Zurich", clear=True)
        third = await _probe(driver, {"kind": "fill", "node": field["node"]})
        assert _by_label(third)["Where from?"]["value"] == "Zurich"
        assert _by_label(third)["Where from?"]["node"] == field["node"]
        assert third["page_key"] != first["page_key"], "a changed field value must change the page key"
    finally:
        await driver.close()


def test_policy_probe_describes_fixture_controls() -> None:
    cdp_url = (os.getenv("BROWSER_CDP_URL") or "").strip()
    asyncio.run(_exercise(cdp_url))
