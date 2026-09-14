# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Python batch step loop over BrowserDriver verbs.

Replaces the Playwright ``page.locator`` JS compiled by
``controllers.action._build_batch_interact_script``. Locator resolution,
actionability polling, and wait_for_* loops live here; the driver never
sees PageState ``target_id`` / ``generation_id``.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Mapping

from openjiuwen.harness.tools.browser_move.backends.contract.base import (
    BrowserDriver,
    ElementRef,
    SelectorRef,
    TextRef,
)

_CONDITION_OPS = frozenset(
    {
        "wait_for_selector",
        "wait_for_text",
        "wait_for_load_state",
        "wait_for_url",
        "wait_for_first_card_title",
        "wait_for_sort_state",
        "wait_for_result_count",
        "wait_for_dom_text_change",
        "wait_for_stable",
        "wait_for_tab",
    }
)
_PRIMARY_TARGET_OPS = frozenset(
    {
        "click",
        "fill",
        "type",
        "autocomplete",
        "select_option",
        "set_checked",
        "extract_text",
        "extract_value",
    }
)
_OPTION_TARGET_OPS = frozenset({"autocomplete", "select_visible_text"})

_LOCATE_PRIMARY_JS = """(params) => {
  const exact = !!params.exact;
  const matchesText = (actual, expected) => {
    const left = String(actual || '').replace(/\\s+/g, ' ').trim();
    const right = String(expected || '').replace(/\\s+/g, ' ').trim();
    if (!right) return false;
    return exact ? left === right : left.includes(right);
  };
  const isVisible = (el) => {
    if (!el || !el.isConnected) return false;
    const style = getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden') return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const isEnabled = (el) => !el.disabled && el.getAttribute('aria-disabled') !== 'true';
  const cssPath = (el) => {
    if (el.id) return '#' + CSS.escape(el.id);
    const attr = 'data-openjiuwen-batch-tmp';
    const token = 'b' + Math.random().toString(36).slice(2, 10);
    el.setAttribute(attr, token);
    return '[' + attr + '="' + token + '"]';
  };
  let nodes = [];
  if (params.selector) {
    try { nodes = Array.from(document.querySelectorAll(String(params.selector))); }
    catch (error) { return { ok: false, error: String(error), match_count: 0 }; }
  } else if (params.testid) {
    nodes = Array.from(document.querySelectorAll('[data-testid="' + String(params.testid).replace(/"/g, '\\\\"') + '"]'));
  } else if (params.placeholder) {
    nodes = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]'))
      .filter((el) => matchesText(el.getAttribute('placeholder') || '', params.placeholder));
  } else if (params.label) {
    const labels = Array.from(document.querySelectorAll('label'));
    for (const label of labels) {
      if (!matchesText(label.innerText || label.textContent || '', params.label)) continue;
      const forId = label.getAttribute('for');
      if (forId) {
        const bound = document.getElementById(forId);
        if (bound) nodes.push(bound);
      } else {
        const nested = label.querySelector('input, textarea, select, button, [role]');
        if (nested) nodes.push(nested);
      }
    }
  } else if (params.role) {
    const role = String(params.role);
    const candidates = Array.from(document.querySelectorAll('[role], a, button, input, select, textarea, summary, option'));
    for (const el of candidates) {
      const elRole = (el.getAttribute('role') || el.tagName.toLowerCase());
      const normalized = elRole === 'a' ? 'link'
        : elRole === 'button' || el.tagName === 'BUTTON' ? 'button'
        : elRole === 'input' && el.type === 'checkbox' ? 'checkbox'
        : elRole === 'input' && el.type === 'radio' ? 'radio'
        : elRole === 'select' ? 'combobox'
        : elRole === 'textarea' || (elRole === 'input' && (el.type === 'text' || el.type === 'search' || !el.type)) ? 'textbox'
        : elRole === 'option' ? 'option'
        : elRole;
      if (normalized !== role && elRole !== role) continue;
      if (params.name !== undefined && params.name !== null && String(params.name).length > 0) {
        const accessible = el.getAttribute('aria-label')
          || el.getAttribute('title')
          || el.innerText
          || el.textContent
          || el.value
          || '';
        if (!matchesText(accessible, params.name)) continue;
      }
      nodes.push(el);
    }
  } else if (params.text) {
    const walker = document.createTreeWalker(document.body || document.documentElement, NodeFilter.SHOW_ELEMENT);
    while (walker.nextNode()) {
      const el = walker.currentNode;
      const text = el.innerText || el.textContent || '';
      if (matchesText(text, params.text) && isVisible(el)) nodes.push(el);
    }
  } else {
    return { ok: false, error: 'step needs selector, role, label, placeholder, testid, or text', match_count: 0 };
  }
  const visibleNodes = nodes.filter(isVisible);
  const matchCount = visibleNodes.length || nodes.length;
  if (matchCount !== 1) {
    return { ok: false, error: 'target must match exactly one element; matched ' + matchCount, match_count: matchCount };
  }
  const target = visibleNodes[0] || nodes[0];
  const visible = isVisible(target);
  const enabled = isEnabled(target);
  return {
    ok: true,
    match_count: 1,
    visible,
    enabled,
    selector: cssPath(target),
    tag: String(target.tagName || '').toLowerCase(),
    text: String(target.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 300),
    value: target.value !== undefined ? String(target.value) : null,
  };
}"""

_QUERY_SELECTOR_STATE_JS = """(params) => {
  const selector = String(params.selector || '');
  let nodes = [];
  try { nodes = Array.from(document.querySelectorAll(selector)); }
  catch (error) { return { ok: false, error: String(error), match_count: 0 }; }
  const el = nodes[0];
  if (!el) return { ok: true, match_count: 0, visible: false, enabled: false, text: '', value: '', attr: '' };
  const style = getComputedStyle(el);
  const rect = el.getBoundingClientRect();
  const visible = Boolean(el.isConnected && rect.width > 0 && rect.height > 0
    && style.display !== 'none' && style.visibility !== 'hidden');
  const enabled = !el.disabled && el.getAttribute('aria-disabled') !== 'true';
  const attribute = String(params.attribute || '');
  const attr = attribute === 'text'
    ? String(el.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 300)
    : (attribute ? String(el.getAttribute(attribute) || '') : '');
  return {
    ok: true,
    match_count: nodes.length,
    visible,
    enabled,
    text: String(el.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, Number(params.max_chars || 1200)),
    value: el.value !== undefined ? String(el.value) : '',
    attr,
  };
}"""

_BODY_TEXT_JS = """() => {
  const text = (document.body && document.body.innerText) ? document.body.innerText : '';
  return text.replace(/\\s+/g, ' ').trim().slice(0, 1200);
}"""

_PAGE_URL_TITLE_JS = """() => ({ url: String(location.href || ''), title: String(document.title || '') })"""

_HAS_TEXT_JS = """(text) => {
  const body = (document.body && document.body.innerText) ? document.body.innerText : '';
  return body.includes(String(text || ''));
}"""


def _compact_text(value: Any, max_len: int = 1200) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:max_len]


def _int_or(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _step_timeout(step: Mapping[str, Any], *, default: int, condition: bool) -> int:
    fallback = default
    return max(250, _int_or(step.get("timeout_ms"), fallback))


def _has_primary_locator(step: Mapping[str, Any]) -> bool:
    return any(
        step.get(key) not in (None, "")
        for key in ("selector", "role", "label", "placeholder", "text", "testid")
    )


def _collect_extracted(items: list[dict[str, Any]]) -> dict[str, Any]:
    extracted: dict[str, Any] = {}
    for item in items:
        field = item.get("field")
        if not field:
            continue
        if item.get("text") is not None:
            extracted[str(field)] = item.get("text")
        elif item.get("value") is not None:
            extracted[str(field)] = item.get("value")
    return extracted


def _ref_from_locator_params(params: Mapping[str, Any], resolved: Mapping[str, Any]) -> ElementRef:
    selector = str(resolved.get("selector") or params.get("selector") or "").strip()
    if selector:
        return SelectorRef(css=selector)
    if params.get("text") not in (None, ""):
        return TextRef(text=str(params.get("text")), role=str(params["role"]) if params.get("role") else None)
    if params.get("role") not in (None, "") and params.get("name") not in (None, ""):
        return TextRef(text=str(params.get("name")), role=str(params.get("role")))
    raise ValueError("unable to build ElementRef from locator")


def _primary_locator_params(step: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {"exact": bool(step.get("exact"))}
    for key in ("selector", "role", "name", "label", "placeholder", "testid", "text"):
        if step.get(key) not in (None, ""):
            params[key] = step.get(key)
    return params


def _option_locator_params(step: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {"exact": bool(step.get("exact"))}
    if step.get("option_selector") or step.get("choose_selector"):
        params["selector"] = step.get("option_selector") or step.get("choose_selector")
    elif step.get("option_role") or step.get("choose_role"):
        params["role"] = step.get("option_role") or step.get("choose_role")
        name = step.get("option_name")
        if name in (None, ""):
            name = step.get("choose_name")
        if name in (None, ""):
            name = step.get("choose_text")
        if name in (None, ""):
            name = step.get("option_text")
        if name not in (None, ""):
            params["name"] = name
    else:
        text = step.get("choose_text")
        if text in (None, ""):
            text = step.get("option_text")
        if text in (None, ""):
            text = step.get("text_to_choose")
        if text in (None, ""):
            text = step.get("value")
        if text not in (None, ""):
            params["text"] = text
    return params


async def _locate(
    driver: BrowserDriver,
    params: Mapping[str, Any],
    *,
    require_enabled: bool,
    timeout_ms: int,
) -> dict[str, Any]:
    deadline = time.perf_counter() + (timeout_ms / 1000.0)
    last_error = "target not found"
    while time.perf_counter() < deadline:
        result = await driver.evaluate(_LOCATE_PRIMARY_JS, args=dict(params))
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                result = {"ok": False, "error": result}
        if isinstance(result, Mapping) and result.get("ok") and result.get("match_count") == 1:
            if not result.get("visible"):
                last_error = "target is not visible"
            elif require_enabled and not result.get("enabled"):
                last_error = "target is not enabled"
            else:
                return dict(result)
            last_error = str(result.get("error") or last_error)
        elif isinstance(result, Mapping):
            last_error = str(result.get("error") or last_error)
        await asyncio.sleep(0.05)
    raise ValueError(last_error)


async def _poll_until(
    predicate,
    *,
    timeout_ms: int,
    poll_interval_ms: Any = 100,
    stable_ms: int = 0,
) -> dict[str, Any]:
    started = time.perf_counter()
    interval = max(0.05, min(1.0, _int_or(poll_interval_ms, 100) / 1000.0))
    stable_since = 0.0
    last_value: str | None = None
    while (time.perf_counter() - started) * 1000 <= timeout_ms:
        observation = await predicate()
        if observation and observation.get("ok"):
            if stable_ms <= 0:
                return observation
            serialized = json.dumps(observation.get("value"), sort_keys=True, default=str)
            if serialized == last_value:
                if not stable_since:
                    stable_since = time.perf_counter()
                if (time.perf_counter() - stable_since) * 1000 >= stable_ms:
                    return observation
            else:
                last_value = serialized
                stable_since = time.perf_counter()
        else:
            stable_since = 0.0
            last_value = None
        await asyncio.sleep(interval)
    raise ValueError(f"condition timed out after {timeout_ms}ms")


async def _click_with_retry(
    driver: BrowserDriver,
    params: Mapping[str, Any],
    *,
    timeout_ms: int,
    initial: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    last_error: Exception | None = None
    for attempt in range(2):
        remaining = max(250, int(timeout_ms - (time.perf_counter() - started) * 1000))
        try:
            checked = (
                dict(initial)
                if attempt == 0 and initial is not None
                else await _locate(driver, params, require_enabled=True, timeout_ms=remaining)
            )
            ref = _ref_from_locator_params(params, checked)
            act = await driver.click(ref)
            if not act.ok:
                raise ValueError(act.detail or "click failed")
            return checked
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            transient = any(
                token in message
                for token in ("detached", "not attached", "intercept", "not stable", "outside of the viewport")
            )
            if not transient or attempt > 0 or (time.perf_counter() - started) * 1000 >= timeout_ms:
                raise
            await asyncio.sleep(min(0.1, max(0.0, timeout_ms / 1000.0 - (time.perf_counter() - started))))
    raise last_error or ValueError("click failed")


async def _page_meta(driver: BrowserDriver) -> tuple[str, str]:
    meta = await driver.evaluate(_PAGE_URL_TITLE_JS)
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    if isinstance(meta, Mapping):
        return str(meta.get("url") or ""), str(meta.get("title") or "")
    return "", ""


async def execute_batch(
    driver: BrowserDriver,
    *,
    steps: list[Mapping[str, Any]],
    timeout_ms: int = 2500,
    condition_timeout_ms: int = 10000,
    wait_after_each_ms: int = 0,
    continue_on_error: bool = False,
    generation_id: str = "g0",
) -> dict[str, Any]:
    """Execute validated batch steps against a BrowserDriver.

    Returns the same compact result shape historically produced by the
    Playwright page-closure script (ok/status/steps/extracted/conditions/url/title).
    """
    started_at = time.perf_counter()
    default_timeout = max(250, int(timeout_ms or 2500))
    default_condition_timeout = max(default_timeout, int(condition_timeout_ms or 10000))
    default_after = max(0, int(wait_after_each_ms or 0))
    results: list[dict[str, Any]] = []
    document_changed = False
    preflight_targets: dict[str, dict[str, Any]] = {}
    preflight_started = time.perf_counter()
    preflight_target_count = 0
    initial_tabs = await driver.list_tabs()
    initial_tab_count = max(1, len(initial_tabs))

    def preflight_key(index: int, kind: str) -> str:
        return f"{index}:{kind}"

    async def cache_preflight(index: int, kind: str, params: Mapping[str, Any], timeout: int, require_enabled: bool):
        nonlocal preflight_target_count
        preflight_target_count += 1
        checked = await _locate(driver, params, require_enabled=require_enabled, timeout_ms=timeout)
        preflight_targets[preflight_key(index, kind)] = checked
        return checked

    for index, step in enumerate(steps):
        op = str(step.get("op") or "").strip().lower()
        timeout = _step_timeout(step, default=default_timeout, condition=False)
        try:
            if op in _PRIMARY_TARGET_OPS or (op == "press" and _has_primary_locator(step)):
                require_enabled = op not in {"extract_text", "extract_value"}
                await cache_preflight(index, "primary", _primary_locator_params(step), timeout, require_enabled)
            if op == "select_visible_text":
                await cache_preflight(index, "option", _option_locator_params(step), timeout, True)
            elif op == "autocomplete" and step.get("resolved_option_target_id"):
                await cache_preflight(index, "option", _option_locator_params(step), timeout, True)
        except Exception as exc:
            message = str(exc)
            preflight_elapsed_ms = int((time.perf_counter() - preflight_started) * 1000)
            url, title = await _page_meta(driver)
            return {
                "ok": False,
                "status": "failed",
                "error": f"batch_preflight_failed: steps[{index}] {message}",
                "steps": [
                    {
                        "index": index,
                        "op": op,
                        "ok": False,
                        "phase": "preflight",
                        "error": message,
                        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                        "generation_id": generation_id,
                    }
                ],
                "extracted": {},
                "conditions": [],
                "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                "internal_steps_elapsed_ms": 0,
                "preflight_elapsed_ms": preflight_elapsed_ms,
                "preflight_target_count": preflight_target_count,
                "url": url,
                "title": title,
                "document_changed": document_changed,
            }

    preflight_elapsed_ms = int((time.perf_counter() - preflight_started) * 1000)

    async def get_checked(index: int, kind: str, params: Mapping[str, Any], timeout: int, require_enabled: bool):
        cached = preflight_targets.get(preflight_key(index, kind))
        if cached is not None:
            return cached
        return await _locate(driver, params, require_enabled=require_enabled, timeout_ms=timeout)

    for index, step in enumerate(steps):
        op = str(step.get("op") or "").strip().lower()
        fallback = default_condition_timeout if op in _CONDITION_OPS else default_timeout
        timeout = _step_timeout(step, default=fallback, condition=op in _CONDITION_OPS)
        step_started = time.perf_counter()
        item: dict[str, Any] = {
            "index": index,
            "op": op,
            "ok": False,
            "elapsed_ms": 0,
            "generation_id": generation_id,
        }
        try:
            if not op:
                raise ValueError("missing op")
            if op == "click":
                if step.get("_navigate_url"):
                    nav = await driver.navigate(str(step.get("_navigate_url")))
                    document_changed = document_changed or bool(nav.changed_document)
                else:
                    params = _primary_locator_params(step)
                    prechecked = await get_checked(index, "primary", params, timeout, True)
                    target = await _click_with_retry(driver, params, timeout_ms=timeout, initial=prechecked)
                    item["target_validation"] = {
                        "match_count": target.get("match_count", 1),
                        "visible": target.get("visible", True),
                        "enabled": target.get("enabled", True),
                        "generation_id": generation_id,
                    }
            elif op in {"fill", "type"}:
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, True)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                ref = _ref_from_locator_params(params, checked)
                value = str(step.get("value") if step.get("value") is not None else step.get("text_value") or "")
                clear = True
                if op == "type" or step.get("mode") == "type":
                    act = await driver.type_text(ref, value, clear=True, press_enter=False)
                else:
                    act = await driver.type_text(ref, value, clear=clear, press_enter=False)
                if not act.ok:
                    raise ValueError(act.detail or f"{op} failed")
                document_changed = document_changed or bool(act.document_changed)
            elif op == "autocomplete":
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, True)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                ref = _ref_from_locator_params(params, checked)
                value = str(
                    step.get("value")
                    if step.get("value") is not None
                    else step.get("query")
                    if step.get("query") is not None
                    else step.get("text_value") or ""
                )
                act = await driver.type_text(ref, value, clear=True, press_enter=False)
                if not act.ok:
                    raise ValueError(act.detail or "autocomplete type failed")
                document_changed = document_changed or bool(act.document_changed)
                if step.get("wait_after_type_ms") is not None:
                    await asyncio.sleep(max(0, _int_or(step.get("wait_after_type_ms"), 0)) / 1000.0)
                option_params = _option_locator_params(step)
                checked_option = await get_checked(index, "option", option_params, timeout, True)
                item["option_validation"] = {
                    "match_count": checked_option.get("match_count", 1),
                    "visible": checked_option.get("visible", True),
                    "enabled": checked_option.get("enabled", True),
                    "generation_id": generation_id,
                }
                await _click_with_retry(driver, option_params, timeout_ms=timeout, initial=checked_option)
            elif op == "select_visible_text":
                option_params = _option_locator_params(step)
                checked_option = await get_checked(index, "option", option_params, timeout, True)
                item["target_validation"] = {
                    "match_count": checked_option.get("match_count", 1),
                    "visible": checked_option.get("visible", True),
                    "enabled": checked_option.get("enabled", True),
                    "generation_id": generation_id,
                }
                await _click_with_retry(driver, option_params, timeout_ms=timeout, initial=checked_option)
            elif op == "press":
                key = str(step.get("key") or "Enter")
                if _has_primary_locator(step):
                    params = _primary_locator_params(step)
                    checked = await get_checked(index, "primary", params, timeout, True)
                    item["target_validation"] = {
                        "match_count": checked.get("match_count", 1),
                        "visible": checked.get("visible", True),
                        "enabled": checked.get("enabled", True),
                        "generation_id": generation_id,
                    }
                    # Focus then press: click first, then press_key.
                    await driver.click(_ref_from_locator_params(params, checked))
                act = await driver.press_key(key)
                if not act.ok:
                    raise ValueError(act.detail or "press failed")
                document_changed = document_changed or bool(act.document_changed)
            elif op == "select_option":
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, True)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                ref = _ref_from_locator_params(params, checked)
                label = None
                value = None
                if step.get("values") is not None:
                    values = step.get("values")
                    first = values[0] if isinstance(values, list) and values else values
                    label = str(first)
                else:
                    if step.get("value") is not None or step.get("option_value") is not None:
                        value = str(step.get("option_value") if step.get("option_value") is not None else step.get("value"))
                    if any(
                        step.get(key) is not None
                        for key in ("label_value", "option_label", "option_text", "choose_text")
                    ):
                        label = str(
                            step.get("label_value")
                            if step.get("label_value") is not None
                            else step.get("option_label")
                            if step.get("option_label") is not None
                            else step.get("option_text")
                            if step.get("option_text") is not None
                            else step.get("choose_text")
                        )
                if value is None and label is None:
                    raise ValueError(
                        "select_option requires value, values, option_value, option_text, "
                        "option_label, label_value, choose_text, or index"
                    )
                act = await driver.select_option(ref, value=value, label=label)
                if not act.ok:
                    raise ValueError(act.detail or "select_option failed")
                document_changed = document_changed or bool(act.document_changed)
            elif op == "set_checked":
                checked_flag = True if step.get("checked") is None else bool(step.get("checked"))
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, True)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                act = await driver.set_checked(_ref_from_locator_params(params, checked), checked_flag)
                if not act.ok:
                    raise ValueError(act.detail or "set_checked failed")
                document_changed = document_changed or bool(act.document_changed)
            elif op == "wait_for_selector":
                selector = str(step.get("selector") or "").strip()
                if not selector:
                    raise ValueError("wait_for_selector requires selector")
                state = str(step.get("state") or "visible")

                async def _selector_ready():
                    state_info = await driver.evaluate(
                        _QUERY_SELECTOR_STATE_JS,
                        args={"selector": selector},
                    )
                    if isinstance(state_info, str):
                        try:
                            state_info = json.loads(state_info)
                        except json.JSONDecodeError:
                            state_info = {}
                    if not isinstance(state_info, Mapping):
                        return {"ok": False, "value": None}
                    count = int(state_info.get("match_count") or 0)
                    visible = bool(state_info.get("visible"))
                    if state == "attached":
                        return {"ok": count >= 1, "value": count}
                    if state == "detached":
                        return {"ok": count == 0, "value": count}
                    if state == "hidden":
                        return {"ok": count >= 1 and not visible, "value": count}
                    return {"ok": count >= 1 and visible, "value": count}

                await _poll_until(_selector_ready, timeout_ms=timeout, poll_interval_ms=step.get("poll_interval_ms"))
            elif op == "wait_for_text":
                needle = str(step.get("text") or "")
                if not needle:
                    raise ValueError("wait_for_text requires text")

                async def _text_ready():
                    present = await driver.evaluate(_HAS_TEXT_JS, args=needle)
                    return {"ok": bool(present), "value": needle}

                await _poll_until(_text_ready, timeout_ms=timeout, poll_interval_ms=step.get("poll_interval_ms"))
            elif op == "wait_for_load_state":
                act = await driver.wait_load_state(str(step.get("state") or "domcontentloaded"), timeout_ms=timeout)
                if not act.ok:
                    raise ValueError(act.detail or "wait_for_load_state failed")
                document_changed = document_changed or bool(act.document_changed)
            elif op == "wait_for_url":
                exact_url = str(step.get("url") if step.get("url") is not None else step.get("expected_url") or "")
                contains_url = str(step.get("url_contains") or "")
                pattern_text = str(step.get("url_pattern") or "")

                async def _url_ready():
                    url, _title = await _page_meta(driver)
                    matched = True
                    if exact_url:
                        matched = url == exact_url
                    if contains_url:
                        matched = matched and contains_url in url
                    if pattern_text:
                        try:
                            matched = matched and re.search(pattern_text, url) is not None
                        except re.error:
                            matched = False
                    return {"ok": matched, "value": url}

                observation = await _poll_until(
                    _url_ready,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                )
                item["url"] = observation.get("value")
            elif op == "wait_for_first_card_title":
                selector = str(step.get("selector") or "")
                expected = str(step.get("expected_text") if step.get("expected_text") is not None else step.get("text") or "")

                async def _card_ready():
                    state_info = await driver.evaluate(
                        _QUERY_SELECTOR_STATE_JS,
                        args={"selector": selector, "max_chars": 300},
                    )
                    if isinstance(state_info, str):
                        try:
                            state_info = json.loads(state_info)
                        except json.JSONDecodeError:
                            state_info = {}
                    if not isinstance(state_info, Mapping) or int(state_info.get("match_count") or 0) < 1:
                        return {"ok": False, "value": ""}
                    title = _compact_text(state_info.get("text"), 300)
                    return {"ok": bool(title) and (not expected or expected in title), "value": title}

                observation = await _poll_until(
                    _card_ready,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                    stable_ms=_int_or(step.get("stable_ms"), 0),
                )
                item["text"] = observation.get("value")
            elif op == "wait_for_sort_state":
                selector = str(step.get("selector") or "")
                attribute = str(step.get("attribute") or "aria-sort")
                expected = str(
                    step.get("expected_value")
                    if step.get("expected_value") is not None
                    else step.get("value")
                    if step.get("value") is not None
                    else step.get("text") or ""
                )

                async def _sort_ready():
                    state_info = await driver.evaluate(
                        _QUERY_SELECTOR_STATE_JS,
                        args={"selector": selector, "attribute": attribute, "max_chars": 300},
                    )
                    if isinstance(state_info, str):
                        try:
                            state_info = json.loads(state_info)
                        except json.JSONDecodeError:
                            state_info = {}
                    if not isinstance(state_info, Mapping) or int(state_info.get("match_count") or 0) != 1:
                        return {"ok": False, "value": ""}
                    value = str(state_info.get("attr") or "")
                    return {"ok": value == expected, "value": value}

                observation = await _poll_until(
                    _sort_ready,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                )
                item["value"] = observation.get("value")
            elif op == "wait_for_result_count":
                selector = str(step.get("selector") or "")
                exact_count = None if step.get("count") is None else _int_or(step.get("count"), -1)
                min_count = None if step.get("min_count") is None else _int_or(step.get("min_count"), 0)
                max_count = None if step.get("max_count") is None else _int_or(step.get("max_count"), 0)

                async def _count_ready():
                    state_info = await driver.evaluate(_QUERY_SELECTOR_STATE_JS, args={"selector": selector})
                    if isinstance(state_info, str):
                        try:
                            state_info = json.loads(state_info)
                        except json.JSONDecodeError:
                            state_info = {}
                    count = int(state_info.get("match_count") or 0) if isinstance(state_info, Mapping) else 0
                    if exact_count is not None:
                        matched = count == exact_count
                    else:
                        matched = (min_count is None or count >= min_count) and (max_count is None or count <= max_count)
                    return {"ok": matched, "value": count}

                observation = await _poll_until(
                    _count_ready,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                    stable_ms=_int_or(step.get("stable_ms"), 0),
                )
                item["count"] = observation.get("value")
            elif op == "wait_for_dom_text_change":
                selector = str(step.get("selector") or "")
                previous = str(
                    step.get("previous_text")
                    if step.get("previous_text") is not None
                    else step.get("initial_text") or ""
                )
                max_chars = _int_or(step.get("max_chars"), 1000)

                async def _text_changed():
                    state_info = await driver.evaluate(
                        _QUERY_SELECTOR_STATE_JS,
                        args={"selector": selector, "max_chars": max_chars},
                    )
                    if isinstance(state_info, str):
                        try:
                            state_info = json.loads(state_info)
                        except json.JSONDecodeError:
                            state_info = {}
                    if not isinstance(state_info, Mapping) or int(state_info.get("match_count") or 0) != 1:
                        return {"ok": False, "value": ""}
                    value = _compact_text(state_info.get("text"), max_chars)
                    return {"ok": value != previous, "value": value}

                observation = await _poll_until(
                    _text_changed,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                    stable_ms=_int_or(step.get("stable_ms"), 0),
                )
                item["text"] = observation.get("value")
            elif op == "wait_for_stable":
                selector = str(step.get("selector") or "").strip()
                max_chars = _int_or(step.get("max_chars"), 1200)

                async def _stable_value():
                    if selector:
                        state_info = await driver.evaluate(
                            _QUERY_SELECTOR_STATE_JS,
                            args={"selector": selector, "max_chars": max_chars},
                        )
                        if isinstance(state_info, str):
                            try:
                                state_info = json.loads(state_info)
                            except json.JSONDecodeError:
                                state_info = {}
                        value = _compact_text((state_info or {}).get("text"), max_chars)
                    else:
                        value = await driver.evaluate(_BODY_TEXT_JS)
                        value = _compact_text(value, max_chars)
                    return {"ok": True, "value": value}

                await _poll_until(
                    _stable_value,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                    stable_ms=_int_or(step.get("stable_ms"), 500),
                )
                item["stable"] = True
            elif op == "wait_for_tab":
                min_tabs = max(1, _int_or(step.get("min_tabs"), initial_tab_count + 1))
                expected_url = str(step.get("url") or step.get("expected_url") or "")
                url_contains = str(step.get("url_contains") or "")
                url_pattern = str(step.get("url_pattern") or "")
                title_contains = str(step.get("title_contains") or "")

                async def _tab_ready():
                    tabs = await driver.list_tabs()
                    serialized = [
                        {"index": i, "url": tab.url, "title": tab.title, "target_id": tab.target_id, "active": tab.active}
                        for i, tab in enumerate(tabs)
                    ]
                    matches = []
                    for tab in serialized:
                        if expected_url and tab["url"] != expected_url:
                            continue
                        if url_contains and url_contains not in tab["url"]:
                            continue
                        if url_pattern:
                            try:
                                if re.search(url_pattern, tab["url"]) is None:
                                    continue
                            except re.error:
                                continue
                        if title_contains and title_contains not in tab["title"]:
                            continue
                        matches.append(tab)
                    matched = len(tabs) >= min_tabs and bool(matches)
                    matched_index = matches[-1]["index"] if matched else -1
                    return {"ok": matched, "value": {"tabs": serialized, "matched_index": matched_index}}

                observation = await _poll_until(
                    _tab_ready,
                    timeout_ms=timeout,
                    poll_interval_ms=step.get("poll_interval_ms"),
                    stable_ms=_int_or(step.get("stable_ms"), 0),
                )
                value = observation.get("value") or {}
                item["tabs"] = value.get("tabs") or []
                item["count"] = len(item["tabs"])
                matched_index = int(value.get("matched_index") if value.get("matched_index") is not None else -1)
                if step.get("activate") is not False and matched_index >= 0:
                    tabs = await driver.list_tabs()
                    if 0 <= matched_index < len(tabs):
                        act = await driver.switch_tab(tabs[matched_index])
                        document_changed = document_changed or bool(act.document_changed)
                        item["url"] = tabs[matched_index].url
            elif op == "sleep":
                await asyncio.sleep(max(0, _int_or(step.get("ms", step.get("time_ms")), 0)) / 1000.0)
            elif op == "extract_text":
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, False)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                selector = str(checked.get("selector") or step.get("selector") or "")
                state_info = await driver.evaluate(
                    _QUERY_SELECTOR_STATE_JS,
                    args={"selector": selector, "max_chars": _int_or(step.get("max_chars"), 500)},
                )
                if isinstance(state_info, str):
                    try:
                        state_info = json.loads(state_info)
                    except json.JSONDecodeError:
                        state_info = {}
                raw_text = str((state_info or {}).get("text") or checked.get("text") or "")
                item["text"] = _compact_text(raw_text, _int_or(step.get("max_chars"), 500))
                item["raw_text"] = raw_text[:1000]
                item["selector"] = selector
                item["field"] = str(step.get("field") or step.get("name") or step.get("description") or f"field_{index + 1}")
            elif op == "extract_value":
                params = _primary_locator_params(step)
                checked = await get_checked(index, "primary", params, timeout, False)
                item["target_validation"] = {
                    "match_count": checked.get("match_count", 1),
                    "visible": checked.get("visible", True),
                    "enabled": checked.get("enabled", True),
                    "generation_id": generation_id,
                }
                selector = str(checked.get("selector") or step.get("selector") or "")
                state_info = await driver.evaluate(_QUERY_SELECTOR_STATE_JS, args={"selector": selector})
                if isinstance(state_info, str):
                    try:
                        state_info = json.loads(state_info)
                    except json.JSONDecodeError:
                        state_info = {}
                item["value"] = str((state_info or {}).get("value") if (state_info or {}).get("value") is not None else checked.get("value") or "")
                item["raw_text"] = str(item["value"])[:1000]
                item["selector"] = selector
                item["field"] = str(step.get("field") or step.get("name") or step.get("description") or f"field_{index + 1}")
            elif op == "screenshot":
                path = str(step.get("path") or "screenshots/batch_interact.png")
                b64 = await driver.screenshot(full_page=bool(step.get("full_page")))
                item["path"] = path
                item["screenshot_b64"] = b64
            else:
                raise ValueError(f"unsupported op: {op}")

            item["ok"] = True
            if step.get("wait_after_ms") is not None:
                await asyncio.sleep(max(0, _int_or(step.get("wait_after_ms"), 0)) / 1000.0)
            elif default_after > 0:
                await asyncio.sleep(default_after / 1000.0)
        except Exception as exc:
            item["error"] = str(exc)
            item["target"] = {
                "selector": step.get("selector"),
                "role": step.get("role"),
                "name": step.get("name"),
                "label": step.get("label"),
                "placeholder": step.get("placeholder"),
                "text": step.get("text"),
                "testid": step.get("testid"),
                "choose_text": step.get("choose_text") or step.get("option_text"),
            }
            item["elapsed_ms"] = int((time.perf_counter() - step_started) * 1000)
            results.append(item)
            if step.get("optional") or continue_on_error:
                continue
            url, title = await _page_meta(driver)
            return {
                "ok": False,
                "status": "partial" if any(result.get("ok") for result in results) else "failed",
                "error": item["error"],
                "steps": results,
                "extracted": _collect_extracted(results),
                "conditions": [result for result in results if result.get("op") in _CONDITION_OPS],
                "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
                "internal_steps_elapsed_ms": sum(int(result.get("elapsed_ms") or 0) for result in results),
                "preflight_elapsed_ms": preflight_elapsed_ms,
                "preflight_target_count": preflight_target_count,
                "url": url,
                "title": title,
                "document_changed": document_changed,
            }
        item["elapsed_ms"] = int((time.perf_counter() - step_started) * 1000)
        results.append(item)

    extracted = _collect_extracted(results)
    failed_steps = [result for result in results if not result.get("ok")]
    conditions = []
    for result in results:
        if result.get("op") not in _CONDITION_OPS:
            continue
        observed: dict[str, Any] = {}
        for key in ("url", "text", "value", "count", "stable", "tabs"):
            if key in result:
                observed[key] = result.get(key)
        conditions.append(
            {
                "index": result.get("index"),
                "op": result.get("op"),
                "ok": result.get("ok"),
                "error": result.get("error"),
                "elapsed_ms": result.get("elapsed_ms"),
                "observed": observed,
            }
        )
    url, title = await _page_meta(driver)
    return {
        "ok": not failed_steps,
        "status": "completed" if not failed_steps else "partial",
        "error": None if not failed_steps else "one or more optional batch steps failed",
        "steps": results,
        "extracted": extracted,
        "conditions": conditions,
        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
        "internal_steps_elapsed_ms": sum(int(result.get("elapsed_ms") or 0) for result in results),
        "preflight_elapsed_ms": preflight_elapsed_ms,
        "preflight_target_count": preflight_target_count,
        "url": url,
        "title": title,
        "document_changed": document_changed,
    }


__all__ = ["execute_batch"]
