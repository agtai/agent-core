# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""In-page policy probe: settle, then one snapshot of every visible control the policy may act on.

Called through ``BrowserAgentRuntime._evaluate_page_js`` as ``(params) => Promise``. Each control is
stamped with ``data-openjiuwen-jev=<id>`` so PageState can register a validated, unique selector, and
ids are kept in a page-side WeakMap so the same node keeps its id across ticks.
"""

from __future__ import annotations

STAMP_ATTRIBUTE = "data-openjiuwen-jev"

POLICY_PROBE_JS = r"""(params) => new Promise((resolve) => {
  const ATTR = params.stamp_attribute || 'data-openjiuwen-jev';
  const after = params.after || null;
  const loadTimeout = Number(params.load_timeout_ms || 3000);
  const settleMax = Number(params.settle_ms || 500);
  const quietMs = Number(params.quiet_ms || 60);
  const maxItems = Number(params.max_items || 250);
  const textLimit = Number(params.text_chars || 6000);
  const started = performance.now();

  const cache = window.__ojwJev ||= {ids: new WeakMap(), nodes: new Map(), next: 1};
  const identity = (e) => {
    if (!cache.ids.has(e)) cache.ids.set(e, cache.next++);
    const id = cache.ids.get(e); cache.nodes.set(id, e); return id;
  };
  const safe = (e) => !['password', 'file', 'hidden'].includes(e.type);
  const visible = (e) => !e.closest('[aria-hidden="true"],[inert]') &&
    e.checkVisibility({checkOpacity: true, checkVisibilityCSS: true});
  const name = (e, seen = new Set()) => {
    if (!e || seen.has(e)) return '';
    seen.add(e);
    const referenced = (e.getAttribute('aria-labelledby') || '').split(/\s+/)
      .map((id) => name(document.getElementById(id), seen)).filter(Boolean).join(' ');
    return referenced || e.getAttribute('aria-label') ||
      [...(e.labels || [])].map((l) => name(l, seen)).filter(Boolean).join(' ') ||
      (['button', 'submit', 'reset'].includes(e.type) ? e.value : '') || e.getAttribute('alt') ||
      (e.tagName === 'INPUT' ? '' : [...e.childNodes].map((n) => n.nodeType === 3 ? n.textContent :
        n.nodeType === 1 && n.getAttribute('aria-hidden') !== 'true' ? name(n, seen) : '').join(' ').trim()) ||
      e.getAttribute('title') || e.getAttribute('placeholder') || '';
  };
  const roles = ['button', 'link', 'checkbox', 'radio', 'switch', 'tab', 'menuitem', 'menuitemradio',
    'option', 'gridcell', 'combobox', 'textbox', 'searchbox', 'spinbutton'];
  const selector = 'a[href],button,input,textarea,select,summary,[contenteditable="true"],' +
    roles.map((role) => '[role="' + role + '"]').join(',');
  const role = (e) => {
    const explicit = e.getAttribute('role');
    if (roles.includes(explicit)) return explicit;
    if (e.tagName === 'BUTTON' || e.tagName === 'SUMMARY') return 'button';
    if (e.tagName === 'A') return 'link';
    if (e.tagName === 'SELECT') return 'combobox';
    if (e.tagName === 'TEXTAREA' || e.isContentEditable) return 'textbox';
    if (e.tagName === 'INPUT') {
      if (['checkbox', 'radio'].includes(e.type)) return e.type;
      if (['button', 'submit', 'reset', 'image'].includes(e.type)) return 'button';
      if (e.type === 'search') return 'searchbox';
      if (e.type === 'number') return 'spinbutton';
      const textTypes = ['text', 'email', 'url', 'tel', 'date', 'datetime-local', 'month', 'time', 'week'];
      if (textTypes.includes(e.type)) return 'textbox';
    }
    return null;
  };
  const region = (e) => {
    const dialog = e.closest('dialog,[role="dialog"],[role="alertdialog"]');
    if (dialog) return 'dialog:' + (name(dialog) || 'unnamed').slice(0, 60);
    const menu = e.closest('[role="menu"],[role="listbox"],[role="grid"]');
    if (menu) return menu.getAttribute('role') + ':' + (name(menu) || '').slice(0, 60);
    return '';
  };
  const pageKey = () => JSON.stringify([location.href, scrollX, scrollY, innerWidth, innerHeight, document.title,
    [...document.querySelectorAll('input,textarea,select')].filter(safe)
      .map((e) => [identity(e), e.value, e.checked, e.selectedIndex, e.disabled, e.readOnly]),
    document.querySelectorAll(selector).length]);

  const snapshot = () => {
    const elements = [];
    let omitted = 0;
    for (const e of document.querySelectorAll(selector)) {
      if (!safe(e) || !visible(e) || e.matches(':disabled') || e.closest('[aria-disabled="true"]')) continue;
      const r = e.getBoundingClientRect(), x = r.x + r.width / 2, y = r.y + r.height / 2, rname = role(e);
      if (!rname || r.width <= 0 || r.height <= 0 || x < 0 || y < 0 || x >= innerWidth || y >= innerHeight) continue;
      if (rname === 'gridcell' && e.querySelector('button,[role="button"]')) continue;
      if (elements.length >= maxItems) { omitted += 1; continue; }
      const id = identity(e);
      if (e.getAttribute(ATTR) !== String(id)) e.setAttribute(ATTR, String(id));
      const editable = !e.readOnly && e.getAttribute('aria-readonly') !== 'true' &&
        (['textbox', 'searchbox', 'spinbutton'].includes(rname) ||
          (rname === 'combobox' && ['INPUT', 'TEXTAREA'].includes(e.tagName)));
      const value = e.tagName === 'SELECT'
        ? [...e.selectedOptions].map((o) => o.label).join(', ')
        : 'value' in e ? String(e.value) : e.isContentEditable || rname === 'combobox' ? e.innerText.trim() : '';
      const item = {
        node: id,
        selector_hint: '[' + ATTR + '="' + id + '"]',
        selector_hint_validated: true,
        match_count: 1,
        role: rname,
        label: (name(e) || rname).slice(0, 200),
        value: value.slice(0, 200),
        region: region(e),
        editable: editable,
        visible: true,
        enabled: true,
        actionable: true,
        clickable: true,
      };
      item.accessible_name = item.label;
      item.text = item.label;
      for (const key of ['checked', 'selected', 'expanded']) {
        const v = e.getAttribute('aria-' + key);
        if (v !== null) item[key] = v;
      }
      if (['checkbox', 'radio'].includes(e.type)) item.checked = String(e.checked);
      if (e.tagName === 'SELECT') {
        item.options = [...e.options]
          .filter((o) => !o.selected && !o.disabled && !o.closest('optgroup[disabled]'))
          .map((o) => ({label: o.label, value: o.value}));
      }
      elements.push(item);
    }
    const words = [], walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const range = document.createRange(); let node, length = 0;
    while ((node = walker.nextNode()) && length < textLimit) {
      const value = node.textContent.trim(), parent = node.parentElement;
      if (!value || !parent || parent.closest('script,style,noscript,template') || !visible(parent)) continue;
      range.selectNodeContents(node); const r = range.getBoundingClientRect();
      if (r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < innerHeight && r.right > 0 && r.left < innerWidth) {
        words.push(value); length += value.length;
      }
    }
    const height = document.documentElement.scrollHeight;
    return {
      url: location.href, title: document.title, text: words.join('\n').slice(0, textLimit),
      scroll: {y: scrollY, height: height, viewport: innerHeight},
      can_scroll_down: scrollY + innerHeight < height - 2, can_scroll_up: scrollY > 0,
      elements: elements, omitted: omitted, page_key: pageKey(),
      settle_ms: Math.round(performance.now() - started), load_ms: loadMs, ready_state: document.readyState,
      visibility: document.visibilityState, has_focus: document.hasFocus(),
    };
  };

  let finished = false;
  const finish = () => {
    if (finished) return;
    finished = true;
    clearTimeout(guard);
    try { resolve(snapshot()); } catch (err) { resolve({error: String((err && err.stack) || err), elements: []}); }
  };
  // Last-resort resolver: a hidden tab throttles timers, and an unresolved promise would hit the driver timeout.
  const guard = setTimeout(finish, loadTimeout + settleMax + 1000);
  const quiet = () => {
    // DOM-quiet window: resolve after `quietMs` without mutations, or at `settleMax` at the latest.
    let timer = null;
    const observer = new MutationObserver(() => { clearTimeout(timer); timer = setTimeout(done, quietMs); });
    const hard = setTimeout(done, settleMax);
    function done() { clearTimeout(timer); clearTimeout(hard); observer.disconnect(); finish(); }
    observer.observe(document.documentElement, {subtree: true, childList: true, attributes: true, characterData: true});
    timer = setTimeout(done, quietMs);
  };
  // Timers instead of requestAnimationFrame: rAF never fires while the tab is hidden.
  const afterInput = () => {
    const field = after && cache.nodes.get(after.node);
    const autocomplete = after && after.kind === 'fill' && field && field.getAttribute('role') === 'combobox';
    if (!autocomplete) { setTimeout(quiet, 32); return; }
    let polls = 0, stopped = false;
    const stop = () => { if (!stopped) { stopped = true; quiet(); } };
    setTimeout(stop, Number(params.autocomplete_ms || 900));
    const ready = () => {
      if (stopped) return;
      // Document-wide: sites often move the typed text into a dialog whose listbox the field does not own.
      const options = [...document.querySelectorAll('[role="option"]')];
      if (++polls >= 2 && options.some((e) => { const r = e.getBoundingClientRect();
        return r.width && r.height && r.bottom > 0 && r.top < innerHeight && visible(e); })) stop();
      else setTimeout(ready, 16);
    };
    setTimeout(ready, 16);
  };
  let loadMs = 0;
  const loaded = () => {
    if (document.readyState === 'complete' || performance.now() - started > loadTimeout) {
      loadMs = Math.round(performance.now() - started); afterInput(); return;
    }
    setTimeout(loaded, 25);
  };
  loaded();
})"""
