# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""In-page policy probe: wait for the page to settle, then describe every visible control once.

Called through ``BrowserAgentRuntime._evaluate_page_js`` as ``(params) => Promise``. The stamp attribute
``data-openjiuwen-jev=<id>`` is both the element's identity across ticks and the unique selector that
PageState registers; a control keeps its number for as long as its node stays in the document.
"""

from __future__ import annotations

STAMP_ATTRIBUTE = "data-openjiuwen-jev"

POLICY_PROBE_JS = r"""(params = {}) => new Promise((resolve) => {
  const ATTR = params.stamp_attribute || 'data-openjiuwen-jev';
  const after = params.after || null;
  const limits = {
    load: Number(params.load_timeout_ms || 3000),
    settle: Number(params.settle_ms || 500),
    quiet: Number(params.quiet_ms || 60),
    autocomplete: Number(params.autocomplete_ms || 900),
    items: Number(params.max_items || 250),
    text: Number(params.text_chars || 6000),
  };
  const t0 = performance.now();
  let loadMs = 0;

  const nextId = () => (window.__ojwProbeSeq = (window.__ojwProbeSeq || 0) + 1);
  const byId = (id) => document.querySelector('[' + ATTR + '="' + id + '"]');
  const stampCount = (id) => document.querySelectorAll('[' + ATTR + '="' + id + '"]').length;

  const SKIPPED_INPUT_TYPES = new Set(['password', 'file', 'hidden']);
  const ROLES = new Set(['button', 'link', 'checkbox', 'radio', 'switch', 'tab', 'menuitem', 'menuitemradio',
    'option', 'gridcell', 'combobox', 'textbox', 'searchbox', 'spinbutton']);
  const INPUT_ROLES = {
    checkbox: 'checkbox', radio: 'radio',
    button: 'button', submit: 'button', reset: 'button', image: 'button',
    search: 'searchbox', number: 'spinbutton',
    text: 'textbox', email: 'textbox', url: 'textbox', tel: 'textbox',
    date: 'textbox', 'datetime-local': 'textbox', month: 'textbox', time: 'textbox', week: 'textbox',
  };
  const EDITABLE_ROLES = new Set(['textbox', 'searchbox', 'spinbutton']);
  const CANDIDATES = 'a[href], button, input, textarea, select, summary, [contenteditable="true"], ' +
    [...ROLES].map((r) => '[role="' + r + '"]').join(', ');

  const acceptsInput = (e) => !SKIPPED_INPUT_TYPES.has(e.type);
  const rendered = (e) => e.checkVisibility({checkOpacity: true, checkVisibilityCSS: true}) &&
    !e.closest('[aria-hidden="true"], [inert]');
  const overlapsViewport = (r) => r.width > 0 && r.height > 0 && r.bottom > 0 && r.right > 0 &&
    r.top < innerHeight && r.left < innerWidth;
  const centreInViewport = (r) => {
    const x = r.left + r.width / 2, y = r.top + r.height / 2;
    return r.width > 0 && r.height > 0 && x >= 0 && y >= 0 && x < innerWidth && y < innerHeight;
  };

  const roleOf = (e) => {
    const declared = e.getAttribute('role');
    if (ROLES.has(declared)) return declared;
    switch (e.tagName) {
      case 'BUTTON': case 'SUMMARY': return 'button';
      case 'A': return 'link';
      case 'SELECT': return 'combobox';
      case 'TEXTAREA': return 'textbox';
      case 'INPUT': return INPUT_ROLES[e.type] || null;
      default: return e.isContentEditable ? 'textbox' : null;
    }
  };

  const accessibleName = (root) => {
    const visited = new Set();
    const nameOf = (e) => {
      if (!e || visited.has(e)) return '';
      visited.add(e);
      const fromIdRefs = (attr) => (e.getAttribute(attr) || '').split(/\s+/).filter(Boolean)
        .map((id) => nameOf(document.getElementById(id))).filter(Boolean).join(' ');
      const fromChildren = () => [...e.childNodes].map((n) =>
        n.nodeType === Node.TEXT_NODE ? n.textContent :
        n.nodeType === Node.ELEMENT_NODE && n.getAttribute('aria-hidden') !== 'true' ? nameOf(n) : '').join(' ');
      const buttonInput = e.tagName === 'INPUT' && ['button', 'submit', 'reset'].includes(e.type);
      const sources = [
        () => fromIdRefs('aria-labelledby'),
        () => e.getAttribute('aria-label'),
        () => [...(e.labels || [])].map((l) => nameOf(l)).filter(Boolean).join(' '),
        () => (buttonInput ? e.value : ''),
        () => e.getAttribute('alt'),
        () => (e.tagName === 'INPUT' ? '' : fromChildren()),
        () => e.getAttribute('title'),
        () => e.getAttribute('placeholder'),
      ];
      for (const source of sources) {
        const found = (source() || '').replace(/\s+/g, ' ').trim();
        if (found) return found;
      }
      return '';
    };
    return nameOf(root);
  };

  const regionOf = (e) => {
    const dialog = e.closest('dialog, [role="dialog"], [role="alertdialog"]');
    if (dialog) return 'dialog:' + (accessibleName(dialog) || 'unnamed').slice(0, 60);
    const list = e.closest('[role="menu"], [role="listbox"], [role="grid"]');
    return list ? list.getAttribute('role') + ':' + accessibleName(list).slice(0, 60) : '';
  };

  const choosable = (o) => !(o.selected || o.disabled || o.closest('optgroup[disabled]'));
  const describe = (e, role, id) => {
    const isSelect = e.tagName === 'SELECT';
    const typedInto = role === 'combobox' && (e.tagName === 'INPUT' || e.tagName === 'TEXTAREA');
    const typed = EDITABLE_ROLES.has(role) || typedInto;
    const label = (accessibleName(e) || role).slice(0, 200);
    let value = '';
    if (isSelect) value = [...e.selectedOptions].map((o) => o.label).join(', ');
    else if ('value' in e) value = String(e.value);
    else if (e.isContentEditable || role === 'combobox') value = e.innerText.trim();
    const item = {
      node: id,
      selector_hint: '[' + ATTR + '="' + id + '"]',
      selector_hint_validated: true,
      match_count: 1,
      role: role,
      label: label,
      accessible_name: label,
      text: label,
      value: value.slice(0, 200),
      region: regionOf(e),
      editable: typed && !e.readOnly && e.getAttribute('aria-readonly') !== 'true',
      visible: true,
      enabled: true,
      actionable: true,
      clickable: true,
    };
    for (const state of ['checked', 'selected', 'expanded']) {
      const declared = e.getAttribute('aria-' + state);
      if (declared !== null) item[state] = declared;
    }
    if (e.type === 'checkbox' || e.type === 'radio') item.checked = String(e.checked);
    if (isSelect) item.options = [...e.options].filter(choosable).map((o) => ({label: o.label, value: o.value}));
    return item;
  };

  const collect = () => {
    const elements = [], taken = new Set();
    let omitted = 0;
    for (const e of document.querySelectorAll(CANDIDATES)) {
      const role = roleOf(e);
      if (!role || !acceptsInput(e) || !rendered(e)) continue;
      if (e.matches(':disabled') || e.closest('[aria-disabled="true"]')) continue;
      if (!centreInViewport(e.getBoundingClientRect())) continue;
      if (role === 'gridcell' && e.querySelector('button, [role="button"]')) continue;
      if (elements.length >= limits.items) { omitted += 1; continue; }
      let id = Number(e.getAttribute(ATTR));
      const reusable = Number.isInteger(id) && id > 0 && !taken.has(id) && stampCount(id) === 1;
      if (!reusable) { id = nextId(); e.setAttribute(ATTR, String(id)); }
      taken.add(id);
      elements.push(describe(e, role, id));
    }
    return {elements, omitted};
  };

  const visibleText = () => {
    const acceptNode = (node) => {
      const parent = node.parentElement;
      const skip = !node.textContent.trim() || !parent || parent.closest('script, style, noscript, template');
      return !skip && rendered(parent) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
    };
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {acceptNode});
    const range = document.createRange();
    const lines = [];
    let total = 0;
    for (let node = walker.nextNode(); node && total < limits.text; node = walker.nextNode()) {
      range.selectNodeContents(node);
      if (!overlapsViewport(range.getBoundingClientRect())) continue;
      const line = node.textContent.trim();
      lines.push(line);
      total += line.length;
    }
    return lines.join('\n').slice(0, limits.text);
  };

  const fieldState = (f) =>
    [f.getAttribute(ATTR) || f.name || f.id, f.value, f.checked, f.selectedIndex, f.disabled, f.readOnly];
  const pageKey = (elements) => JSON.stringify({
    url: location.href,
    title: document.title,
    scroll: [scrollX, scrollY, innerWidth, innerHeight],
    nodes: elements.map((item) => item.node),
    fields: [...document.querySelectorAll('input, textarea, select')].filter(acceptsInput).map(fieldState),
    controls: document.querySelectorAll(CANDIDATES).length,
  });

  const snapshot = () => {
    const {elements, omitted} = collect();
    const height = document.documentElement.scrollHeight;
    return {
      url: location.href, title: document.title, text: visibleText(),
      scroll: {y: scrollY, height: height, viewport: innerHeight},
      can_scroll_down: scrollY + innerHeight < height - 2, can_scroll_up: scrollY > 0,
      elements: elements, omitted: omitted, page_key: pageKey(elements),
      settle_ms: Math.round(performance.now() - t0), load_ms: loadMs, ready_state: document.readyState,
      visibility: document.visibilityState, has_focus: document.hasFocus(),
    };
  };

  let finished = false;
  const finish = () => {
    if (finished) return;
    finished = true;
    clearTimeout(lastResort);
    try { resolve(snapshot()); } catch (err) { resolve({error: String((err && err.stack) || err), elements: []}); }
  };
  // Last-resort resolver: a hidden tab throttles timers, and an unresolved promise would hit the driver timeout.
  const lastResort = setTimeout(finish, limits.load + limits.settle + 1000);
  const awaitQuietDom = () => {
    // Resolve after `quiet` ms without mutations, or at `settle` ms at the latest.
    let quietTimer = null;
    const observer = new MutationObserver(() => {
      clearTimeout(quietTimer);
      quietTimer = setTimeout(done, limits.quiet);
    });
    const hardStop = setTimeout(done, limits.settle);
    function done() { clearTimeout(quietTimer); clearTimeout(hardStop); observer.disconnect(); finish(); }
    const watched = {subtree: true, childList: true, attributes: true, characterData: true};
    observer.observe(document.documentElement, watched);
    quietTimer = setTimeout(done, limits.quiet);
  };
  // Timers instead of requestAnimationFrame: rAF never fires while the tab is hidden.
  const awaitAutocomplete = () => {
    const field = after && after.node != null ? byId(after.node) : null;
    const expectsSuggestions = after && after.kind === 'fill' && field &&
      field.getAttribute('role') === 'combobox';
    if (!expectsSuggestions) { setTimeout(awaitQuietDom, 32); return; }
    let polls = 0, stopped = false;
    const stop = () => { if (!stopped) { stopped = true; awaitQuietDom(); } };
    setTimeout(stop, limits.autocomplete);
    const poll = () => {
      if (stopped) return;
      // Document-wide: sites often move the typed text into a dialog whose listbox the field does not own.
      const shown = [...document.querySelectorAll('[role="option"]')]
        .some((o) => overlapsViewport(o.getBoundingClientRect()) && rendered(o));
      if (++polls >= 2 && shown) stop(); else setTimeout(poll, 16);
    };
    setTimeout(poll, 16);
  };
  const awaitLoad = () => {
    if (document.readyState === 'complete' || performance.now() - t0 > limits.load) {
      loadMs = Math.round(performance.now() - t0);
      awaitAutocomplete();
      return;
    }
    setTimeout(awaitLoad, 25);
  };
  awaitLoad();
})"""
