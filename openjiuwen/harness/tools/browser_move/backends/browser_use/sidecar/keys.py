# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Playwright-style key names normalized to browser-use's key-combo format.

STDLIB ONLY. browser-use's ``SendKeysEvent.keys`` accepts combos like
``"ctrl+a"`` or a bare named key like ``"Enter"``. Playwright-style callers
(and this repo's existing tool schemas) use combos like ``"Control+A"``.
This module normalizes the modifier tokens to what browser-use expects while
leaving the final key token's casing alone, since browser-use's own examples
keep named keys capitalized (``"Enter"``).
"""

from __future__ import annotations

_MODIFIER_ALIASES: dict[str, str] = {
    "control": "ctrl",
    "ctrl": "ctrl",
    "cmd": "cmd",
    "command": "cmd",
    "meta": "cmd",
    "alt": "alt",
    "option": "alt",
    "shift": "shift",
}


def normalize_key_combo(keys: str) -> str:
    """Normalize a Playwright-style key combo to browser-use's expected format.

    ``"Control+A"`` -> ``"ctrl+A"``; ``"Enter"`` -> ``"Enter"`` (unchanged);
    unknown modifier tokens pass through lowercased, best effort.
    """
    parts = [p for p in keys.split("+") if p]
    if not parts:
        return keys
    *modifiers, final_key = parts
    normalized_modifiers = [_MODIFIER_ALIASES.get(m.strip().lower(), m.strip().lower()) for m in modifiers]
    return "+".join([*normalized_modifiers, final_key.strip()])


__all__ = ["normalize_key_combo"]
