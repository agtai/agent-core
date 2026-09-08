# coding: utf-8
"""Compact browser page probes for Playwright runtime / BrowserDriver."""

from __future__ import annotations

import json
from typing import Any

from .probe_js import (
    BROWSER_STATE_SEMANTIC_JS,
    CARD_PROBE_JS,
    INTERACTIVE_PROBE_JS,
)


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def build_browser_state_semantic_params() -> dict[str, Any]:
    """Metadata probe takes no args; kept for API symmetry with other builders."""
    return {}


def build_interactive_probe_params(
    *,
    max_items: int = 50,
    viewport_only: bool = True,
    query: str = "",
    site_profiles: list[dict[str, Any]] | None = None,
    generation_id: str = "g0",
) -> dict[str, Any]:
    """Normalize interactive-probe args for driver.evaluate(..., args=params)."""
    return {
        "max_items": _clamp_int(max_items, default=50, minimum=1, maximum=100),
        "viewport_only": bool(viewport_only),
        "query": str(query or "").strip().lower(),
        "site_profiles": site_profiles or [],
        "generation_id": str(generation_id or "g0"),
    }


def build_card_probe_params(
    *,
    max_cards: int = 20,
    viewport_only: bool = True,
    include_buttons: bool = True,
    query: str = "",
    site_profiles: list[dict[str, Any]] | None = None,
    selector_cache_records: list[dict[str, Any]] | None = None,
    generation_id: str = "g0",
) -> dict[str, Any]:
    """Normalize card-probe args for driver.evaluate(..., args=params)."""
    return {
        "max_cards": _clamp_int(max_cards, default=20, minimum=1, maximum=50),
        "viewport_only": bool(viewport_only),
        "include_buttons": bool(include_buttons),
        "query": str(query or "").strip().lower(),
        "site_profiles": site_profiles or [],
        "selector_cache_records": selector_cache_records or [],
        "generation_id": str(generation_id or "g0"),
    }


def build_browser_state_metadata_js() -> str:
    """Build Playwright page-closure that wraps pure semantic JS + tab listing.

    Driver path should call ``BROWSER_STATE_SEMANTIC_JS`` via evaluate and
    ``list_tabs()`` separately; this wrapper remains for MCP-era tests and
    the generated-script parse check.
    """
    return (
        "async (page) => {\n"
        "  const pages = page.context().pages();\n"
        "\n"
        "  const tabs = await Promise.all(\n"
        "    pages.map(async (tab, index) => {\n"
        "      let title = '';\n"
        "      try {\n"
        "        title = await tab.title();\n"
        "      } catch (_error) {\n"
        "        title = '';\n"
        "      }\n"
        "      return {\n"
        "        index,\n"
        "        current: tab === page,\n"
        "        url: tab.url(),\n"
        "        title,\n"
        "      };\n"
        "    })\n"
        "  );\n"
        "\n"
        f"  const pageMetadata = await page.evaluate({BROWSER_STATE_SEMANTIC_JS});\n"
        "\n"
        "  let title = '';\n"
        "  try {\n"
        "    title = await page.title();\n"
        "  } catch (_error) {\n"
        "    title = '';\n"
        "  }\n"
        "\n"
        "  return {\n"
        "    ok: true,\n"
        "    url: page.url(),\n"
        "    title,\n"
        "    tabs,\n"
        "    page_position: pageMetadata.page_position,\n"
        "    semantic_state: pageMetadata.semantic_state,\n"
        "  };\n"
        "}"
    )


def build_interactive_probe_js(
    *,
    max_items: int = 50,
    viewport_only: bool = True,
    query: str = "",
    site_profiles: list[dict[str, Any]] | None = None,
    generation_id: str = "g0",
) -> str:
    """Build Playwright page-closure embedding params around pure probe JS."""
    params = build_interactive_probe_params(
        max_items=max_items,
        viewport_only=viewport_only,
        query=query,
        site_profiles=site_profiles,
        generation_id=generation_id,
    )
    params_json = json.dumps(params, ensure_ascii=False)
    return (
        "async (page) => {\n"
        f"  const params = {params_json};\n"
        f"  return await page.evaluate({INTERACTIVE_PROBE_JS}, params);\n"
        "}"
    )


def build_card_probe_js(
    *,
    max_cards: int = 20,
    viewport_only: bool = True,
    include_buttons: bool = True,
    query: str = "",
    site_profiles: list[dict[str, Any]] | None = None,
    selector_cache_records: list[dict[str, Any]] | None = None,
    generation_id: str = "g0",
) -> str:
    """Build Playwright page-closure embedding params around pure card-probe JS."""
    params = build_card_probe_params(
        max_cards=max_cards,
        viewport_only=viewport_only,
        include_buttons=include_buttons,
        query=query,
        site_profiles=site_profiles,
        selector_cache_records=selector_cache_records,
        generation_id=generation_id,
    )
    params_json = json.dumps(params, ensure_ascii=False)
    return (
        "async (page) => {\n"
        f"  const params = {params_json};\n"
        f"  return await page.evaluate({CARD_PROBE_JS}, params);\n"
        "}"
    )
