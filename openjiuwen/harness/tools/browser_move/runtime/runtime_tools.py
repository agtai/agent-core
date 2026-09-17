# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""DeepAgent Tool wrappers around BrowserAgentRuntime."""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List

from openjiuwen.core.foundation.tool import Tool, ToolCard
from openjiuwen.harness.tools.base_tool import ToolOutput

if TYPE_CHECKING:
    from .runtime import BrowserAgentRuntime

_ctx_parent_session_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "playwright_runtime_parent_session_id",
    default="",
)
_ctx_parent_request_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "playwright_runtime_parent_request_id",
    default="",
)

_CANCEL_DESC = (
    "Cancel an in-progress browser task by session_id. "
    "Optionally pass request_id to target a specific request within the session. "
    "Returns JSON with ok/session_id/request_id/error."
)
_CANCEL_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "session_id": {"type": "string", "description": "Session ID of the task to cancel"},
        "request_id": {"type": "string", "description": "Optional: specific request ID to cancel"},
    },
    "required": ["session_id"],
}

_CLEAR_CANCEL_DESC = (
    "Clear the cancellation flag for a browser session or request. "
    "Returns JSON with ok/session_id/request_id/error."
)
_CLEAR_CANCEL_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "session_id": {"type": "string", "description": "Session ID to clear"},
        "request_id": {"type": "string", "description": "Optional: specific request ID to clear"},
    },
    "required": ["session_id"],
}

_NAVIGATE_DESC = "Navigate to a URL."
_NAVIGATE_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "url": {
            "type": "string",
            "description": "Absolute URL to open, e.g. https://example.com",
        },
        "wait_until": {
            "type": "string",
            "description": "Navigation wait condition. Default load.",
        },
        "timeout_ms": {
            "type": "integer",
            "description": "Optional navigation timeout in milliseconds.",
        },
    },
    "required": ["url"],
}

_NAVIGATE_BACK_DESC = "Navigate back in browser history."
_NAVIGATE_BACK_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {},
    "required": [],
}

_ELEMENT_TARGET_PROPERTIES: Dict[str, Any] = {
    "generation_id": {
        "type": "string",
        "description": "Current PageState generation_id for the element target.",
    },
    "target_id": {
        "type": "string",
        "description": "Generation-scoped target_id from PageState / probes.",
    },
    "ref": {
        "type": "string",
        "description": "Element ref from the current page observation.",
    },
    "selector": {
        "type": "string",
        "description": "CSS selector when a validated selector is already known.",
    },
}

_CLICK_DESC = "Click an element."
_CLICK_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
        "button": {
            "type": "string",
            "description": "Mouse button: left, right, or middle. Default left.",
        },
        "click_count": {
            "type": "integer",
            "description": "Number of clicks. Default 1.",
        },
    },
    "required": ["generation_id"],
}

_TYPE_DESC = "Type text into an element."
_TYPE_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
        "text": {
            "type": "string",
            "description": "Text to type into the element.",
        },
        "clear": {
            "type": "boolean",
            "description": "Clear existing content before typing. Default true.",
        },
        "press_enter": {
            "type": "boolean",
            "description": "Press Enter after typing. Default false.",
        },
        "sensitive": {
            "type": "boolean",
            "description": "Mark input as sensitive (e.g. password). Default false.",
        },
    },
    "required": ["generation_id", "text"],
}

_PRESS_KEY_DESC = "Press a keyboard key or key combination."
_PRESS_KEY_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "key": {
            "type": "string",
            "description": "Key or combination, e.g. Enter, Control+a, Escape.",
        },
    },
    "required": ["key"],
}

_SCREENSHOT_DESC = "Take a screenshot of the current page."
_SCREENSHOT_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "full_page": {
            "type": "boolean",
            "description": "Capture the full scrollable page. Default false.",
        },
    },
    "required": [],
}

_TABS_DESC = "List, select, or close browser tabs."
_TABS_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "description": "Tab action: list, select, or close. Default list.",
            "enum": ["list", "select", "close"],
        },
        "index": {
            "type": "integer",
            "description": "Zero-based tab index for select/close.",
        },
    },
    "required": [],
}

_CLOSE_DESC = "Close the active browser tab."
_CLOSE_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {},
    "required": [],
}

_SELECT_OPTION_DESC = "Select an option from a native select element."
_SELECT_OPTION_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
        "value": {
            "type": "string",
            "description": "Option value attribute to select.",
        },
        "label": {
            "type": "string",
            "description": "Visible option label to select.",
        },
    },
    "required": ["generation_id"],
}

_EVALUATE_DESC = "Evaluate a small JavaScript expression in the page. Do not dump the full document."
_EVALUATE_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "function": {
            "type": "string",
            "description": "JavaScript source to evaluate (expression or function body).",
        },
        "args": {
            "description": "Optional JSON-serializable argument passed to the script.",
        },
    },
    "required": ["function"],
}

_DRAG_DESC = "Drag from a source element to a target element."
_DRAG_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "generation_id": _ELEMENT_TARGET_PROPERTIES["generation_id"],
        "source_target_id": {
            "type": "string",
            "description": "Source element target_id from PageState.",
        },
        "source_ref": {
            "type": "string",
            "description": "Source element ref.",
        },
        "source_selector": {
            "type": "string",
            "description": "Source CSS selector.",
        },
        "target_target_id": {
            "type": "string",
            "description": "Target element target_id from PageState.",
        },
        "target_ref": {
            "type": "string",
            "description": "Target element ref.",
        },
        "target_selector": {
            "type": "string",
            "description": "Target CSS selector.",
        },
        "steps": {
            "type": "integer",
            "description": "Intermediate drag steps. Default 10.",
        },
    },
    "required": ["generation_id"],
}

_FILE_UPLOAD_DESC = "Upload one or more files to a file input element."
_FILE_UPLOAD_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
        "paths": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Absolute or workspace-relative file paths to upload. "
                "Relative/bare names also resolve under BROWSER_UPLOAD_ROOT when set "
                "(see list_upload_files)."
            ),
        },
    },
    "required": ["generation_id", "paths"],
}

_HOVER_DESC = "Hover the mouse over an element (tooltips / :hover menus)."
_HOVER_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
    },
    "required": ["generation_id"],
}

_FIND_DESC = (
    "Search the current accessibility / page snapshot for text or a regex. "
    "Returns matching nodes with target_id/ref and short context. "
    "Requires a prior browser_snapshot (or probe) for the current generation."
)
_FIND_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "Text or regex pattern to search for in the current snapshot.",
        },
        "regex": {
            "type": "boolean",
            "description": "Treat query as a regular expression. Default false.",
        },
        "limit": {
            "type": "integer",
            "description": "Maximum matches to return (1-100). Default 20.",
        },
        "generation_id": {
            "type": "string",
            "description": "Optional PageState generation_id; defaults to current.",
        },
    },
    "required": ["query"],
}

_HANDLE_DIALOG_DESC = (
    "Accept or dismiss a JavaScript alert/confirm/prompt. "
    "If no dialog is open yet, arms handling for the next dialog — "
    "call this before the click/type that opens it."
)
_HANDLE_DIALOG_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "accept": {
            "type": "boolean",
            "description": "True to accept/OK, false to dismiss/Cancel.",
        },
        "promptText": {
            "type": "string",
            "description": "Optional text to submit for a prompt() dialog.",
        },
        "prompt_text": {
            "type": "string",
            "description": "Alias for promptText.",
        },
    },
    "required": ["accept"],
}

_DROP_DESC = (
    "Drop local files and/or MIME-typed data onto an element as if dragged from "
    "outside the page. Distinct from browser_drag (element→element). "
    "At least one of paths or data is required. Supported MIME types: "
    "text/plain, text/uri-list, text/html."
)
_DROP_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        **_ELEMENT_TARGET_PROPERTIES,
        "paths": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Absolute or workspace-relative file paths to drop. "
                "Resolved like browser_file_upload (BROWSER_UPLOAD_ROOT)."
            ),
        },
        "data": {
            "type": "array",
            "description": "MIME-typed drag data items (mimeType + data).",
            "items": {
                "type": "object",
                "properties": {
                    "mimeType": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "data": {"type": "string"},
                },
            },
        },
    },
    "required": ["generation_id"],
}

_FILL_FORM_DESC = "Fill multiple form fields in one call (type / select / checkbox)."
_FILL_FORM_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "generation_id": _ELEMENT_TARGET_PROPERTIES["generation_id"],
        "fields": {
            "type": "array",
            "description": "Ordered field fill steps.",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Field type: textbox, combobox/select, checkbox/radio.",
                    },
                    "target_id": {"type": "string"},
                    "ref": {"type": "string"},
                    "selector": {"type": "string"},
                    "value": {},
                    "label": {"type": "string"},
                    "sensitive": {"type": "boolean"},
                },
                "required": [],
            },
        },
    },
    "required": ["generation_id", "fields"],
}

_SNAPSHOT_DESC = "Capture an accessibility/page observation of the current page."
_SNAPSHOT_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "include_screenshot": {
            "type": "boolean",
            "description": "Include a screenshot in the observation. Default false.",
        },
    },
    "required": [],
}

_CUSTOM_ACTION_DESC = (
    "Run a registered custom browser action by name. "
    "Use for deterministic helpers such as drag-and-drop or coordinate resolution "
    "alongside browser_navigate and other runtime browser tools. "
    "Call browser_list_custom_actions first to discover available actions and parameters. "
    "Aliases source/target and source_x/source_y/target_x/target_y are accepted."
)
_CUSTOM_ACTION_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "description": "Name of the custom action to run"},
        "session_id": {"type": "string", "description": "Session ID (optional)"},
        "request_id": {"type": "string", "description": "Request ID (optional)"},
        "params": {
            "type": "object",
            "description": "Extra key-value parameters forwarded to the action",
            "properties": {},
            "required": [],
        },
    },
    "required": ["action"],
}

_LIST_ACTIONS_DESC = (
    "List available custom browser actions and detailed parameter guidance "
    "for browser_custom_action."
)
_LIST_ACTIONS_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {},
    "required": [],
}

_RUNTIME_HEALTH_DESC = (
    "Return runtime readiness, heartbeat status, and selected provider/model configuration."
)
_RUNTIME_HEALTH_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {},
    "required": [],
}

_PROBE_INTERACTIVES_DESC = (
    "Return a compact list of visible, high-value interactive elements on the current page. "
    "Use this for page-level controls such as buttons, links, inputs, forms, navigation, login, "
    "pagination, menus, calendar dates, sort tabs, rating filters, and visible actions. "
    "Dynamic controls are returned as generation-scoped targets with region/kind semantics. "
    "The optional query filter is alias-aware for common "
    "search/input terms, including placeholders, aria labels, input type/name/id/class, and Chinese "
    "search text such as 搜索/关键词. Prefer max_items around 20-30 unless a larger inventory "
    "is needed. For product/search/listing card data, prefer browser_probe_cards first. "
    "The result includes compact PageState and generation-scoped target_id values that can be "
    "passed directly to browser_batch_interact without rebuilding CSS. It also includes "
    "role/text/region/kind plus match_count/visible/enabled/actionable. The model-facing "
    "result does not expose Probe-local ids or internal selectors."
)
_PROBE_INTERACTIVES_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "max_items": {
            "type": "integer",
            "description": "Maximum number of elements to return. Default 30, hard-capped at 40.",
        },
        "viewport_only": {
            "type": "boolean",
            "description": "When true, only return elements currently visible in the viewport. Default true.",
        },
        "query": {
            "type": "string",
            "description": "Optional text filter, e.g. 'cart', 'search', 'next', or 'login'.",
        },
    },
    "required": [],
}

_PROBE_CARDS_DESC = (
    "Return compact repeated card/listing structures from the current page. "
    "Use this first on product pages, marketplace pages, search-result pages, catalog pages, "
    "article-list pages, table/list-row result pages, or any page with repeated visible cards/listings. "
    "The result includes compact PageState with generation-scoped card/control target_id values, "
    "candidate card title, author/source, summary/snippet, price, rating, "
    "review count, availability, primary link, and visible controls, "
    "region/kind/result_index/is_ad semantics that distinguish main results, accounts, sidebars, "
    "hot searches, shops, chats, and product links, "
    "match_count/visible/enabled/clickable/generation_id, recurring structure signatures, and "
    "cache diagnostics. Navigate primary_link/href directly instead of clicking a card hint. "
    "If this returns the fields needed "
    "for the task, including article/search-result title/link/author/source/summary fields, "
    "use the compact card result directly instead of taking screenshots/snapshots or running "
    "broad DOM evaluation. Only evaluate again when a required field is missing."
)
_PROBE_CARDS_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "max_cards": {
            "type": "integer",
            "description": "Maximum number of cards to return. Default 12, hard-capped at 20.",
        },
        "viewport_only": {
            "type": "boolean",
            "description": "When true, only inspect cards visible in the current viewport. Default true.",
        },
        "include_buttons": {
            "type": "boolean",
            "description": "When true, include visible buttons/links inside each card. Default true.",
        },
        "query": {
            "type": "string",
            "description": "Optional text filter, e.g. 'mouse', 'book', 'laptop', or 'cart'.",
        },
    },
    "required": [],
}

_BATCH_INTERACT_DESC = (
    "Execute deterministic browser interactions in one standalone runtime tool call. "
    "Use after browser_probe_interactives/browser_probe_cards when a page-level flow has multiple "
    "known targets, such as three or more form fields, click+type+choose autocomplete, dropdown or "
    "date-picker selection, filter panels, search submit plus result wait, or compact extraction. "
    "This is a first-class helper like the probe tools; do not route this through browser_custom_action. "
    "Pass the current PageState generation_id and use target_id from probes for actions. "
    "Only read-only extraction and explicit wait operations accept a validated selector. "
    "Locator strategies are mutually exclusive. "
    "A one-step call is rewritten by the runtime to an equivalent official browser primitive when "
    "one exists, so it does not fail and require another model turn. Multi-step calls are preflighted "
    "before side effects. Results use status=completed/partial/failed and contain only compact step "
    "status, extracted fields, condition observations, metrics, and PageState."
)
_BATCH_INTERACT_PARAMS: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "steps": {
            "type": "array",
            "description": (
                "Ordered browser steps to execute in one batch. Supported ops: click, fill, type, "
                "autocomplete, select_visible_text, press, select_option, set_checked, "
                "wait_for_selector, wait_for_text, wait_for_load_state, wait_for_url, "
                "wait_for_first_card_title, wait_for_sort_state, wait_for_result_count, "
                "wait_for_dom_text_change, wait_for_stable, wait_for_tab, extract_text, "
                "extract_value, screenshot."
            ),
            "minItems": 1,
            "maxItems": 25,
            "items": {
                "type": "object",
                "properties": {
                    "op": {
                        "type": "string",
                        "description": "Step operation name.",
                        "enum": [
                            "click",
                            "fill",
                            "type",
                            "autocomplete",
                            "select_visible_text",
                            "press",
                            "select_option",
                            "set_checked",
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
                            "extract_text",
                            "extract_value",
                            "screenshot",
                        ],
                    },
                    "target_id": {
                        "type": "string",
                        "description": (
                            "Generation-scoped target_id returned by the current PageState/probe. "
                            "Preferred for actions and mutually exclusive with ref/selector/role/"
                            "label/placeholder/text/testid."
                        ),
                    },
                    "ref": {
                        "type": "string",
                        "description": (
                            "Native AX ref returned by browser_snapshot/browser_find in the current generation. "
                            "The runtime resolves it without model-generated CSS."
                        ),
                    },
                    "selector": {
                        "type": "string",
                        "description": (
                            "Current-generation selector_hint from a probe, or an explicit selector only "
                            "for a wait condition. "
                            "Do not combine with another locator strategy."
                        ),
                    },
                    "role": {
                        "type": "string",
                        "description": "ARIA role to locate, e.g. button/textbox/link.",
                    },
                    "name": {
                        "type": "string",
                        "description": "Accessible name for role-based locating.",
                    },
                    "label": {
                        "type": "string",
                        "description": "Label text for labeled form controls.",
                    },
                    "placeholder": {
                        "type": "string",
                        "description": "Placeholder text for form controls.",
                    },
                    "testid": {
                        "type": "string",
                        "description": "Exact data-testid value for a target.",
                    },
                    "text": {
                        "type": "string",
                        "description": "Visible text for locate/click/wait_for_text.",
                    },
                    "value": {
                        "type": "string",
                        "description": "Value to fill/type/select, or autocomplete query.",
                    },
                    "option_text": {
                        "type": "string",
                        "description": (
                            "Visible option text for autocomplete/select_visible_text/native select label."
                        ),
                    },
                    "choose_text": {
                        "type": "string",
                        "description": "Alias for option_text in autocomplete flows.",
                    },
                    "option_selector": {
                        "type": "string",
                        "description": "CSS selector for an autocomplete/dropdown option to choose.",
                    },
                    "choose_selector": {
                        "type": "string",
                        "description": "Alias for option_selector.",
                    },
                    "option_role": {
                        "type": "string",
                        "description": ("ARIA role for an autocomplete/dropdown option, e.g. option/menuitem."),
                    },
                    "choose_role": {
                        "type": "string",
                        "description": "Alias for option_role.",
                    },
                    "option_name": {
                        "type": "string",
                        "description": "Accessible name for option_role.",
                    },
                    "choose_name": {
                        "type": "string",
                        "description": "Alias for option_name.",
                    },
                    "option_value": {
                        "type": "string",
                        "description": "Native select option value. Alias for value when op=select_option.",
                    },
                    "values": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "MCP/Playwright-style native select values list. Also accepted for single-select fields."
                        ),
                    },
                    "option_label": {
                        "type": "string",
                        "description": ("Native select visible label. Alias for option_text when op=select_option."),
                    },
                    "label_value": {
                        "type": "string",
                        "description": "Native select visible label alias.",
                    },
                    "index": {
                        "type": "integer",
                        "description": "Native select option index.",
                    },
                    "key": {
                        "type": "string",
                        "description": "Keyboard key for press, e.g. Enter or Escape.",
                    },
                    "checked": {
                        "type": "boolean",
                        "description": "Desired checked state for set_checked.",
                    },
                    "state": {
                        "type": "string",
                        "description": (
                            "Wait state for wait_for_selector/load_state, e.g. visible/attached/domcontentloaded."
                        ),
                    },
                    "option_target_id": {
                        "type": "string",
                        "description": "PageState target_id for an autocomplete/dropdown option.",
                    },
                    "option_ref": {
                        "type": "string",
                        "description": "Current-generation AX ref for an autocomplete/dropdown option.",
                    },
                    "url": {
                        "type": "string",
                        "description": "Exact URL expected by wait_for_url.",
                    },
                    "expected_url": {
                        "type": "string",
                        "description": "Alias for the exact URL expected by wait_for_url.",
                    },
                    "url_contains": {
                        "type": "string",
                        "description": "URL substring expected by wait_for_url.",
                    },
                    "url_pattern": {
                        "type": "string",
                        "description": "Regular expression expected to match the current URL.",
                    },
                    "title_contains": {
                        "type": "string",
                        "description": "Title substring expected on a newly opened tab.",
                    },
                    "min_tabs": {
                        "type": "integer",
                        "description": (
                            "Minimum context tab count for wait_for_tab. By default the runtime waits "
                            "for one more tab than existed when the batch started."
                        ),
                    },
                    "activate": {
                        "type": "boolean",
                        "description": (
                            "For wait_for_tab, activate the matched new tab for subsequent steps. Default true."
                        ),
                    },
                    "expected_text": {
                        "type": "string",
                        "description": "Expected first-card title text.",
                    },
                    "attribute": {
                        "type": "string",
                        "description": "Attribute inspected by wait_for_sort_state; defaults to aria-sort.",
                    },
                    "expected_value": {
                        "type": "string",
                        "description": "Expected attribute or text value for a wait condition.",
                    },
                    "count": {
                        "type": "integer",
                        "description": "Exact result count expected.",
                    },
                    "min_count": {
                        "type": "integer",
                        "description": "Minimum result count expected.",
                    },
                    "max_count": {
                        "type": "integer",
                        "description": "Maximum result count expected.",
                    },
                    "previous_text": {
                        "type": "string",
                        "description": "Previous DOM text used by wait_for_dom_text_change.",
                    },
                    "poll_interval_ms": {
                        "type": "integer",
                        "description": (
                            "Dynamic-condition polling interval, clamped to 50..1000ms; default 100ms. "
                            "Polling always shares the step's total timeout."
                        ),
                    },
                    "stable_ms": {
                        "type": "integer",
                        "description": "How long a matched value must remain unchanged.",
                    },
                    "field": {
                        "type": "string",
                        "description": "Structured output field name for extraction steps.",
                    },
                    "exact": {
                        "type": "boolean",
                        "description": "Use exact matching for role/label/text locators.",
                    },
                    "optional": {
                        "type": "boolean",
                        "description": "When true, failure records the step but continues.",
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "description": "Optional per-step timeout override.",
                    },
                    "delay_ms": {
                        "type": "integer",
                        "description": "Optional typing delay in milliseconds.",
                    },
                    "max_chars": {
                        "type": "integer",
                        "description": "Maximum characters for extract_text.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Screenshot path for op=screenshot.",
                    },
                    "full_page": {
                        "type": "boolean",
                        "description": "When true, screenshot the full page.",
                    },
                    "description": {
                        "type": "string",
                        "description": "Human-readable purpose for logs/debugging.",
                    },
                },
                "required": ["op"],
            },
        },
        "timeout_ms": {
            "type": "integer",
            "description": ("Default per-step timeout in milliseconds. Default 2500, clamped to 250..30000."),
        },
        "generation_id": {
            "type": "string",
            "pattern": "^g[0-9]+$",
            "description": (
                "Current PageState generation_id returned by navigate, probe, snapshot, or the previous batch. "
                "Targets from older generations are rejected before browser execution."
            ),
        },
        "condition_timeout_ms": {
            "type": "integer",
            "description": (
                "Default timeout for condition waits in milliseconds. "
                "Default 10000, clamped to the action timeout..30000."
            ),
        },
        "continue_on_error": {
            "type": "boolean",
            "description": "When true, continue after failed steps and return per-step errors.",
        },
        "global_timeout_ms": {
            "type": "integer",
            "description": (
                "Hard timeout for the whole batch. Default is computed from step count, capped at "
                "120000; explicit values are capped at 180000."
            ),
        },
        "session_id": {
            "type": "string",
            "description": "Optional browser task session id.",
        },
        "request_id": {
            "type": "string",
            "description": "Optional browser task request id.",
        },
    },
    "required": ["steps", "generation_id"],
}


class BrowserNavigateTool(Tool):
    """Navigate to a URL via BrowserDriver (runtime/BU path)."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_navigate",
                description=_NAVIGATE_DESC,
                input_params=_NAVIGATE_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        url = str(inputs.get("url") or "").strip()
        wait_until = str(inputs.get("wait_until") or "load").strip() or "load"
        timeout_raw = inputs.get("timeout_ms")
        timeout_ms: int | None
        if timeout_raw in (None, ""):
            timeout_ms = None
        else:
            try:
                timeout_ms = int(timeout_raw)
            except (TypeError, ValueError):
                return ToolOutput(success=False, error="'timeout_ms' must be an integer")
        try:
            result = await self._runtime.navigate(
                url=url,
                wait_until=wait_until,
                timeout_ms=timeout_ms,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserNavigateBackTool(Tool):
    """Navigate back via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_navigate_back",
                description=_NAVIGATE_BACK_DESC,
                input_params=_NAVIGATE_BACK_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del inputs, kwargs
        try:
            result = await self._runtime.navigate_back()
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


def _parse_generation_id(inputs: Dict[str, Any]) -> str | ToolOutput:
    generation_id = str(inputs.get("generation_id") or "").strip()
    if not generation_id:
        return ToolOutput(success=False, error="'generation_id' is required")
    return generation_id


class BrowserClickTool(Tool):
    """Click an element via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_click",
                description=_CLICK_DESC,
                input_params=_CLICK_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        click_count_raw = inputs.get("click_count", 1)
        try:
            click_count = int(click_count_raw if click_count_raw not in (None, "") else 1)
        except (TypeError, ValueError):
            return ToolOutput(success=False, error="'click_count' must be an integer")
        try:
            result = await self._runtime.click(
                generation_id=generation_id,
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
                button=str(inputs.get("button") or "left").strip() or "left",
                click_count=click_count,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserTypeTool(Tool):
    """Type text into an element via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_type",
                description=_TYPE_DESC,
                input_params=_TYPE_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        clear_raw = inputs.get("clear", True)
        if isinstance(clear_raw, str):
            clear = clear_raw.strip().lower() not in {"0", "false", "no"}
        else:
            clear = bool(clear_raw)
        press_enter_raw = inputs.get("press_enter", False)
        if isinstance(press_enter_raw, str):
            press_enter = press_enter_raw.strip().lower() in {"1", "true", "yes"}
        else:
            press_enter = bool(press_enter_raw)
        sensitive_raw = inputs.get("sensitive", False)
        if isinstance(sensitive_raw, str):
            sensitive = sensitive_raw.strip().lower() in {"1", "true", "yes"}
        else:
            sensitive = bool(sensitive_raw)
        try:
            result = await self._runtime.type_text(
                generation_id=generation_id,
                text=str(inputs.get("text") or ""),
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
                clear=clear,
                press_enter=press_enter,
                sensitive=sensitive,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserPressKeyTool(Tool):
    """Press a key via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_press_key",
                description=_PRESS_KEY_DESC,
                input_params=_PRESS_KEY_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        try:
            result = await self._runtime.press_key(keys=str(inputs.get("key") or ""))
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserTakeScreenshotTool(Tool):
    """Take a screenshot via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_take_screenshot",
                description=_SCREENSHOT_DESC,
                input_params=_SCREENSHOT_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        full_page_raw = inputs.get("full_page", False)
        if isinstance(full_page_raw, str):
            full_page = full_page_raw.strip().lower() in {"1", "true", "yes"}
        else:
            full_page = bool(full_page_raw)
        try:
            result = await self._runtime.take_screenshot(full_page=full_page)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserTabsTool(Tool):
    """List, select, or close tabs via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_tabs",
                description=_TABS_DESC,
                input_params=_TABS_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        index_raw = inputs.get("index")
        index: int | None
        if index_raw in (None, ""):
            index = None
        else:
            try:
                index = int(index_raw)
            except (TypeError, ValueError):
                return ToolOutput(success=False, error="'index' must be an integer")
        try:
            result = await self._runtime.tabs(
                action=str(inputs.get("action") or "list"),
                index=index,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserCloseTool(Tool):
    """Close the active tab via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_close",
                description=_CLOSE_DESC,
                input_params=_CLOSE_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del inputs, kwargs
        try:
            result = await self._runtime.close_page()
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserSelectOptionTool(Tool):
    """Select a native option via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_select_option",
                description=_SELECT_OPTION_DESC,
                input_params=_SELECT_OPTION_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        try:
            result = await self._runtime.select_option(
                generation_id=generation_id,
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
                value=None if inputs.get("value") in (None, "") else str(inputs.get("value")),
                label=None if inputs.get("label") in (None, "") else str(inputs.get("label")),
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserEvaluateTool(Tool):
    """Evaluate a small page script via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_evaluate",
                description=_EVALUATE_DESC,
                input_params=_EVALUATE_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        try:
            result = await self._runtime.evaluate(
                source=str(inputs.get("function") or inputs.get("source") or ""),
                args=inputs.get("args"),
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserDragTool(Tool):
    """Drag between two elements via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_drag",
                description=_DRAG_DESC,
                input_params=_DRAG_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        steps_raw = inputs.get("steps", 10)
        try:
            steps = int(steps_raw if steps_raw not in (None, "") else 10)
        except (TypeError, ValueError):
            return ToolOutput(success=False, error="'steps' must be an integer")
        try:
            result = await self._runtime.drag(
                generation_id=generation_id,
                source_target_id=str(inputs.get("source_target_id") or "").strip(),
                source_ref=str(inputs.get("source_ref") or "").strip(),
                source_selector=str(inputs.get("source_selector") or "").strip(),
                target_target_id=str(inputs.get("target_target_id") or "").strip(),
                target_ref=str(inputs.get("target_ref") or "").strip(),
                target_selector=str(inputs.get("target_selector") or "").strip(),
                steps=steps,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserFileUploadTool(Tool):
    """Upload files via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_file_upload",
                description=_FILE_UPLOAD_DESC,
                input_params=_FILE_UPLOAD_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        paths_raw = inputs.get("paths")
        if not isinstance(paths_raw, list):
            return ToolOutput(success=False, error="'paths' must be a list of strings")
        try:
            result = await self._runtime.file_upload(
                generation_id=generation_id,
                paths=[str(path) for path in paths_raw],
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserHoverTool(Tool):
    """Hover over an element via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_hover",
                description=_HOVER_DESC,
                input_params=_HOVER_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        try:
            result = await self._runtime.hover(
                generation_id=generation_id,
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserFindTool(Tool):
    """Search the current PageState snapshot for text/regex matches."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_find",
                description=_FIND_DESC,
                input_params=_FIND_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        query = str(inputs.get("query") or "").strip()
        if not query:
            return ToolOutput(success=False, error="'query' is required")
        regex_raw = inputs.get("regex", False)
        if isinstance(regex_raw, str):
            regex = regex_raw.strip().lower() in {"1", "true", "yes"}
        else:
            regex = bool(regex_raw)
        limit_raw = inputs.get("limit", 20)
        try:
            limit = int(limit_raw if limit_raw not in (None, "") else 20)
        except (TypeError, ValueError):
            return ToolOutput(success=False, error="'limit' must be an integer")
        generation_id = str(inputs.get("generation_id") or "").strip()
        try:
            result = await self._runtime.find(
                query=query,
                regex=regex,
                limit=limit,
                generation_id=generation_id,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserHandleDialogTool(Tool):
    """Accept/dismiss a JS dialog, or arm for the next one."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_handle_dialog",
                description=_HANDLE_DIALOG_DESC,
                input_params=_HANDLE_DIALOG_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        if "accept" not in inputs:
            return ToolOutput(success=False, error="'accept' is required")
        accept_raw = inputs.get("accept")
        if isinstance(accept_raw, str):
            accept = accept_raw.strip().lower() in {"1", "true", "yes"}
        else:
            accept = bool(accept_raw)
        prompt_raw = inputs.get("promptText", inputs.get("prompt_text"))
        prompt_text = None if prompt_raw is None else str(prompt_raw)
        try:
            result = await self._runtime.handle_dialog(accept=accept, prompt_text=prompt_text)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserDropTool(Tool):
    """Drop external files / MIME data onto an element via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_drop",
                description=_DROP_DESC,
                input_params=_DROP_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        paths_raw = inputs.get("paths")
        data_raw = inputs.get("data")
        paths: list[str] | None = None
        data: list[Dict[str, Any]] | None = None
        if paths_raw is not None:
            if not isinstance(paths_raw, list):
                return ToolOutput(success=False, error="'paths' must be a list of strings")
            paths = [str(path) for path in paths_raw]
        if data_raw is not None:
            if not isinstance(data_raw, list):
                return ToolOutput(success=False, error="'data' must be a list of objects")
            data = [item for item in data_raw if isinstance(item, dict)]
            if len(data) != len(data_raw):
                return ToolOutput(success=False, error="'data' items must be objects")
        try:
            result = await self._runtime.drop(
                generation_id=generation_id,
                paths=paths,
                data=data,
                target_id=str(inputs.get("target_id") or "").strip(),
                ref=str(inputs.get("ref") or "").strip(),
                selector=str(inputs.get("selector") or "").strip(),
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserFillFormTool(Tool):
    """Fill multiple form fields via composed driver actions."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_fill_form",
                description=_FILL_FORM_DESC,
                input_params=_FILL_FORM_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        generation_id = _parse_generation_id(inputs)
        if isinstance(generation_id, ToolOutput):
            return generation_id
        fields = inputs.get("fields")
        if not isinstance(fields, list):
            return ToolOutput(success=False, error="'fields' must be a list")
        try:
            result = await self._runtime.fill_form(generation_id=generation_id, fields=fields)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserSnapshotTool(Tool):
    """Capture a page observation via BrowserDriver."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_snapshot",
                description=_SNAPSHOT_DESC,
                input_params=_SNAPSHOT_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        include_raw = inputs.get("include_screenshot", False)
        if isinstance(include_raw, str):
            include_screenshot = include_raw.strip().lower() in {"1", "true", "yes"}
        else:
            include_screenshot = bool(include_raw)
        try:
            result = await self._runtime.snapshot(include_screenshot=include_screenshot)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserCancelTool(Tool):
    """Cancel an in-progress browser task."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_cancel_run",
                description=_CANCEL_DESC,
                input_params=_CANCEL_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        await self._runtime.ensure_runtime_ready()
        session_id = inputs.get("session_id", "")
        request_id = inputs.get("request_id") or None
        try:
            result = await self._runtime.cancel_run(session_id=session_id, request_id=request_id)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserClearCancelTool(Tool):
    """Clear a cancellation flag for a browser task."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_clear_cancel",
                description=_CLEAR_CANCEL_DESC,
                input_params=_CLEAR_CANCEL_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        await self._runtime.ensure_runtime_ready()
        session_id = inputs.get("session_id", "")
        request_id = inputs.get("request_id") or None
        try:
            result = await self._runtime.clear_cancel(session_id=session_id, request_id=request_id)
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserCustomActionTool(Tool):
    """Run a registered custom browser action."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_custom_action",
                description=_CUSTOM_ACTION_DESC,
                input_params=_CUSTOM_ACTION_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs
        action = inputs.get("action", "")
        session_id = (inputs.get("session_id") or "").strip() or _ctx_parent_session_id.get()
        request_id = (inputs.get("request_id") or "").strip() or _ctx_parent_request_id.get()
        params: Dict[str, Any] = inputs.get("params") or {}
        try:
            result = await self._runtime.run_custom_action(
                action=action,
                session_id=session_id,
                request_id=request_id,
                params=params,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserListActionsTool(Tool):
    """List available custom browser actions."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_list_custom_actions",
                description=_LIST_ACTIONS_DESC,
                input_params=_LIST_ACTIONS_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del inputs, kwargs
        try:
            data = await self._runtime.list_actions()
            return ToolOutput(success=True, data=data)
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserProbeInteractivesTool(Tool):
    """Compact visible-interactive-element probe."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_probe_interactives",
                description=_PROBE_INTERACTIVES_DESC,
                input_params=_PROBE_INTERACTIVES_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs

        try:
            max_items = int(inputs.get("max_items", 30))
        except (TypeError, ValueError):
            max_items = 30
        max_items = max(1, min(40, max_items))

        viewport_only_raw = inputs.get("viewport_only", True)
        if isinstance(viewport_only_raw, str):
            viewport_only = viewport_only_raw.strip().lower() not in {"0", "false", "no"}
        else:
            viewport_only = bool(viewport_only_raw)

        query = str(inputs.get("query") or "").strip()

        try:
            data = await self._runtime.probe_interactives(
                max_items=max_items,
                viewport_only=viewport_only,
                query=query,
            )
            return ToolOutput(
                success=bool(data.get("ok", True)),
                data=data,
                error=data.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserProbeCardsTool(Tool):
    """Compact repeated-card/listing probe."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_probe_cards",
                description=_PROBE_CARDS_DESC,
                input_params=_PROBE_CARDS_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs

        try:
            max_cards = int(inputs.get("max_cards", 12))
        except (TypeError, ValueError):
            max_cards = 12
        max_cards = max(1, min(20, max_cards))

        viewport_only_raw = inputs.get("viewport_only", True)
        if isinstance(viewport_only_raw, str):
            viewport_only = viewport_only_raw.strip().lower() not in {"0", "false", "no"}
        else:
            viewport_only = bool(viewport_only_raw)

        include_buttons_raw = inputs.get("include_buttons", True)
        if isinstance(include_buttons_raw, str):
            include_buttons = include_buttons_raw.strip().lower() not in {
                "0",
                "false",
                "no",
            }
        else:
            include_buttons = bool(include_buttons_raw)

        query = str(inputs.get("query") or "").strip()

        try:
            data = await self._runtime.probe_cards(
                max_cards=max_cards,
                viewport_only=viewport_only,
                include_buttons=include_buttons,
                query=query,
            )
            return ToolOutput(
                success=bool(data.get("ok", True)),
                data=data,
                error=data.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserBatchInteractTool(Tool):
    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_batch_interact",
                description=_BATCH_INTERACT_DESC,
                input_params=_BATCH_INTERACT_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del kwargs

        steps = inputs.get("steps")
        session_id = (inputs.get("session_id") or "").strip() or _ctx_parent_session_id.get()
        request_id = (inputs.get("request_id") or "").strip() or _ctx_parent_request_id.get()

        try:
            result = await self._runtime.batch_interact(
                steps=steps,
                generation_id=str(inputs.get("generation_id") or ""),
                timeout_ms=inputs.get("timeout_ms"),
                condition_timeout_ms=inputs.get("condition_timeout_ms"),
                wait_after_each_ms=inputs.get("wait_after_each_ms"),
                continue_on_error=bool(inputs.get("continue_on_error", False)),
                global_timeout_ms=inputs.get("global_timeout_ms"),
                session_id=session_id,
                request_id=request_id,
            )
            return ToolOutput(
                success=bool(result.get("ok", True)),
                data=result,
                error=result.get("error"),
            )
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


class BrowserRuntimeHealthTool(Tool):
    """Return runtime readiness and heartbeat metadata."""

    def __init__(self, runtime: "BrowserAgentRuntime", language: str = "cn") -> None:
        del language
        super().__init__(
            ToolCard(
                name="browser_runtime_health",
                description=_RUNTIME_HEALTH_DESC,
                input_params=_RUNTIME_HEALTH_PARAMS,
            )
        )
        self._runtime = runtime

    async def invoke(self, inputs: Dict[str, Any], **kwargs: Any) -> ToolOutput:
        del inputs, kwargs
        try:
            data = await self._runtime.runtime_health()
            return ToolOutput(success=True, data=data)
        except Exception as exc:
            return ToolOutput(success=False, error=str(exc))

    async def stream(self, inputs: Dict[str, Any], **kwargs: Any) -> AsyncIterator[Any]:
        del inputs, kwargs
        if False:
            yield None


def _runtime_uses_browser_driver(runtime: "BrowserAgentRuntime") -> bool:
    """True when ``runtime`` drives Chrome through a BrowserDriver backend.

    Mirrors the defensive lookup in ``controllers/action.py`` so lightweight
    test doubles without the predicate keep the Playwright MCP path.
    """

    uses = getattr(runtime, "_uses_browser_driver", None)
    if callable(uses):
        try:
            return bool(uses())
        except Exception:
            return False
    return bool(uses)


def build_browser_runtime_tools(
    runtime: "BrowserAgentRuntime",
    language: str = "cn",
) -> List[Tool]:
    """Build model-facing browser tools backed by ``BrowserAgentRuntime``.

    The returned set depends on which backend drives Chrome:

    * **Playwright MCP** — deterministic helper tools only. Low-level actions
      (click / type / navigate / ...) are served by the MCP server's own
      primitives, which the browser subagent calls directly. Injecting the
      local CORE catalog here would shadow those primitives with methods that
      require a :class:`BrowserDriver` the MCP path never creates.
    * **BrowserDriver (browser_use)** — the registered CORE catalog plus
      session-control exceptions (cancel / clear_cancel). Probe / batch /
      custom_action helpers stay internal runtime APIs on this path.
    """

    if not _runtime_uses_browser_driver(runtime):
        # Ported from agtai/develop #1147: routing the browser through the
        # synchronous task tool makes the cancel / custom-action / list-actions /
        # health tools redundant on the Playwright MCP path.
        return [
            BrowserProbeInteractivesTool(runtime, language),
            BrowserProbeCardsTool(runtime, language),
            BrowserBatchInteractTool(runtime, language),
        ]

    return [
        BrowserNavigateTool(runtime, language),
        BrowserNavigateBackTool(runtime, language),
        BrowserClickTool(runtime, language),
        BrowserTypeTool(runtime, language),
        BrowserPressKeyTool(runtime, language),
        BrowserTakeScreenshotTool(runtime, language),
        BrowserTabsTool(runtime, language),
        BrowserCloseTool(runtime, language),
        BrowserSelectOptionTool(runtime, language),
        BrowserEvaluateTool(runtime, language),
        BrowserDragTool(runtime, language),
        BrowserFileUploadTool(runtime, language),
        BrowserHoverTool(runtime, language),
        BrowserFindTool(runtime, language),
        BrowserHandleDialogTool(runtime, language),
        BrowserDropTool(runtime, language),
        BrowserFillFormTool(runtime, language),
        BrowserSnapshotTool(runtime, language),
        # Non-CORE exceptions: session cancellation control for long-running tasks.
        BrowserCancelTool(runtime, language),
        BrowserClearCancelTool(runtime, language),
    ]
