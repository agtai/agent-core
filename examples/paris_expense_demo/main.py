# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""
Recall memory → plan → permission → collect → fill ERP → approve → submit.
"""

from __future__ import annotations

import argparse
import sys
import webbrowser

from .approval import render_approval_card, render_submit_result
from .config import list_case_ids, load_case, source_label, template_labels
from .erp_client import (
    DEFAULT_ERP_ORIGIN,
    ERP_WRITE,
    ErpUnavailable,
    apply_erp_lines,
    cancel_erp,
    ensure_erp,
    fill_draft,
    submit_erp,
    wait_decision,
)
from .pipeline import collect_case, submit_draft
from .registry import list_sources
from . import sources as _sources  # noqa: F401


def _prompt_approval(auto: str | None) -> bool:
    if auto is not None:
        answer = auto.strip().lower()
        print(f"\n(auto) approval answer: {answer}")
    else:
        try:
            answer = input("\nApprove and submit? [y/n]: ").strip().lower()
        except EOFError:
            print("\nNo input; cancelling.")
            return False
    return answer in {"y", "yes"}


def _fill_erp(
    draft,
    *,
    origin: str,
    autostart: bool,
    open_browser: bool,
    pause_s: float,
) -> str | None:
    if draft.template != "expense" or draft.sink != "expense":
        return None
    try:
        origin = ensure_erp(origin, autostart=autostart)
    except ErpUnavailable as exc:
        print(f"\n(ERP skipped) {exc}")
        print("Start the mock ERP:  bash examples/paris_expense_demo/web_mocks/start-web.sh\n")
        return None
    url = origin.rstrip("/") + "/expense.html"
    print(f"Agent: Filling the reimbursement form in the browser…\n  {url}\n")
    if open_browser:
        webbrowser.open(url)
    fill_draft(
        draft,
        origin=origin,
        pause_s=pause_s,
        recalled=draft.memory_snippets,
        cost_center=draft.cost_center,
    )
    draft.permissions.append(ERP_WRITE)
    print("ERP form is filled. Review / edit on the page, then Submit or Cancel.\n")
    return origin


def _finish_submit(draft, origin: str | None) -> int:
    labels = template_labels(draft.template)
    if not draft.lines:
        print("\nNo lines to submit (all source permissions may have been denied). Aborting.")
        return 1
    print(f"\nAgent: {labels['submit_verb']}\n")
    result, write_perm = submit_draft(draft)
    draft.permissions.append(write_perm)
    if origin:
        try:
            state = submit_erp(result, origin=origin)
            apply_erp_lines(draft, state)
        except ErpUnavailable as exc:
            print(f"(ERP page not updated) {exc}")
    print(render_submit_result(draft, result, write_perm))
    if origin:
        print(f" ERP page: {origin.rstrip('/')}/expense.html")
    return 0


def _approve_on_page(draft, origin: str) -> int:
    url = origin.rstrip("/") + "/expense.html"
    print(render_approval_card(draft))
    print(f"\nApprove on the ERP page (not this terminal):\n  {url}")
    print("You can edit a line, delete a line, or add a receipt, then Submit or Cancel.\n")
    try:
        state = wait_decision(origin)
    except KeyboardInterrupt:
        try:
            cancel_erp(origin)
        except ErpUnavailable:
            pass
        print("\nCancelled. Nothing was submitted.")
        return 1
    except ErpUnavailable as exc:
        print(f"\n(ERP wait failed) {exc}")
        return 1
    if state.get("status") != "submitted":
        print("\nCancelled on the ERP page. Nothing was submitted.")
        return 1
    apply_erp_lines(draft, state)
    print(render_approval_card(draft))
    return _finish_submit(draft, origin)


def run(
    case_id: str,
    auto_approve: str | None = None,
    *,
    plan_mode: str = "auto",
    auto_grant: bool = False,
    deny_sources: set[str] | None = None,
    skip_erp: bool = False,
    erp_origin: str = DEFAULT_ERP_ORIGIN,
    open_browser: bool = True,
    erp_autostart: bool = True,
    erp_pause_s: float = 0.28,
    cli_approve: bool = False,
) -> int:
    case = load_case(case_id)
    print(f"User: {case.query}\n")
    print("Agent: Recalling what I already know, then asking permission before each read…\n")

    draft = collect_case(
        case,
        plan_mode=plan_mode,
        auto_grant=auto_grant,
        deny_sources=deny_sources,
    )

    origin = None
    if not skip_erp and draft.lines:
        origin = _fill_erp(
            draft,
            origin=erp_origin,
            autostart=erp_autostart,
            open_browser=open_browser,
            pause_s=erp_pause_s,
        )

    use_page = bool(origin) and not cli_approve and auto_approve is None
    if use_page:
        return _approve_on_page(draft, origin)

    print(render_approval_card(draft))
    if not _prompt_approval(auto_approve):
        if origin:
            try:
                cancel_erp(origin)
            except ErpUnavailable:
                pass
        print("\nCancelled. Nothing was submitted.")
        return 1
    return _finish_submit(draft, origin)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Memory recall + collect + fill mock ERP + approve demo."
    )
    parser.add_argument(
        "--case",
        default="paris_expense",
        help=f"Case id under cases/ (available: {', '.join(list_case_ids())})",
    )
    parser.add_argument(
        "--list-cases",
        action="store_true",
        help="Print case ids and exit.",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print registered source adapters and exit.",
    )
    parser.add_argument(
        "--plan",
        choices=["auto", "heuristic", "llm", "fixed"],
        default="auto",
        help="Source plan: auto=LLM if API_KEY else heuristic; fixed=YAML sources.",
    )
    parser.add_argument(
        "--grant-all",
        action="store_true",
        help="Auto-allow all source read permissions.",
    )
    parser.add_argument(
        "--deny-sources",
        default="",
        help="Comma-separated sources to force-deny (demo missing receipts), e.g. photos,booking",
    )
    parser.add_argument("--yes", action="store_true", help="Auto-approve submit (implies grant-all).")
    parser.add_argument("--no", action="store_true", help="Auto-cancel submit (implies grant-all).")
    parser.add_argument(
        "--skip-erp",
        action="store_true",
        help="Do not fill the mock ERP page (CLI-only).",
    )
    parser.add_argument(
        "--cli-approve",
        action="store_true",
        help="Ask y/n in the terminal instead of the ERP page.",
    )
    parser.add_argument(
        "--erp-origin",
        default=DEFAULT_ERP_ORIGIN,
        help=f"Mock ERP origin (default {DEFAULT_ERP_ORIGIN}).",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not open the ERP page in a browser.",
    )
    parser.add_argument(
        "--no-autostart",
        action="store_true",
        help="Do not start the mock ERP server if it is down.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Fill the ERP form without the typing pause.",
    )
    args = parser.parse_args(argv)

    if args.list_cases:
        for case_id in list_case_ids():
            case = load_case(case_id)
            print(f"{case_id}\t{case.template}\t{case.query}")
        return 0
    if args.list_sources:
        for name in list_sources():
            print(f"{name}\t{source_label(name)}")
        return 0
    if args.yes and args.no:
        print("Use only one of --yes / --no.", file=sys.stderr)
        return 2

    auto = "y" if args.yes else ("n" if args.no else None)
    auto_grant = bool(args.grant_all or args.yes or args.no)
    deny = {s.strip() for s in args.deny_sources.split(",") if s.strip()}
    return run(
        args.case,
        auto_approve=auto,
        plan_mode=args.plan,
        auto_grant=auto_grant,
        deny_sources=deny,
        skip_erp=args.skip_erp,
        erp_origin=args.erp_origin,
        open_browser=not args.no_open,
        erp_autostart=not args.no_autostart,
        erp_pause_s=0.0 if args.fast else 0.28,
        cli_approve=args.cli_approve,
    )


if __name__ == "__main__":
    raise SystemExit(main())
