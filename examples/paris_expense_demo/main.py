# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""
Multi-source collect → merge → approve → submit (fixed orchestration).

A1–A5 + B1–B3: case YAML, source registry, Evidence/DraftLine, templates,
merge rules; LLM/heuristic source plan; permission interrupt; policy tips.
"""

from __future__ import annotations

import argparse
import sys

from .approval import render_approval_card, render_submit_result
from .config import list_case_ids, load_case, source_label, template_labels
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


def run(
    case_id: str,
    auto_approve: str | None = None,
    *,
    plan_mode: str = "auto",
    auto_grant: bool = False,
    deny_sources: set[str] | None = None,
) -> int:
    case = load_case(case_id)
    labels = template_labels(case.template)
    print(f"User: {case.query}\n")
    print("Agent: Planning which sources to open, and asking permission before each read…\n")

    draft = collect_case(
        case,
        plan_mode=plan_mode,
        auto_grant=auto_grant,
        deny_sources=deny_sources,
    )
    print(render_approval_card(draft))

    if not _prompt_approval(auto_approve):
        print("\nCancelled. Nothing was submitted.")
        return 1

    if not draft.lines:
        print("\nNo lines to submit (all source permissions may have been denied). Aborting.")
        return 1

    print(f"\nAgent: {labels['submit_verb']}\n")
    result, write_perm = submit_draft(draft)
    draft.permissions.append(write_perm)
    print(render_submit_result(draft, result, write_perm))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Multi-source collect / plan / permission / policy / approve demo."
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
    )


if __name__ == "__main__":
    raise SystemExit(main())
