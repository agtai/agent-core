# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Terminal permission interrupt before reading a mock source."""

from __future__ import annotations

from .config import source_label
from .models import PermissionUsed


def ask_source_permission(
    permission: PermissionUsed,
    *,
    auto_grant: bool = False,
    forced_deny: set[str] | None = None,
) -> bool:
    """
    Ask the user before calling a sensitive source adapter.

    Returns True if granted. ``forced_deny`` simulates user rejecting a source
    (useful with --yes to demo missing-receipt tips).
    """
    forced_deny = forced_deny or set()
    label = source_label(permission.source)
    header = (
        f"\n── Permission check ──────────────────────\n"
        f"  Source:  {label} ({permission.source})\n"
        f"  Scope:   {permission.scope}\n"
        f"  Purpose: {permission.purpose}\n"
        f"────────────────────────────────────────"
    )
    print(header)

    if permission.source in forced_deny:
        print(f"(forced deny) Denied access to «{label}».\n")
        return False

    if auto_grant:
        print(f"(auto grant) Allowed access to «{label}».\n")
        return True

    try:
        answer = input(f"Allow reading «{label}»? [y/n]: ").strip().lower()
    except EOFError:
        print("No input; treating as deny.\n")
        return False

    granted = answer in {"y", "yes"}
    print(("Allowed.\n" if granted else "Denied; skipping this source.\n"))
    return granted
