# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Mock source adapters — read fixtures from CaseConfig."""

from __future__ import annotations

from typing import Any

from .models import CaseConfig, EvidenceItem, PermissionUsed
from .registry import register_source

# Metadata used for permission prompts *before* reading fixtures.
SOURCE_PERMISSION_META: dict[str, PermissionUsed] = {
    "email": PermissionUsed(
        source="email",
        scope="read:inbox.receipts",
        purpose="Find trip receipts and messages",
    ),
    "photos": PermissionUsed(
        source="photos",
        scope="read:library.camera",
        purpose="Locate photographed / album assets",
    ),
    "booking": PermissionUsed(
        source="booking",
        scope="read:reservations.hotel",
        purpose="Attach lodging / booking records",
    ),
    "rides": PermissionUsed(
        source="rides",
        scope="read:trips.history",
        purpose="Include ride / transfer history",
    ),
    "browser": PermissionUsed(
        source="browser",
        scope="read:browser.tabs",
        purpose="Capture page content from the browser",
    ),
    "db": PermissionUsed(
        source="db",
        scope="read:finance.transactions",
        purpose="Pull ledger / database rows",
    ),
}


def permission_meta(name: str) -> PermissionUsed:
    if name not in SOURCE_PERMISSION_META:
        return PermissionUsed(source=name, scope=f"read:{name}", purpose=f"Read {name}")
    return SOURCE_PERMISSION_META[name]


def _rows(case: CaseConfig, key: str) -> list[dict[str, Any]]:
    raw = case.fixtures.get(key)
    if raw is None:
        return []
    if isinstance(raw, list):
        return [dict(x) for x in raw]
    if isinstance(raw, dict):
        return [dict(raw)]
    return []


@register_source("email")
def source_email(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = [
        EvidenceItem(
            item_id=f"email:{row['message_id']}",
            source="email",
            date=str(row["date"]),
            description=str(row["description"]),
            evidence_ref=str(row.get("attachment") or row["message_id"]),
            amount=float(row["amount"]) if row.get("amount") is not None else None,
            currency=row.get("currency"),
            category=row.get("category"),
        )
        for row in _rows(case, "email")
    ]
    return items, permission_meta("email")


@register_source("photos")
def source_photos(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = [
        EvidenceItem(
            item_id=f"photos:{row['asset_id']}",
            source="photos",
            date=str(row["date"]),
            description=str(row["description"]),
            evidence_ref=str(row["asset_id"]),
            amount=float(row["amount"]) if row.get("amount") is not None else None,
            currency=row.get("currency"),
            category=row.get("category"),
        )
        for row in _rows(case, "photos")
    ]
    return items, permission_meta("photos")


@register_source("booking")
def source_booking(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = []
    for row in _rows(case, "booking"):
        items.append(
            EvidenceItem(
                item_id=f"booking:{row['confirmation']}",
                source="booking",
                date=str(row.get("check_in") or row.get("date")),
                description=str(row["description"]),
                evidence_ref=str(row["confirmation"]),
                amount=float(row["amount"]) if row.get("amount") is not None else None,
                currency=row.get("currency"),
                category=row.get("category"),
            )
        )
    return items, permission_meta("booking")


@register_source("rides")
def source_rides(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = [
        EvidenceItem(
            item_id=f"rides:{row['ride_id']}",
            source="rides",
            date=str(row["date"]),
            description=str(row["description"]),
            evidence_ref=str(row["ride_id"]),
            amount=float(row["amount"]) if row.get("amount") is not None else None,
            currency=row.get("currency"),
            category=row.get("category"),
        )
        for row in _rows(case, "rides")
    ]
    return items, permission_meta("rides")


@register_source("browser")
def source_browser(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = [
        EvidenceItem(
            item_id=f"browser:{row['page_id']}",
            source="browser",
            date=str(row["date"]),
            description=str(row["description"]),
            evidence_ref=str(row.get("evidence_ref") or row.get("url") or row["page_id"]),
            amount=float(row["amount"]) if row.get("amount") is not None else None,
            currency=row.get("currency"),
            category=row.get("category"),
            extra={"url": row.get("url")},
        )
        for row in _rows(case, "browser")
    ]
    return items, permission_meta("browser")


@register_source("db")
def source_db(case: CaseConfig) -> tuple[list[EvidenceItem], PermissionUsed]:
    items = [
        EvidenceItem(
            item_id=f"db:{row['row_id']}",
            source="db",
            date=str(row["date"]),
            description=str(row["description"]),
            evidence_ref=str(row.get("table") or row["row_id"]),
            amount=float(row["amount"]) if row.get("amount") is not None else None,
            currency=row.get("currency"),
            category=row.get("category"),
        )
        for row in _rows(case, "db")
    ]
    return items, permission_meta("db")
