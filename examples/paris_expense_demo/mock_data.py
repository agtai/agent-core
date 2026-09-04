# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Back-compat shim: Paris fixtures now live in cases/paris_expense.yaml."""

from __future__ import annotations

from .config import load_case

_case = load_case("paris_expense")

PARIS_TRIP = {
    "trip_id": _case.case_id,
    "trip_title": _case.title,
    "traveler": _case.actor,
    "query": _case.query,
}

EMAIL_RECEIPTS = list(_case.fixtures.get("email") or [])
PHOTO_RECEIPTS = list(_case.fixtures.get("photos") or [])
BOOKING_HOTEL = dict(_case.fixtures.get("booking") or {})
RIDE_TRIPS = list(_case.fixtures.get("rides") or [])
