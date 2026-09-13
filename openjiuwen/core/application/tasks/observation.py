# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bounded application execution observation cursors."""

MAX_OBSERVATION_WAIT_MS = 1000


def observation_cursor(value):
    """Validate the existing wire cursor without accepting coercions."""
    if value is None:
        return None
    if (type(value) is not dict or set(value) != {"epoch", "sequence", "read_sequence"}
            or type(value["epoch"]) is not str or len(value["epoch"]) != 32
            or any(c not in "0123456789abcdef" for c in value["epoch"])
            or any(type(value[k]) is not int or not 0 <= value[k] <= 9_007_199_254_740_991
                   for k in ("sequence", "read_sequence"))):
        raise ValueError("invalid Native observation cursor")
    return dict(value)
