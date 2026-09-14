# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Logging an execution error must not require copying that exception."""

import pytest

from openjiuwen.core.common.logging.events import BaseLogEvent
from openjiuwen.core.runner.callback.errors import AbortError


class UncopyableError(Exception):
    def __deepcopy__(self, _memo):
        raise TypeError("exception cannot be copied")


@pytest.mark.parametrize("error", [AbortError("rejected"), UncopyableError("rejected")])
def test_error_serialization_preserves_exception_and_original_metadata(error):
    event = BaseLogEvent(exception=error, metadata={"values": [1]},
                         error_code="explicit", error_message="retained")
    payload = event.to_dict()
    assert payload["exception"] == str(error)
    assert payload["error_code"] == "explicit"
    assert payload["error_message"] == "retained"
    payload["metadata"]["values"].append(2)
    assert event.metadata == {"values": [1]}
    assert event.exception is error
