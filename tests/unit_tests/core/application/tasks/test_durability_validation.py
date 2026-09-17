# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Preserve domain error contracts while sharing pure identity validation."""

from dataclasses import replace
from importlib import import_module

import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.durability.durability_identity import DurabilityProfileBinding

DOMAINS = [
    ("identity", "DurabilityIdentityViolation", "INVALID_DURABILITY_PROFILE", None, None, None, None),
    (
        "checkpoint",
        "DurabilityCheckpointViolation",
        "INVALID_DURABILITY_TEXT",
        "INVALID_DURABILITY_SCOPE",
        "checkpoint scope",
        "INVALID_DURABILITY_PROFILE",
        "checkpoint profile binding",
    ),
    (
        "effects",
        "ExternalEffectContractViolation",
        "INVALID_EFFECT_TEXT",
        "INVALID_EFFECT_SCOPE",
        "effect scope",
        "INVALID_EFFECT_PROFILE",
        "effect profile binding",
    ),
    (
        "readers",
        "DurabilityPrefixViolation",
        "INVALID_DURABILITY_BINDING",
        "INVALID_DURABILITY_BINDING",
        "durability read scope",
        "INVALID_DURABILITY_BINDING",
        "durability read profile",
    ),
    (
        "recovery_facts",
        "ExecutorRecoveryFactsViolation",
        "INVALID_RECOVERY_TEXT",
        "INVALID_RECOVERY_SCOPE",
        "recovery facts scope",
        "INVALID_RECOVERY_PROFILE",
        "recovery profile binding",
    ),
]


class StringSubclass(str):
    pass


def assert_error(operation, error_type, reason, message, cause_type=None):
    with pytest.raises(error_type) as caught:
        operation()
    assert type(caught.value) is error_type
    assert caught.value.reason == reason
    assert str(caught.value) == message
    if cause_type is None:
        assert caught.value.__cause__ is None
    else:
        assert isinstance(caught.value.__cause__, cause_type)


@pytest.mark.parametrize("domain", DOMAINS, ids=lambda item: item[0])
def test_text_domain_contract_is_exact_and_bounded(domain):
    name, error_name, reason, *_ = domain
    module = import_module(f"openjiuwen.core.application.tasks.durability.durability_{name}")
    error_type = getattr(module, error_name)
    for value in ("a", "a" * 512, "é" * 256, " a ", "a\x00b"):
        assert module._text(value, "field") == value
    for value in (None, 1, True, b"a", "", " \t", StringSubclass("a")):
        assert_error(lambda: module._text(value, "field"), error_type, reason, "field must be a non-empty exact string")
    for value in ("a" * 513, "é" * 257):
        assert_error(lambda: module._text(value, "field"), error_type, reason, "field is outside the bounded range")
    assert_error(
        lambda: module._text("\ud800", "field"),
        error_type,
        reason,
        "field must contain valid Unicode scalar values",
        UnicodeEncodeError,
    )


@pytest.mark.parametrize("domain", DOMAINS[1:], ids=lambda item: item[0])
def test_scope_and_profile_revalidation_preserve_domain_errors(domain):
    name, error_name, _, scope_reason, scope_label, profile_reason, profile_label = domain
    module = import_module(f"openjiuwen.core.application.tasks.durability.durability_{name}")
    error_type = getattr(module, error_name)
    scope = ScopeRef("subject", "project", None, Assurance.AUTHENTICATED)
    assert module._scope(scope) == scope
    assert_error(lambda: module._scope(scope.to_dict()), error_type, scope_reason, f"{scope_label} must be exact")
    unauthenticated = replace(scope, assurance=Assurance.REQUEST_ASSERTED)
    assert_error(
        lambda: module._scope(unauthenticated), error_type, scope_reason, f"{scope_label} must be authenticated"
    )
    corrupt_scope = replace(scope)
    object.__setattr__(corrupt_scope, "subject_id", None)
    assert_error(
        lambda: module._scope(corrupt_scope), error_type, scope_reason, f"{scope_label} is invalid", ValueError
    )
    profile = DurabilityProfileBinding("executor", "adapter", "profile", "v1", "a" * 64, "D1", "v1")
    assert module._profile(profile) == profile
    assert_error(
        lambda: module._profile(profile.to_dict()), error_type, profile_reason, f"{profile_label} must be exact"
    )
    object.__setattr__(profile, "profile_digest", "bad")
    assert_error(
        lambda: module._profile(profile), error_type, profile_reason, f"{profile_label} is invalid", ValueError
    )
