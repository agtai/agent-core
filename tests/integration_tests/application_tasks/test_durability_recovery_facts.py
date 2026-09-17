# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from dataclasses import replace

import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityProfileBinding,
)
from openjiuwen.core.application.tasks.durability.durability_recovery_facts import (
    ExecutorRecoveryFacts,
    ExecutorRecoveryFactsViolation,
)


def _scope() -> ScopeRef:
    return ScopeRef(
        subject_id="subject-用户",
        project_id="project-项目",
        session_id=None,
        assurance=Assurance.AUTHENTICATED,
    )


def _profile() -> DurabilityProfileBinding:
    return DurabilityProfileBinding(
        executor_id="jiuwenswarm_code_agent.project_code",
        adapter_id="direct-project-code-executor",
        profile_id="direct-profile",
        profile_version="profile.v1",
        profile_digest="1" * 64,
        durability_level="D1",
        durability_capability_version="recovery-facts.v1",
    )


def _facts() -> ExecutorRecoveryFacts:
    return ExecutorRecoveryFacts.create(
        scope=_scope(),
        task_id="task-1",
        producer_attempt_id="attempt-producer",
        candidate_recovery_attempt_id="attempt-candidate",
        profile=_profile(),
        recovery_generation=3,
        executor_epoch_id="epoch-一",
        executor_owner_generation=8,
        observed_at="2026-08-19T10:00:00Z",
        expires_at="2026-08-19T10:05:00Z",
        evidence_digest="2" * 64,
    )


def test_recovery_facts_roundtrip_and_expiry_are_canonical() -> None:
    facts = _facts()
    wire = facts.canonical_bytes()

    assert ExecutorRecoveryFacts.from_bytes(wire) == facts
    assert ExecutorRecoveryFacts.from_bytes(wire).canonical_bytes() == wire
    assert facts.is_expired(at="2026-08-19T10:04:59.999999999Z") is False
    assert facts.is_expired(at="2026-08-19T10:05:00Z") is True

    with pytest.raises(ExecutorRecoveryFactsViolation) as noncanonical_time:
        replace(facts, observed_at="2026-08-19T10:00:00.000Z")
    assert noncanonical_time.value.reason == "INVALID_RECOVERY_TIMESTAMP"


def test_recovery_facts_reject_same_attempt_expiry_and_forgery() -> None:
    facts = _facts()

    with pytest.raises(ExecutorRecoveryFactsViolation) as same_attempt:
        replace(
            facts,
            candidate_recovery_attempt_id=facts.producer_attempt_id,
        )
    assert same_attempt.value.reason == "RECOVERY_ATTEMPT_BINDING_INVALID"

    with pytest.raises(ExecutorRecoveryFactsViolation) as expiry:
        replace(facts, expires_at="2026-08-19T09:59:59Z")
    assert expiry.value.reason == "RECOVERY_EXPIRY_INVALID"

    forged = facts.canonical_bytes().replace(b'"recovery_generation":3', b'"recovery_generation":4')
    with pytest.raises(ExecutorRecoveryFactsViolation) as forged_error:
        ExecutorRecoveryFacts.from_bytes(forged)
    assert forged_error.value.reason == "RECOVERY_FACTS_DIGEST_MISMATCH"


def test_recovery_facts_are_authority_false() -> None:
    facts = _facts()

    assert facts.recovery_authority is False
    assert facts.lease_authority is False
    assert facts.checkpoint_resume_authority is False
    assert facts.executor_invocation_authority is False
    assert facts.task_mutation_authority is False
    assert facts.quiescence_authority is False


def test_recovery_facts_reject_invalid_unicode_and_unsafe_generation() -> None:
    with pytest.raises(ExecutorRecoveryFactsViolation) as unicode_error:
        ExecutorRecoveryFacts.create(
            scope=_scope(),
            task_id="task-1",
            producer_attempt_id="attempt-producer",
            candidate_recovery_attempt_id="attempt-candidate",
            profile=_profile(),
            recovery_generation=3,
            executor_epoch_id="epoch-\ud800",
            executor_owner_generation=8,
            observed_at="2026-08-19T10:00:00Z",
            expires_at="2026-08-19T10:05:00Z",
            evidence_digest="2" * 64,
        )
    assert unicode_error.value.reason == "INVALID_RECOVERY_TEXT"

    with pytest.raises(ExecutorRecoveryFactsViolation) as generation_error:
        replace(_facts(), recovery_generation=-1)
    assert generation_error.value.reason == "INVALID_RECOVERY_INTEGER"
