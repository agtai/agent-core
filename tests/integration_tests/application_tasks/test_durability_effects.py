# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from dataclasses import replace

import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.durability.durability_effects import (
    EffectDispatchReceipt,
    EffectObservationKind,
    EffectReconciliationKind,
    EffectSettlementKind,
    ExternalEffectBinding,
    ExternalEffectContractViolation,
    ExternalEffectDispatch,
    ExternalEffectIntent,
    ExternalEffectObservation,
    ExternalEffectSettlement,
    decide_effect_reconciliation,
    effect_fact_bytes,
    effect_fact_from_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityProfileBinding,
)


def _scope(*, subject_id: str = "subject-用户") -> ScopeRef:
    return ScopeRef(
        subject_id=subject_id,
        project_id="project-项目",
        session_id=None,
        assurance=Assurance.AUTHENTICATED,
    )


def _profile(*, profile_id: str = "direct-profile") -> DurabilityProfileBinding:
    return DurabilityProfileBinding(
        executor_id="jiuwenswarm_code_agent.project_code",
        adapter_id="direct-project-code-executor",
        profile_id=profile_id,
        profile_version="profile.v1",
        profile_digest="1" * 64,
        durability_level="D2",
        durability_capability_version="d2.v1",
    )


def _binding(*, task_id: str = "task-1") -> ExternalEffectBinding:
    return ExternalEffectBinding(
        scope=_scope(),
        task_id=task_id,
        origin_attempt_id="attempt-1",
        profile=_profile(),
        effect_id="effect-1",
        operation_kind="tool.write",
        operation_ordinal=1,
        target_digest="2" * 64,
        intended_effect_digest="3" * 64,
    )


def _intent(*, replay_safe: bool = False) -> ExternalEffectIntent:
    return ExternalEffectIntent(binding=_binding(), replay_safe=replay_safe)


def _receipt() -> EffectDispatchReceipt:
    return EffectDispatchReceipt(
        binding=_binding(),
        actor_attempt_id="attempt-1",
        dispatch_ordinal=1,
        recovery_generation=0,
        provider_operation_key="provider-effect-1",
        accepted=True,
        receipt_digest="4" * 64,
    )


def _observation(kind: EffectObservationKind) -> ExternalEffectObservation:
    return ExternalEffectObservation(
        binding=_binding(),
        actor_attempt_id="attempt-1",
        observation_ordinal=1,
        dispatch_ordinal=1,
        recovery_generation=0,
        kind=kind,
        evidence_digest="5" * 64,
    )


@pytest.mark.parametrize(
    ("replay_safe", "observation", "manual_required", "expected"),
    [
        (
            False,
            EffectObservationKind.NO_EFFECT,
            False,
            EffectReconciliationKind.NO_EFFECT,
        ),
        (
            True,
            EffectObservationKind.NO_EFFECT,
            False,
            EffectReconciliationKind.SAFELY_RETRYABLE,
        ),
        (False, EffectObservationKind.APPLIED, False, EffectReconciliationKind.APPLIED),
        (False, EffectObservationKind.UNKNOWN, False, EffectReconciliationKind.UNKNOWN),
        (
            False,
            EffectObservationKind.UNKNOWN,
            True,
            EffectReconciliationKind.MANUAL_REQUIRED,
        ),
    ],
)
def test_reconciliation_is_a_pure_closed_decision(
    replay_safe: bool,
    observation: EffectObservationKind,
    manual_required: bool,
    expected: EffectReconciliationKind,
) -> None:
    decision = decide_effect_reconciliation(
        intent=_intent(replay_safe=replay_safe),
        receipt=_receipt(),
        observations=(_observation(observation),),
        manual_required=manual_required,
    )

    assert decision.kind is expected
    assert decision.external_call_authority is False
    assert decision.compensation_authority is False
    assert decision.task_mutation_authority is False
    assert decision.settlement_authority is False


def test_effect_facts_roundtrip_with_canonical_parity() -> None:
    facts = (
        _intent(replay_safe=True),
        ExternalEffectDispatch(
            binding=_binding(),
            actor_attempt_id="attempt-1",
            dispatch_ordinal=1,
            recovery_generation=0,
            provider_operation_key="provider-effect-1",
        ),
        _receipt(),
        _observation(EffectObservationKind.APPLIED),
        ExternalEffectSettlement(
            binding=_binding(),
            actor_attempt_id="attempt-1",
            settlement_ordinal=1,
            recovery_generation=0,
            kind=EffectSettlementKind.RESOLVED,
            evidence_head=1,
            evidence_digest="6" * 64,
        ),
    )

    for fact in facts:
        wire = effect_fact_bytes(fact)
        assert effect_fact_from_bytes(wire) == fact
        assert effect_fact_bytes(effect_fact_from_bytes(wire)) == wire


def test_effect_facts_reject_wrong_binding_and_forgery() -> None:
    wrong_task_receipt = replace(_receipt(), binding=_binding(task_id="task-2"))
    with pytest.raises(ExternalEffectContractViolation) as mismatch:
        decide_effect_reconciliation(
            intent=_intent(),
            receipt=wrong_task_receipt,
            observations=(),
            manual_required=False,
        )
    assert mismatch.value.reason == "EFFECT_BINDING_MISMATCH"

    forged = effect_fact_bytes(_observation(EffectObservationKind.APPLIED)).replace(b'"applied"', b'"unknown"')
    with pytest.raises(ExternalEffectContractViolation) as corrupt:
        effect_fact_from_bytes(forged)
    assert corrupt.value.reason == "EFFECT_FACT_DIGEST_MISMATCH"


def test_effect_identity_is_unicode_exact_and_bounded() -> None:
    binding = replace(_binding(), operation_kind="工具.写入")
    assert ExternalEffectBinding.from_dict(binding.to_dict()) == binding

    with pytest.raises(ExternalEffectContractViolation) as surrogate:
        replace(binding, operation_kind="tool.\ud800")
    assert surrogate.value.reason == "INVALID_EFFECT_TEXT"

    with pytest.raises(ExternalEffectContractViolation) as oversized:
        replace(binding, effect_id="e" * 513)
    assert oversized.value.reason == "INVALID_EFFECT_TEXT"


def test_intent_without_observation_remains_unknown() -> None:
    decision = decide_effect_reconciliation(
        intent=_intent(replay_safe=True),
        receipt=None,
        observations=(),
        manual_required=False,
    )

    assert decision.kind is EffectReconciliationKind.UNKNOWN


def test_effect_facts_cannot_acquire_forged_runtime_authority() -> None:
    intent = _intent()

    assert not hasattr(intent, "__dict__")
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(intent, "runtime_authority", True)
