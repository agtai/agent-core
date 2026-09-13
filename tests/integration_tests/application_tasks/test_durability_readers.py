# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import builtins
import hashlib
import socket
import sqlite3
import subprocess
from dataclasses import replace

import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.durability.durability_checkpoint import D1Checkpoint
from openjiuwen.core.application.tasks.durability.durability_effects import (
    EffectDispatchReceipt,
    EffectFact,
    EffectObservationKind,
    ExternalEffectBinding,
    ExternalEffectDispatch,
    ExternalEffectIntent,
    ExternalEffectObservation,
    effect_fact_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityProfileBinding,
)
from openjiuwen.core.application.tasks.durability.durability_readers import (
    MAX_DURABILITY_PREFIX_ITEM_BYTES,
    MAX_DURABILITY_PREFIX_ROWS,
    CheckpointPrefixRow,
    DurabilityPrefixViolation,
    DurabilityReadBinding,
    EffectPrefixRow,
    verify_checkpoint_prefix,
    verify_effect_prefix,
)


def _scope(*, subject_id: str = "subject-1") -> ScopeRef:
    return ScopeRef(
        subject_id=subject_id,
        project_id="project-1",
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
        durability_capability_version="d1-d2.v1",
    )


def _read_binding(
    *,
    scope: ScopeRef | None = None,
    task_id: str = "task-1",
    attempt_id: str = "attempt-1",
    profile: DurabilityProfileBinding | None = None,
) -> DurabilityReadBinding:
    return DurabilityReadBinding(
        scope=scope or _scope(),
        task_id=task_id,
        origin_attempt_id=attempt_id,
        profile=profile or _profile(),
    )


def _checkpoint(
    *,
    task_id: str = "task-1",
    checkpoint_id: str = "checkpoint-1",
    checkpoint_sequence: int = 1,
) -> D1Checkpoint:
    return D1Checkpoint.create(
        checkpoint_id=checkpoint_id,
        scope=_scope(),
        task_id=task_id,
        producer_attempt_id="attempt-1",
        checkpoint_sequence=checkpoint_sequence,
        recovery_generation=0,
        profile=_profile(),
        complete=True,
        task_spec_digest="4" * 64,
        context_version="context.v1",
        context_digest="2" * 64,
        input_digest="3" * 64,
        state_schema_id="agent-state",
        state_schema_version=1,
        state_bytes=b"state",
        effect_head=0,
        effect_prefix_digest="5" * 64,
    )


def _checkpoint_row(
    *,
    row_sequence: int = 1,
    task_id: str = "task-1",
    checkpoint: D1Checkpoint | None = None,
) -> CheckpointPrefixRow:
    wire = (checkpoint or _checkpoint(task_id=task_id)).canonical_bytes()
    return CheckpointPrefixRow(
        row_sequence=row_sequence,
        binding=_read_binding(task_id=task_id),
        canonical_bytes=wire,
        payload_digest=hashlib.sha256(wire).hexdigest(),
    )


def _effect_binding(
    *,
    task_id: str = "task-1",
    effect_id: str = "effect-1",
    operation_ordinal: int = 1,
) -> ExternalEffectBinding:
    return ExternalEffectBinding(
        scope=_scope(),
        task_id=task_id,
        origin_attempt_id="attempt-1",
        profile=_profile(),
        effect_id=effect_id,
        operation_kind="tool.write",
        operation_ordinal=operation_ordinal,
        target_digest="4" * 64,
        intended_effect_digest="5" * 64,
    )


def _effect_row(
    *,
    row_sequence: int = 1,
    task_id: str = "task-1",
    fact: EffectFact | None = None,
) -> EffectPrefixRow:
    fact = fact or ExternalEffectIntent(
        binding=_effect_binding(task_id=task_id),
        replay_safe=False,
    )
    wire = effect_fact_bytes(fact)
    return EffectPrefixRow(
        row_sequence=row_sequence,
        binding=_read_binding(task_id=task_id),
        canonical_bytes=wire,
        payload_digest=hashlib.sha256(wire).hexdigest(),
    )


def test_checkpoint_reader_accepts_exact_replay_and_rejects_changed_fact() -> None:
    row = _checkpoint_row()
    verified = verify_checkpoint_prefix(
        (row, row),
        expected_binding=_read_binding(),
        expected_head=1,
    )

    assert len(verified.records) == 1
    assert verified.records[0] == _checkpoint()
    assert (
        verify_checkpoint_prefix(
            (row,),
            expected_binding=_read_binding(),
            expected_head=1,
            expected_prefix_digest=verified.prefix_digest,
        )
        == verified
    )

    changed_wire = _checkpoint(task_id="task-2").canonical_bytes()
    conflict = replace(
        row,
        canonical_bytes=changed_wire,
        payload_digest=hashlib.sha256(changed_wire).hexdigest(),
    )
    with pytest.raises(DurabilityPrefixViolation) as changed:
        verify_checkpoint_prefix(
            (row, conflict),
            expected_binding=_read_binding(),
            expected_head=1,
        )
    assert changed.value.reason == "DURABILITY_PREFIX_CONFLICT"


def test_readers_deduplicate_exact_semantic_replay_and_reject_changed_fact() -> None:
    checkpoint_first = _checkpoint_row(row_sequence=1)
    checkpoint_replay = _checkpoint_row(row_sequence=2)
    checkpoint_prefix = verify_checkpoint_prefix(
        (checkpoint_first, checkpoint_replay),
        expected_binding=_read_binding(),
        expected_head=2,
    )
    assert len(checkpoint_prefix.records) == 1

    changed_checkpoint = D1Checkpoint.create(
        checkpoint_id="checkpoint-changed",
        scope=_scope(),
        task_id="task-1",
        producer_attempt_id="attempt-1",
        checkpoint_sequence=1,
        recovery_generation=0,
        profile=_profile(),
        complete=True,
        task_spec_digest="4" * 64,
        context_version="context.v1",
        context_digest="2" * 64,
        input_digest="3" * 64,
        state_schema_id="agent-state",
        state_schema_version=1,
        state_bytes=b"state",
        effect_head=0,
        effect_prefix_digest="5" * 64,
    )
    with pytest.raises(DurabilityPrefixViolation) as checkpoint_conflict:
        verify_checkpoint_prefix(
            (
                checkpoint_first,
                _checkpoint_row(row_sequence=2, checkpoint=changed_checkpoint),
            ),
            expected_binding=_read_binding(),
            expected_head=2,
        )
    assert checkpoint_conflict.value.reason == "DURABILITY_PREFIX_CONFLICT"

    intent = ExternalEffectIntent(binding=_effect_binding(), replay_safe=False)
    effect_first = _effect_row(row_sequence=1, fact=intent)
    effect_replay = _effect_row(row_sequence=2, fact=intent)
    effect_prefix = verify_effect_prefix(
        (effect_first, effect_replay),
        expected_binding=_read_binding(),
        expected_head=2,
    )
    assert len(effect_prefix.records) == 1

    changed_intent = replace(intent, replay_safe=True)
    with pytest.raises(DurabilityPrefixViolation) as effect_conflict:
        verify_effect_prefix(
            (
                effect_first,
                _effect_row(row_sequence=2, fact=changed_intent),
            ),
            expected_binding=_read_binding(),
            expected_head=2,
        )
    assert effect_conflict.value.reason == "DURABILITY_PREFIX_CONFLICT"


def test_checkpoint_reader_rejects_descending_checkpoint_sequence() -> None:
    initial = _checkpoint(
        checkpoint_id="checkpoint-0",
        checkpoint_sequence=0,
    )
    assert verify_checkpoint_prefix(
        (_checkpoint_row(row_sequence=1, checkpoint=initial),),
        expected_binding=_read_binding(),
        expected_head=1,
    ).records == (initial,)

    newest = _checkpoint(
        checkpoint_id="checkpoint-2",
        checkpoint_sequence=2,
    )
    older = _checkpoint(
        checkpoint_id="checkpoint-1",
        checkpoint_sequence=1,
    )

    with pytest.raises(DurabilityPrefixViolation) as descending:
        verify_checkpoint_prefix(
            (
                _checkpoint_row(row_sequence=1, checkpoint=newest),
                _checkpoint_row(row_sequence=2, checkpoint=older),
            ),
            expected_binding=_read_binding(),
            expected_head=2,
        )
    assert descending.value.reason == "DURABILITY_PREFIX_CORRUPT"


def test_effect_reader_requires_intent_and_scopes_ordinals_to_effect_identity() -> None:
    first_binding = _effect_binding(effect_id="effect-1", operation_ordinal=1)
    second_binding = _effect_binding(effect_id="effect-2", operation_ordinal=2)
    first_intent = ExternalEffectIntent(binding=first_binding, replay_safe=False)
    first_dispatch = ExternalEffectDispatch(
        binding=first_binding,
        actor_attempt_id="attempt-1",
        dispatch_ordinal=1,
        recovery_generation=0,
        provider_operation_key="provider-key-1",
    )
    first_receipt = EffectDispatchReceipt(
        binding=first_binding,
        actor_attempt_id="attempt-1",
        dispatch_ordinal=1,
        recovery_generation=0,
        provider_operation_key="provider-key-1",
        accepted=True,
        receipt_digest="6" * 64,
    )
    first_observation = ExternalEffectObservation(
        binding=_effect_binding(),
        actor_attempt_id="attempt-1",
        observation_ordinal=1,
        dispatch_ordinal=1,
        recovery_generation=0,
        kind=EffectObservationKind.APPLIED,
        evidence_digest="7" * 64,
    )
    second_intent = ExternalEffectIntent(binding=second_binding, replay_safe=True)
    second_dispatch = ExternalEffectDispatch(
        binding=second_binding,
        actor_attempt_id="attempt-1",
        dispatch_ordinal=1,
        recovery_generation=0,
        provider_operation_key="provider-key-2",
    )
    second_receipt = EffectDispatchReceipt(
        binding=second_binding,
        actor_attempt_id="attempt-1",
        dispatch_ordinal=1,
        recovery_generation=0,
        provider_operation_key="provider-key-2",
        accepted=True,
        receipt_digest="8" * 64,
    )
    second_observation = ExternalEffectObservation(
        binding=second_binding,
        actor_attempt_id="attempt-1",
        observation_ordinal=1,
        dispatch_ordinal=1,
        recovery_generation=0,
        kind=EffectObservationKind.NO_EFFECT,
        evidence_digest="9" * 64,
    )
    effect_prefix = verify_effect_prefix(
        (
            _effect_row(row_sequence=1, fact=first_intent),
            _effect_row(row_sequence=2, fact=first_dispatch),
            _effect_row(row_sequence=3, fact=first_receipt),
            _effect_row(row_sequence=4, fact=first_observation),
            _effect_row(row_sequence=5, fact=second_intent),
            _effect_row(row_sequence=6, fact=second_dispatch),
            _effect_row(row_sequence=7, fact=second_receipt),
            _effect_row(row_sequence=8, fact=second_observation),
        ),
        expected_binding=_read_binding(),
        expected_head=8,
    )
    assert len(effect_prefix.records) == 8

    with pytest.raises(DurabilityPrefixViolation) as missing_intent:
        verify_effect_prefix(
            (_effect_row(row_sequence=1, fact=first_observation),),
            expected_binding=_read_binding(),
            expected_head=1,
        )
    assert missing_intent.value.reason == "DURABILITY_PREFIX_CORRUPT"

    with pytest.raises(DurabilityPrefixViolation) as orphan_receipt:
        verify_effect_prefix(
            (_effect_row(row_sequence=1, fact=first_receipt),),
            expected_binding=_read_binding(),
            expected_head=1,
        )
    assert orphan_receipt.value.reason == "DURABILITY_PREFIX_CORRUPT"

    with pytest.raises(DurabilityPrefixViolation) as missing_receipt:
        verify_effect_prefix(
            (
                _effect_row(row_sequence=1, fact=first_intent),
                _effect_row(row_sequence=2, fact=first_observation),
            ),
            expected_binding=_read_binding(),
            expected_head=2,
        )
    assert missing_receipt.value.reason == "DURABILITY_PREFIX_CORRUPT"

    changed_binding = replace(first_binding, intended_effect_digest="9" * 64)
    changed_receipt = replace(first_receipt, binding=changed_binding)
    with pytest.raises(DurabilityPrefixViolation) as changed_identity:
        verify_effect_prefix(
            (
                _effect_row(row_sequence=1, fact=first_intent),
                _effect_row(row_sequence=2, fact=changed_receipt),
            ),
            expected_binding=_read_binding(),
            expected_head=2,
        )
    assert changed_identity.value.reason == "DURABILITY_PREFIX_CONFLICT"

    later_receipt = replace(first_receipt, dispatch_ordinal=2, receipt_digest="a" * 64)
    with pytest.raises(DurabilityPrefixViolation) as descending_receipt:
        verify_effect_prefix(
            (
                _effect_row(row_sequence=1, fact=first_intent),
                _effect_row(row_sequence=2, fact=later_receipt),
                _effect_row(row_sequence=3, fact=first_receipt),
            ),
            expected_binding=_read_binding(),
            expected_head=3,
        )
    assert descending_receipt.value.reason == "DURABILITY_PREFIX_CORRUPT"

    later_observation = replace(
        first_observation,
        observation_ordinal=2,
        evidence_digest="b" * 64,
    )
    with pytest.raises(DurabilityPrefixViolation) as descending_observation:
        verify_effect_prefix(
            (
                _effect_row(row_sequence=1, fact=first_intent),
                _effect_row(row_sequence=2, fact=first_receipt),
                _effect_row(row_sequence=3, fact=later_observation),
                _effect_row(row_sequence=4, fact=first_observation),
            ),
            expected_binding=_read_binding(),
            expected_head=4,
        )
    assert descending_observation.value.reason == "DURABILITY_PREFIX_CORRUPT"


@pytest.mark.parametrize(
    "expected_binding",
    [
        _read_binding(scope=_scope(subject_id="subject-2")),
        _read_binding(task_id="task-2"),
        _read_binding(attempt_id="attempt-2"),
        _read_binding(profile=_profile(profile_id="legacy-profile")),
    ],
)
def test_checkpoint_reader_rejects_wrong_scope_task_attempt_or_profile(
    expected_binding: DurabilityReadBinding,
) -> None:
    with pytest.raises(DurabilityPrefixViolation) as mismatch:
        verify_checkpoint_prefix(
            (_checkpoint_row(),),
            expected_binding=expected_binding,
            expected_head=1,
        )
    assert mismatch.value.reason == "DURABILITY_BINDING_MISMATCH"


def test_readers_reject_stale_corrupt_oversized_and_partial_prefixes() -> None:
    checkpoint_row = _checkpoint_row()
    effect_row = _effect_row()

    for reader, row in (
        (verify_checkpoint_prefix, checkpoint_row),
        (verify_effect_prefix, effect_row),
    ):
        with pytest.raises(DurabilityPrefixViolation) as oversized_head:
            reader(
                (),
                expected_binding=_read_binding(),
                expected_head=MAX_DURABILITY_PREFIX_ROWS + 1,
            )
        assert oversized_head.value.reason == "DURABILITY_PREFIX_OUT_OF_BOUNDS"

        with pytest.raises(DurabilityPrefixViolation) as stale_head:
            reader((row,), expected_binding=_read_binding(), expected_head=2)
        assert stale_head.value.reason == "DURABILITY_PREFIX_PARTIAL"

        verified = reader((row,), expected_binding=_read_binding(), expected_head=1)
        with pytest.raises(DurabilityPrefixViolation) as stale_prefix:
            reader(
                (row,),
                expected_binding=_read_binding(),
                expected_head=1,
                expected_prefix_digest="0" * 64,
            )
        assert stale_prefix.value.reason == "DURABILITY_PREFIX_STALE"
        assert verified.prefix_digest != "0" * 64

        corrupt = replace(row, canonical_bytes=b"{}", payload_digest=hashlib.sha256(b"{}").hexdigest())
        with pytest.raises(DurabilityPrefixViolation) as corrupt_error:
            reader((corrupt,), expected_binding=_read_binding(), expected_head=1)
        assert corrupt_error.value.reason == "DURABILITY_PREFIX_CORRUPT"

        oversized = replace(
            row,
            canonical_bytes=b"x" * (MAX_DURABILITY_PREFIX_ITEM_BYTES + 1),
            payload_digest="0" * 64,
        )
        with pytest.raises(DurabilityPrefixViolation) as oversized_error:
            reader((oversized,), expected_binding=_read_binding(), expected_head=1)
        assert oversized_error.value.reason == "DURABILITY_PREFIX_OUT_OF_BOUNDS"


def test_effect_reader_returns_facts_only_with_zero_external_effects(
    monkeypatch,
) -> None:
    calls: list[str] = []

    def forbidden(*args: object, **kwargs: object) -> None:
        del args, kwargs
        calls.append("called")
        raise AssertionError("pure durability reader attempted an external effect")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)

    verified = verify_effect_prefix(
        (_effect_row(),),
        expected_binding=_read_binding(),
        expected_head=1,
    )

    assert calls == []
    assert verified.recovery_authority is False
    assert verified.task_mutation_authority is False
    assert verified.executor_authority is False
    assert verified.external_effect_authority is False
