# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import base64
import hashlib
import json
from dataclasses import replace

import pytest

from openjiuwen.core.application.tasks.contracts import (
    Assurance,
    ScopeRef,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_checkpoint import (
    MAX_D1_CHECKPOINT_STATE_BYTES,
    D1Checkpoint,
    DurabilityCheckpointViolation,
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
        durability_level="D1",
        durability_capability_version="d1.v1",
    )


def _checkpoint(*, state_bytes: bytes = "{状态:继续}".encode()) -> D1Checkpoint:
    return D1Checkpoint.create(
        checkpoint_id="checkpoint-1",
        scope=_scope(),
        task_id="task-1",
        producer_attempt_id="attempt-1",
        checkpoint_sequence=7,
        recovery_generation=0,
        profile=_profile(),
        complete=True,
        task_spec_digest="4" * 64,
        context_version="context.v3",
        context_digest="2" * 64,
        input_digest="3" * 64,
        state_schema_id="agent-state",
        state_schema_version=4,
        state_bytes=state_bytes,
        effect_head=0,
        effect_prefix_digest="5" * 64,
    )


def test_checkpoint_roundtrip_is_canonical_and_unicode_exact() -> None:
    checkpoint = _checkpoint()

    wire = checkpoint.canonical_bytes()
    decoded = D1Checkpoint.from_bytes(wire)

    assert decoded == checkpoint
    assert decoded.state_bytes == "{状态:继续}".encode()
    assert wire == canonical_json_bytes(json.loads(wire))
    assert decoded.integrity_digest == hashlib.sha256(canonical_json_bytes(decoded.unsigned_dict())).hexdigest()


def test_checkpoint_rejects_noncanonical_corrupt_and_forged_data() -> None:
    checkpoint = _checkpoint()
    payload = checkpoint.to_dict()

    with pytest.raises(DurabilityCheckpointViolation) as noncanonical:
        D1Checkpoint.from_bytes(b" " + checkpoint.canonical_bytes())
    assert noncanonical.value.reason == "NON_CANONICAL_CHECKPOINT"

    payload["state_bytes_base64"] = base64.b64encode(b"forged").decode("ascii")
    forged = canonical_json_bytes(payload)
    with pytest.raises(DurabilityCheckpointViolation) as corrupt:
        D1Checkpoint.from_bytes(forged)
    assert corrupt.value.reason == "CHECKPOINT_STATE_DIGEST_MISMATCH"

    with pytest.raises(DurabilityCheckpointViolation) as invalid_digest:
        replace(checkpoint, integrity_digest="A" * 64)
    assert invalid_digest.value.reason == "INVALID_DURABILITY_DIGEST"


def test_checkpoint_enforces_unicode_and_state_bounds() -> None:
    maximum = b"x" * MAX_D1_CHECKPOINT_STATE_BYTES
    assert len(_checkpoint(state_bytes=maximum).state_bytes) == len(maximum)

    with pytest.raises(DurabilityCheckpointViolation) as oversized:
        _checkpoint(state_bytes=maximum + b"x")
    assert oversized.value.reason == "CHECKPOINT_STATE_OUT_OF_BOUNDS"

    with pytest.raises(DurabilityCheckpointViolation) as empty:
        _checkpoint(state_bytes=b"")
    assert empty.value.reason == "CHECKPOINT_STATE_OUT_OF_BOUNDS"

    with pytest.raises(DurabilityCheckpointViolation) as invalid_unicode:
        D1Checkpoint.create(
            checkpoint_id="checkpoint-\ud800",
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
    assert invalid_unicode.value.reason == "INVALID_DURABILITY_TEXT"


def test_checkpoint_and_profile_never_claim_runtime_authority() -> None:
    checkpoint = _checkpoint()

    assert checkpoint.resume_authority is False
    assert checkpoint.task_mutation_authority is False
    assert checkpoint.executor_authority is False
    assert checkpoint.profile.capability_authority is False
