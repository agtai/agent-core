# Shared Goal observation and control

`GoalManager` remains the session's only Goal state owner. Use `peek()` for a
copied observation without acquiring the interaction control lock. Stores must
provide `peek()` without repairs or writes. `SessionGoalStore.peek()` raises
`GoalOperationError(code="invalid_state")` for corrupt or wrong-session data;
its existing `load()` recovery behavior is unchanged.

For a command derived from an observed Goal, pass both preconditions:

```python
observed = manager.peek()
paused = await manager.pause(
    expected_goal_id=observed.goal_id,
    expected_control_revision=observed.control_revision,
)
```

`set`, `pause`, `resume`, and `clear` check these optional preconditions under
the existing interaction control lock. A stale target raises `stale_goal` before
any persistence, notification, queued work or cancellation. Unconditional calls
remain supported. `before_effect`, when supplied, is an application admission
check called inside that same lock; it may await fresh authority I/O and must
raise to reject. It must not block the event loop or reacquire the Goal lock.

`control_revision` starts at 1, including for legacy persisted records, and
changes on Goal status transitions. It is separate from `revision`, which binds
execution attempts. Pause stops subsequent attempts while allowing the current
attempt to finish and record its assessment. In-flight pause/resume must not
invalidate that attempt generation. Set/resume execution still requires the
existing output attachment and stream consumer; a state update alone is not
evidence of execution. A read or rejected control never starts a consumer.

For an interactive host, use `DeepAgent.set_goal(objective, **controls)` or
`DeepAgent.resume_goal(**controls)`. These return `(record, output_stream)` and
use the same `GoalManager`, store and scheduler. Identity, authority and state
validation happen before output attachment under the control lock. These entry
points read corrupt state strictly, without the legacy `load()` repair.

Consume the returned stream when it is non-null. A null stream means an existing
reader owns output and receives the admitted Goal's progress. If that reader is
already finishing, the call waits for its buffered output to drain and then
revalidates admission before acquiring a new lease. No Goal control lock is held
during that wait. Cancelling the waiting command does not cancel the old reader.

`attach_output`, `set_goal` and `resume_goal` accept an optional synchronous
`on_output_ready(token, acquired)` host callback. The opaque token identifies
the actual existing or newly acquired output lease; `acquired` is false when an
older reader will consume the new work. It runs under the interaction control
lock before scheduling, after Goal admission checks for Goal commands. A host
can retain its actual reader here or raise if that reader is already closing.
A rejected/stale Goal or a finishing old lease never invokes the callback. A
new lease is released if the callback fails, preserving unrelated queued work.
The callback must not await, block, reacquire SDK locks or mutate Goal state.

Do not attach output before validating a Goal command: ordinary `attach_output`
may ensure active Goal work, and ordinary stream close discards queued work.
On failure after acquisition, the new APIs wait for their own lease to be
released, including through repeated cancellation, and preserve unrelated queued
work. An exception after a store write/commit begins is an uncertain outcome;
inspect the authoritative Goal before retrying rather than assuming no mutation.
This does not make a successful admission a claim of business completion.

`InteractionOutputStream.close(discard_pending_work=False,
abort_active_round=False)` releases just that stream's lease. Existing close
defaults still discard pending work and cancel the active round. A stale handle
cannot release a newer reader.

## Per-work host context and output provenance

`GoalManager.set/resume` and `DeepAgent.set_goal/resume_goal` accept optional
`run_context: dict | None`. The Goal record snapshots that context and copies it
into every scheduled attempt through the existing `RunContext`/task metadata
path. The SDK overwrites Goal identity, session identity and attempt revision;
host context cannot replace those values. Existing calls without this argument
retain their behavior. The context must be pure JSON (string keys, finite numbers,
no Python objects), at most 64 KiB UTF-8, depth 32 and 4096 values. `extra`, when
present, must be an object. Invalid context rejects before output preparation or
Goal mutation with `GoalOperationError(code="invalid_run_context")`.
Top-level `source_metadata` and `_interaction_request_id` are reserved and
rejected; normalization never promotes them into the public map. Any supplied
`extra._interaction_request_id` is discarded and the actual work request id is
written by the SDK after normalization.

Resume with `None` preserves the saved context. A different non-null context is
accepted only for an idle PAUSED/BLOCKED Goal after the existing control CAS;
`{}` explicitly replaces it with an empty context. ACTIVE or still-running
attempts reject changes with `run_context_conflict`, before output preparation.
Pause/resume of the same running attempt keeps its original context and revision.
The existing Goal store encodes this field inside its record as a JSON string to
avoid recursive state merging and preserve JSON null. Legacy dict/absent/null
fields remain readable. Corrupt bindings fail both `peek()` and `load()` without
clearing the record. `GoalRecord.to_dict()` returns an isolated dictionary field.

Host rails read the actual scheduled invocation's context from
`ctx.extra["run_context"]` (or the outer iteration's `inputs.run_context`).
Request ContextVars do not establish context for an already-running supervisor
or task scheduler. The SDK does not interpret host permissions or model choices.
Hosts must validate missing, stale and restored bindings before their effects.

Only `run_context["extra"]["source_metadata"]` is an explicit public output map.
For example:

```python
run_context = {"extra": {
    "host_execution": {"model_identity": "catalog-entry", "config_version": "v1"},
    "source_metadata": {"source_binding_id": "binding-1",
                        "source_origin_request_id": "request-1"},
}}
record, stream = await agent.set_goal("Prepare the report", run_context=run_context)
```

This map is copied with a 4 KiB bound; the final source map including SDK fields
and static Session labels must also fit 4 KiB. Set and resume preflight the real
session/Goal identity and next revision with the SDK's 32-character task-id size
before output, persistence or queue effects. User input preflights its real
request/session identity before enqueue. Restored invalid projections are
rejected without repairing the stored record. Use the map only for public labels.
The SDK adds authoritative
`source_session_id`, `source_task_id`, `source_request_id`, `source_run_kind`,
`source_goal_id` and `source_goal_revision`, overriding supplied values. Goal
work has no input request id, so `source_request_id` is null; hosts can correlate
it using their binding/origin ids. Private run context is never automatically
copied into these labels. `goal.updated` and execution-error Goal snapshots omit
`run_context`; hosts serializing a returned record must apply their own projection.

`Session.with_source_metadata(metadata)` returns a source view sharing the same
state, interaction and writer. It owns no new session lifecycle, stream closure
or provider-cache identity. Existing views and queued chunks retain isolated
metadata, including late output after another round begins. DeepAgent uses a
round view, and the actual executor reconstructs its view from task metadata.
Controller output preserves its typed payload and places source labels in
`payload.metadata`; ordinary output places them in its payload (scalar payloads
retain the existing `value` wrapper). Executor labels survive the scheduler's
later write through the original Session. Context-free legacy rounds retain
their existing output shape. EOF and controller settlement remain distinct
from Goal completion and any host presentation acknowledgement.
