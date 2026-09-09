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
