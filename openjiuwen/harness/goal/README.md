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
