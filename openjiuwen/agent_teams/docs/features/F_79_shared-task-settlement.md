# Shared physical task settlement

| Item | Value |
|---|---|
| Date | 2026-09-09 |
| Scope | core.common.background_tasks, NativeHarness async tools, SwarmFlow resume, Live Voice Work consumer |
| Authority | User-approved source-installed AgentCore / Live Voice unified execution Goal |

Cancellation previously marked a terminal record before the coroutine's finally
block exited. Duplicate IDs could overwrite a live run and its old callback
could remove the replacement. Live Voice implemented its own physical wait.

The existing core.common.background_tasks module now owns a bounded settlement
wait, consumed by both NativeHarness async tools and Live Voice Work. It borrows
the execution task, preserves its result/exception, and never cancels that task
because the observer disappeared. Product status, durable identity and effects
remain with their existing owners.

AsyncToolRuntime retains IDs through worker and notification cleanup. A request
sets cancelling; an expired settlement wait reports unknown and retains ownership.
The record separately reports execution_settled. Late results after cancellation
do not start completion injection. A spill thread stays owned until it exits.
The two-phase tool protocol and completed results remain unchanged. SwarmFlow
relaunch creates a fresh ID, so its controller explicitly fences the original
record and avatar-session abort before relaunch; an unsuccessful resume stays retryable.

Rejected: unconditionally treating cancellation as completion; cancelling OS
thread wrappers and releasing ownership; treating a fresh SwarmFlow task ID as proof
that its predecessor has stopped; moving Live Voice's whole durable Task/Work
state machine into a generic SDK wrapper.

Verification covers both consumers, delayed/swallowed cancellation, duplicate
IDs, fresh-ID SwarmFlow resume, thread spill, observer cancellation, known errors and bounded
waits. Native Provider/media and the full common Agent stream are separate
candidate verification scopes and are not claimed by these deterministic tests.

Avatar abort failures propagate after attempting every avatar. Disposal retains
the original session row and hooks until the actual harness dispose succeeds;
the best-effort KV cleanup helper's normal return is not proof of disposal.
The controller retries backend aclose before relaunch. Both abort and disposal
failures therefore retain a retryable paused intent even when the outer workflow
coroutine has already exited. Real manager/backend/KV-helper fault tests cover
this boundary with failure injected only in the underlying harness operations.
