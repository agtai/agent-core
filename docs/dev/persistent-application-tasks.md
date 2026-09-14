# Persistent application tasks

This addition is based on `b5f189ba1054a338d8fa3e009b13053776340e44`, the
AgentCore revision pinned by the consuming JiuwenSwarm branch. It extracts the
existing durable delivery implementation into `openjiuwen.core.application.tasks`.
It does not replace the Controller, TeamAgent or Harness task APIs.

## Capability comparison and reuse decisions

| Existing capability | Code and call flow | Relationship to this addition |
| --- | --- | --- |
| Coroutine task management | `core/common/task_manager/manager.py:create_task/task_group` and `task.py:execute/cancel` | The existing structured-concurrency task group owns transient coroutines and propagates cancellation through an AnyIO cancel scope. Work keeps UNKNOWN and its physical reservation until actual settlement, and recovery must not rerun the Agent. Replacing those facts with coroutine completion would lose required semantics; no second coroutine-status projection is introduced. |
| Controller tasks | `core/controller/schema/task.py`; `TaskManager.add_task/get_state/load_state` maintains indexed session tasks; `TaskScheduler` selects a registered executor by task type and streams its output | Reuse for Controller execution. Its task/session state, pause and output events are not the durable command/attempt/outbox protocol. Do not map a scheduler cancellation boolean to a committed delivery result. |
| Team tasks | `agent_teams/tools/task_manager.py`, `tools/database/task_dao.py`; task tools call the manager/DAO and publish team events for assignment, dependencies, completion and review | Reuse for team decomposition and coordination. A team's subtask ID is not automatically a user delivery ID or an execution attempt ID. No second team scheduler is added. |
| Harness asynchronous tools | `agent_teams/harness/async_tools.py`; invoke launches an async tool, tracks status/output, and injects completion via the owning harness; `tools/tool_async.py` provides list/output/cancel | Retain as the async tool runtime. It does not by itself prove durable outbox delivery, project effects or safe restart/retry. No duplicate async-tool loop is added. |
| Subagent execution | `harness/tools/subagent/task_tool.py` and existing Agent/Runner/model/tool loop | Remains the execution mechanism. The new core receives an application-supplied executor, not model credentials or a new Agent. |
| Session/checkpoint facilities | Existing session state and Agent checkpoint mechanisms | Retain for Agent execution state. New durability records bind the exact task/attempt/profile and external-effect prefix; they do not substitute a second conversation-memory implementation. |

The missing layer is durable **delivery authority**: accepted command versus
dispatched attempt, fenced adjustments and cancellation, at-most-once command
replay, scoped results, unknown outcomes and verified recovery facts. Those
responsibilities are additive to execution scheduling. No claim is made that
existing Controller or TeamAgent consumers now automatically use this store.

## Ownership and flow

```mermaid
flowchart TD
  A[Application authorization and context] --> C[PersistentTaskCore]
  C <--> S[SqliteTaskStore: commands / tasks / attempts / outbox]
  C -->|exact dispatch / cancel / adjust| E[Application FormalExecutor]
  E --> R[Existing Agent / Controller / Team / Harness execution]
  R --> E
  E -->|observations and recovery facts| C
  S --> F[Authorized task event subscription]
  F --> U[Application UI / notifications / voice projection]
```

- `formal_task_models.py`: immutable task, attempt, authority, result and recovery contracts.
- `task_store.py`: SQLite authority, exact command replay, attempt/outbox transactions and result facts.
- `persistent_task_core.py`: authorization and delivery orchestration through `FormalExecutor`.
- `task_adjustment_queue.py`: ordered changes and successor cutover without rewriting predecessor results.
- `task_event_subscription.py`: scoped event delivery and authoritative replay.
- `executor_capabilities.py`, `file_effect_plan.py`: executor declarations and file-effect contracts.
- `durability/`: checkpoint/effect identity, prefix verification, recovery facts and single-operation authorization.
- `contracts.py`: shared command/query/result/scope/progress values. Applications retain concrete identity and committed-input ledgers behind admission protocols.
- `source.py`: trusted, immutable source evidence codecs. Unknown or malformed evidence fails closed.

The initial admission profile deliberately retains the existing **project-mutation**
requirements, including exact model configuration and project-write context.
This is not yet a replacement for arbitrary read-only Work. Making admission
profiles generic is a separate behavioral change and is not silently introduced.

## JiuwenSwarm adapter boundary

JiuwenSwarm now imports this implementation. It still owns project authorization,
confirmation, model resolution, the concrete project/Agent adapter, and voice/UI
projection. Its `NativeTaskSource` implements `TaskSourceEvidence` and registers
the existing source version during module initialization. SDK code has no
JiuwenSwarm import and contains no Realtime client, microphone, turn ledger or UI.

The concrete project executor remains in JiuwenSwarm because it resolves its
project registry, configured Agent facade, tool registration and local project
baseline. Moving that adapter unchanged would introduce an SDK-to-application
dependency. It uses the SDK's task/durability contracts and existing Agent rails.

Applications instantiate `SqliteTaskStore(path)` and
`PersistentTaskCore(store, executor)`, validate their entrypoint, then call
`execute(command, grant, context=resolved_context)` and drain the outbox. A
successful creation receipt means accepted, not executed. An executor must
return exact attempt observations; queued, timeout and unknown must not claim
completion. `tests/integration_tests/application_tasks/test_application_boundary.py`
is a runnable application-independent admission/delivery/reopen example.

## Compatibility and verification

SQLite schema version 6 and existing serialized identifiers/keys remain unchanged.
Legacy wire strings (`live-voice.contract.v2`, `native_source`, and namespaced
extensions) remain readable; they are compatibility values, not SDK imports.
The originating application's source decoder remains mandatory when source data
is present. A missing decoder never downgrades a record to legacy/no-source.

Run:

```powershell
python -m pytest -q -o addopts= -o asyncio_mode=auto tests/integration_tests/application_tasks
python -m ruff check openjiuwen/core/application/tasks tests/integration_tests/application_tasks
```

These tests use real temporary SQLite files and deterministic executor facts.
They establish SDK/storage boundaries, not physical voice or cloud-model acceptance.

## Local distribution

The branch package version is `0.1.17+livevoice.4`; official `0.1.17` does not
contain this addition and must not satisfy the consuming application's pin.
JiuwenSwarm uses the sibling `../agent-core` editable uv source for local
work. For wheel installation, install the matching SDK wheel together with
the Host wheel. Neither this version nor the branch commit is assumed published.

Validation for this migration: 102 SDK application-task tests, plus 57 existing
Controller TaskManager/TaskExecutor tests pass (159 combined). Ruff passes.
Host integration additionally exercises task creation/delivery, SQLite replay,
adjust/cancel/recovery, project file effects, real SDK tool registration and
source validation. A wheel-to-wheel import verifies the packaged Host uses the
packaged SDK task implementation. Physical audio/cloud-model acceptance and
migration of Work were outside that initial change. The Work follow-up is below.

## Project execution ownership (2026-09-13 follow-up)

`DirectProjectCodeExecutorAdapter` now belongs to this SDK. It owns the attempt
journal, exact dispatch/cancel/adjust handling, isolated Git checkout, protected
file verification, apply/cleanup settlement and durable recovery. Existing
database tables, profile identifiers and legacy serialized strings are retained.
Their historical `live_voice`/`jiuwenswarm` spelling does not introduce an import
dependency or a second implementation.

Construct it with a `ProjectExecutionApplication` implementation. The application
maps immutable `ProjectTaskInvocation` values to its Agent request, supplies the
execution guard and protected runtime-support paths, and may supply telemetry.
Project/configuration resolution remains behind `ProjectExecutionBindingResolver`.
The SDK does not select a user's Agent, instantiate a JiuwenSwarm request, or
grant project permissions. JiuwenSwarm's subclass supplies only this integration.

`execution_checkpoint` provides one process-local checkpoint identity shared by
executor, Agent callback and the file-effect declaration tool. Wrong-session or
closed checkpoint access fails; adjustment adoption cannot forge user authority.
`TaskResultReader` verifies exact Task/Attempt ownership and artifact hashes before
bounded content disclosure. Dialogue context selection and voice playback remain
application responsibilities.

The SDK-only project test applies a real local Git/file result and reopens its
attempt journal without replaying the Agent. Host regression covers current D2
execution, original-file/index preservation, cancel/adjust, partial effects and
restart. Existing synchronous filesystem authority checks are retained during
extraction; their four explicit lint exceptions avoid changing scheduling in a
behavior-preserving migration. No cloud latency policy changes are included.

## Work ownership (2026-09-13 follow-up)

`WorkRuntime` and `SqliteWorkStore` own admitted analysis work, revisions,
deduplication, exact-scope queries, cancellation settlement and checkpoint CAS.
Admission is saved before scheduling the injected runner. Reopening a checkpoint
whose process execution ownership was lost records UNKNOWN; it never replays
Agent/tool effects or invents completion. Work and project Tasks can use the same
SQLite file without creating a Task for every analysis.

The application supplies the configured Agent runner and authorized context;
its source/input journal and presentation receipts remain application-owned.
Store initialization/validation hooks keep those tables in the same transaction
without introducing application imports into the SDK. Existing wire fields,
table names, bounds and error strings remain compatible.

`execution_control` shares transient interaction control and read-only await
settlement. Admitted Work clears the transient caller context, so stopping speech
does not silently cancel accepted work. Persistent writes never use the read-only
cancellation helper. `observation` owns bounded cursor validation for SDK waits
and application queries; no Host package is required, including deferred calls.

This extends SDK application execution, rather than replacing Controller tasks or
AgentTeam AsyncToolRuntime. The latter owns per-harness background tools and does
not provide this checkpoint/revision/UNKNOWN contract. Task remains responsible
for authorized project mutations, durable attempts, outbox and verified effects;
Work remains the lighter analysis lifecycle. They reuse contracts and execution
primitives without pretending those different state meanings are interchangeable.

## Code-level integration audit (2026-09-13)

The current cleanup removes `LegacyProjectTaskService` and the unused `service`
member of `ProjectExecutionBinding`. No production executor read that member;
only historical scheduler fixtures used it. The consuming Host no longer passes
or starts/stops a legacy carrier; SDK project execution and Host Agent cleanup
remain the actual owners. Historical carrier bindings belong to test support.
This is a paired development-branch `.4` change: external callers constructing
`.3` bindings with `service=` or importing the removed protocol must update.
No claim of compatibility with unknown direct `.3` consumers is made.

The broader addition still does not replace Controller or Team task management.
`TaskManager.get_state/load_state` copies Controller session task/index state;
`TaskScheduler.cancel_task` requests executor cancellation and marks CANCELED.
Team DAO assignment/dependency/review transactions own different coordination
facts. Harness async tool completion also injects into its owning harness.
None of these inspected paths establishes the application's durable outbox,
exact attempt/effect settlement or Work checkpoint CAS. Existing Agent/tool
execution is reused; durable delivery remains an added subsystem, whose wider
management convergence is unproved.

A root-registered checkpoint Rail replacement failed a real wrong-Agent tool
case and was reverted. The final TaskCheckpointRail uses the existing native
AgentCallbackManager's new scoped_agent_rail entry instead. Host supplies only
cached-instance and session identity readers; its dynamic presentation-rail
checkpoint callback fields have been removed. Physical voice, running-cancel
user acceptance and current external-provider behavior are not established by
these checks.

### Execution-scoped native rails

`openjiuwen.core.single_agent.agent_callback_manager.scoped_agent_rail` binds an
AgentRail to the current Python execution context, including inherited async
tasks. Ordinary callers without a scope keep their existing callback behavior.
This opt-in facility never registers process-global callbacks or tools. Callback
failures propagate. Nested scopes execute outer-to-inner, independent of normal
registry priorities. After scope exit, inherited tasks fail closed, including
when exit happens while a callback is suspended. Callers must keep the scope
open through actual stream cleanup; it is not a sandbox against trusted Python
code explicitly replacing its execution context.

```python
from openjiuwen.core.single_agent.agent_callback_manager import scoped_agent_rail
from openjiuwen.core.single_agent.rail.base import AgentCallbackEvent

# task_rail is a TaskCheckpointRail with application-owned identity adapters.
with scoped_agent_rail(
    task_rail,
    before_events=frozenset({AgentCallbackEvent.BEFORE_TOOL_CALL}),
):
    # Consume and close the existing SDK Agent stream in this same scope.
    ...
```

By default scoped hooks run after ordinary registered hooks. `before_events`
selects hooks that must precede ordinary projection callbacks: Task tool-plan
validation rejects before tool_call/history emission, while model adoption
runs after ordinary model-context preprocessing. Hooks are trusted SDK code;
this does not authorize arbitrary later hooks to rewrite protected arguments.
The inspected Host hook only cleans call_goal for file tools. Applications
using argument-rewriting hooks must preserve the same authorization boundary.


### Atomic application authority reads (.5)

`SqliteTaskStore.list_task_authority_snapshots_page(scope, cursor=None, limit=...)`
returns `(snapshots, next_cursor, has_more)`. Each `TaskAuthorityReadSnapshot`
contains the Task, current Attempt/admission, canonical event head and result
availability/record/reason from one SQLite read transaction. It reuses ordinary
keyset paging and existing row decoders. A missing event head fails closed;
this reader neither repairs storage nor authorizes a command.

An application may project product permissions and result digests from this
snapshot without polling the Task page twice for convergence. Later mutations
remain subject to their existing command preconditions. Ordinary page APIs retain
their signatures and behavior; a sequence of separate pages is not one atomic
collection snapshot. Callers requiring a complete bounded collection must reject
`has_more`/a next cursor, as the paired Host adapter does.

Real SQLite tests cover concurrent completion/insertion between task and auxiliary
reads, original pagination/attempt isolation, wrong subject/project/session,
invalid bounds, missing event heads and no read-side writes. Pair the Host's new
aggregate consumer with `0.1.17+livevoice.5`; `.4` lacks this method. There is no
persisted schema migration or reverse Host dependency.


### Shared durability validation

Checkpoint, effect, recovery and verified-prefix values reuse the existing
`durability_identity` text, authenticated-scope and profile validators. Each binds
its own exception type, reason and field label. Removed duplicate algorithms do
not change accepted values, canonical encoding, error causes, persistent schema
or public exports. Exact strings and the 512-byte UTF-8 bound remain distinct
from transport identity rules. Validation never grants recovery or dispatch
permission; Store transactions and executor fences remain authoritative.


### Shared subscription lifecycle and durable consumption (.6)

`TaskEventSubscription` now also reads the existing Store consumer authority
pages with `enabled=True, authority_atomic_replay=True, consumer_scope=True,
presentation_class="text"` (or `"voice"`). JiuwenSwarm's
`TaskEventAuthorityProgressSource` directly constructs this SDK reader for all
three modes; its separate consumer subscription implementation is removed.
The Store remains the only owner of persisted cursors and canonical event facts.
The reader never ACKs a presentation or changes a Task/outbox record.

| Mode | Start position | Read management | Cancellation of next_event |
|---|---|---|---|
| Default live-only | Current Store head | Existing background tail | Detaches reader |
| Authority prefix, no presentation_class | Validated current-attempt prefix | Prefix then existing background tail | Detaches reader |
| Authority consumer pages | Durable text/voice watermark | Demand pages, frozen head while paging | Cancels that wait; reader remains active |

Consumer pages preserve idempotent `start()` results, bounded rolling identity
validation, delayed ACK advancement through an already-read prefix, and replay
across historical attempts. Only the current terminal closes that stream.
`consumer_cursor_baseline()` returns the Store-proven cursor after start;
`consumer_terminal_closes_stream(event)` distinguishes current from historical
terminal events for the application projection. Both are read-only.
Queue, authorization, state snapshots, close intent and owner-loop handling reuse
the existing SDK manager. Initialization and close intent share its lock; a close
that wins before queue allocation prevents event delivery. Cursor page validation
is retained mode-specific code, not counted as eliminated behavior.

Host consumers of this additive API require `0.1.17+livevoice.6`. There is no
schema migration or change to older call defaults. Presentation/heard-history
ACK policy remains an application responsibility; no SDK-to-Host dependency.
