# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A frozen restriction on an authorized Task's file effects, never a grant.

The existing Code Agent interprets the user's requirements. This contract checks
the resulting paths and bytes; neither a digest nor a proposed replacement proves
that the Agent interpreted overwrite consent correctly.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath

from openjiuwen.core.application.tasks.contracts import ScopeRef, canonical_json_bytes

FILE_EFFECT_PLAN_VERSION = "live-voice.file-effect-plan.v1"
MAX_EFFECT_PATHS = 32
MAX_PLAN_BYTES = 131072
_HEX = re.compile(r"[0-9a-f]{64}")
_RESERVED = re.compile(r"(?:con|prn|aux|nul|com[0-9]|lpt[0-9])(?:\..*)?", re.I)


class FileEffectPlanError(ValueError):
    def __init__(self, reason="FILE_EFFECT_PLAN_INVALID"):
        super().__init__(reason)
        self.reason = reason


def _digest(value):
    if type(value) is not str or _HEX.fullmatch(value) is None:
        raise FileEffectPlanError()
    return value


def canonical_effect_path(value):
    if (
        type(value) is not str
        or not value
        or len(value.encode("utf-8")) > 2048
        or "~" in value  # SDK home expansion and Windows short-name aliases.
        or "\\" in value
        or any(character in value for character in ':*?<>|"')
        or any(ord(character) < 32 for character in value)
        or PurePosixPath(value).is_absolute()
        or PureWindowsPath(value).drive
        or any(
            part in {"", ".", ".."} or part.endswith((".", " ")) or part.lower() == ".git" or _RESERVED.fullmatch(part)
            for part in value.split("/")
        )
    ):
        raise FileEffectPlanError("FILE_EFFECT_PATH_INVALID")
    return value


def effect_target(root: Path, relative: str) -> Path:
    """Reject path aliases and every observed link/reparse component."""
    relative = canonical_effect_path(relative)
    root = root.resolve(strict=True)
    candidate = root
    for part in relative.split("/"):
        candidate = candidate / part
        try:
            stat = candidate.lstat()
        except FileNotFoundError:
            continue
        if candidate.is_symlink() or getattr(stat, "st_file_attributes", 0) & 0x400:
            raise FileEffectPlanError("FILE_EFFECT_PATH_LINK")
    try:
        candidate.resolve(strict=False).relative_to(root)
    except ValueError:
        raise FileEffectPlanError("FILE_EFFECT_PATH_ESCAPE") from None
    return candidate


def file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise FileEffectPlanError("FILE_EFFECT_TARGET_NOT_FILE")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class PlannedFileEffect:
    path: str
    operation: str
    original_sha256: str | None

    def __post_init__(self):
        canonical_effect_path(self.path)
        if type(self.operation) is not str or self.operation not in {"create", "replace", "delete"}:
            raise FileEffectPlanError()
        if self.operation == "create":
            if self.original_sha256 is not None:
                raise FileEffectPlanError("FILE_EFFECT_CREATE_REQUIRES_ABSENT_TARGET")
        else:
            _digest(self.original_sha256)

    def to_dict(self):
        return {"path": self.path, "operation": self.operation, "original_sha256": self.original_sha256}

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {"path", "operation", "original_sha256"}:
            raise FileEffectPlanError()
        return cls(**value)


@dataclass(frozen=True, slots=True)
class FileEffectPlan:
    scope: ScopeRef
    task_id: str
    attempt_id: str
    revision: int
    prior_plan_digest: str | None
    requirement_head: str
    source_digest: str
    baseline_digest: str
    preserve_existing: bool
    effects: tuple[PlannedFileEffect, ...]
    required_outputs: tuple[str, ...]

    def __post_init__(self):
        if (
            not isinstance(self.scope, ScopeRef)
            or any(type(value) is not str or not value or len(value) > 256 for value in (self.task_id, self.attempt_id))
            or type(self.revision) is not int
            or not 1 <= self.revision <= 4096
            or type(self.preserve_existing) is not bool
            or type(self.effects) is not tuple
            or not 1 <= len(self.effects) <= MAX_EFFECT_PATHS
            or not all(isinstance(effect, PlannedFileEffect) for effect in self.effects)
            or type(self.required_outputs) is not tuple
            or len(self.required_outputs) > MAX_EFFECT_PATHS
        ):
            raise FileEffectPlanError()
        for value in (self.requirement_head, self.source_digest, self.baseline_digest):
            _digest(value)
        if self.revision == 1:
            if self.prior_plan_digest is not None:
                raise FileEffectPlanError()
        else:
            _digest(self.prior_plan_digest)
        paths = [effect.path.casefold() for effect in self.effects]
        if len(set(paths)) != len(paths):
            raise FileEffectPlanError("FILE_EFFECT_PATH_ALIAS")
        if any(other.startswith(path + "/") for path in paths for other in paths if other != path):
            raise FileEffectPlanError("FILE_EFFECT_PATH_COLLISION")
        if self.preserve_existing and any(effect.operation != "create" for effect in self.effects):
            raise FileEffectPlanError("FILE_EFFECT_PRESERVATION_CONFLICT")
        outputs = tuple(canonical_effect_path(path) for path in self.required_outputs)
        writable = {effect.path for effect in self.effects if effect.operation != "delete"}
        if len(set(outputs)) != len(outputs) or not set(outputs) <= writable:
            raise FileEffectPlanError("FILE_EFFECT_REQUIRED_OUTPUT_INVALID")
        if len(canonical_json_bytes(self.to_dict())) > MAX_PLAN_BYTES:
            raise FileEffectPlanError("FILE_EFFECT_PLAN_TOO_LARGE")

    def to_dict(self):
        return {
            "contract_version": FILE_EFFECT_PLAN_VERSION,
            "scope": self.scope.to_dict(),
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "revision": self.revision,
            "prior_plan_digest": self.prior_plan_digest,
            "requirement_head": self.requirement_head,
            "source_digest": self.source_digest,
            "baseline_digest": self.baseline_digest,
            "preserve_existing": self.preserve_existing,
            "effects": [effect.to_dict() for effect in self.effects],
            "required_outputs": list(self.required_outputs),
        }

    @classmethod
    def from_dict(cls, value):
        fields = {
            "contract_version",
            "scope",
            "task_id",
            "attempt_id",
            "revision",
            "prior_plan_digest",
            "requirement_head",
            "source_digest",
            "baseline_digest",
            "preserve_existing",
            "effects",
            "required_outputs",
        }
        if (
            type(value) is not dict
            or set(value) != fields
            or value["contract_version"] != FILE_EFFECT_PLAN_VERSION
            or type(value["effects"]) is not list
            or len(value["effects"]) > MAX_EFFECT_PATHS
            or type(value["required_outputs"]) is not list
            or len(value["required_outputs"]) > MAX_EFFECT_PATHS
        ):
            raise FileEffectPlanError()
        return cls(
            scope=ScopeRef.from_dict(value["scope"]),
            task_id=value["task_id"],
            attempt_id=value["attempt_id"],
            revision=value["revision"],
            prior_plan_digest=value["prior_plan_digest"],
            requirement_head=value["requirement_head"],
            source_digest=value["source_digest"],
            baseline_digest=value["baseline_digest"],
            preserve_existing=value["preserve_existing"],
            effects=tuple(PlannedFileEffect.from_dict(effect) for effect in value["effects"]),
            required_outputs=tuple(value["required_outputs"]),
        )

    @property
    def digest(self):
        return hashlib.sha256(canonical_json_bytes(self.to_dict())).hexdigest()

    def require_baseline(self, root: Path):
        for effect in self.effects:
            if file_digest(effect_target(root, effect.path)) != effect.original_sha256:
                raise FileEffectPlanError("FILE_EFFECT_BASELINE_CHANGED")

    def require_paths(self, changed_paths):
        changed = {canonical_effect_path(path) for path in changed_paths}
        effects = {effect.path: effect for effect in self.effects}
        if not changed <= effects.keys():
            raise FileEffectPlanError("FILE_EFFECT_UNPLANNED_DELTA")
        if not set(self.required_outputs) <= changed:
            raise FileEffectPlanError("FILE_EFFECT_REQUIRED_OUTPUT_UNCHANGED")
        return changed

    def require_delta(self, root: Path, changed_paths):
        changed = self.require_paths(changed_paths)
        effects = {effect.path: effect for effect in self.effects}
        for path in changed:
            exists = file_digest(effect_target(root, path)) is not None
            if exists != (effects[path].operation != "delete"):
                raise FileEffectPlanError("FILE_EFFECT_OPERATION_MISMATCH")
        for path in self.required_outputs:
            if file_digest(effect_target(root, path)) is None:
                raise FileEffectPlanError("FILE_EFFECT_REQUIRED_OUTPUT_MISSING")

    def require_operations(self, operations):
        self.require_paths(operations)
        planned = {effect.path: effect.operation for effect in self.effects}
        if any(planned[path] != operation for path, operation in operations.items()):
            raise FileEffectPlanError("FILE_EFFECT_OPERATION_MISMATCH")


def file_plan_source_digest(spec):
    """A continued Task binds its complete durable spec, without invented speech."""
    return (
        spec.native_source.digest
        if spec.native_source is not None
        else hashlib.sha256(spec.fingerprint_bytes()).hexdigest()
    )


class FileEffectPlanSession:
    """One Executor-owned interpretation window; a tool cannot choose its owner."""

    def __init__(self, *, item, target: Path, worktree: Path, baseline_digest: str, validate_target, protected_paths):
        self.item = item
        self.target = target.resolve(strict=True)
        self.worktree = worktree.resolve(strict=True)
        self.baseline_digest = _digest(baseline_digest)
        self.source_digest = file_plan_source_digest(item.spec)
        self.spec_digest = hashlib.sha256(item.spec.fingerprint_bytes()).hexdigest()
        self.validate_target = validate_target
        self.protected_paths = tuple(path.casefold() for path in protected_paths)
        self.adopted = []
        self.plan = None
        self._proposal_bytes = None
        self._lock = asyncio.Lock()
        self._restore_worker = None
        self.closed = False

    @property
    def requirement_head(self):
        return hashlib.sha256(
            canonical_json_bytes({"spec_digest": self.spec_digest, "adopted": self.adopted})
        ).hexdigest()

    def prompt(self):
        return (
            "Before any write_file or edit_file, read enough to propose one complete exact file plan "
            "with declare_file_effect_plan. Call that tool alone and wait for its accepted result. "
            "Use the user's original requirements to choose paths and create/replace operations; "
            "preserve every requirement. A preserve-existing-files request conflicts with replacing "
            "an existing output. A plan is only a restriction of current Task authority, never new consent. "
            "The server checks original existence and hashes. List all files this Task must produce as "
            "required_outputs. An accepted plan is frozen until a new user adjustment is adopted. "
            "You may keep editing this attempt's newly created output under its original create operation. "
            "After an adjustment, the server restores excluded prior-plan paths in this attempt's "
            "isolated workspace to their original baseline when accepting the replacement plan. "
            "Do not write unlisted paths or bypass this boundary. The exact current requirement_head is "
            + self.requirement_head
            + "."
        )

    def adopt(self, request):
        self._require_open()
        value = {
            "adjustment_id": request.adjustment_id,
            "requested_seq": request.requested_seq,
            "request_digest": hashlib.sha256(canonical_json_bytes(request.to_dict())).hexdigest(),
        }
        existing = next((entry for entry in self.adopted if entry["adjustment_id"] == request.adjustment_id), None)
        if existing is not None:
            if existing != value:
                raise FileEffectPlanError("FILE_EFFECT_ADJUSTMENT_REPLAY_CONFLICT")
            return
        if self.adopted and request.requested_seq <= self.adopted[-1]["requested_seq"]:
            raise FileEffectPlanError("FILE_EFFECT_ADJUSTMENT_ORDER_INVALID")
        self.adopted.append(value)

    def _require_open(self):
        if self.closed:
            raise FileEffectPlanError("FILE_EFFECT_PLAN_SESSION_CLOSED")

    def _require_writable_path(self, value):
        path = canonical_effect_path(value)
        key = path.casefold()
        if any(key == protected or key.startswith(protected + "/") for protected in self.protected_paths):
            raise FileEffectPlanError("FILE_EFFECT_PROTECTED_PATH")
        return path

    async def seal(self, proposal):
        async with self._lock:
            self._require_open()
            fields = {"requirement_head", "preserve_existing", "effects", "required_outputs"}
            if (
                type(proposal) is not dict
                or set(proposal) != fields
                or proposal["requirement_head"] != self.requirement_head
                or type(proposal["effects"]) is not list
                or not 1 <= len(proposal["effects"]) <= MAX_EFFECT_PATHS
                or type(proposal["required_outputs"]) is not list
            ):
                raise FileEffectPlanError("FILE_EFFECT_PROPOSAL_INVALID_OR_STALE")
            encoded = canonical_json_bytes(proposal)
            if len(encoded) > MAX_PLAN_BYTES:
                raise FileEffectPlanError("FILE_EFFECT_PLAN_TOO_LARGE")
            if self.plan is not None and self.plan.requirement_head == self.requirement_head:
                if self._proposal_bytes != encoded:
                    raise FileEffectPlanError("FILE_EFFECT_PLAN_ALREADY_FROZEN")
                return self.plan
            head = self.requirement_head
            await asyncio.to_thread(self.validate_target)
            effects = []
            for effect in proposal["effects"]:
                if type(effect) is not dict or set(effect) != {"path", "operation"}:
                    raise FileEffectPlanError()
                path = self._require_writable_path(effect["path"])
                original = await asyncio.to_thread(lambda: file_digest(effect_target(self.target, path)))
                effects.append(PlannedFileEffect(path, effect["operation"], original))
            candidate = FileEffectPlan(
                scope=self.item.scope,
                task_id=self.item.task_id,
                attempt_id=self.item.attempt_id,
                revision=1 if self.plan is None else self.plan.revision + 1,
                prior_plan_digest=None if self.plan is None else self.plan.digest,
                requirement_head=head,
                source_digest=self.source_digest,
                baseline_digest=self.baseline_digest,
                preserve_existing=proposal["preserve_existing"],
                effects=tuple(effects),
                required_outputs=tuple(proposal["required_outputs"]),
            )
            self._require_open()
            if self.requirement_head != head:
                raise FileEffectPlanError("FILE_EFFECT_REQUIREMENTS_CHANGED")
            worker = asyncio.create_task(asyncio.to_thread(self._restore_excluded_paths, candidate))
            self._restore_worker = worker
            try:
                await self._drain_restore_worker()
            finally:
                self._restore_worker = None
            self._require_open()
            if self.requirement_head != head:
                raise FileEffectPlanError("FILE_EFFECT_REQUIREMENTS_CHANGED")
            self.plan, self._proposal_bytes = candidate, encoded
            return candidate

    async def _drain_restore_worker(self):
        worker = self._restore_worker
        if worker is None:
            return
        cancellation = None
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError as error:
                # to_thread cannot stop a filesystem operation. Keep ownership
                # until it ends so cancellation never races checkout cleanup.
                self.closed = True
                cancellation = error
            except Exception:
                break
        if cancellation is not None:
            if not worker.cancelled():
                worker.exception()
            raise cancellation
        worker.result()

    async def close(self):
        self.closed = True
        await self._drain_restore_worker()

    def _restore_excluded_paths(self, candidate):
        """Undo only this attempt's abandoned paths; never mutate the formal target."""
        if self.plan is None:
            return
        retained = {effect.path for effect in candidate.effects}
        excluded = [effect for effect in self.plan.effects if effect.path not in retained]
        if not excluded:
            return
        self.validate_target()
        self.plan.require_baseline(self.target)
        # Resolve and validate all paths before the first isolated mutation.
        restores = [
            (effect, effect_target(self.target, effect.path), effect_target(self.worktree, effect.path))
            for effect in excluded
        ]
        for effect, original, isolated in restores:
            current = file_digest(isolated)
            if current == effect.original_sha256:
                continue
            if effect.original_sha256 is None:
                isolated.unlink(missing_ok=True)
            else:
                isolated.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(original, isolated)
                if file_digest(isolated) != effect.original_sha256:
                    raise FileEffectPlanError("FILE_EFFECT_BASELINE_CHANGED")
        self.validate_target()
        self.plan.require_baseline(self.target)

    def current_plan(self):
        self._require_open()
        if self.plan is None or self.plan.requirement_head != self.requirement_head:
            raise FileEffectPlanError("FILE_EFFECT_CURRENT_PLAN_REQUIRED")
        return self.plan

    async def before_tool(self, name, arguments):
        self._require_open()
        if name not in {"write_file", "edit_file"}:
            return
        async with self._lock:
            plan = self.current_plan()
            if type(arguments) is not dict or type(arguments.get("file_path")) is not str:
                raise FileEffectPlanError("FILE_EFFECT_WRITE_ARGUMENTS_INVALID")
            supplied = Path(arguments["file_path"])
            if supplied.is_absolute():
                try:
                    relative = supplied.relative_to(self.worktree).as_posix()
                except ValueError:
                    raise FileEffectPlanError("FILE_EFFECT_PATH_ESCAPE") from None
            else:
                relative = arguments["file_path"].replace("\\", "/")
            relative = self._require_writable_path(relative)
            await asyncio.to_thread(effect_target, self.worktree, relative)
            matched = next((effect for effect in plan.effects if effect.path == relative), None)
            if matched is None or matched.operation == "delete":
                raise FileEffectPlanError("FILE_EFFECT_WRITE_NOT_PLANNED")
            await asyncio.to_thread(self.validate_target)
            await asyncio.to_thread(plan.require_baseline, self.target)
            if self.current_plan() != plan:
                raise FileEffectPlanError("FILE_EFFECT_REQUIREMENTS_CHANGED")
