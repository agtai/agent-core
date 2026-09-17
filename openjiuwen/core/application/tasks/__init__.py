# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Persistent task delivery independent of UI, transport and Agent selection.

Applications supply an authorized command, a store and a FormalExecutor. Existing
Agent/team runtimes remain execution owners; this layer owns durable attempts,
outbox delivery, result authority and recovery facts.
"""

from .formal_task_models import FormalTaskSpec, TaskAuthorizationGrant
from .persistent_task_core import FormalExecutor, PersistentTaskCore
from .project_executor import DirectProjectCodeExecutorAdapter, ProjectExecutionApplication, ProjectTaskInvocation
from .source import TaskSourceEvidence, register_source_codec
from .task_result_reader import TaskResultReader
from .task_store import SqliteTaskStore
from .work_runtime import WorkControl, WorkRuntime, WorkSnapshot, WorkState, WorkViolation
from .work_store import SqliteWorkStore

__all__ = [
    "WorkControl",
    "WorkRuntime",
    "WorkSnapshot",
    "WorkState",
    "WorkViolation",
    "SqliteWorkStore",
    "DirectProjectCodeExecutorAdapter",
    "ProjectExecutionApplication",
    "ProjectTaskInvocation",
    "TaskResultReader",
    "FormalExecutor",
    "FormalTaskSpec",
    "PersistentTaskCore",
    "SqliteTaskStore",
    "TaskAuthorizationGrant",
    "TaskSourceEvidence",
    "register_source_codec",
]
