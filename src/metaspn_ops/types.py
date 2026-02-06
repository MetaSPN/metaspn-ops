from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(slots=True)
class Task:
    task_id: str
    task_type: str
    payload: dict[str, Any]
    trace_context: dict[str, Any] = field(default_factory=dict)
    attempt_count: int = 0
    max_attempts: int = 3
    created_at: str = field(default_factory=lambda: utc_now().isoformat())

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Task":
        return cls(
            task_id=str(raw["task_id"]),
            task_type=str(raw.get("task_type", "unknown")),
            payload=dict(raw.get("payload", {})),
            trace_context=dict(raw.get("trace_context", {})),
            attempt_count=int(raw.get("attempt_count", 0)),
            max_attempts=int(raw.get("max_attempts", 3)),
            created_at=str(raw.get("created_at", utc_now().isoformat())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "trace_context": self.trace_context,
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "created_at": self.created_at,
        }


@dataclass(slots=True)
class Result:
    task_id: str
    status: str
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    trace_context: dict[str, Any] = field(default_factory=dict)
    produced_at: str = field(default_factory=lambda: utc_now().isoformat())

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Result":
        return cls(
            task_id=str(raw["task_id"]),
            status=str(raw.get("status", "ok")),
            payload=dict(raw.get("payload", {})),
            error=raw.get("error"),
            trace_context=dict(raw.get("trace_context", {})),
            produced_at=str(raw.get("produced_at", utc_now().isoformat())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "payload": self.payload,
            "error": self.error,
            "trace_context": self.trace_context,
            "produced_at": self.produced_at,
        }


@dataclass(slots=True)
class RunRecord:
    run_id: str
    worker_name: str
    task_id: str
    started_at: str
    finished_at: str
    duration_ms: int
    status: str
    error: str | None
    trace_context: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "worker_name": self.worker_name,
            "task_id": self.task_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error": self.error,
            "trace_context": self.trace_context,
        }
