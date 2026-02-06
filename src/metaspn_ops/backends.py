from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .types import Result, RunRecord, Task


class QueueBackend(Protocol):
    worker_name: str

    def enqueue_task(self, task: Task, *, scheduled_for=None) -> Path:
        ...

    def lease_next_task(self, *, owner: str, lease_seconds: int) -> tuple[Task, Path] | None:
        ...

    def ack_task(self, leased_path: Path) -> None:
        ...

    def fail_task(self, leased_path: Path, task: Task, error: str) -> None:
        ...

    def write_result(self, result: Result) -> Path:
        ...

    def write_run_record(self, record: RunRecord) -> Path:
        ...

    def stats(self) -> dict[str, int]:
        ...


class SQLiteQueueStub:
    """Interface placeholder for PRD milestone M3."""

    def __init__(self, worker_name: str, db_path: str | Path):
        self.worker_name = worker_name
        self.db_path = Path(db_path)

    def _not_implemented(self):
        raise NotImplementedError("SQLite backend is a v0.1 stub; implement in M3")

    def enqueue_task(self, task: Task, *, scheduled_for=None) -> Path:
        self._not_implemented()

    def lease_next_task(self, *, owner: str, lease_seconds: int) -> tuple[Task, Path] | None:
        self._not_implemented()

    def ack_task(self, leased_path: Path) -> None:
        self._not_implemented()

    def fail_task(self, leased_path: Path, task: Task, error: str) -> None:
        self._not_implemented()

    def write_result(self, result: Result) -> Path:
        self._not_implemented()

    def write_run_record(self, record: RunRecord) -> Path:
        self._not_implemented()

    def stats(self) -> dict[str, int]:
        self._not_implemented()
