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
