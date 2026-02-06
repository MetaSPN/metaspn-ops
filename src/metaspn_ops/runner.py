from __future__ import annotations

import concurrent.futures
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import uuid4

from .backends import QueueBackend
from .types import Result, RunRecord, Task


class Worker(Protocol):
    name: str

    def handle(self, task: Task) -> Result:
        ...


@dataclass(slots=True)
class RunnerConfig:
    every_seconds: int | None = None
    max_tasks: int | None = None
    parallel: int = 1
    lease_seconds: int = 120
    once: bool = False
    poll_interval_seconds: float = 0.5


class WorkerRunner:
    def __init__(self, *, queue: QueueBackend, worker: Worker, config: RunnerConfig | None = None):
        self.queue = queue
        self.worker = worker
        self.config = config or RunnerConfig()
        self.owner = f"{socket.gethostname()}:{uuid4().hex[:8]}"

    def run(self) -> int:
        if self.config.once:
            return self._run_batch()

        processed = 0
        while True:
            start = time.monotonic()
            processed += self._run_batch()
            if self.config.every_seconds is None:
                time.sleep(self.config.poll_interval_seconds)
                continue
            elapsed = time.monotonic() - start
            sleep_for = max(0.0, self.config.every_seconds - elapsed)
            time.sleep(sleep_for)
        return processed

    def _run_batch(self) -> int:
        target = self.config.max_tasks or 1
        processed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.config.parallel)) as pool:
            futures = []
            for _ in range(target):
                leased = self.queue.lease_next_task(owner=self.owner, lease_seconds=self.config.lease_seconds)
                if leased is None:
                    break
                task, path = leased
                futures.append(pool.submit(self._process_one, task, path))

            for future in concurrent.futures.as_completed(futures):
                processed += 1
                future.result()

        return processed

    def _process_one(self, task: Task, path):
        started = datetime.now(timezone.utc)
        error = None
        status = "ok"
        try:
            result = self.worker.handle(task)
            if result.task_id != task.task_id:
                result.task_id = task.task_id
            self.queue.write_result(result)
            self.queue.ack_task(path)
        except Exception as exc:
            status = "error"
            error = str(exc)
            self.queue.fail_task(path, task, error)
            self.queue.write_result(
                Result(
                    task_id=task.task_id,
                    status="error",
                    payload={},
                    error=error,
                    trace_context=task.trace_context,
                )
            )
        finally:
            finished = datetime.now(timezone.utc)
            duration_ms = int((finished - started).total_seconds() * 1000)
            self.queue.write_run_record(
                RunRecord(
                    run_id=uuid4().hex,
                    worker_name=self.worker.name,
                    task_id=task.task_id,
                    started_at=started.isoformat(),
                    finished_at=finished.isoformat(),
                    duration_ms=duration_ms,
                    status=status,
                    error=error,
                    trace_context=task.trace_context,
                )
            )
