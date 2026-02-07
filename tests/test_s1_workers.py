from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.cli import main
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.types import Result
from metaspn_ops.workers import (
    ProjectRewardsWorker,
    PublishSeasonSummaryWorker,
    S1JsonlStore,
    SettleSeasonWorker,
    UpdateAttentionScoresWorker,
)
from metaspn_ops.workers.s1 import run_local_s1


class _AlwaysFailProjectWorker:
    name = "project_rewards"

    def handle(self, task: Task) -> Result:
        raise RuntimeError(f"project failure for {task.payload.get('date')}")


class S1WorkerFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.store = S1JsonlStore(self.workspace)

        # Seed M1 scores as S1 input artifact.
        scores = [
            {
                "id": "score-alice",
                "entity_ref": "person:alice",
                "score": 95,
                "model": "heuristic.v1",
                "occurred_at": "2026-02-06T10:00:00+00:00",
            },
            {
                "id": "score-bob",
                "entity_ref": "person:bob",
                "score": 64,
                "model": "heuristic.v1",
                "occurred_at": "2026-02-06T10:01:00+00:00",
            },
        ]
        with self.store.m1_scores_path.open("a", encoding="utf-8") as f:
            for row in scores:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

        self.date_key = "2026-02-07"

    def tearDown(self):
        self.tmp.cleanup()

    def test_s1_pipeline_idempotency(self):
        update = UpdateAttentionScoresWorker(store=self.store)
        project = ProjectRewardsWorker(store=self.store)
        settle = SettleSeasonWorker(store=self.store)
        publish = PublishSeasonSummaryWorker(store=self.store)

        q_update = FilesystemQueue(workspace=self.workspace, worker_name=update.name)
        q_project = FilesystemQueue(workspace=self.workspace, worker_name=project.name)
        q_settle = FilesystemQueue(workspace=self.workspace, worker_name=settle.name)
        q_publish = FilesystemQueue(workspace=self.workspace, worker_name=publish.name)

        for suffix in ["1", "2"]:
            q_update.enqueue_task(Task(task_id=f"update-{suffix}", task_type="update", payload={"date": self.date_key}))
            WorkerRunner(queue=q_update, worker=update, config=RunnerConfig(once=True, max_tasks=1)).run()

            q_project.enqueue_task(Task(task_id=f"project-{suffix}", task_type="project", payload={"date": self.date_key}))
            WorkerRunner(queue=q_project, worker=project, config=RunnerConfig(once=True, max_tasks=1)).run()

            q_settle.enqueue_task(Task(task_id=f"settle-{suffix}", task_type="settle", payload={"date": self.date_key}))
            WorkerRunner(queue=q_settle, worker=settle, config=RunnerConfig(once=True, max_tasks=1)).run()

            q_publish.enqueue_task(Task(task_id=f"publish-{suffix}", task_type="publish", payload={"date": self.date_key}))
            WorkerRunner(queue=q_publish, worker=publish, config=RunnerConfig(once=True, max_tasks=1)).run()

        attention = [json.loads(l) for l in self.store.attention_scores_path.read_text().splitlines() if l.strip()]
        projections = [json.loads(l) for l in self.store.reward_projections_path.read_text().splitlines() if l.strip()]
        settlements = [json.loads(l) for l in self.store.settlements_path.read_text().splitlines() if l.strip()]
        summaries = [json.loads(l) for l in self.store.summaries_path.read_text().splitlines() if l.strip()]

        self.assertEqual(len(attention), 2)
        self.assertEqual(len(projections), 2)
        self.assertEqual(len(settlements), 2)
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["season_date"], self.date_key)

    def test_s1_local_runner_and_cli(self):
        first = run_local_s1(workspace=self.workspace, date=self.date_key)
        second = run_local_s1(workspace=self.workspace, date=self.date_key)

        self.assertEqual(first["update_processed"], 1)
        self.assertEqual(first["project_processed"], 1)
        self.assertEqual(first["settle_processed"], 1)
        self.assertEqual(first["publish_processed"], 1)
        self.assertEqual(second["publish_processed"], 1)

        attention = [json.loads(l) for l in Path(first["attention_scores_path"]).read_text().splitlines() if l.strip()]
        projections = [json.loads(l) for l in Path(first["reward_projections_path"]).read_text().splitlines() if l.strip()]
        settlements = [json.loads(l) for l in Path(first["settlements_path"]).read_text().splitlines() if l.strip()]
        summaries = [json.loads(l) for l in Path(first["summaries_path"]).read_text().splitlines() if l.strip()]

        self.assertEqual(len(attention), 2)
        self.assertEqual(len(projections), 2)
        self.assertEqual(len(settlements), 2)
        self.assertEqual(len(summaries), 1)

        exit_code = main(["s1", "run-local", "--workspace", str(self.workspace), "--date", self.date_key])
        self.assertEqual(exit_code, 0)

    def test_s1_retry_backoff_and_deadletter_integration(self):
        worker = _AlwaysFailProjectWorker()
        queue = FilesystemQueue(workspace=self.workspace, worker_name=worker.name)
        queue.enqueue_task(Task(task_id="s1-fail", task_type="project", payload={"date": self.date_key}, max_attempts=2))

        before = datetime.now(timezone.utc)
        WorkerRunner(queue=queue, worker=worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        inbox_files = sorted(queue.inbox_dir.glob("*.json"))
        self.assertEqual(len(inbox_files), 1)
        retry_name = inbox_files[0].name.split("__", 1)[0]
        self.assertGreaterEqual(retry_name, before.strftime("%Y-%m-%dT%H%M%SZ"))

        retry_payload = json.loads(inbox_files[0].read_text(encoding="utf-8"))
        self.assertEqual(retry_payload["attempt_count"], 1)

        time.sleep(5.2)
        WorkerRunner(queue=queue, worker=worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        self.assertEqual(len(list(queue.inbox_dir.glob("*.json"))), 0)
        deadletter = list(queue.deadletter_dir.glob("*.json"))
        self.assertEqual(len(deadletter), 1)


if __name__ == "__main__":
    unittest.main()
