from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.workers import M1JsonlStore, ProfilerWorker, RouterWorker, ScorerWorker
from metaspn_ops.workers.m1 import run_local_m1


class M1WorkerFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.store = M1JsonlStore(self.workspace)

        rows = [
            {
                "id": "em_res_1",
                "emission_type": "entity.resolved",
                "entity_ref": "person:alice",
                "occurred_at": "2026-02-06T12:00:00+00:00",
            },
            {
                "id": "em_res_2",
                "emission_type": "entity.resolved",
                "entity_ref": "person:bob",
                "occurred_at": "2026-02-06T12:01:00+00:00",
            },
        ]
        with self.store.emissions_path.open("a", encoding="utf-8") as f:
            for row in rows:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_profile_score_route_chain_and_retry_safety(self):
        profiler = ProfilerWorker(store=self.store)
        scorer = ScorerWorker(store=self.store)
        router = RouterWorker(store=self.store)

        pq = FilesystemQueue(workspace=self.workspace, worker_name=profiler.name)
        sq = FilesystemQueue(workspace=self.workspace, worker_name=scorer.name)
        rq = FilesystemQueue(workspace=self.workspace, worker_name=router.name)

        pq.enqueue_task(Task(task_id="p1", task_type="profile", payload={"limit": 100}))
        WorkerRunner(queue=pq, worker=profiler, config=RunnerConfig(once=True, max_tasks=1)).run()

        sq.enqueue_task(Task(task_id="s1", task_type="score", payload={"limit": 100}))
        WorkerRunner(queue=sq, worker=scorer, config=RunnerConfig(once=True, max_tasks=1)).run()

        rq.enqueue_task(Task(task_id="r1", task_type="route", payload={"limit": 100}))
        WorkerRunner(queue=rq, worker=router, config=RunnerConfig(once=True, max_tasks=1)).run()

        # Retry same chain should not duplicate persisted outputs.
        pq.enqueue_task(Task(task_id="p2", task_type="profile", payload={"limit": 100}))
        sq.enqueue_task(Task(task_id="s2", task_type="score", payload={"limit": 100}))
        rq.enqueue_task(Task(task_id="r2", task_type="route", payload={"limit": 100}))
        WorkerRunner(queue=pq, worker=profiler, config=RunnerConfig(once=True, max_tasks=1)).run()
        WorkerRunner(queue=sq, worker=scorer, config=RunnerConfig(once=True, max_tasks=1)).run()
        WorkerRunner(queue=rq, worker=router, config=RunnerConfig(once=True, max_tasks=1)).run()

        profiles = [json.loads(l) for l in self.store.profiles_path.read_text().splitlines() if l.strip()]
        scores = [json.loads(l) for l in self.store.scores_path.read_text().splitlines() if l.strip()]
        routes = [json.loads(l) for l in self.store.routes_path.read_text().splitlines() if l.strip()]

        self.assertEqual(len(profiles), 2)
        self.assertEqual(len(scores), 2)
        self.assertEqual(len(routes), 2)

    def test_local_m1_runner(self):
        summary = run_local_m1(workspace=self.workspace, limit=100)
        self.assertEqual(summary["profile_processed"], 1)
        self.assertEqual(summary["score_processed"], 1)
        self.assertEqual(summary["route_processed"], 1)

        self.assertTrue(Path(summary["profiles_path"]).exists())
        self.assertTrue(Path(summary["scores_path"]).exists())
        self.assertTrue(Path(summary["routes_path"]).exists())


if __name__ == "__main__":
    unittest.main()
