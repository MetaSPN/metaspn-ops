from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.workers import HeuristicEntityResolver, IngestSocialWorker, JsonlStoreAdapter, ResolveEntityWorker


class M0WorkerFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.input_path = self.workspace / "sample_social.jsonl"
        rows = [
            {
                "id": "post-1",
                "occurred_at": "2026-02-06T10:00:00+00:00",
                "author_handle": "Alice",
                "text": "hello world",
            },
            {
                "id": "post-2",
                "occurred_at": "2026-02-06T10:01:00+00:00",
                "author_handle": "Bob",
                "text": "second post",
            },
        ]
        with self.input_path.open("w", encoding="utf-8") as f:
            for row in rows:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_ingest_and_resolve_queue_flow(self):
        store = JsonlStoreAdapter(self.workspace)

        ingest_worker = IngestSocialWorker(store=store)
        ingest_queue = FilesystemQueue(workspace=self.workspace, worker_name=ingest_worker.name)

        ingest_queue.enqueue_task(
            Task(
                task_id="ingest-1",
                task_type="ingest_social",
                payload={
                    "input_jsonl_path": str(self.input_path),
                    "source": "social.ingest",
                    "schema_version": "v1",
                },
            )
        )
        WorkerRunner(queue=ingest_queue, worker=ingest_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        # Re-run ingest with same source data: should not persist duplicates.
        ingest_queue.enqueue_task(
            Task(
                task_id="ingest-2",
                task_type="ingest_social",
                payload={
                    "input_jsonl_path": str(self.input_path),
                    "source": "social.ingest",
                    "schema_version": "v1",
                },
            )
        )
        WorkerRunner(queue=ingest_queue, worker=ingest_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        with store.signals_path.open("r", encoding="utf-8") as f:
            signals = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(len(signals), 2)

        resolve_worker = ResolveEntityWorker(store=store, resolver=HeuristicEntityResolver())
        resolve_queue = FilesystemQueue(workspace=self.workspace, worker_name=resolve_worker.name)

        resolve_queue.enqueue_task(Task(task_id="resolve-1", task_type="resolve_entity", payload={"limit": 100}))
        WorkerRunner(queue=resolve_queue, worker=resolve_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        # Re-run resolver: unresolved scan should be empty due to prior emissions.
        resolve_queue.enqueue_task(Task(task_id="resolve-2", task_type="resolve_entity", payload={"limit": 100}))
        WorkerRunner(queue=resolve_queue, worker=resolve_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        with store.emissions_path.open("r", encoding="utf-8") as f:
            emissions = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(len(emissions), 2)

    def test_cli_local_m0_command(self):
        from metaspn_ops.cli import main

        exit_code = main(
            [
                "m0",
                "run-local",
                "--workspace",
                str(self.workspace),
                "--input-jsonl",
                str(self.input_path),
            ]
        )
        self.assertEqual(exit_code, 0)

        signals_path = self.workspace / "store" / "signals.jsonl"
        emissions_path = self.workspace / "store" / "emissions.jsonl"
        self.assertTrue(signals_path.exists())
        self.assertTrue(emissions_path.exists())


if __name__ == "__main__":
    unittest.main()
