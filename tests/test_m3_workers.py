from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.workers import (
    CalibrationReporterWorker,
    CalibrationReviewWorker,
    FailureAnalystWorker,
    M3JsonlStore,
    OutcomeEvaluatorWorker,
)
from metaspn_ops.workers.m3 import run_local_m3


class M3WorkerFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.store = M3JsonlStore(self.workspace)

        attempts = [
            {
                "id": "att-1",
                "entity_ref": "person:alice",
                "occurred_at": "2026-02-01T10:00:00+00:00",
                "priority": "high",
                "channel": "email",
                "score": 80,
            },
            {
                "id": "att-2",
                "entity_ref": "person:bob",
                "occurred_at": "2026-02-01T11:00:00+00:00",
                "priority": "medium",
                "channel": "email",
                "score": 45,
            },
        ]
        outcomes = [
            {
                "id": "out-1",
                "entity_ref": "person:alice",
                "occurred_at": "2026-02-02T09:00:00+00:00",
                "outcome": "reply",
            }
        ]

        with self.store.attempts_path.open("a", encoding="utf-8") as f:
            for row in attempts:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

        with self.store.outcomes_path.open("a", encoding="utf-8") as f:
            for row in outcomes:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_attempt_outcome_failure_calibration_pipeline(self):
        evaluator = OutcomeEvaluatorWorker(store=self.store)
        analyst = FailureAnalystWorker(store=self.store)
        reporter = CalibrationReporterWorker(store=self.store)
        reviewer = CalibrationReviewWorker(store=self.store)

        eq = FilesystemQueue(workspace=self.workspace, worker_name=evaluator.name)
        fq = FilesystemQueue(workspace=self.workspace, worker_name=analyst.name)
        cq = FilesystemQueue(workspace=self.workspace, worker_name=reporter.name)
        rq = FilesystemQueue(workspace=self.workspace, worker_name=reviewer.name)

        payload = {
            "window_start": "2026-02-01T00:00:00+00:00",
            "window_end": "2026-02-03T00:00:00+00:00",
            "success_within_hours": 48,
        }

        eq.enqueue_task(Task(task_id="eval-1", task_type="evaluate", payload=payload))
        WorkerRunner(queue=eq, worker=evaluator, config=RunnerConfig(once=True, max_tasks=1)).run()
        eq.enqueue_task(Task(task_id="eval-2", task_type="evaluate", payload=payload))
        WorkerRunner(queue=eq, worker=evaluator, config=RunnerConfig(once=True, max_tasks=1)).run()

        evals = [json.loads(l) for l in self.store.evaluations_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(evals), 2)

        fq.enqueue_task(
            Task(
                task_id="fail-1",
                task_type="analyze",
                payload={"window_start": payload["window_start"], "window_end": payload["window_end"]},
            )
        )
        WorkerRunner(queue=fq, worker=analyst, config=RunnerConfig(once=True, max_tasks=1)).run()
        fq.enqueue_task(
            Task(
                task_id="fail-2",
                task_type="analyze",
                payload={"window_start": payload["window_start"], "window_end": payload["window_end"]},
            )
        )
        WorkerRunner(queue=fq, worker=analyst, config=RunnerConfig(once=True, max_tasks=1)).run()

        failures = [json.loads(l) for l in self.store.failures_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(failures), 1)

        cq.enqueue_task(
            Task(
                task_id="calib-1",
                task_type="report",
                payload={"window_start": payload["window_start"], "window_end": payload["window_end"]},
            )
        )
        WorkerRunner(queue=cq, worker=reporter, config=RunnerConfig(once=True, max_tasks=1)).run()

        reports = [json.loads(l) for l in self.store.calibration_reports_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["total_evaluations"], 2)
        self.assertEqual(reports[0]["failures"], 1)

        rq.enqueue_task(
            Task(
                task_id="review-1",
                task_type="review",
                payload={"report_id": reports[0]["id"], "decision": "approve", "reviewer": "ops"},
            )
        )
        WorkerRunner(queue=rq, worker=reviewer, config=RunnerConfig(once=True, max_tasks=1)).run()

        reviews = [json.loads(l) for l in self.store.calibration_reviews_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["decision"], "approve")
        self.assertIsNotNone(reviews[0]["adopted_policy"])

    def test_m3_local_runner(self):
        summary = run_local_m3(
            workspace=self.workspace,
            window_start="2026-02-01T00:00:00+00:00",
            window_end="2026-02-03T00:00:00+00:00",
            success_within_hours=48,
            auto_review_decision="approve",
        )
        self.assertEqual(summary["eval_processed"], 1)
        self.assertEqual(summary["fail_processed"], 1)
        self.assertEqual(summary["report_processed"], 1)
        self.assertEqual(summary["review_processed"], 1)
        self.assertTrue(Path(summary["calibration_reports_path"]).exists())


if __name__ == "__main__":
    unittest.main()
