from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.workers import (
    CalibrationReporterWorker,
    DigestWorker,
    M2JsonlStore,
    M3JsonlStore,
    PromiseCalibrationWorker,
    PromiseEvaluatorWorker,
    ResolveTokenWorker,
    TokenHealthScorerWorker,
    TokenPromiseStore,
)
from metaspn_ops.workers.token_promises import run_local_token_promises


class TokenPromiseWorkersTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.store = TokenPromiseStore(self.workspace)

        token_signals = [
            {"id": "sig-t1", "token_ref": "ABC", "network": "ethereum", "occurred_at": "2026-02-06T10:00:00+00:00"},
            {"id": "sig-t2", "token_ref": "XYZ", "network": "other", "occurred_at": "2026-02-06T11:00:00+00:00"},
        ]
        promises = [
            {
                "id": "promise-1",
                "evaluation_path": "on_chain",
                "expected_success": True,
                "on_chain_success": True,
            },
            {
                "id": "promise-2",
                "evaluation_path": "observable_signal",
                "expected_success": True,
                "observable_signal_success": False,
            },
            {
                "id": "promise-3",
                "evaluation_path": "human_judgment",
                "expected_success": True,
            },
        ]

        with self.store.token_signals_path.open("a", encoding="utf-8") as f:
            for row in token_signals:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

        with self.store.promises_path.open("a", encoding="utf-8") as f:
            for row in promises:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_token_signal_to_promise_calibration_pipeline(self):
        summary1 = run_local_token_promises(
            workspace=self.workspace,
            window_key="2026-02-06",
            limit=100,
            baseline_weight=1.0,
        )
        self.assertEqual(summary1["resolve_processed"], 1)
        self.assertEqual(summary1["score_processed"], 1)
        self.assertEqual(summary1["eval_processed"], 1)
        self.assertEqual(summary1["calib_processed"], 1)

        summary2 = run_local_token_promises(
            workspace=self.workspace,
            window_key="2026-02-06",
            limit=100,
            baseline_weight=1.0,
        )

        resolutions = [json.loads(l) for l in self.store.token_resolutions_path.read_text().splitlines() if l.strip()]
        scores = [json.loads(l) for l in self.store.token_health_scores_path.read_text().splitlines() if l.strip()]
        evals = [json.loads(l) for l in self.store.promise_evaluations_path.read_text().splitlines() if l.strip()]
        manual = [json.loads(l) for l in self.store.promise_manual_reviews_path.read_text().splitlines() if l.strip()]
        calib = [json.loads(l) for l in self.store.promise_calibration_reports_path.read_text().splitlines() if l.strip()]

        self.assertEqual(len(resolutions), 2)
        self.assertEqual(len(scores), 2)
        self.assertEqual(len(evals), 2)
        self.assertEqual(len(manual), 1)
        self.assertEqual(len(calib), 1)
        self.assertEqual(calib[0]["predictive_accuracy"], 0.5)
        self.assertEqual(calib[0]["proposed_weight_adjustments"]["promise_accuracy_weight"]["strategy"], "no_change")

        self.assertEqual(summary2["resolve_processed"], 1)

    def test_digest_optional_token_and_promise_entries(self):
        m2_store = M2JsonlStore(self.workspace)
        with m2_store.routes_path.open("a", encoding="utf-8") as f:
            json.dump(
                {
                    "id": "route-1",
                    "entity_ref": "person:alice",
                    "score": 80,
                    "priority": "high",
                    "playbook": "hot_lead_followup",
                },
                f,
                ensure_ascii=True,
            )
            f.write("\n")

        # Seed token/promise artifacts for digest inclusion.
        with self.store.token_health_scores_path.open("a", encoding="utf-8") as f:
            json.dump({"id": "ths-1", "token_id": "tok:abc", "network": "ethereum", "health_score": 88.0}, f, ensure_ascii=True)
            f.write("\n")
        with self.store.promise_evaluations_path.open("a", encoding="utf-8") as f:
            json.dump({"id": "pe-1", "promise_id": "promise-1", "evaluation_path": "on_chain", "correct": True}, f, ensure_ascii=True)
            f.write("\n")
        with self.store.promise_manual_reviews_path.open("a", encoding="utf-8") as f:
            json.dump({"id": "pm-1", "promise_id": "promise-x", "decision": "pending"}, f, ensure_ascii=True)
            f.write("\n")

        digest_worker = DigestWorker(store=m2_store)
        q = FilesystemQueue(workspace=self.workspace, worker_name=digest_worker.name)
        q.enqueue_task(
            Task(
                task_id="digest-token",
                task_type="digest",
                payload={
                    "window_key": "2026-02-06",
                    "top_n": 5,
                    "include_token_health": True,
                    "include_promise_items": True,
                },
            )
        )
        WorkerRunner(queue=q, worker=digest_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        digest = [json.loads(l) for l in m2_store.digests_path.read_text().splitlines() if l.strip()][0]
        self.assertEqual(len(digest.get("token_health", [])), 1)
        self.assertEqual(len(digest.get("promise_items", {}).get("evaluated", [])), 1)
        self.assertEqual(len(digest.get("promise_items", {}).get("manual_pending", [])), 1)

    def test_m3_calibration_report_includes_promise_metrics(self):
        m3_store = M3JsonlStore(self.workspace)
        with m3_store.attempts_path.open("a", encoding="utf-8") as f:
            json.dump(
                {
                    "id": "att-1",
                    "entity_ref": "person:alice",
                    "occurred_at": "2026-02-01T10:00:00+00:00",
                    "priority": "high",
                    "channel": "email",
                    "score": 80,
                },
                f,
                ensure_ascii=True,
            )
            f.write("\n")
        with m3_store.outcomes_path.open("a", encoding="utf-8") as f:
            json.dump(
                {
                    "id": "out-1",
                    "entity_ref": "person:alice",
                    "occurred_at": "2026-02-01T12:00:00+00:00",
                    "outcome": "reply",
                },
                f,
                ensure_ascii=True,
            )
            f.write("\n")

        # Seed promise calibration artifact.
        with m3_store.promise_evaluations_path.open("a", encoding="utf-8") as f:
            json.dump({"id": "pe-1", "correct": True}, f, ensure_ascii=True)
            f.write("\n")
            json.dump({"id": "pe-2", "correct": False}, f, ensure_ascii=True)
            f.write("\n")
        with m3_store.promise_calibration_reports_path.open("a", encoding="utf-8") as f:
            json.dump(
                {
                    "id": "pcal-1",
                    "proposed_weight_adjustments": {"promise_accuracy_weight": {"from": 1.0, "to": 0.9}},
                },
                f,
                ensure_ascii=True,
            )
            f.write("\n")

        worker = CalibrationReporterWorker(store=m3_store)
        q = FilesystemQueue(workspace=self.workspace, worker_name=worker.name)
        q.enqueue_task(
            Task(
                task_id="m3-calib",
                task_type="report",
                payload={"window_start": "2026-02-01T00:00:00+00:00", "window_end": "2026-02-03T00:00:00+00:00"},
            )
        )
        WorkerRunner(queue=q, worker=worker, config=RunnerConfig(once=True, max_tasks=1)).run()

        rows = [json.loads(l) for l in m3_store.calibration_reports_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["promise_predictive_accuracy"], 0.5)
        self.assertIsNotNone(rows[0]["promise_weight_adjustment_proposal"])


if __name__ == "__main__":
    unittest.main()
