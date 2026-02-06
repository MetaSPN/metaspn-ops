from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops import FilesystemQueue, Task, WorkerRunner
from metaspn_ops.runner import RunnerConfig
from metaspn_ops.workers import ApprovalWorker, DigestWorker, DrafterWorker, M2JsonlStore
from metaspn_ops.workers.m2 import run_local_m2


class M2WorkerFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)
        self.store = M2JsonlStore(self.workspace)

        # Seed recommendation candidates from M1 routes.
        rows = [
            {"id": "route1", "entity_ref": "person:bob", "score": 70, "priority": "medium", "playbook": "nurture_sequence"},
            {"id": "route2", "entity_ref": "person:alice", "score": 92, "priority": "high", "playbook": "hot_lead_followup"},
            {"id": "route3", "entity_ref": "person:zoe", "score": 40, "priority": "low", "playbook": "low_touch_watch"},
        ]
        with self.store.routes_path.open("a", encoding="utf-8") as f:
            for row in rows:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_recommend_digest_draft_approval_pipeline_and_retry_safety(self):
        digest = DigestWorker(store=self.store)
        drafter = DrafterWorker(store=self.store)
        approval = ApprovalWorker(store=self.store)

        dq = FilesystemQueue(workspace=self.workspace, worker_name=digest.name)
        rq = FilesystemQueue(workspace=self.workspace, worker_name=drafter.name)
        aq = FilesystemQueue(workspace=self.workspace, worker_name=approval.name)

        dq.enqueue_task(Task(task_id="d1", task_type="digest", payload={"window_key": "2026-02-06", "top_n": 2}))
        WorkerRunner(queue=dq, worker=digest, config=RunnerConfig(once=True, max_tasks=1)).run()

        # Retry same digest input: deterministic ID and duplicate-safe write.
        dq.enqueue_task(Task(task_id="d2", task_type="digest", payload={"window_key": "2026-02-06", "top_n": 2}))
        WorkerRunner(queue=dq, worker=digest, config=RunnerConfig(once=True, max_tasks=1)).run()

        digests = [json.loads(l) for l in self.store.digests_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(digests), 1)
        self.assertEqual(digests[0]["items"][0]["entity_ref"], "person:alice")

        digest_id = digests[0]["id"]
        rq.enqueue_task(Task(task_id="r1", task_type="draft", payload={"digest_id": digest_id, "channel": "email"}))
        WorkerRunner(queue=rq, worker=drafter, config=RunnerConfig(once=True, max_tasks=1)).run()
        rq.enqueue_task(Task(task_id="r2", task_type="draft", payload={"digest_id": digest_id, "channel": "email"}))
        WorkerRunner(queue=rq, worker=drafter, config=RunnerConfig(once=True, max_tasks=1)).run()

        drafts = [json.loads(l) for l in self.store.drafts_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(drafts), 2)

        first_draft = drafts[0]
        aq.enqueue_task(
            Task(
                task_id="a1",
                task_type="approval",
                payload={
                    "draft_id": first_draft["id"],
                    "decision": "edit",
                    "override_body": "Custom human-approved message.",
                    "editor": "tester",
                },
            )
        )
        WorkerRunner(queue=aq, worker=approval, config=RunnerConfig(once=True, max_tasks=1)).run()

        # Same edit again should not duplicate approval record.
        aq.enqueue_task(
            Task(
                task_id="a2",
                task_type="approval",
                payload={
                    "draft_id": first_draft["id"],
                    "decision": "edit",
                    "override_body": "Custom human-approved message.",
                    "editor": "tester",
                },
            )
        )
        WorkerRunner(queue=aq, worker=approval, config=RunnerConfig(once=True, max_tasks=1)).run()

        approvals = [json.loads(l) for l in self.store.approvals_path.read_text().splitlines() if l.strip()]
        self.assertEqual(len(approvals), 1)
        self.assertEqual(approvals[0]["decision"], "edit")
        self.assertEqual(approvals[0]["final_body"], "Custom human-approved message.")

    def test_m2_cli_local_runner(self):
        summary = run_local_m2(
            workspace=self.workspace,
            window_key="2026-02-06",
            top_n=2,
            channel="linkedin",
        )
        self.assertEqual(summary["digest_processed"], 1)
        self.assertEqual(summary["draft_processed"], 1)
        self.assertTrue(Path(summary["digests_path"]).exists())
        self.assertTrue(Path(summary["drafts_path"]).exists())


if __name__ == "__main__":
    unittest.main()
