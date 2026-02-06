from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from metaspn_ops.workers.demo import run_demo_once


class DemoFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name)

        self.resolved_path = self.workspace / "resolved_entities.jsonl"
        resolved_rows = [
            {"id": "res-1", "entity_ref": "person:alice", "occurred_at": "2026-02-06T10:00:00+00:00"},
            {"id": "res-2", "entity_ref": "person:bob", "occurred_at": "2026-02-06T10:05:00+00:00"},
        ]
        with self.resolved_path.open("w", encoding="utf-8") as f:
            for row in resolved_rows:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

        self.outcomes_path = self.workspace / "manual_outcomes.jsonl"
        outcome_rows = [
            {
                "id": "out-1",
                "entity_ref": "person:alice",
                "occurred_at": "2026-02-07T10:00:00+00:00",
                "outcome": "manual_reply",
            }
        ]
        with self.outcomes_path.open("w", encoding="utf-8") as f:
            for row in outcome_rows:
                json.dump(row, f, ensure_ascii=True)
                f.write("\n")

    def tearDown(self):
        self.tmp.cleanup()

    def test_demo_run_once_full_cycle_and_idempotency(self):
        first = run_demo_once(
            workspace=self.workspace,
            window_key="2026-02-06",
            limit=100,
            top_n=2,
            channel="email",
            max_attempts=1,
            resolved_entities_jsonl=self.resolved_path,
            outcomes_jsonl=self.outcomes_path,
        )

        self.assertEqual(first["seeded_resolved_entities"]["inserted"], 2)
        self.assertEqual(first["digest_processed"], 1)
        self.assertEqual(first["draft_processed"], 1)
        self.assertEqual(first["manual_outcomes"]["inserted"], 1)

        second = run_demo_once(
            workspace=self.workspace,
            window_key="2026-02-06",
            limit=100,
            top_n=2,
            channel="email",
            max_attempts=1,
            resolved_entities_jsonl=self.resolved_path,
            outcomes_jsonl=self.outcomes_path,
        )

        self.assertEqual(second["seeded_resolved_entities"]["inserted"], 0)
        self.assertGreaterEqual(second["seeded_resolved_entities"]["duplicates"], 2)
        self.assertEqual(second["manual_outcomes"]["inserted"], 0)
        self.assertGreaterEqual(second["manual_outcomes"]["duplicates"], 1)

        profiles = (self.workspace / "store" / "m1_profiles.jsonl").read_text(encoding="utf-8").strip().splitlines()
        scores = (self.workspace / "store" / "m1_scores.jsonl").read_text(encoding="utf-8").strip().splitlines()
        routes = (self.workspace / "store" / "m1_routes.jsonl").read_text(encoding="utf-8").strip().splitlines()
        digests = (self.workspace / "store" / "m2_digests.jsonl").read_text(encoding="utf-8").strip().splitlines()
        drafts = (self.workspace / "store" / "m2_drafts.jsonl").read_text(encoding="utf-8").strip().splitlines()

        self.assertEqual(len(profiles), 2)
        self.assertEqual(len(scores), 2)
        self.assertEqual(len(routes), 2)
        self.assertEqual(len(digests), 1)
        self.assertEqual(len(drafts), 2)


if __name__ == "__main__":
    unittest.main()
