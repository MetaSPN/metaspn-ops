from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from metaspn_ops.fs_queue import FilesystemQueue
from metaspn_ops.types import Result, Task


class FilesystemQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.queue = FilesystemQueue(workspace=self.tmp.name, worker_name="enrich")

    def tearDown(self):
        self.tmp.cleanup()

    def test_lock_correctness_under_concurrency(self):
        task = Task(task_id="t1", task_type="enrich", payload={"x": 1})
        self.queue.enqueue_task(task)

        got = []
        lock = threading.Lock()

        def attempt():
            leased = self.queue.lease_next_task(owner=threading.current_thread().name, lease_seconds=30)
            with lock:
                got.append(leased is not None)

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sum(1 for x in got if x), 1)

    def test_lease_expiration_behavior(self):
        task = Task(task_id="t-lease", task_type="enrich", payload={})
        self.queue.enqueue_task(task)

        first = self.queue.lease_next_task(owner="a", lease_seconds=1)
        self.assertIsNotNone(first)

        time.sleep(1.2)
        second = self.queue.lease_next_task(owner="b", lease_seconds=10)
        self.assertIsNotNone(second)

    def test_retry_scheduling(self):
        task = Task(task_id="t-retry", task_type="enrich", payload={}, max_attempts=3)
        self.queue.enqueue_task(task)
        leased = self.queue.lease_next_task(owner="runner", lease_seconds=10)
        self.assertIsNotNone(leased)
        leased_task, leased_path = leased

        before = datetime.now(timezone.utc)
        self.queue.fail_task(leased_path, leased_task, "boom")

        inbox_files = sorted(self.queue.inbox_dir.glob("*.json"))
        self.assertEqual(len(inbox_files), 1)
        scheduled = inbox_files[0].name.split("__", 1)[0]
        self.assertGreaterEqual(scheduled, before.strftime("%Y-%m-%dT%H%M%SZ"))

        raw = json.loads(inbox_files[0].read_text())
        self.assertEqual(raw["attempt_count"], 1)

    def test_deadletter_handling(self):
        task = Task(task_id="t-dead", task_type="enrich", payload={}, max_attempts=1)
        self.queue.enqueue_task(task)
        leased = self.queue.lease_next_task(owner="runner", lease_seconds=10)
        self.assertIsNotNone(leased)
        leased_task, leased_path = leased

        self.queue.fail_task(leased_path, leased_task, "permanent")

        self.assertEqual(len(list(self.queue.inbox_dir.glob("*.json"))), 0)
        dead = list(self.queue.deadletter_dir.glob("*.json"))
        self.assertEqual(len(dead), 1)

    def test_deterministic_ordering(self):
        base = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.queue.enqueue_task(Task(task_id="c", task_type="x", payload={}), scheduled_for=base + timedelta(seconds=3))
        self.queue.enqueue_task(Task(task_id="a", task_type="x", payload={}), scheduled_for=base + timedelta(seconds=1))
        self.queue.enqueue_task(Task(task_id="b", task_type="x", payload={}), scheduled_for=base + timedelta(seconds=2))

        leased_ids = []
        for _ in range(3):
            leased = self.queue.lease_next_task(owner="runner", lease_seconds=20)
            self.assertIsNotNone(leased)
            t, p = leased
            leased_ids.append(t.task_id)
            self.queue.ack_task(p)

        self.assertEqual(leased_ids, sorted(leased_ids))

    def test_results_roundtrip_json(self):
        path = self.queue.write_result(Result(task_id="t1", status="ok", payload={"x": 1}))
        data = json.loads(path.read_text())
        self.assertEqual(data["task_id"], "t1")
        self.assertEqual(data["payload"]["x"], 1)


if __name__ == "__main__":
    unittest.main()
