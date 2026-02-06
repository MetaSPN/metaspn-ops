from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from metaspn_ops.lease import LeaseManager


class LeaseRaceTests(unittest.TestCase):
    def test_transient_parse_error_not_treated_as_expired(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = LeaseManager(Path(tmp))
            lock_path = Path(tmp) / "t1.lock"
            lock_path.write_text("{\"expires_at\":", encoding="utf-8")

            lease = manager.try_acquire(task_id="t1", worker_name="w", owner="o2", lease_seconds=30)
            self.assertIsNone(lease)

    def test_expired_lock_can_be_reacquired(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = LeaseManager(Path(tmp))
            lock_path = Path(tmp) / "t2.lock"
            expired = {
                "task_id": "t2",
                "worker_name": "w",
                "owner": "o1",
                "acquired_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat(),
            }
            lock_path.write_text(json.dumps(expired), encoding="utf-8")

            lease = manager.try_acquire(task_id="t2", worker_name="w", owner="o2", lease_seconds=30)
            self.assertIsNotNone(lease)
            self.assertEqual(lease.owner, "o2")


if __name__ == "__main__":
    unittest.main()
