from __future__ import annotations

import unittest

import metaspn_ops


class PublicApiTests(unittest.TestCase):
    def test_sqlite_queue_stub_removed(self):
        self.assertFalse(hasattr(metaspn_ops, "SQLiteQueueStub"))
        self.assertNotIn("SQLiteQueueStub", getattr(metaspn_ops, "__all__", []))


if __name__ == "__main__":
    unittest.main()
