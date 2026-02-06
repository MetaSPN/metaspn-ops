from __future__ import annotations

from datetime import datetime, timedelta, timezone


class TaskScheduler:
    def __init__(self, *, base_delay_seconds: int = 5, max_delay_seconds: int = 3600):
        self.base_delay_seconds = max(1, base_delay_seconds)
        self.max_delay_seconds = max(self.base_delay_seconds, max_delay_seconds)

    def next_retry_at(self, *, attempt_count: int, now: datetime | None = None) -> datetime:
        now = now or datetime.now(timezone.utc)
        delay = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** max(0, attempt_count - 1)))
        return now + timedelta(seconds=delay)
