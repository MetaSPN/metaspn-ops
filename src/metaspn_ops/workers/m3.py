from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..fs_queue import FilesystemQueue
from ..runner import RunnerConfig, WorkerRunner
from ..types import Result, Task


def _stable_hash(parts: list[str]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(raw: str) -> datetime:
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(slots=True)
class M3JsonlStore:
    workspace: Path
    store_dir: Path = field(init=False)
    attempts_path: Path = field(init=False)
    outcomes_path: Path = field(init=False)
    evaluations_path: Path = field(init=False)
    failures_path: Path = field(init=False)
    calibration_reports_path: Path = field(init=False)
    calibration_reviews_path: Path = field(init=False)
    promise_evaluations_path: Path = field(init=False)
    promise_calibration_reports_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.attempts_path = self.store_dir / "m3_attempts.jsonl"
        self.outcomes_path = self.store_dir / "m3_outcomes.jsonl"
        self.evaluations_path = self.store_dir / "m3_evaluations.jsonl"
        self.failures_path = self.store_dir / "m3_failures.jsonl"
        self.calibration_reports_path = self.store_dir / "m3_calibration_reports.jsonl"
        self.calibration_reviews_path = self.store_dir / "m3_calibration_reviews.jsonl"
        self.promise_evaluations_path = self.store_dir / "promise_evaluations.jsonl"
        self.promise_calibration_reports_path = self.store_dir / "promise_calibration_reports.jsonl"

    def attempts_in_window(self, *, window_start: str, window_end: str) -> list[dict[str, Any]]:
        start = _parse_iso(window_start)
        end = _parse_iso(window_end)
        out = []
        for row in self._read_jsonl(self.attempts_path):
            ts = _parse_iso(row["occurred_at"])
            if start <= ts <= end:
                out.append(row)
        return sorted(out, key=lambda r: (str(r.get("occurred_at")), str(r.get("id"))))

    def outcomes_for_entity(self, entity_ref: str) -> list[dict[str, Any]]:
        rows = [r for r in self._read_jsonl(self.outcomes_path) if str(r.get("entity_ref")) == str(entity_ref)]
        return sorted(rows, key=lambda r: (str(r.get("occurred_at")), str(r.get("id"))))

    def failed_evaluations(self, *, window_start: str, window_end: str) -> list[dict[str, Any]]:
        start = _parse_iso(window_start)
        end = _parse_iso(window_end)
        out = []
        for row in self._read_jsonl(self.evaluations_path):
            evaluated_at = _parse_iso(row["window_end"])
            if not row.get("success") and start <= evaluated_at <= end:
                out.append(row)
        return sorted(out, key=lambda r: str(r.get("id")))

    def evaluations_in_window(self, *, window_start: str, window_end: str) -> list[dict[str, Any]]:
        start = _parse_iso(window_start)
        end = _parse_iso(window_end)
        out = []
        for row in self._read_jsonl(self.evaluations_path):
            evaluated_at = _parse_iso(row["window_end"])
            if start <= evaluated_at <= end:
                out.append(row)
        return sorted(out, key=lambda r: str(r.get("id")))

    def latest_calibration_report(self) -> dict[str, Any] | None:
        rows = self._read_jsonl(self.calibration_reports_path)
        if not rows:
            return None
        return rows[-1]

    def find_calibration_report(self, report_id: str) -> dict[str, Any] | None:
        for row in self._read_jsonl(self.calibration_reports_path):
            if str(row.get("id")) == str(report_id):
                return row
        return None

    def write_evaluation_if_absent(self, row: dict[str, Any]) -> bool:
        return self._write_if_absent(self.evaluations_path, row)

    def write_failure_if_absent(self, row: dict[str, Any]) -> bool:
        return self._write_if_absent(self.failures_path, row)

    def write_calibration_report_if_absent(self, row: dict[str, Any]) -> bool:
        return self._write_if_absent(self.calibration_reports_path, row)

    def write_calibration_review_if_absent(self, row: dict[str, Any]) -> bool:
        return self._write_if_absent(self.calibration_reviews_path, row)

    def _write_if_absent(self, path: Path, payload: dict[str, Any]) -> bool:
        rec_id = str(payload["id"])
        if self._id_exists(path, rec_id):
            return False
        self._append_jsonl(path, payload)
        return True

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True)
            f.write("\n")

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return rows

    def _id_exists(self, path: Path, rec_id: str) -> bool:
        if not path.exists():
            return False
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                if str(row.get("id")) == rec_id:
                    return True
        return False


@dataclass(slots=True)
class OutcomeEvaluatorWorker:
    store: M3JsonlStore
    name: str = "evaluate_outcomes"

    def handle(self, task: Task) -> Result:
        window_start = str(task.payload["window_start"])
        window_end = str(task.payload["window_end"])
        success_within_hours = int(task.payload.get("success_within_hours", 72))

        attempts = self.store.attempts_in_window(window_start=window_start, window_end=window_end)
        evaluated = 0
        successes = 0
        duplicates = 0

        for attempt in attempts:
            attempt_at = _parse_iso(attempt["occurred_at"])
            deadline = attempt_at + timedelta(hours=success_within_hours)
            outcomes = self.store.outcomes_for_entity(str(attempt.get("entity_ref")))

            matched = None
            for outcome in outcomes:
                outcome_at = _parse_iso(outcome["occurred_at"])
                if attempt_at <= outcome_at <= deadline:
                    matched = outcome
                    break

            evaluation_id = f"eval_{_stable_hash([str(attempt['id']), window_end, str(success_within_hours)])}"
            row = {
                "id": evaluation_id,
                "attempt_id": attempt["id"],
                "entity_ref": attempt.get("entity_ref"),
                "window_start": window_start,
                "window_end": window_end,
                "success_within_hours": success_within_hours,
                "success": matched is not None,
                "matched_outcome_id": matched.get("id") if matched else None,
                "attempt": attempt,
                "occurred_at": _utc_now_iso(),
            }
            wrote = self.store.write_evaluation_if_absent(row)
            evaluated += 1
            if row["success"]:
                successes += 1
            if not wrote:
                duplicates += 1

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "evaluated": evaluated,
                "successes": successes,
                "failures": evaluated - successes,
                "duplicates": duplicates,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class FailureAnalystWorker:
    store: M3JsonlStore
    name: str = "analyze_failures"

    def handle(self, task: Task) -> Result:
        window_start = str(task.payload["window_start"])
        window_end = str(task.payload["window_end"])

        failures = self.store.failed_evaluations(window_start=window_start, window_end=window_end)
        labeled = 0
        duplicates = 0

        for ev in failures:
            attempt = ev.get("attempt") or {}
            priority = str(attempt.get("priority", "")).lower()
            channel = str(attempt.get("channel", "")).lower()
            score = float(attempt.get("score", 0))

            if priority == "high":
                taxonomy = "high_priority_miss"
            elif score < 50:
                taxonomy = "low_signal_quality"
            elif channel == "email":
                taxonomy = "channel_mismatch"
            else:
                taxonomy = "unknown_failure"

            failure_id = f"failure_{_stable_hash([str(ev['id']), taxonomy])}"
            row = {
                "id": failure_id,
                "evaluation_id": ev["id"],
                "attempt_id": ev.get("attempt_id"),
                "entity_ref": ev.get("entity_ref"),
                "taxonomy": taxonomy,
                "evidence": {
                    "priority": priority,
                    "channel": channel,
                    "score": score,
                    "matched_outcome_id": ev.get("matched_outcome_id"),
                },
                "window_start": window_start,
                "window_end": window_end,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_failure_if_absent(row):
                labeled += 1
            else:
                duplicates += 1

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"labeled": labeled, "duplicates": duplicates, "considered": len(failures)},
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class CalibrationReporterWorker:
    store: M3JsonlStore
    name: str = "report_calibration"

    def handle(self, task: Task) -> Result:
        window_start = str(task.payload["window_start"])
        window_end = str(task.payload["window_end"])
        baseline_threshold = float(task.payload.get("baseline_threshold", 70))
        baseline_cooldown_hours = int(task.payload.get("baseline_cooldown_hours", 48))

        evaluations = self.store.evaluations_in_window(window_start=window_start, window_end=window_end)
        failures = self.store.failed_evaluations(window_start=window_start, window_end=window_end)

        total = len(evaluations)
        failed = len(failures)
        failure_rate = 0.0 if total == 0 else failed / total

        if failure_rate >= 0.5:
            proposed = {
                "score_threshold": max(0.0, baseline_threshold - 5.0),
                "cooldown_hours": baseline_cooldown_hours + 12,
                "strategy": "tighten_after_high_failure_rate",
            }
        else:
            proposed = {
                "score_threshold": baseline_threshold,
                "cooldown_hours": baseline_cooldown_hours,
                "strategy": "no_change",
            }

        promise_evals = self.store._read_jsonl(self.store.promise_evaluations_path)
        p_total = len(promise_evals)
        p_correct = sum(1 for r in promise_evals if bool(r.get("correct")))
        promise_predictive_accuracy = 0.0 if p_total == 0 else p_correct / p_total
        promise_reports = self.store._read_jsonl(self.store.promise_calibration_reports_path)
        latest_promise_report = promise_reports[-1] if promise_reports else None

        report_id = f"calib_{_stable_hash([window_start, window_end, str(total), str(failed), str(proposed['score_threshold']), str(proposed['cooldown_hours'])])}"
        row = {
            "id": report_id,
            "window_start": window_start,
            "window_end": window_end,
            "total_evaluations": total,
            "failures": failed,
            "failure_rate": round(failure_rate, 6),
            "proposed_policy": proposed,
            "promise_predictive_accuracy": round(promise_predictive_accuracy, 6),
            "promise_weight_adjustment_proposal": (
                latest_promise_report.get("proposed_weight_adjustments") if latest_promise_report else None
            ),
            "occurred_at": _utc_now_iso(),
        }

        wrote = self.store.write_calibration_report_if_absent(row)
        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"report_id": report_id, "duplicate": not wrote, "failure_rate": row["failure_rate"]},
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class CalibrationReviewWorker:
    store: M3JsonlStore
    name: str = "review_calibration"

    def handle(self, task: Task) -> Result:
        report_id = str(task.payload["report_id"])
        decision = str(task.payload["decision"]).lower()  # approve|edit|reject
        reviewer = str(task.payload.get("reviewer", "human"))
        override_policy = task.payload.get("override_policy")

        report = self.store.find_calibration_report(report_id)
        if report is None:
            raise ValueError(f"calibration report not found: {report_id}")
        if decision not in {"approve", "edit", "reject"}:
            raise ValueError("decision must be approve|edit|reject")

        proposed_policy = report.get("proposed_policy") or {}
        if decision == "approve":
            adopted_policy = proposed_policy
        elif decision == "edit":
            adopted_policy = override_policy or proposed_policy
        else:
            adopted_policy = None

        review_id = f"review_{_stable_hash([report_id, decision, json.dumps(adopted_policy, sort_keys=True)])}"
        row = {
            "id": review_id,
            "report_id": report_id,
            "decision": decision,
            "reviewer": reviewer,
            "proposed_policy": proposed_policy,
            "override_policy": override_policy,
            "adopted_policy": adopted_policy,
            "occurred_at": _utc_now_iso(),
        }

        wrote = self.store.write_calibration_review_if_absent(row)
        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"review_id": review_id, "duplicate": not wrote, "decision": decision},
            trace_context=task.trace_context,
        )


def run_local_m3(
    *,
    workspace: str | Path,
    window_start: str,
    window_end: str,
    success_within_hours: int = 72,
    auto_review_decision: str | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace)
    store = M3JsonlStore(workspace)

    evaluator = OutcomeEvaluatorWorker(store=store)
    analyst = FailureAnalystWorker(store=store)
    reporter = CalibrationReporterWorker(store=store)
    reviewer = CalibrationReviewWorker(store=store)

    q_eval = FilesystemQueue(workspace=workspace, worker_name=evaluator.name)
    q_fail = FilesystemQueue(workspace=workspace, worker_name=analyst.name)
    q_rep = FilesystemQueue(workspace=workspace, worker_name=reporter.name)

    q_eval.enqueue_task(
        Task(
            task_id=f"m3_eval_{_stable_hash([window_start, window_end, str(success_within_hours)])}",
            task_type="evaluate_outcomes",
            payload={
                "window_start": window_start,
                "window_end": window_end,
                "success_within_hours": success_within_hours,
            },
        )
    )
    eval_processed = WorkerRunner(queue=q_eval, worker=evaluator, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_fail.enqueue_task(
        Task(
            task_id=f"m3_fail_{_stable_hash([window_start, window_end])}",
            task_type="analyze_failures",
            payload={"window_start": window_start, "window_end": window_end},
        )
    )
    fail_processed = WorkerRunner(queue=q_fail, worker=analyst, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_rep.enqueue_task(
        Task(
            task_id=f"m3_report_{_stable_hash([window_start, window_end])}",
            task_type="report_calibration",
            payload={"window_start": window_start, "window_end": window_end},
        )
    )
    report_processed = WorkerRunner(queue=q_rep, worker=reporter, config=RunnerConfig(once=True, max_tasks=1)).run()

    review_processed = 0
    if auto_review_decision is not None:
        latest = store.latest_calibration_report()
        if latest is not None:
            q_rev = FilesystemQueue(workspace=workspace, worker_name=reviewer.name)
            q_rev.enqueue_task(
                Task(
                    task_id=f"m3_review_{_stable_hash([str(latest['id']), auto_review_decision])}",
                    task_type="review_calibration",
                    payload={
                        "report_id": latest["id"],
                        "decision": auto_review_decision,
                    },
                )
            )
            review_processed = WorkerRunner(queue=q_rev, worker=reviewer, config=RunnerConfig(once=True, max_tasks=1)).run()

    return {
        "workspace": str(workspace),
        "window_start": window_start,
        "window_end": window_end,
        "eval_processed": eval_processed,
        "fail_processed": fail_processed,
        "report_processed": report_processed,
        "review_processed": review_processed,
        "evaluations_path": str(store.evaluations_path),
        "failures_path": str(store.failures_path),
        "calibration_reports_path": str(store.calibration_reports_path),
        "calibration_reviews_path": str(store.calibration_reviews_path),
    }
