from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..fs_queue import FilesystemQueue
from ..runner import RunnerConfig, WorkerRunner
from ..types import Result, Task


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_hash(parts: list[str]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                out.append(json.loads(line))
    return out


def _append_if_absent(path: Path, row: dict[str, Any]) -> bool:
    rec_id = str(row["id"])
    for existing in _read_jsonl(path):
        if str(existing.get("id")) == rec_id:
            return False
    with path.open("a", encoding="utf-8") as f:
        json.dump(row, f, ensure_ascii=True)
        f.write("\n")
    return True


@dataclass(slots=True)
class TokenPromiseStore:
    workspace: Path
    store_dir: Path = field(init=False)
    token_signals_path: Path = field(init=False)
    token_resolutions_path: Path = field(init=False)
    token_health_scores_path: Path = field(init=False)
    promises_path: Path = field(init=False)
    promise_evaluations_path: Path = field(init=False)
    promise_manual_reviews_path: Path = field(init=False)
    promise_calibration_reports_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.token_signals_path = self.store_dir / "token_signals.jsonl"
        self.token_resolutions_path = self.store_dir / "token_resolutions.jsonl"
        self.token_health_scores_path = self.store_dir / "token_health_scores.jsonl"
        self.promises_path = self.store_dir / "promises.jsonl"
        self.promise_evaluations_path = self.store_dir / "promise_evaluations.jsonl"
        self.promise_manual_reviews_path = self.store_dir / "promise_manual_reviews.jsonl"
        self.promise_calibration_reports_path = self.store_dir / "promise_calibration_reports.jsonl"

    def unresolved_token_signals(self, *, limit: int = 100) -> list[dict[str, Any]]:
        resolved_signal_ids = {str(r.get("signal_id")) for r in _read_jsonl(self.token_resolutions_path)}
        out = []
        for row in _read_jsonl(self.token_signals_path):
            if str(row.get("id")) in resolved_signal_ids:
                continue
            out.append(row)
            if len(out) >= limit:
                break
        return out

    def unresolved_token_resolutions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        scored_token_ids = {str(r.get("token_id")) for r in _read_jsonl(self.token_health_scores_path)}
        out = []
        for row in _read_jsonl(self.token_resolutions_path):
            token_id = str(row.get("token_id"))
            if token_id in scored_token_ids:
                continue
            out.append(row)
            if len(out) >= limit:
                break
        return out

    def unevaluated_promises(self, *, limit: int = 100) -> list[dict[str, Any]]:
        evaluated_ids = {str(r.get("promise_id")) for r in _read_jsonl(self.promise_evaluations_path)}
        reviewed_ids = {str(r.get("promise_id")) for r in _read_jsonl(self.promise_manual_reviews_path)}
        out = []
        for row in _read_jsonl(self.promises_path):
            promise_id = str(row.get("id"))
            if promise_id in evaluated_ids or promise_id in reviewed_ids:
                continue
            out.append(row)
            if len(out) >= limit:
                break
        return out

    def evaluated_promises(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        rows = _read_jsonl(self.promise_evaluations_path)
        return rows[:limit]

    def write_token_resolution_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.token_resolutions_path, row)

    def write_token_health_score_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.token_health_scores_path, row)

    def write_promise_evaluation_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.promise_evaluations_path, row)

    def write_promise_manual_review_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.promise_manual_reviews_path, row)

    def write_promise_calibration_report_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.promise_calibration_reports_path, row)


@dataclass(slots=True)
class ResolveTokenWorker:
    store: TokenPromiseStore
    name: str = "resolve_token"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        pending = self.store.unresolved_token_signals(limit=limit)
        resolved = 0
        duplicates = 0
        resolution_ids: list[str] = []

        for signal in pending:
            signal_id = str(signal["id"])
            token_raw = str(signal.get("token_ref") or signal.get("payload", {}).get("token_ref") or signal_id)
            token_id = f"tok:{token_raw.strip().lower()}"
            resolution_id = f"token_res_{_stable_hash([signal_id, token_id])}"
            row = {
                "id": resolution_id,
                "signal_id": signal_id,
                "token_id": token_id,
                "network": str(signal.get("network", "unknown")),
                "occurred_at": str(signal.get("occurred_at") or _utc_now_iso()),
            }
            if self.store.write_token_resolution_if_absent(row):
                resolved += 1
            else:
                duplicates += 1
            resolution_ids.append(resolution_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"resolved": resolved, "duplicates": duplicates, "resolution_ids": resolution_ids},
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class TokenHealthScorerWorker:
    store: TokenPromiseStore
    name: str = "score_token_health"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        pending = self.store.unresolved_token_resolutions(limit=limit)
        scored = 0
        duplicates = 0
        score_ids: list[str] = []

        for res in pending:
            token_id = str(res["token_id"])
            network = str(res.get("network", "unknown"))
            base = 50.0
            if network in {"ethereum", "solana"}:
                base += 15.0
            stability = float(len(token_id) % 10)
            health_score = round(min(100.0, base + stability), 2)
            score_id = f"token_score_{_stable_hash([token_id])}"
            row = {
                "id": score_id,
                "token_id": token_id,
                "network": network,
                "health_score": health_score,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_token_health_score_if_absent(row):
                scored += 1
            else:
                duplicates += 1
            score_ids.append(score_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"scored": scored, "duplicates": duplicates, "score_ids": score_ids},
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class PromiseEvaluatorWorker:
    store: TokenPromiseStore
    name: str = "evaluate_promise"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        pending = self.store.unevaluated_promises(limit=limit)

        evaluated = 0
        routed_manual = 0
        duplicates = 0
        evaluation_ids: list[str] = []
        manual_ids: list[str] = []

        for promise in pending:
            promise_id = str(promise["id"])
            path = str(promise.get("evaluation_path", "observable_signal"))
            expected = bool(promise.get("expected_success", True))

            if path == "human_judgment":
                manual_id = f"promise_manual_{_stable_hash([promise_id])}"
                row = {
                    "id": manual_id,
                    "promise_id": promise_id,
                    "decision": "pending",
                    "reason": "requires_human_judgment",
                    "occurred_at": _utc_now_iso(),
                }
                if self.store.write_promise_manual_review_if_absent(row):
                    routed_manual += 1
                else:
                    duplicates += 1
                manual_ids.append(manual_id)
                continue

            if path == "on_chain":
                observed_success = bool(promise.get("on_chain_success", False))
            else:  # observable_signal default
                observed_success = bool(promise.get("observable_signal_success", False))

            evaluation_id = f"promise_eval_{_stable_hash([promise_id, path])}"
            row = {
                "id": evaluation_id,
                "promise_id": promise_id,
                "evaluation_path": path,
                "expected_success": expected,
                "observed_success": observed_success,
                "correct": expected == observed_success,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_promise_evaluation_if_absent(row):
                evaluated += 1
            else:
                duplicates += 1
            evaluation_ids.append(evaluation_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "evaluated": evaluated,
                "routed_manual": routed_manual,
                "duplicates": duplicates,
                "evaluation_ids": evaluation_ids,
                "manual_ids": manual_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class PromiseCalibrationWorker:
    store: TokenPromiseStore
    name: str = "calibrate_promise"

    def handle(self, task: Task) -> Result:
        baseline_weight = float(task.payload.get("baseline_weight", 1.0))
        window_key = str(task.payload.get("window_key", datetime.now(timezone.utc).strftime("%Y-%m-%d")))

        evaluations = self.store.evaluated_promises(limit=10000)
        total = len(evaluations)
        correct = sum(1 for r in evaluations if bool(r.get("correct")))
        accuracy = 0.0 if total == 0 else correct / total

        if accuracy < 0.5:
            proposed_weight = round(max(0.1, baseline_weight * 0.85), 4)
            strategy = "decrease_weight_low_accuracy"
        elif accuracy > 0.8:
            proposed_weight = round(min(3.0, baseline_weight * 1.1), 4)
            strategy = "increase_weight_high_accuracy"
        else:
            proposed_weight = baseline_weight
            strategy = "no_change"

        report_id = f"promise_calib_{_stable_hash([window_key, str(total), str(correct), str(proposed_weight)])}"
        row = {
            "id": report_id,
            "window_key": window_key,
            "total_evaluations": total,
            "correct_evaluations": correct,
            "predictive_accuracy": round(accuracy, 6),
            "proposed_weight_adjustments": {
                "promise_accuracy_weight": {
                    "from": baseline_weight,
                    "to": proposed_weight,
                    "strategy": strategy,
                }
            },
            "review_required": True,
            "occurred_at": _utc_now_iso(),
        }

        wrote = self.store.write_promise_calibration_report_if_absent(row)
        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "report_id": report_id,
                "duplicate": not wrote,
                "predictive_accuracy": row["predictive_accuracy"],
                "review_required": True,
            },
            trace_context=task.trace_context,
        )


def run_local_token_promises(
    *,
    workspace: str | Path,
    window_key: str,
    limit: int = 100,
    baseline_weight: float = 1.0,
) -> dict[str, Any]:
    workspace = Path(workspace)
    store = TokenPromiseStore(workspace)

    resolver = ResolveTokenWorker(store=store)
    scorer = TokenHealthScorerWorker(store=store)
    evaluator = PromiseEvaluatorWorker(store=store)
    calibrator = PromiseCalibrationWorker(store=store)

    q_resolve = FilesystemQueue(workspace=workspace, worker_name=resolver.name)
    q_score = FilesystemQueue(workspace=workspace, worker_name=scorer.name)
    q_eval = FilesystemQueue(workspace=workspace, worker_name=evaluator.name)
    q_calib = FilesystemQueue(workspace=workspace, worker_name=calibrator.name)

    q_resolve.enqueue_task(Task(task_id=f"tok_res_{_stable_hash([window_key, str(limit)])}", task_type="resolve_token", payload={"limit": limit}))
    resolve_processed = WorkerRunner(queue=q_resolve, worker=resolver, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_score.enqueue_task(Task(task_id=f"tok_score_{_stable_hash([window_key, str(limit)])}", task_type="score_token_health", payload={"limit": limit}))
    score_processed = WorkerRunner(queue=q_score, worker=scorer, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_eval.enqueue_task(Task(task_id=f"promise_eval_{_stable_hash([window_key, str(limit)])}", task_type="evaluate_promise", payload={"limit": limit}))
    eval_processed = WorkerRunner(queue=q_eval, worker=evaluator, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_calib.enqueue_task(
        Task(
            task_id=f"promise_calib_{_stable_hash([window_key, str(baseline_weight)])}",
            task_type="calibrate_promise",
            payload={"window_key": window_key, "baseline_weight": baseline_weight},
        )
    )
    calib_processed = WorkerRunner(queue=q_calib, worker=calibrator, config=RunnerConfig(once=True, max_tasks=1)).run()

    return {
        "workspace": str(workspace),
        "window_key": window_key,
        "resolve_processed": resolve_processed,
        "score_processed": score_processed,
        "eval_processed": eval_processed,
        "calib_processed": calib_processed,
        "token_resolutions_path": str(store.token_resolutions_path),
        "token_health_scores_path": str(store.token_health_scores_path),
        "promise_evaluations_path": str(store.promise_evaluations_path),
        "promise_manual_reviews_path": str(store.promise_manual_reviews_path),
        "promise_calibration_reports_path": str(store.promise_calibration_reports_path),
    }
