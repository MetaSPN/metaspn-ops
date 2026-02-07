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
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


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
class S1JsonlStore:
    workspace: Path
    store_dir: Path = field(init=False)
    m1_scores_path: Path = field(init=False)
    attention_scores_path: Path = field(init=False)
    reward_projections_path: Path = field(init=False)
    settlements_path: Path = field(init=False)
    summaries_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.m1_scores_path = self.store_dir / "m1_scores.jsonl"
        self.attention_scores_path = self.store_dir / "s1_attention_scores.jsonl"
        self.reward_projections_path = self.store_dir / "s1_reward_projections.jsonl"
        self.settlements_path = self.store_dir / "s1_settlements.jsonl"
        self.summaries_path = self.store_dir / "s1_summaries.jsonl"

    def scored_entities(self) -> list[dict[str, Any]]:
        rows = _read_jsonl(self.m1_scores_path)
        return sorted(rows, key=lambda r: (str(r.get("entity_ref", "")), str(r.get("id", ""))))

    def attention_for_date(self, date_key: str) -> list[dict[str, Any]]:
        rows = [r for r in _read_jsonl(self.attention_scores_path) if str(r.get("season_date")) == date_key]
        return sorted(rows, key=lambda r: (str(r.get("subject_ref", "")), str(r.get("id", ""))))

    def projections_for_date(self, date_key: str) -> list[dict[str, Any]]:
        rows = [r for r in _read_jsonl(self.reward_projections_path) if str(r.get("season_date")) == date_key]
        return sorted(rows, key=lambda r: (str(r.get("subject_ref", "")), str(r.get("id", ""))))

    def settlements_for_date(self, date_key: str) -> list[dict[str, Any]]:
        rows = [r for r in _read_jsonl(self.settlements_path) if str(r.get("season_date")) == date_key]
        return sorted(rows, key=lambda r: (str(r.get("subject_ref", "")), str(r.get("id", ""))))

    def write_attention_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.attention_scores_path, row)

    def write_projection_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.reward_projections_path, row)

    def write_settlement_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.settlements_path, row)

    def write_summary_if_absent(self, row: dict[str, Any]) -> bool:
        return _append_if_absent(self.summaries_path, row)


@dataclass(slots=True)
class UpdateAttentionScoresWorker:
    store: S1JsonlStore
    name: str = "update_attention_scores"

    def handle(self, task: Task) -> Result:
        season_date = str(task.payload["date"])
        scored = self.store.scored_entities()

        updated = 0
        duplicates = 0
        attention_ids: list[str] = []

        for row in scored:
            subject_ref = str(row.get("entity_ref"))
            base_score = float(row.get("score", 0.0))
            attention = round(min(100.0, max(0.0, base_score * 0.75 + 5.0)), 2)

            attention_id = f"att_{_stable_hash([season_date, subject_ref])}"
            out = {
                "id": attention_id,
                "season_date": season_date,
                "subject_ref": subject_ref,
                "attention_score": attention,
                "source_score": base_score,
                "model": "season1.attention.v1",
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_attention_if_absent(out):
                updated += 1
            else:
                duplicates += 1
            attention_ids.append(attention_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "season_date": season_date,
                "updated": updated,
                "duplicates": duplicates,
                "considered": len(scored),
                "attention_ids": attention_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class ProjectRewardsWorker:
    store: S1JsonlStore
    name: str = "project_rewards"

    def handle(self, task: Task) -> Result:
        season_date = str(task.payload["date"])
        multiplier = float(task.payload.get("multiplier", 1.2))
        attention_rows = self.store.attention_for_date(season_date)

        projected = 0
        duplicates = 0
        projection_ids: list[str] = []

        for att in attention_rows:
            subject_ref = str(att.get("subject_ref"))
            attention_score = float(att.get("attention_score", 0.0))
            projected_reward = round(attention_score * multiplier, 2)
            tier = "gold" if projected_reward >= 80 else ("silver" if projected_reward >= 50 else "bronze")

            projection_id = f"reward_{_stable_hash([season_date, subject_ref, str(multiplier)])}"
            out = {
                "id": projection_id,
                "season_date": season_date,
                "subject_ref": subject_ref,
                "attention_score": attention_score,
                "multiplier": multiplier,
                "projected_reward": projected_reward,
                "tier": tier,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_projection_if_absent(out):
                projected += 1
            else:
                duplicates += 1
            projection_ids.append(projection_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "season_date": season_date,
                "projected": projected,
                "duplicates": duplicates,
                "considered": len(attention_rows),
                "projection_ids": projection_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class SettleSeasonWorker:
    store: S1JsonlStore
    name: str = "settle_season"

    def handle(self, task: Task) -> Result:
        season_date = str(task.payload["date"])
        projections = self.store.projections_for_date(season_date)

        settled = 0
        duplicates = 0
        settlement_ids: list[str] = []

        for projection in projections:
            subject_ref = str(projection.get("subject_ref"))
            projected_reward = float(projection.get("projected_reward", 0.0))
            settlement_id = f"settlement_{_stable_hash([season_date, subject_ref])}"
            tx_ref = f"tx_{_stable_hash([settlement_id, str(projected_reward)])}"
            out = {
                "id": settlement_id,
                "season_date": season_date,
                "subject_ref": subject_ref,
                "projected_reward": projected_reward,
                "status": "settled",
                "tx_ref": tx_ref,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_settlement_if_absent(out):
                settled += 1
            else:
                duplicates += 1
            settlement_ids.append(settlement_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "season_date": season_date,
                "settled": settled,
                "duplicates": duplicates,
                "considered": len(projections),
                "settlement_ids": settlement_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class PublishSeasonSummaryWorker:
    store: S1JsonlStore
    name: str = "publish_season_summary"

    def handle(self, task: Task) -> Result:
        season_date = str(task.payload["date"])
        settlements = self.store.settlements_for_date(season_date)

        participants = len(settlements)
        total_reward = round(sum(float(r.get("projected_reward", 0.0)) for r in settlements), 2)
        average_reward = 0.0 if participants == 0 else round(total_reward / participants, 4)

        summary_id = f"summary_{_stable_hash([season_date, str(participants), str(total_reward)])}"
        out = {
            "id": summary_id,
            "season_date": season_date,
            "participants": participants,
            "total_reward": total_reward,
            "average_reward": average_reward,
            "status": "published",
            "occurred_at": _utc_now_iso(),
        }
        wrote = self.store.write_summary_if_absent(out)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "season_date": season_date,
                "summary_id": summary_id,
                "participants": participants,
                "total_reward": total_reward,
                "duplicate": not wrote,
            },
            trace_context=task.trace_context,
        )


def run_local_s1(*, workspace: str | Path, date: str) -> dict[str, Any]:
    workspace = Path(workspace)
    store = S1JsonlStore(workspace)

    update_worker = UpdateAttentionScoresWorker(store=store)
    project_worker = ProjectRewardsWorker(store=store)
    settle_worker = SettleSeasonWorker(store=store)
    publish_worker = PublishSeasonSummaryWorker(store=store)

    q_update = FilesystemQueue(workspace=workspace, worker_name=update_worker.name)
    q_project = FilesystemQueue(workspace=workspace, worker_name=project_worker.name)
    q_settle = FilesystemQueue(workspace=workspace, worker_name=settle_worker.name)
    q_publish = FilesystemQueue(workspace=workspace, worker_name=publish_worker.name)

    q_update.enqueue_task(
        Task(
            task_id=f"s1_update_{_stable_hash([date])}",
            task_type="update_attention_scores",
            payload={"date": date},
        )
    )
    update_processed = WorkerRunner(queue=q_update, worker=update_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_project.enqueue_task(
        Task(
            task_id=f"s1_project_{_stable_hash([date])}",
            task_type="project_rewards",
            payload={"date": date},
        )
    )
    project_processed = WorkerRunner(queue=q_project, worker=project_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_settle.enqueue_task(
        Task(
            task_id=f"s1_settle_{_stable_hash([date])}",
            task_type="settle_season",
            payload={"date": date},
        )
    )
    settle_processed = WorkerRunner(queue=q_settle, worker=settle_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_publish.enqueue_task(
        Task(
            task_id=f"s1_publish_{_stable_hash([date])}",
            task_type="publish_season_summary",
            payload={"date": date},
        )
    )
    publish_processed = WorkerRunner(queue=q_publish, worker=publish_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    return {
        "workspace": str(workspace),
        "date": date,
        "update_processed": update_processed,
        "project_processed": project_processed,
        "settle_processed": settle_processed,
        "publish_processed": publish_processed,
        "attention_scores_path": str(store.attention_scores_path),
        "reward_projections_path": str(store.reward_projections_path),
        "settlements_path": str(store.settlements_path),
        "summaries_path": str(store.summaries_path),
    }
