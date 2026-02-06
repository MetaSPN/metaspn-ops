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


@dataclass(slots=True)
class M1JsonlStore:
    workspace: Path
    store_dir: Path = field(init=False)
    emissions_path: Path = field(init=False)
    profiles_path: Path = field(init=False)
    scores_path: Path = field(init=False)
    routes_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.emissions_path = self.store_dir / "emissions.jsonl"
        self.profiles_path = self.store_dir / "m1_profiles.jsonl"
        self.scores_path = self.store_dir / "m1_scores.jsonl"
        self.routes_path = self.store_dir / "m1_routes.jsonl"

    def unresolved_entities(self, *, limit: int = 100) -> list[str]:
        resolved = [
            row for row in self._read_jsonl(self.emissions_path) if row.get("emission_type") == "entity.resolved"
        ]
        profiled_entities = {str(row.get("entity_ref")) for row in self._read_jsonl(self.profiles_path)}
        pending: list[str] = []
        for row in resolved:
            ref = str(row.get("entity_ref"))
            if ref in profiled_entities:
                continue
            pending.append(ref)
            if len(pending) >= limit:
                break
        return pending

    def unscored_profiles(self, *, limit: int = 100) -> list[dict[str, Any]]:
        scored_entities = {str(row.get("entity_ref")) for row in self._read_jsonl(self.scores_path)}
        pending: list[dict[str, Any]] = []
        for row in self._read_jsonl(self.profiles_path):
            ref = str(row.get("entity_ref"))
            if ref in scored_entities:
                continue
            pending.append(row)
            if len(pending) >= limit:
                break
        return pending

    def unrouted_scores(self, *, limit: int = 100) -> list[dict[str, Any]]:
        routed_entities = {str(row.get("entity_ref")) for row in self._read_jsonl(self.routes_path)}
        pending: list[dict[str, Any]] = []
        for row in self._read_jsonl(self.scores_path):
            ref = str(row.get("entity_ref"))
            if ref in routed_entities:
                continue
            pending.append(row)
            if len(pending) >= limit:
                break
        return pending

    def write_profile_if_absent(self, profile: dict[str, Any]) -> bool:
        return self._write_if_absent(self.profiles_path, profile)

    def write_score_if_absent(self, score: dict[str, Any]) -> bool:
        return self._write_if_absent(self.scores_path, score)

    def write_route_if_absent(self, route: dict[str, Any]) -> bool:
        return self._write_if_absent(self.routes_path, route)

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
class ProfilerWorker:
    store: M1JsonlStore
    name: str = "profile_entity"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        entities = self.store.unresolved_entities(limit=limit)
        created = 0
        duplicates = 0
        profile_ids: list[str] = []

        for ref in entities:
            features = {
                "is_person": ref.startswith("person:"),
                "handle_length": len(ref.split(":", 1)[-1]),
                "source": "m1.profiler.v1",
            }
            profile_id = f"profile_{_stable_hash([ref])}"
            profile = {
                "id": profile_id,
                "entity_ref": ref,
                "occurred_at": _utc_now_iso(),
                "features": features,
            }
            if self.store.write_profile_if_absent(profile):
                created += 1
            else:
                duplicates += 1
            profile_ids.append(profile_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "profiled": created,
                "duplicates": duplicates,
                "considered": len(entities),
                "profile_ids": profile_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class ScorerWorker:
    store: M1JsonlStore
    name: str = "score_entity"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        profiles = self.store.unscored_profiles(limit=limit)
        scored = 0
        duplicates = 0
        score_ids: list[str] = []

        for profile in profiles:
            ref = str(profile["entity_ref"])
            features = profile.get("features") or {}
            score = 10.0
            if features.get("is_person"):
                score += 40.0
            score += min(50.0, float(features.get("handle_length", 0)) * 2.0)

            score_id = f"score_{_stable_hash([ref])}"
            score_row = {
                "id": score_id,
                "entity_ref": ref,
                "occurred_at": _utc_now_iso(),
                "score": round(score, 2),
                "model": "heuristic.v1",
            }
            if self.store.write_score_if_absent(score_row):
                scored += 1
            else:
                duplicates += 1
            score_ids.append(score_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "scored": scored,
                "duplicates": duplicates,
                "considered": len(profiles),
                "score_ids": score_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class RouterWorker:
    store: M1JsonlStore
    name: str = "route_entity"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        scores = self.store.unrouted_scores(limit=limit)
        routed = 0
        duplicates = 0
        route_ids: list[str] = []

        for row in scores:
            ref = str(row["entity_ref"])
            score = float(row.get("score", 0))
            if score >= 80:
                playbook = "hot_lead_followup"
                priority = "high"
            elif score >= 50:
                playbook = "nurture_sequence"
                priority = "medium"
            else:
                playbook = "low_touch_watch"
                priority = "low"

            route_id = f"route_{_stable_hash([ref])}"
            route = {
                "id": route_id,
                "entity_ref": ref,
                "occurred_at": _utc_now_iso(),
                "score": score,
                "playbook": playbook,
                "priority": priority,
            }
            if self.store.write_route_if_absent(route):
                routed += 1
            else:
                duplicates += 1
            route_ids.append(route_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "routed": routed,
                "duplicates": duplicates,
                "considered": len(scores),
                "route_ids": route_ids,
            },
            trace_context=task.trace_context,
        )


def run_local_m1(*, workspace: str | Path, limit: int = 100) -> dict[str, Any]:
    workspace = Path(workspace)
    store = M1JsonlStore(workspace)

    profiler = ProfilerWorker(store=store)
    scorer = ScorerWorker(store=store)
    router = RouterWorker(store=store)

    profile_queue = FilesystemQueue(workspace=workspace, worker_name=profiler.name)
    score_queue = FilesystemQueue(workspace=workspace, worker_name=scorer.name)
    route_queue = FilesystemQueue(workspace=workspace, worker_name=router.name)

    profile_queue.enqueue_task(Task(task_id=f"profile_{_stable_hash([_utc_now_iso()])}", task_type="profile", payload={"limit": limit}))
    p_processed = WorkerRunner(queue=profile_queue, worker=profiler, config=RunnerConfig(once=True, max_tasks=1)).run()

    score_queue.enqueue_task(Task(task_id=f"score_{_stable_hash([_utc_now_iso()])}", task_type="score", payload={"limit": limit}))
    s_processed = WorkerRunner(queue=score_queue, worker=scorer, config=RunnerConfig(once=True, max_tasks=1)).run()

    route_queue.enqueue_task(Task(task_id=f"route_{_stable_hash([_utc_now_iso()])}", task_type="route", payload={"limit": limit}))
    r_processed = WorkerRunner(queue=route_queue, worker=router, config=RunnerConfig(once=True, max_tasks=1)).run()

    return {
        "workspace": str(workspace),
        "profile_processed": p_processed,
        "score_processed": s_processed,
        "route_processed": r_processed,
        "profiles_path": str(store.profiles_path),
        "scores_path": str(store.scores_path),
        "routes_path": str(store.routes_path),
    }
