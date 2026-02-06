from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..fs_queue import FilesystemQueue
from ..runner import RunnerConfig, WorkerRunner
from ..types import Task
from .m1 import M1JsonlStore, ProfilerWorker, RouterWorker, ScorerWorker
from .m2 import DigestWorker, DrafterWorker, M2JsonlStore


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


def _append_if_absent(path: Path, payload: dict[str, Any]) -> bool:
    rec_id = str(payload.get("id"))
    if rec_id:
        for row in _read_jsonl(path):
            if str(row.get("id")) == rec_id:
                return False
    with path.open("a", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
        f.write("\n")
    return True


def seed_resolved_entities(*, workspace: str | Path, resolved_entities_jsonl: str | Path) -> dict[str, int]:
    workspace = Path(workspace)
    source_path = Path(resolved_entities_jsonl)
    emissions_path = workspace / "store" / "emissions.jsonl"
    emissions_path.parent.mkdir(parents=True, exist_ok=True)

    inserted = 0
    duplicates = 0

    with source_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            row = json.loads(line)
            entity_ref = str(row.get("entity_ref"))
            occurred_at = str(row.get("occurred_at") or _utc_now_iso())
            emission_id = str(row.get("id") or f"em_resolved_{_stable_hash([entity_ref, occurred_at, str(i)])}")
            emission = {
                "id": emission_id,
                "emission_type": "entity.resolved",
                "entity_ref": entity_ref,
                "occurred_at": occurred_at,
                "source": str(row.get("source", "demo.seed")),
            }
            if _append_if_absent(emissions_path, emission):
                inserted += 1
            else:
                duplicates += 1

    return {"inserted": inserted, "duplicates": duplicates}


def ingest_manual_outcomes(*, workspace: str | Path, outcomes_jsonl: str | Path) -> dict[str, int]:
    workspace = Path(workspace)
    source_path = Path(outcomes_jsonl)
    outcomes_path = workspace / "store" / "m3_outcomes.jsonl"
    outcomes_path.parent.mkdir(parents=True, exist_ok=True)

    inserted = 0
    duplicates = 0

    with source_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            row = json.loads(line)
            entity_ref = str(row.get("entity_ref"))
            occurred_at = str(row.get("occurred_at") or _utc_now_iso())
            outcome_id = str(row.get("id") or f"out_{_stable_hash([entity_ref, occurred_at, str(i)])}")
            outcome = {
                "id": outcome_id,
                "entity_ref": entity_ref,
                "occurred_at": occurred_at,
                "outcome": str(row.get("outcome", "manual_outcome")),
                "source": str(row.get("source", "demo.manual_outcome")),
            }
            if _append_if_absent(outcomes_path, outcome):
                inserted += 1
            else:
                duplicates += 1

    return {"inserted": inserted, "duplicates": duplicates}


def run_demo_once(
    *,
    workspace: str | Path,
    window_key: str,
    limit: int = 100,
    top_n: int = 5,
    channel: str | None = None,
    max_attempts: int = 1,
    resolved_entities_jsonl: str | Path | None = None,
    outcomes_jsonl: str | Path | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace)

    seeded = {"inserted": 0, "duplicates": 0}
    if resolved_entities_jsonl is not None:
        seeded = seed_resolved_entities(workspace=workspace, resolved_entities_jsonl=resolved_entities_jsonl)

    m1_store = M1JsonlStore(workspace)
    m2_store = M2JsonlStore(workspace)

    profiler = ProfilerWorker(store=m1_store)
    scorer = ScorerWorker(store=m1_store)
    router = RouterWorker(store=m1_store)
    digest = DigestWorker(store=m2_store)

    q_profile = FilesystemQueue(workspace=workspace, worker_name=profiler.name)
    q_score = FilesystemQueue(workspace=workspace, worker_name=scorer.name)
    q_router = FilesystemQueue(workspace=workspace, worker_name=router.name)
    q_digest = FilesystemQueue(workspace=workspace, worker_name=digest.name)

    q_profile.enqueue_task(
        Task(
            task_id=f"demo_profile_{_stable_hash([window_key, str(limit)])}",
            task_type="profile_entity",
            payload={"limit": limit},
            max_attempts=max_attempts,
        )
    )
    profile_processed = WorkerRunner(queue=q_profile, worker=profiler, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_score.enqueue_task(
        Task(
            task_id=f"demo_score_{_stable_hash([window_key, str(limit)])}",
            task_type="score_entity",
            payload={"limit": limit},
            max_attempts=max_attempts,
        )
    )
    score_processed = WorkerRunner(queue=q_score, worker=scorer, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_router.enqueue_task(
        Task(
            task_id=f"demo_route_{_stable_hash([window_key, str(limit)])}",
            task_type="route_entity",
            payload={"limit": limit},
            max_attempts=max_attempts,
        )
    )
    route_processed = WorkerRunner(queue=q_router, worker=router, config=RunnerConfig(once=True, max_tasks=1)).run()

    q_digest.enqueue_task(
        Task(
            task_id=f"demo_digest_{_stable_hash([window_key, str(top_n)])}",
            task_type="digest_recommendations",
            payload={"window_key": window_key, "top_n": top_n},
            max_attempts=max_attempts,
        )
    )
    digest_processed = WorkerRunner(queue=q_digest, worker=digest, config=RunnerConfig(once=True, max_tasks=1)).run()

    draft_processed = 0
    if channel is not None:
        drafter = DrafterWorker(store=m2_store)
        q_draft = FilesystemQueue(workspace=workspace, worker_name=drafter.name)
        latest_digest = m2_store.latest_digest()
        digest_id = latest_digest.get("id") if latest_digest else None
        q_draft.enqueue_task(
            Task(
                task_id=f"demo_draft_{_stable_hash([window_key, str(top_n), channel])}",
                task_type="draft_outreach",
                payload={"digest_id": digest_id, "channel": channel},
                max_attempts=max_attempts,
            )
        )
        draft_processed = WorkerRunner(queue=q_draft, worker=drafter, config=RunnerConfig(once=True, max_tasks=1)).run()

    outcomes = {"inserted": 0, "duplicates": 0}
    if outcomes_jsonl is not None:
        outcomes = ingest_manual_outcomes(workspace=workspace, outcomes_jsonl=outcomes_jsonl)

    return {
        "workspace": str(workspace),
        "window_key": window_key,
        "profile_processed": profile_processed,
        "score_processed": score_processed,
        "route_processed": route_processed,
        "digest_processed": digest_processed,
        "draft_processed": draft_processed,
        "seeded_resolved_entities": seeded,
        "manual_outcomes": outcomes,
        "profiles_path": str(m1_store.profiles_path),
        "scores_path": str(m1_store.scores_path),
        "routes_path": str(m1_store.routes_path),
        "digests_path": str(m2_store.digests_path),
        "drafts_path": str(m2_store.drafts_path),
        "m3_outcomes_path": str(workspace / "store" / "m3_outcomes.jsonl"),
    }
