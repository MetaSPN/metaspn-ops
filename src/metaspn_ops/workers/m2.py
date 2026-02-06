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


def _priority_rank(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(priority).lower(), 3)


@dataclass(slots=True)
class M2JsonlStore:
    workspace: Path
    store_dir: Path = field(init=False)
    routes_path: Path = field(init=False)
    digests_path: Path = field(init=False)
    drafts_path: Path = field(init=False)
    approvals_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.routes_path = self.store_dir / "m1_routes.jsonl"
        self.digests_path = self.store_dir / "m2_digests.jsonl"
        self.drafts_path = self.store_dir / "m2_drafts.jsonl"
        self.approvals_path = self.store_dir / "m2_approvals.jsonl"

    def recommendation_candidates(self) -> list[dict[str, Any]]:
        rows = self._read_jsonl(self.routes_path)
        return sorted(
            rows,
            key=lambda r: (
                _priority_rank(str(r.get("priority", ""))),
                -float(r.get("score", 0)),
                str(r.get("entity_ref", "")),
            ),
        )

    def latest_digest(self) -> dict[str, Any] | None:
        rows = self._read_jsonl(self.digests_path)
        if not rows:
            return None
        return rows[-1]

    def write_digest_if_absent(self, digest: dict[str, Any]) -> bool:
        return self._write_if_absent(self.digests_path, digest)

    def write_draft_if_absent(self, draft: dict[str, Any]) -> bool:
        return self._write_if_absent(self.drafts_path, draft)

    def write_approval_if_absent(self, approval: dict[str, Any]) -> bool:
        return self._write_if_absent(self.approvals_path, approval)

    def get_draft(self, draft_id: str) -> dict[str, Any] | None:
        for row in self._read_jsonl(self.drafts_path):
            if str(row.get("id")) == str(draft_id):
                return row
        return None

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
class DigestWorker:
    store: M2JsonlStore
    name: str = "digest_recommendations"

    def handle(self, task: Task) -> Result:
        window_key = str(task.payload.get("window_key", datetime.now(timezone.utc).strftime("%Y-%m-%d")))
        top_n = int(task.payload.get("top_n", 10))
        candidates = self.store.recommendation_candidates()
        selected = candidates[:top_n]

        digest_id = f"digest_{_stable_hash([window_key, str(top_n), *[str(r.get('id')) for r in selected]])}"
        digest = {
            "id": digest_id,
            "window_key": window_key,
            "occurred_at": _utc_now_iso(),
            "top_n": top_n,
            "items": [
                {
                    "rank": i + 1,
                    "route_id": r.get("id"),
                    "entity_ref": r.get("entity_ref"),
                    "score": r.get("score"),
                    "priority": r.get("priority"),
                    "playbook": r.get("playbook"),
                }
                for i, r in enumerate(selected)
            ],
        }

        wrote = self.store.write_digest_if_absent(digest)
        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "digest_id": digest_id,
                "candidates": len(candidates),
                "selected": len(selected),
                "duplicate": not wrote,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class DrafterWorker:
    store: M2JsonlStore
    name: str = "draft_outreach"

    def handle(self, task: Task) -> Result:
        channel = str(task.payload.get("channel", "email")).lower()
        digest_id = task.payload.get("digest_id")

        digest = None
        if digest_id is not None:
            for row in self.store._read_jsonl(self.store.digests_path):
                if str(row.get("id")) == str(digest_id):
                    digest = row
                    break
        else:
            digest = self.store.latest_digest()

        if digest is None:
            return Result(task_id=task.task_id, status="ok", payload={"drafted": 0, "duplicates": 0, "draft_ids": []})

        drafted = 0
        duplicates = 0
        draft_ids: list[str] = []

        for item in digest.get("items", []):
            entity_ref = str(item.get("entity_ref"))
            route_id = str(item.get("route_id"))
            playbook = str(item.get("playbook"))
            rank = int(item.get("rank", 0))

            draft_id = f"draft_{_stable_hash([str(digest['id']), entity_ref, channel])}"
            if channel == "linkedin":
                body = f"Hi {entity_ref}, quick note based on {playbook}. Open to connect this week?"
            else:
                body = f"Subject: Quick idea for {entity_ref}\n\nHi {entity_ref},\nBased on {playbook}, sharing a short recommendation."

            draft = {
                "id": draft_id,
                "digest_id": digest["id"],
                "route_id": route_id,
                "entity_ref": entity_ref,
                "channel": channel,
                "rank": rank,
                "playbook": playbook,
                "body": body,
                "occurred_at": _utc_now_iso(),
            }
            if self.store.write_draft_if_absent(draft):
                drafted += 1
            else:
                duplicates += 1
            draft_ids.append(draft_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"drafted": drafted, "duplicates": duplicates, "draft_ids": draft_ids},
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class ApprovalWorker:
    store: M2JsonlStore
    name: str = "capture_approval"

    def handle(self, task: Task) -> Result:
        draft_id = str(task.payload["draft_id"])
        decision = str(task.payload["decision"]).lower()  # approve | edit | reject
        editor = str(task.payload.get("editor", "human"))
        override_body = task.payload.get("override_body")

        draft = self.store.get_draft(draft_id)
        if draft is None:
            raise ValueError(f"draft not found: {draft_id}")

        if decision not in {"approve", "edit", "reject"}:
            raise ValueError("decision must be approve|edit|reject")

        final_body = str(override_body) if (decision == "edit" and override_body is not None) else str(draft.get("body", ""))
        approval_id = f"approval_{_stable_hash([draft_id, decision, final_body])}"

        approval = {
            "id": approval_id,
            "draft_id": draft_id,
            "decision": decision,
            "editor": editor,
            "override_body": override_body,
            "final_body": final_body,
            "occurred_at": _utc_now_iso(),
        }
        wrote = self.store.write_approval_if_absent(approval)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={"approval_id": approval_id, "duplicate": not wrote, "decision": decision},
            trace_context=task.trace_context,
        )


def run_local_m2(*, workspace: str | Path, window_key: str, top_n: int = 5, channel: str = "email") -> dict[str, Any]:
    workspace = Path(workspace)
    store = M2JsonlStore(workspace)

    digest_worker = DigestWorker(store=store)
    drafter_worker = DrafterWorker(store=store)

    digest_queue = FilesystemQueue(workspace=workspace, worker_name=digest_worker.name)
    draft_queue = FilesystemQueue(workspace=workspace, worker_name=drafter_worker.name)

    digest_task_id = f"digest_task_{_stable_hash([window_key, str(top_n)])}"
    digest_queue.enqueue_task(
        Task(task_id=digest_task_id, task_type="digest", payload={"window_key": window_key, "top_n": top_n})
    )
    d_processed = WorkerRunner(queue=digest_queue, worker=digest_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    digest = store.latest_digest()
    digest_id = digest["id"] if digest else None

    draft_task_id = f"draft_task_{_stable_hash([str(digest_id), channel])}"
    draft_queue.enqueue_task(
        Task(task_id=draft_task_id, task_type="draft", payload={"digest_id": digest_id, "channel": channel})
    )
    r_processed = WorkerRunner(queue=draft_queue, worker=drafter_worker, config=RunnerConfig(once=True, max_tasks=1)).run()

    return {
        "workspace": str(workspace),
        "window_key": window_key,
        "digest_processed": d_processed,
        "draft_processed": r_processed,
        "digests_path": str(store.digests_path),
        "drafts_path": str(store.drafts_path),
        "approvals_path": str(store.approvals_path),
    }
