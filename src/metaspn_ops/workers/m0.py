from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ..fs_queue import FilesystemQueue
from ..runner import RunnerConfig, WorkerRunner
from ..types import Result, Task


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_hash(parts: list[str]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


class SignalStore(Protocol):
    def write_signal_if_absent(self, signal: dict[str, Any]) -> bool:
        ...

    def iter_unresolved_signals(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        ...

    def write_emission_if_absent(self, emission: dict[str, Any]) -> bool:
        ...


class EntityResolver(Protocol):
    def resolve(self, signal: dict[str, Any]) -> dict[str, Any]:
        ...


@dataclass(slots=True)
class JsonlStoreAdapter:
    workspace: Path
    store_dir: Path = field(init=False)
    signals_path: Path = field(init=False)
    emissions_path: Path = field(init=False)

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.store_dir = self.workspace / "store"
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.signals_path = self.store_dir / "signals.jsonl"
        self.emissions_path = self.store_dir / "emissions.jsonl"

    def write_signal_if_absent(self, signal: dict[str, Any]) -> bool:
        signal_id = str(signal["id"])
        if self._id_exists(self.signals_path, signal_id):
            return False
        self._append_jsonl(self.signals_path, signal)
        return True

    def write_emission_if_absent(self, emission: dict[str, Any]) -> bool:
        emission_id = str(emission["id"])
        if self._id_exists(self.emissions_path, emission_id):
            return False
        self._append_jsonl(self.emissions_path, emission)
        return True

    def iter_unresolved_signals(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        signals = self._read_jsonl(self.signals_path)
        resolved_signal_ids = {
            str(em.get("signal_id"))
            for em in self._read_jsonl(self.emissions_path)
            if em.get("signal_id") is not None
        }

        unresolved: list[dict[str, Any]] = []
        for signal in signals:
            signal_id = str(signal.get("id"))
            if signal_id in resolved_signal_ids:
                continue
            unresolved.append(signal)
            if limit is not None and len(unresolved) >= limit:
                break
        return unresolved

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
class HeuristicEntityResolver:
    default_prefix: str = "entity"

    def resolve(self, signal: dict[str, Any]) -> dict[str, Any]:
        payload = signal.get("payload") or {}
        author = payload.get("author_handle") or payload.get("author")
        if author:
            entity_id = f"person:{str(author).strip().lower()}"
            confidence = 0.95
        else:
            entity_id = f"{self.default_prefix}:{signal['id'][:8]}"
            confidence = 0.4
        return {
            "entity_ref": entity_id,
            "confidence": confidence,
            "resolver": "heuristic-v1",
        }


@dataclass(slots=True)
class IngestSocialWorker:
    store: SignalStore
    name: str = "ingest_social"

    def handle(self, task: Task) -> Result:
        payload = task.payload
        input_path = Path(str(payload["input_jsonl_path"]))
        source = str(payload.get("source", "social.ingest"))
        schema_version = str(payload.get("schema_version", "v1"))
        max_records = payload.get("max_records")
        max_records = int(max_records) if max_records is not None else None

        ingested = 0
        duplicates = 0
        signal_ids: list[str] = []

        with input_path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if max_records is not None and i >= max_records:
                    break
                if not line.strip():
                    continue
                row = json.loads(line)
                record_id = str(row.get("record_id") or row.get("id") or i)
                occurred_at = str(row.get("occurred_at") or _utc_now_iso())
                signal_id = f"sig_{_stable_hash([source, record_id, occurred_at])}"
                signal = {
                    "id": signal_id,
                    "schema_version": schema_version,
                    "source": source,
                    "signal_type": "social.ingested",
                    "occurred_at": occurred_at,
                    "payload": row,
                }
                wrote = self.store.write_signal_if_absent(signal)
                if wrote:
                    ingested += 1
                else:
                    duplicates += 1
                signal_ids.append(signal_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "ingested": ingested,
                "duplicates": duplicates,
                "total_seen": ingested + duplicates,
                "signal_ids": signal_ids,
            },
            trace_context=task.trace_context,
        )


@dataclass(slots=True)
class ResolveEntityWorker:
    store: SignalStore
    resolver: EntityResolver
    name: str = "resolve_entity"

    def handle(self, task: Task) -> Result:
        limit = int(task.payload.get("limit", 100))
        unresolved = self.store.iter_unresolved_signals(limit=limit)

        emitted = 0
        duplicates = 0
        emission_ids: list[str] = []

        for signal in unresolved:
            resolution = self.resolver.resolve(signal)
            signal_id = str(signal["id"])
            emission_id = f"em_{_stable_hash([signal_id, str(resolution['entity_ref'])])}"
            emission = {
                "id": emission_id,
                "schema_version": "v1",
                "source": "resolve_entity",
                "emission_type": "entity.resolved",
                "occurred_at": _utc_now_iso(),
                "signal_id": signal_id,
                "entity_ref": resolution["entity_ref"],
                "confidence": resolution.get("confidence"),
                "resolver": resolution.get("resolver"),
            }
            wrote = self.store.write_emission_if_absent(emission)
            if wrote:
                emitted += 1
            else:
                duplicates += 1
            emission_ids.append(emission_id)

        return Result(
            task_id=task.task_id,
            status="ok",
            payload={
                "resolved": emitted,
                "duplicates": duplicates,
                "considered": len(unresolved),
                "emission_ids": emission_ids,
            },
            trace_context=task.trace_context,
        )


def run_local_m0(*, workspace: str | Path, input_jsonl_path: str | Path, max_records: int | None = None) -> dict[str, Any]:
    workspace = Path(workspace)
    input_jsonl_path = Path(input_jsonl_path)

    store = JsonlStoreAdapter(workspace)
    ingest = IngestSocialWorker(store=store)
    resolve = ResolveEntityWorker(store=store, resolver=HeuristicEntityResolver())

    ingest_queue = FilesystemQueue(workspace=workspace, worker_name=ingest.name)
    resolve_queue = FilesystemQueue(workspace=workspace, worker_name=resolve.name)

    ingest_task = Task(
        task_id=f"ingest_{_stable_hash([str(input_jsonl_path), _utc_now_iso()])}",
        task_type="ingest_social",
        payload={
            "input_jsonl_path": str(input_jsonl_path),
            "source": "social.ingest",
            "schema_version": "v1",
            **({"max_records": max_records} if max_records is not None else {}),
        },
    )
    ingest_queue.enqueue_task(ingest_task)
    ingest_processed = WorkerRunner(
        queue=ingest_queue,
        worker=ingest,
        config=RunnerConfig(once=True, max_tasks=1),
    ).run()

    resolve_task = Task(
        task_id=f"resolve_{_stable_hash([str(input_jsonl_path), _utc_now_iso()])}",
        task_type="resolve_entity",
        payload={"limit": max_records or 1000},
    )
    resolve_queue.enqueue_task(resolve_task)
    resolve_processed = WorkerRunner(
        queue=resolve_queue,
        worker=resolve,
        config=RunnerConfig(once=True, max_tasks=1),
    ).run()

    return {
        "workspace": str(workspace),
        "input_jsonl_path": str(input_jsonl_path),
        "ingest_processed": ingest_processed,
        "resolve_processed": resolve_processed,
        "signals_path": str(store.signals_path),
        "emissions_path": str(store.emissions_path),
    }
