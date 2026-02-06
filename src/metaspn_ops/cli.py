from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from .fs_queue import FilesystemQueue
from .runner import RunnerConfig, WorkerRunner
from .workers import run_local_m0, run_local_m1


def _load_worker(spec: str):
    if ":" not in spec:
        raise ValueError("Worker must be import path in form module:attr")
    module_name, attr = spec.split(":", 1)
    module = importlib.import_module(module_name)
    worker = getattr(module, attr)
    if isinstance(worker, type):
        worker = worker()
    if not hasattr(worker, "name") or not hasattr(worker, "handle"):
        raise ValueError("Loaded worker must expose name and handle(task)")
    return worker


def _parse_every(raw: str | None) -> int | None:
    if raw is None:
        return None
    raw = raw.strip().lower()
    if raw.endswith("ms"):
        return max(1, int(raw[:-2]) // 1000)
    if raw.endswith("s"):
        return int(raw[:-1])
    if raw.endswith("m"):
        return int(raw[:-1]) * 60
    if raw.endswith("h"):
        return int(raw[:-1]) * 3600
    return int(raw)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="metaspn")
    sub = parser.add_subparsers(dest="command", required=True)

    worker_p = sub.add_parser("worker")
    worker_sub = worker_p.add_subparsers(dest="worker_cmd", required=True)
    run_p = worker_sub.add_parser("run")
    run_p.add_argument("worker", help="Worker import path module:attr")
    run_p.add_argument("--workspace", default=".")
    run_p.add_argument("--every", default=None)
    run_p.add_argument("--max-tasks", type=int, default=None)
    run_p.add_argument("--parallel", type=int, default=1)
    run_p.add_argument("--lease-seconds", type=int, default=120)
    run_p.add_argument("--once", action="store_true")

    queue_p = sub.add_parser("queue")
    queue_sub = queue_p.add_subparsers(dest="queue_cmd", required=True)

    stats_p = queue_sub.add_parser("stats")
    stats_p.add_argument("worker")
    stats_p.add_argument("--workspace", default=".")

    retry_p = queue_sub.add_parser("retry")
    retry_p.add_argument("worker")
    retry_p.add_argument("--workspace", default=".")
    retry_p.add_argument("--task-id", default=None)

    deadletter_p = queue_sub.add_parser("deadletter")
    deadletter_sub = deadletter_p.add_subparsers(dest="deadletter_cmd", required=True)
    dl_list_p = deadletter_sub.add_parser("list")
    dl_list_p.add_argument("worker")
    dl_list_p.add_argument("--workspace", default=".")

    m0_p = sub.add_parser("m0")
    m0_sub = m0_p.add_subparsers(dest="m0_cmd", required=True)
    m0_run_p = m0_sub.add_parser("run-local")
    m0_run_p.add_argument("--workspace", default=".")
    m0_run_p.add_argument("--input-jsonl", required=True)
    m0_run_p.add_argument("--max-records", type=int, default=None)

    m1_p = sub.add_parser("m1")
    m1_sub = m1_p.add_subparsers(dest="m1_cmd", required=True)
    m1_run_p = m1_sub.add_parser("run-local")
    m1_run_p.add_argument("--workspace", default=".")
    m1_run_p.add_argument("--limit", type=int, default=100)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "worker" and args.worker_cmd == "run":
        worker = _load_worker(args.worker)
        queue = FilesystemQueue(workspace=Path(args.workspace), worker_name=worker.name)
        cfg = RunnerConfig(
            every_seconds=_parse_every(args.every),
            max_tasks=args.max_tasks,
            parallel=args.parallel,
            lease_seconds=args.lease_seconds,
            once=args.once,
        )
        runner = WorkerRunner(queue=queue, worker=worker, config=cfg)
        processed = runner.run()
        print(json.dumps({"processed": processed}))
        return 0

    if args.command == "queue" and args.queue_cmd == "stats":
        queue = FilesystemQueue(workspace=Path(args.workspace), worker_name=args.worker)
        print(json.dumps(queue.stats()))
        return 0

    if args.command == "queue" and args.queue_cmd == "retry":
        queue = FilesystemQueue(workspace=Path(args.workspace), worker_name=args.worker)
        retried = queue.retry_deadletter(task_id=args.task_id)
        print(json.dumps({"retried": retried}))
        return 0

    if args.command == "queue" and args.queue_cmd == "deadletter" and args.deadletter_cmd == "list":
        queue = FilesystemQueue(workspace=Path(args.workspace), worker_name=args.worker)
        items = [str(p) for p in queue.deadletter_items()]
        print(json.dumps({"items": items}))
        return 0

    if args.command == "m0" and args.m0_cmd == "run-local":
        summary = run_local_m0(
            workspace=Path(args.workspace),
            input_jsonl_path=Path(args.input_jsonl),
            max_records=args.max_records,
        )
        print(json.dumps(summary))
        return 0

    if args.command == "m1" and args.m1_cmd == "run-local":
        summary = run_local_m1(
            workspace=Path(args.workspace),
            limit=args.limit,
        )
        print(json.dumps(summary))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
