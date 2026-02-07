from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from .fs_queue import FilesystemQueue
from .runner import RunnerConfig, WorkerRunner
from .workers import (
    run_demo_once,
    run_local_m0,
    run_local_m1,
    run_local_m2,
    run_local_m3,
    run_local_s1,
    run_local_token_promises,
)


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

    m2_p = sub.add_parser("m2")
    m2_sub = m2_p.add_subparsers(dest="m2_cmd", required=True)
    m2_run_p = m2_sub.add_parser("run-local")
    m2_run_p.add_argument("--workspace", default=".")
    m2_run_p.add_argument("--window-key", required=True)
    m2_run_p.add_argument("--top-n", type=int, default=5)
    m2_run_p.add_argument("--channel", default="email")

    m3_p = sub.add_parser("m3")
    m3_sub = m3_p.add_subparsers(dest="m3_cmd", required=True)
    m3_run_p = m3_sub.add_parser("run-local")
    m3_run_p.add_argument("--workspace", default=".")
    m3_run_p.add_argument("--window-start", required=True)
    m3_run_p.add_argument("--window-end", required=True)
    m3_run_p.add_argument("--success-within-hours", type=int, default=72)
    m3_run_p.add_argument("--auto-review-decision", default=None)

    demo_p = sub.add_parser("demo")
    demo_sub = demo_p.add_subparsers(dest="demo_cmd", required=True)
    demo_run_p = demo_sub.add_parser("run-once")
    demo_run_p.add_argument("--workspace", default=".")
    demo_run_p.add_argument("--window-key", required=True)
    demo_run_p.add_argument("--limit", type=int, default=100)
    demo_run_p.add_argument("--top-n", type=int, default=5)
    demo_run_p.add_argument("--channel", default=None)
    demo_run_p.add_argument("--max-attempts", type=int, default=1)
    demo_run_p.add_argument("--resolved-entities-jsonl", default=None)
    demo_run_p.add_argument("--outcomes-jsonl", default=None)

    token_p = sub.add_parser("token")
    token_sub = token_p.add_subparsers(dest="token_cmd", required=True)
    token_run_p = token_sub.add_parser("run-local")
    token_run_p.add_argument("--workspace", default=".")
    token_run_p.add_argument("--window-key", required=True)
    token_run_p.add_argument("--limit", type=int, default=100)
    token_run_p.add_argument("--baseline-weight", type=float, default=1.0)

    s1_p = sub.add_parser("s1")
    s1_sub = s1_p.add_subparsers(dest="s1_cmd", required=True)
    s1_run_p = s1_sub.add_parser("run-local")
    s1_run_p.add_argument("--workspace", default=".")
    s1_run_p.add_argument("--date", required=True)

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

    if args.command == "m2" and args.m2_cmd == "run-local":
        summary = run_local_m2(
            workspace=Path(args.workspace),
            window_key=args.window_key,
            top_n=args.top_n,
            channel=args.channel,
        )
        print(json.dumps(summary))
        return 0

    if args.command == "m3" and args.m3_cmd == "run-local":
        summary = run_local_m3(
            workspace=Path(args.workspace),
            window_start=args.window_start,
            window_end=args.window_end,
            success_within_hours=args.success_within_hours,
            auto_review_decision=args.auto_review_decision,
        )
        print(json.dumps(summary))
        return 0

    if args.command == "demo" and args.demo_cmd == "run-once":
        summary = run_demo_once(
            workspace=Path(args.workspace),
            window_key=args.window_key,
            limit=args.limit,
            top_n=args.top_n,
            channel=args.channel,
            max_attempts=args.max_attempts,
            resolved_entities_jsonl=Path(args.resolved_entities_jsonl) if args.resolved_entities_jsonl else None,
            outcomes_jsonl=Path(args.outcomes_jsonl) if args.outcomes_jsonl else None,
        )
        print(json.dumps(summary))
        return 0

    if args.command == "token" and args.token_cmd == "run-local":
        summary = run_local_token_promises(
            workspace=Path(args.workspace),
            window_key=args.window_key,
            limit=args.limit,
            baseline_weight=args.baseline_weight,
        )
        print(json.dumps(summary))
        return 0

    if args.command == "s1" and args.s1_cmd == "run-local":
        summary = run_local_s1(
            workspace=Path(args.workspace),
            date=args.date,
        )
        print(json.dumps(summary))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
