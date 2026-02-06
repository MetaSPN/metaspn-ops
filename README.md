# metaspn-ops

Standard inbox/outbox worker runtime for MetaSPN agent systems.

## Features

- Filesystem queue backend with inbox/outbox semantics
- Task leasing with lock files and lease expiration
- Retries with exponential backoff and dead-letter queue
- Worker runner with polling and parallel execution
- CLI for worker runs and queue operations

## Installation

```bash
pip install metaspn-ops
```

## Quickstart

### 1) Define a worker

```python
# example_worker.py
from metaspn_ops import Result, Task


class EnrichWorker:
    name = "enrich"

    def handle(self, task: Task) -> Result:
        payload = {"seen": task.payload}
        return Result(task_id=task.task_id, status="ok", payload=payload)
```

### 2) Run a worker once

```bash
metaspn worker run example_worker:EnrichWorker --workspace . --once --max-tasks 10
```

### 3) Queue inspection

```bash
metaspn queue stats enrich --workspace .
metaspn queue deadletter list enrich --workspace .
metaspn queue retry enrich --workspace .
```

## M0 Local Ingestion Flow

One command local run sequence (ingest + resolve):

```bash
metaspn m0 run-local --workspace . --input-jsonl ./sample_social.jsonl
```

This writes:
- `./store/signals.jsonl`
- `./store/emissions.jsonl`

### M0 task/result contracts

`ingest_social` task payload:

```json
{
  "input_jsonl_path": "/abs/or/rel/path.jsonl",
  "source": "social.ingest",
  "schema_version": "v1",
  "max_records": 100
}
```

`ingest_social` result payload:

```json
{
  "ingested": 10,
  "duplicates": 0,
  "total_seen": 10,
  "signal_ids": ["sig_..."]
}
```

`resolve_entity` task payload:

```json
{
  "limit": 100
}
```

`resolve_entity` result payload:

```json
{
  "resolved": 10,
  "duplicates": 0,
  "considered": 10,
  "emission_ids": ["em_..."]
}
```

## M1 Local Routing Flow

Prerequisite: `./store/emissions.jsonl` contains `entity.resolved` emissions.

Run profile -> score -> route in one command:

```bash
metaspn m1 run-local --workspace . --limit 100
```

Outputs:
- `./store/m1_profiles.jsonl`
- `./store/m1_scores.jsonl`
- `./store/m1_routes.jsonl`

### M1 payload contracts

`profile_entity` task payload:

```json
{ "limit": 100 }
```

`profile_entity` result payload:

```json
{
  "profiled": 10,
  "duplicates": 0,
  "considered": 10,
  "profile_ids": ["profile_..."]
}
```

`score_entity` task payload:

```json
{ "limit": 100 }
```

`score_entity` result payload:

```json
{
  "scored": 10,
  "duplicates": 0,
  "considered": 10,
  "score_ids": ["score_..."]
}
```

`route_entity` task payload:

```json
{ "limit": 100 }
```

`route_entity` result payload:

```json
{
  "routed": 10,
  "duplicates": 0,
  "considered": 10,
  "route_ids": ["route_..."]
}
```

## M2 Local Recommendations Surfaces

Prerequisite: `./store/m1_routes.jsonl` exists with recommendation candidates.

```bash
metaspn m2 run-local --workspace . --window-key 2026-02-06 --top-n 5 --channel email
```

Outputs:
- `./store/m2_digests.jsonl`
- `./store/m2_drafts.jsonl`
- `./store/m2_approvals.jsonl`

### M2 payload contracts

`digest_recommendations` task payload:

```json
{ "window_key": "2026-02-06", "top_n": 5 }
```

`draft_outreach` task payload:

```json
{ "digest_id": "digest_...", "channel": "email" }
```

`capture_approval` task payload:

```json
{
  "draft_id": "draft_...",
  "decision": "approve",
  "override_body": "optional edited body",
  "editor": "human_name"
}
```

## Queue layout

```text
workspace/
  inbox/{worker_name}/
  outbox/{worker_name}/
  runs/{worker_name}/
  deadletter/{worker_name}/
  locks/{worker_name}/
```

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python -m pytest -q
python -m build
python -m twine check dist/*
```

## Release

- Tag a release in GitHub (for example `v0.1.0`).
- GitHub Actions builds and publishes to PyPI using trusted publishing.
- Configure a PyPI Trusted Publisher for this repository before the first release.
- See `/Users/leoguinan/MetaSPN/metaspn-ops/PUBLISHING.md` for the full flow.

## License

MIT
