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

## M3 Local Learning Loop

Prerequisite:
- `./store/m3_attempts.jsonl`
- `./store/m3_outcomes.jsonl`

Run evaluate -> failure analysis -> calibration report (and optional review):

```bash
metaspn m3 run-local \
  --workspace . \
  --window-start 2026-02-01T00:00:00+00:00 \
  --window-end 2026-02-03T00:00:00+00:00 \
  --success-within-hours 48 \
  --auto-review-decision approve
```

Outputs:
- `./store/m3_evaluations.jsonl`
- `./store/m3_failures.jsonl`
- `./store/m3_calibration_reports.jsonl`
- `./store/m3_calibration_reviews.jsonl`

## Demo Orchestration

Run one-shot demo sequence (profile -> score -> route -> digest -> optional draft) and optionally ingest manual outcomes:

```bash
metaspn demo run-once \
  --workspace . \
  --window-key 2026-02-06 \
  --limit 100 \
  --top-n 5 \
  --channel email \
  --max-attempts 1 \
  --resolved-entities-jsonl ./resolved_entities.jsonl \
  --outcomes-jsonl ./manual_outcomes.jsonl
```

Notes:
- `--max-attempts` defaults to `1` in demo mode for predictable failure behavior.
- Re-running with the same inputs is idempotent at output artifact level.

## Token Promise Pipeline

Run token resolution -> health scoring -> promise evaluation -> promise calibration:

```bash
metaspn token run-local \
  --workspace . \
  --window-key 2026-02-06 \
  --limit 100 \
  --baseline-weight 1.0
```

Outputs:
- `./store/token_resolutions.jsonl`
- `./store/token_health_scores.jsonl`
- `./store/promise_evaluations.jsonl`
- `./store/promise_manual_reviews.jsonl`
- `./store/promise_calibration_reports.jsonl`

Evaluation paths:
- `on_chain` auto-evaluation
- `observable_signal` auto-evaluation
- `human_judgment` routed to manual review queue (`promise_manual_reviews.jsonl`)

Optional digest integration:
- `DigestWorker` supports `include_token_health=true` and `include_promise_items=true` in task payload.

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
