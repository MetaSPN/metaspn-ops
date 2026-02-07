# Season 1 Operations Runbook

## Scope
Operational handling for Season 1 workers in `metaspn-ops`:
- `update_attention_scores`
- `project_rewards`
- `settle_season`
- `publish_season_summary`

## Standard local run

```bash
metaspn s1 run-local --workspace . --date YYYY-MM-DD
```

Artifacts:
- `store/s1_attention_scores.jsonl`
- `store/s1_reward_projections.jsonl`
- `store/s1_settlements.jsonl`
- `store/s1_summaries.jsonl`

## Worker health and queue status

```bash
metaspn queue stats update_attention_scores --workspace .
metaspn queue stats project_rewards --workspace .
metaspn queue stats settle_season --workspace .
metaspn queue stats publish_season_summary --workspace .
```

Check dead-letter queues:

```bash
metaspn queue deadletter list update_attention_scores --workspace .
metaspn queue deadletter list project_rewards --workspace .
metaspn queue deadletter list settle_season --workspace .
metaspn queue deadletter list publish_season_summary --workspace .
```

## Retry/backoff behavior

- Failed tasks are re-queued with exponential backoff.
- `attempt_count` increments on each failure.
- Retries continue until `attempt_count >= max_attempts`.

Force single-run processing for diagnosis:

```bash
metaspn worker run metaspn_ops.workers.s1:ProjectRewardsWorker \
  --workspace . \
  --once \
  --max-tasks 1
```

## Dead-letter handling

When retries are exhausted, task payloads are moved to `deadletter/<worker>/`.

Recovery options:
1. Fix root cause (bad input, missing upstream artifact, code regression).
2. Requeue all dead-letter tasks:

```bash
metaspn queue retry project_rewards --workspace .
```

3. Requeue a specific task by ID:

```bash
metaspn queue retry project_rewards --workspace . --task-id <task_id>
```

4. Re-run the local orchestration for deterministic replay:

```bash
metaspn s1 run-local --workspace . --date YYYY-MM-DD
```

## Idempotency and replay

Season 1 workers use deterministic IDs derived from `(date, subject_ref, stage parameters)`.
Re-running the same date does not duplicate artifacts and is safe for operational replay.
