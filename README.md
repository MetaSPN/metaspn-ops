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
