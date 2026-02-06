# Changelog

## 0.1.2 - 2026-02-06

- Added M0 worker templates:
  - `IngestSocialWorker` for JSONL ingestion into signal envelopes
  - `ResolveEntityWorker` for unresolved signal resolution into emissions
- Added local JSONL store adapter and heuristic resolver templates for local execution
- Added `metaspn m0 run-local --workspace ... --input-jsonl ...` command
- Added queue execution tests for ingest + resolve path and duplicate-safe retries

## 0.1.1 - 2026-02-06

- Stabilized test import paths for `src/` layout by adding `tests/conftest.py`
- Switched local and CI test invocation to `python -m pytest -q`
- Added CI packaging sanity check by installing built wheel and importing package

## 0.1.0 - 2026-02-06

- Initial release of `metaspn-ops`
- Filesystem queue backend with inbox/outbox
- Lease and lock management with expiration
- Retry with exponential backoff and dead-letter handling
- Worker runner and CLI commands
