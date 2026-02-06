# Changelog

## 0.1.8 - 2026-02-06

- Added token/promise workers:
  - `ResolveTokenWorker`
  - `TokenHealthScorerWorker`
  - `PromiseEvaluatorWorker`
  - `PromiseCalibrationWorker`
- Added `metaspn token run-local` orchestration command
- Added `human_judgment` promise routing to manual review artifacts
- Extended digest generation with optional token health and promise evaluation sections
- Extended M3 calibration report output with promise predictive accuracy and weight-adjustment proposal passthrough
- Added integration tests for token signal -> promise evaluation -> calibration flow

## 0.1.7 - 2026-02-06

- Added demo-oriented one-shot orchestration: `metaspn demo run-once`
- Added staged queue execution helper for profile -> score -> route -> digest -> optional draft
- Added manual outcomes ingestion hook for learning pass (`--outcomes-jsonl`)
- Added resolved-entity seeding helper (`--resolved-entities-jsonl`)
- Added integration test for full demo queue cycle with idempotent re-run behavior

## 0.1.6 - 2026-02-06

- Removed `SQLiteQueueStub` from `metaspn_ops.backends` and top-level package exports
- Public API now explicitly reflects filesystem queue runtime in `metaspn-ops`
- Added regression test to ensure removed symbol is not reintroduced

Migration note:
- If downstream code imports `SQLiteQueueStub`, remove that import and use `FilesystemQueue` from `metaspn_ops` for queue/runtime concerns.

## 0.1.5 - 2026-02-06

- Added M3 operational learning workers:
  - `OutcomeEvaluatorWorker`
  - `FailureAnalystWorker`
  - `CalibrationReporterWorker`
  - `CalibrationReviewWorker`
- Added `metaspn m3 run-local` command for scheduled evaluate/analyze/report/review flow
- Added end-to-end tests for attempt -> outcome -> failure -> calibration pipeline
- Added deterministic, reviewable calibration proposal capture without implicit policy mutation

## 0.1.4 - 2026-02-06

- Added M2 workers:
  - `DigestWorker` for deterministic ranked daily top-N digests
  - `DrafterWorker` for channel-specific outreach drafts
  - `ApprovalWorker` for approve/edit/reject capture with override support
- Added `metaspn m2 run-local --workspace ... --window-key ... --top-n ... --channel ...`
- Added end-to-end tests for recommend -> digest -> draft -> approval flow with duplicate-safe retries

## 0.1.3 - 2026-02-06

- Fixed filesystem lease race conditions with atomic lock publication (`tempfile + link`) and conservative parse handling
- Added M1 worker runtime templates:
  - `ProfilerWorker`
  - `ScorerWorker`
  - `RouterWorker`
- Added `metaspn m1 run-local --workspace ... --limit ...` one-command local stage chain
- Added integration tests for profile->score->route chaining and duplicate-safe retries
- Added lease race regression tests

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
