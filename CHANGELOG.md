# Changelog

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
