# Publishing to PyPI

## One-time setup

1. Create GitHub repository `metaspn-ops`.
2. Push this project to GitHub.
3. In PyPI, create a Trusted Publisher for this repository:
   - Owner: your GitHub org/user
   - Repository: `metaspn-ops`
   - Workflow: `.github/workflows/publish.yml`
   - Environment: `pypi`
4. In GitHub, create environment `pypi` (Settings -> Environments).

## Release flow

1. Update version in `pyproject.toml`.
2. Update `CHANGELOG.md`.
3. Create and push a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

4. Publish a GitHub Release for the tag.
5. `publish.yml` builds and uploads to PyPI.

## Local checks

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
python -m build
python -m twine check dist/*
```
