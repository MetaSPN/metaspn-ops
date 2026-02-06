# Contributing

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Run tests

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Build package

```bash
python -m build
python -m twine check dist/*
```
