# Python Release

This document covers the manual release flow for the `datalevin` Python wheel.

## Scope

This script builds and optionally uploads the universal wheel. It is useful for
local dry runs and explicit manual releases. The GitHub Actions workflow builds
the same wheel and smoke-tests it on Linux/macOS/Windows, but it does not
publish to PyPI or TestPyPI.

## Prerequisites

- Python 3.10+
- Clojure CLI
- A Python environment with `pip`
- A PyPI token or TestPyPI token

Typical credentials:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=...
```

## Dry Run

Build the universal wheel and run the smoke test:

```bash
./script/deploy-python --dry-run
```

Skip the smoke test if you only want the wheel build:

```bash
./script/deploy-python --dry-run --skip-smoke
```

## Publish

Upload to PyPI:

```bash
./script/deploy-python
```

Upload to TestPyPI:

```bash
./script/deploy-python --testpypi
```
