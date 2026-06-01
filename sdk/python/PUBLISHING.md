# Publishing `udb-client` to PyPI

## 1. Prepare the package

```bash
cd sdk/python
uv sync --extra dev
uv run python scripts/generate_protos.py
uv run pytest
uv run pyrefly check
uv run python -m build
uv run twine check dist/*
```

If you are not using `uv`, replace `uv run ...` with the same commands inside a virtualenv after `python -m pip install -e ".[dev]"`.

Make sure `pyproject.toml` has the version you want to publish. PyPI versions are immutable, so bump the version before rebuilding if a publish attempt already uploaded that version.

## 2. Test on TestPyPI

```bash
twine upload --repository testpypi dist/*
python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  udb-client==0.2.0
```

## 3. Publish to PyPI

This repo has a GitHub `production` environment secret named `PYPI_API_TOKEN`.
Publish from GitHub Actions with either:

```bash
git tag v0.2.0
git push origin v0.2.0
```

or run the `Release Python SDK` workflow manually and enter `0.2.0`.

Manual fallback:

```bash
twine upload dist/*
```

Use a PyPI API token, not your account password. For username, use `__token__`; for password, paste the token value.

## 4. Suggested GitHub Actions release job

```yaml
name: Publish Python SDK

on:
  push:
    tags:
      - "v*"

jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: python -m pip install build grpcio-tools
        working-directory: sdk/python
      - run: python scripts/generate_protos.py
        working-directory: sdk/python
      - run: python -m build
        working-directory: sdk/python
      - run: twine upload dist/*
        working-directory: sdk/python
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
```
