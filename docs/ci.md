# CI

Workflow: `.github/workflows/ci.yml`. Runs on every pull request and on pushes to `main`
(ubuntu-latest, Python 3.11, actions pinned to major tags). Each check is its own job so a
failure points at one cause.

| Job | What it runs |
| --- | --- |
| `python lint + format` | `ruff check .` and `ruff format --check .` |
| `pytest` | `make -f mk/quality.mk test` |
| `sqlfluff` | `sqlfluff lint .` (dialect `sparksql`, see `.sqlfluff`) |
| `discover terraform dirs` | Lists `infra/*` directories as a JSON matrix |
| `terraform (<dir>)` | Per directory: `terraform fmt -check -recursive`, `init -backend=false`, `validate`. Skipped when `infra/` has no subdirectories. |
| `terraform (gate)` | Single stable check name; passes when terraform succeeded or was skipped. Use this one as the required status check, since matrix job names change as `infra/*` grows. |

## Run locally

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
make -f mk/quality.mk lint test   # fmt target auto-fixes
```

Tool versions are pinned in `requirements-dev.txt`; ruff and pytest settings live in
`pyproject.toml`.

## Required status checks on `main`

Branch protection is a repo setting, not code. Require: `python lint + format`, `pytest`,
`sqlfluff`, `terraform (gate)`.
