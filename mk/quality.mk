# Quality targets, used locally and by CI: make -f mk/quality.mk lint test
.PHONY: lint test fmt sqlfluff

PY_PATHS ?= .
SQL_PATHS ?= .

lint:
	ruff check $(PY_PATHS)
	ruff format --check $(PY_PATHS)
	sqlfluff lint $(SQL_PATHS)

test:
	pytest

fmt:
	ruff check --fix $(PY_PATHS)
	ruff format $(PY_PATHS)
	sqlfluff fix --force $(SQL_PATHS)
