"""Wheel contents and (when the CLI is installed) real ``databricks bundle validate``."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_wheel_ships_job_packages_only_and_no_runtime_dependencies(tmp_path):
    pytest.importorskip("setuptools")
    from setuptools import build_meta

    shutil.copytree(ROOT / "src", tmp_path / "src", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy(ROOT / "pyproject.toml", tmp_path)
    out = tmp_path / "dist"
    out.mkdir()
    import os

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        name = build_meta.build_wheel(str(out))
    finally:
        os.chdir(cwd)
    with zipfile.ZipFile(out / name) as z:
        names = z.namelist()
        meta = z.read(next(n for n in names if n.endswith("METADATA"))).decode()
    tops = {n.split("/")[0] for n in names if not n.endswith(".dist-info") and "/" in n}
    assert {"lakehouse", "lakehouse_common"} <= tops
    assert "claimsgen" not in tops
    assert "Requires-Dist" not in meta  # PySpark/Delta come from the Databricks runtime
    assert "lakehouse/bronze/cli.py" in names and "lakehouse/gold/cli.py" in names


@pytest.mark.skipif(shutil.which("databricks") is None, reason="databricks CLI not installed")
def test_bundle_validate_against_local_stub(capfd):
    import offline_validate

    assert offline_validate.validate() == 0


def test_wiring_makefile_ci_and_deploy_workflow():
    mk = (ROOT / "mk" / "databricks.mk").read_text()
    for target in ("databricks-validate:", "databricks-test:", "databricks-deploy:"):
        assert target in mk
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "databricks bundle (validate)" in ci and "offline_validate.py" in ci
    deploy = (ROOT / ".github" / "workflows" / "databricks-deploy.yml").read_text()
    triggers = yaml.safe_load(deploy)[True]  # YAML 1.1 parses the key `on` as True
    assert set(triggers) == {"workflow_dispatch"}
    assert "secrets.DATABRICKS_TOKEN" in deploy and "dapi" not in deploy
