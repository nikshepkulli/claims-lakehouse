"""Offline checks of databricks/databricks.yml (ticket #10).

These prove the bundle is internally consistent and that it calls CLIs/arguments that exist.
They do not prove anything runs on Databricks.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest
import yaml

from lakehouse_common.storage import StorageConfigError, parse_uri

ROOT = Path(__file__).resolve().parents[2]
BUNDLE_DIR = ROOT / "databricks"
BUNDLE_FILE = BUNDLE_DIR / "databricks.yml"
TARGETS = ("dev", "prod")
LAYERS = ("bronze", "silver", "gold")
ORDER = {"bronze": None, "silver": "bronze", "gold": "silver"}
VAR_RE = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


@pytest.fixture(scope="module")
def bundle() -> dict:
    return yaml.safe_load(BUNDLE_FILE.read_text())


def variables(bundle: dict) -> dict[str, str]:
    return {k: str(v["default"]) for k, v in bundle["variables"].items()}


def resolve(value: str, defaults: dict[str, str]) -> str:
    for _ in range(5):  # variables reference variables (volume paths)
        value = VAR_RE.sub(lambda m: defaults[m.group(1)], value)
    return value


def jobs(bundle: dict, target: str) -> dict:
    return bundle["targets"][target]["resources"]["jobs"]


def tasks(bundle: dict, target: str) -> dict[str, dict]:
    (job,) = jobs(bundle, target).values()
    return {t["task_key"]: t for t in job["tasks"]}


def test_targets_and_default(bundle):
    assert set(bundle["targets"]) == set(TARGETS)
    assert bundle["targets"]["dev"]["default"] is True
    assert bundle["targets"]["dev"]["mode"] == "development"


@pytest.mark.parametrize("target", TARGETS)
def test_task_graph_retries_and_script(bundle, target):
    t = tasks(bundle, target)
    assert list(t) == list(LAYERS)
    for layer, upstream in ORDER.items():
        deps = [d["task_key"] for d in t[layer].get("depends_on", [])]
        assert deps == ([upstream] if upstream else [])
        assert t[layer]["max_retries"] >= 1
        assert t[layer]["min_retry_interval_millis"] > 0
        task = t[layer]["spark_python_task"]
        assert (BUNDLE_DIR / task["python_file"]).is_file()
        assert task["parameters"][:2] == ["--layer", layer]


def test_dev_is_serverless_prod_is_cluster(bundle):
    (dev,) = jobs(bundle, "dev").values()
    assert "job_clusters" not in dev and "new_cluster" not in str(dev)
    (env,) = dev["environments"]
    assert env["spec"]["environment_version"]
    assert all(t["environment_key"] == env["environment_key"] for t in dev["tasks"])
    (prod,) = jobs(bundle, "prod").values()
    keys = {c["job_cluster_key"] for c in prod["job_clusters"]}
    assert {t["job_cluster_key"] for t in prod["tasks"]} <= keys
    assert all(t["libraries"] == [{"whl": "../dist/*.whl"}] for t in prod["tasks"])


@pytest.mark.parametrize("target", TARGETS)
def test_schedule_paused_by_default(bundle, target):
    (job,) = jobs(bundle, target).values()
    assert job["schedule"]["pause_status"] == "${var.schedule_status}"
    assert bundle["variables"]["schedule_status"]["default"] == "PAUSED"
    assert job["schedule"]["quartz_cron_expression"]


def test_all_variable_references_are_defined(bundle):
    defined = set(bundle["variables"])
    used = set(VAR_RE.findall(BUNDLE_FILE.read_text()))
    assert used <= defined, f"undefined: {used - defined}"
    assert defined <= used, f"unused: {defined - used}"


@pytest.mark.parametrize("target", TARGETS)
def test_cli_arguments_exist(bundle, target):
    """Each task's parameters must parse with the real layer CLI (unknown flag => SystemExit)."""
    defaults = variables(bundle)
    for layer, task in tasks(bundle, target).items():
        params = [
            resolve(p, defaults).replace("{{job.parameters.batch_date}}", "2024-01-05")
            for p in task["spark_python_task"]["parameters"]
        ]
        assert "{{" not in " ".join(params).replace("2024-01-05", "")
        cli = importlib.import_module(f"lakehouse.{layer}.cli")
        ns = cli.parse_args(params[2:])  # drop --layer <layer>
        assert ns.batch_date == "2024-01-05"
        for key in ("input", "bronze", "silver", "gold"):
            if hasattr(ns, key) and getattr(ns, key) is not None:
                assert str(getattr(ns, key)).startswith("/Volumes/")


def test_job_parameter_matches_template(bundle):
    for target in TARGETS:
        (job,) = jobs(bundle, target).values()
        assert [p["name"] for p in job["parameters"]] == ["batch_date"]
        text = str(job["tasks"])
        assert set(re.findall(r"\{\{job\.parameters\.(\w+)\}\}", text)) == {"batch_date"}


@pytest.mark.parametrize(
    "uri",
    [
        "s3a://claims-dev-bronze/",
        "abfss://bronze@claimsdevstorage.dfs.core.windows.net/",
    ],
)
def test_cloud_roots_the_bundle_documents_pass_the_resolver(uri):
    assert parse_uri(uri)["scheme"] in ("s3a", "abfss")


@pytest.mark.parametrize("uri", ["s3://b/", "wasbs://c@a.blob.core.windows.net/"])
def test_unsupported_schemes_are_rejected(uri):
    with pytest.raises(StorageConfigError):
        parse_uri(uri)


def test_roots_default_to_volumes_under_one_catalog_schema_volume(bundle):
    d = variables(bundle)
    for name in ("raw_input", "bronze_root", "silver_root", "gold_root"):
        assert resolve(d[name], d).startswith("/Volumes/workspace/claims_lakehouse/lake/")
    assert len({resolve(d[n], d) for n in ("bronze_root", "silver_root", "gold_root")}) == 3


def test_no_hosts_tokens_or_secrets_in_bundle_files():
    text = "\n".join(
        p.read_text() for p in BUNDLE_DIR.iterdir() if p.suffix in (".yml", ".py") and p.is_file()
    )
    # offline_validate.py uses a loopback stub; any other URL or credential-looking value fails.
    urls = [u for u in re.findall(r"https?://[^\s\"')]+", text) if "127.0.0.1" not in u]
    assert urls == [], urls
    assert not re.search(r"dapi[0-9a-f]{20,}|\bAKIA[0-9A-Z]{12,}|workspace_id|account_id", text)
    assert not re.search(r"^\s*(host|token|client_secret|profile):", text, re.M)
    assert not re.search(r"\.(cloud\.databricks\.com|azuredatabricks\.net)", text)


def test_wheel_artifact_points_at_repo_pyproject(bundle):
    art = bundle["artifacts"]["lakehouse_wheel"]
    assert art["type"] == "whl"
    assert (BUNDLE_DIR / art["path"] / "pyproject.toml").is_file()
    assert art["files"] == [{"source": "../dist/*.whl"}]
