"""Entrypoint behaviour with a fake session and a fake CLI: no Spark, no Databricks."""

from __future__ import annotations

import sys
import types

import entrypoint
import pytest


class FakeSession:
    def __init__(self):
        self.stopped = False
        self.read = "reader"

    def stop(self):
        self.stopped = True


@pytest.fixture(autouse=True)
def restore_modules():
    saved = {k: sys.modules.get(k) for k in ("lakehouse.bronze.spark",)}
    yield
    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def test_proxy_neutralises_context_and_stop_but_delegates_the_rest():
    fake = FakeSession()
    proxy = entrypoint.SessionProxy(fake)
    proxy.sparkContext.setLogLevel("WARN")  # would raise under Spark Connect
    proxy.stop()
    assert fake.stopped is False
    assert proxy.read == "reader"


def test_shim_replaces_build_spark_import():
    fake = FakeSession()
    entrypoint.install_session_shim(lambda: fake)
    from lakehouse.bronze.spark import build_spark  # resolved from the shim, no delta import

    session = build_spark("x", master="local[*]", bronze_root="/Volumes/c/s/v/bronze", conf={})
    assert session._session is fake


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_main_forwards_remaining_args_to_the_layer_cli(monkeypatch, layer):
    seen = {}
    fake_cli = types.SimpleNamespace(main=lambda argv: seen.setdefault("argv", argv) and 0)
    monkeypatch.setitem(sys.modules, f"lakehouse.{layer}.cli", fake_cli)
    rc = entrypoint.main(
        ["--layer", layer, "--bronze", "/Volumes/c/s/v/bronze", "--batch-date", "all"],
        get_session=FakeSession,
    )
    assert rc == 0
    assert seen["argv"] == ["--bronze", "/Volumes/c/s/v/bronze", "--batch-date", "all"]


def test_cli_exit_code_is_propagated(monkeypatch):
    monkeypatch.setitem(sys.modules, "lakehouse.gold.cli", types.SimpleNamespace(main=lambda a: 1))
    assert entrypoint.main(["--layer", "gold", "--batch-date", "all"], get_session=FakeSession) == 1


def test_unknown_layer_rejected():
    with pytest.raises(SystemExit):
        entrypoint.parse(["--layer", "platinum"])


@pytest.mark.parametrize(
    "root",
    [
        "/Volumes/workspace/claims_lakehouse/lake/silver",
        "s3a://claims-dev-silver/",
        "abfss://silver@claimsdevstorage.dfs.core.windows.net/",
    ],
)
def test_valid_roots_accepted(root):
    entrypoint.validate_roots(["--silver", root])


@pytest.mark.parametrize(
    "root", ["s3://claims-dev-silver/", "wasbs://c@a.blob.core.windows.net/", "data/silver"]
)
def test_bad_roots_rejected(root):
    with pytest.raises(SystemExit):
        entrypoint.validate_roots(["--silver", root])


def test_root_flag_without_value_rejected():
    with pytest.raises(SystemExit):
        entrypoint.validate_roots(["--gold"])
