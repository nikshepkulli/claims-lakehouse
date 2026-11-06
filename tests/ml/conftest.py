import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
pytest.importorskip("sklearn")


@pytest.fixture(scope="session")
def feature_store(tmp_path_factory) -> Path:
    """A real feature store (claimsgen -> gold-shaped export -> features.store.build), ~6k claims.
    Denials in this data are iid noise, so metrics here are ~chance: used for plumbing tests."""
    work = tmp_path_factory.mktemp("ml_store")
    env = {**os.environ, "PYTHONPATH": str(SRC)}
    r = subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "6000", "--seed", "7",
         "--out", str(work / "raw"), "--quiet"],
        env=env, capture_output=True, text=True,
    )  # fmt: skip
    assert r.returncode == 0, r.stderr[-2000:]
    from lakehouse.features import store
    from lakehouse.features.fixture import from_claimsgen

    from_claimsgen(work / "raw", work / "export", seed=7)
    store.build(work / "export", work / "store", guard=False)
    return work / "store"


@pytest.fixture(scope="session")
def signal_store(feature_store, tmp_path_factory) -> Path:
    """Copy of the store whose labels are REPLACED by a planted rule (billed_amount, line_count),
    so the pipeline has something learnable (the generator's labels have none). The manifest hash
    of the labels is stale on purpose: tests using it pass verify_inputs=False."""
    import duckdb

    dst = tmp_path_factory.mktemp("ml_signal") / "store"
    shutil.copytree(feature_store, dst)
    con = duckdb.connect()
    feats = f"{dst}/feature_store/claim_features/*/*.parquet"
    for p in sorted(dst.glob("labels/claim_denial/*/*.parquet")):
        new = Path(f"{p}.new")
        con.execute(
            f"""COPY (
              SELECT l.claim_id, l.as_of,
                     (f.billed_amount > 5000
                      OR (f.line_count >= 4 AND hash(l.claim_id) % 5 = 0)) AS is_denied,
                     l.label_available_at
              FROM read_parquet('{p}') l JOIN read_parquet('{feats}') f USING (claim_id)
            ) TO '{new}' (FORMAT parquet)"""
        )
        new.replace(p)
    return dst


@pytest.fixture(scope="session")
def trained(signal_store, tmp_path_factory):
    """(model_dir, report) trained once on the planted-signal store."""
    from lakehouse.ml.hyperparams import Hyperparameters
    from lakehouse.ml.train import train

    d = tmp_path_factory.mktemp("ml_trained")
    hp = Hyperparameters(verify_inputs=False, hgb_max_iter=60)
    report = train(signal_store, d / "model", d / "out", hp)
    return d / "model", report
