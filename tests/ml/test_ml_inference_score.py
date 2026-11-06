import json
import tarfile

import numpy as np
import pyarrow.parquet as pq
import pytest

from lakehouse.ml import artifact, data, inference
from lakehouse.ml.score import score


@pytest.fixture(scope="module")
def model(trained):
    return inference.model_fn(str(trained[0]))


def test_json_roundtrip_matches_batch_prediction(model, signal_store):
    f = data.load_features(signal_store, model.specs)
    ix = np.arange(25)
    rows = [
        {
            k: (None if (isinstance(v[i], float) and np.isnan(v[i])) else v[i])
            for k, v in f.cols.items()
        }
        for i in ix
    ]
    rows = [{k: (v.item() if hasattr(v, "item") else v) for k, v in r.items()} for r in rows]
    body = json.dumps({"instances": rows})
    parsed = inference.input_fn(body, "application/json")
    pred = inference.predict_fn(parsed, model)
    out = json.loads(inference.output_fn(pred, "application/json"))
    assert out["model_version"] == model.version
    assert out["feature_manifest_sha256"] == model.meta["feature_manifest_sha256"]
    expect = model.predict({k: v[ix] for k, v in f.cols.items()})
    assert np.allclose(out["scores"], expect, atol=1e-12)


def test_csv_roundtrip_headerless_in_feature_order(model, signal_store):
    f = data.load_features(signal_store, model.specs)
    names = [s.name for s in model.specs]
    lines = []
    for i in range(10):
        cells = []
        for n in names:
            v = f.cols[n][i]
            missing = v is None or (isinstance(v, float) and np.isnan(v))
            cells.append("" if missing else repr(float(v)) if isinstance(v, float) else str(v))
        lines.append(",".join(cells))
    pred = inference.predict_fn(inference.input_fn("\n".join(lines), "text/csv"), model)
    expect = model.predict({k: v[:10] for k, v in f.cols.items()})
    assert np.allclose(pred["scores"], expect, atol=1e-9)
    assert inference.output_fn(pred, "text/csv").count("\n") == 9


def test_missing_features_are_nulls_not_errors(model):
    pred = inference.predict_fn(
        inference.input_fn(json.dumps([{"billed_amount": 9000.0}]), "application/json"), model
    )
    assert len(pred["scores"]) == 1 and 0 <= pred["scores"][0] <= 1


@pytest.mark.parametrize("extra", ["member_id", "claim_id", "is_denied", "birth_year", "as_of"])
def test_requests_carrying_ids_labels_or_pii_are_rejected(model, extra):
    body = json.dumps({"instances": [{"billed_amount": 10.0, extra: "x"}]})
    with pytest.raises(ValueError, match=extra):
        inference.predict_fn(inference.input_fn(body), model)


def test_bad_payloads(model):
    with pytest.raises(ValueError):
        inference.input_fn("x", "text/plain")
    with pytest.raises(ValueError):
        inference.input_fn(json.dumps({"instances": [1, 2]}))
    with pytest.raises(ValueError, match="fields"):
        inference.predict_fn(inference.input_fn("1,2,3", "text/csv"), model)
    with pytest.raises(ValueError, match="not numeric"):
        n = len(model.specs)
        inference.predict_fn(inference.input_fn(",".join(["abc"] * n), "text/csv"), model)
    with pytest.raises(ValueError):
        inference.output_fn({"scores": []}, "image/png")


def test_model_dir_may_hold_only_sagemakers_model_tar_gz(trained, tmp_path):
    tgz = tmp_path / "model.tar.gz"
    with tarfile.open(tgz, "w:gz") as tf:
        for name in ("model.joblib", "model_meta.json"):
            tf.add(trained[0] / name, arcname=name)
    art = artifact.load(tmp_path)
    assert art.version == json.loads((trained[0] / "model_meta.json").read_text())["model_version"]


def test_scorer_writes_versioned_scores_and_is_idempotent(trained, signal_store, tmp_path):
    out = tmp_path / "scores"
    r1 = score(signal_store, trained[0], out)
    assert r1["status"] == "written"
    t = pq.read_table(out / "part-00000.parquet")
    assert t.column_names == [
        "claim_id",
        "as_of",
        "denial_risk_score",
        "model_name",
        "model_version",
        "feature_manifest_sha256",
    ]
    assert set(t.column("model_version").to_pylist()) == {r1["model_version"]}
    assert set(t.column("feature_manifest_sha256").to_pylist()) == {r1["feature_manifest_sha256"]}
    ids = t.column("claim_id").to_pylist()
    assert ids == sorted(ids) and len(set(ids)) == len(ids) == r1["rows"]
    s = np.array(t.column("denial_risk_score").to_pylist())
    assert ((s >= 0) & (s <= 1)).all()
    assert "member_id" not in t.column_names and "is_denied" not in t.column_names

    before = (out / "part-00000.parquet").read_bytes()
    mtime = (out / "part-00000.parquet").stat().st_mtime_ns
    r2 = score(signal_store, trained[0], out)
    assert r2["status"] == "skipped" and (out / "part-00000.parquet").stat().st_mtime_ns == mtime
    r3 = score(signal_store, trained[0], out, force=True)
    assert r3["status"] == "written" and r3["scores_sha256"] == r1["scores_sha256"]
    assert (out / "part-00000.parquet").read_bytes() == before
    assert not (tmp_path / "scores.tmp").exists()


def test_feature_manifest_hash_tracks_the_feature_table(trained, signal_store, tmp_path):
    from lakehouse.ml.manifest import feature_manifest_sha256

    a = feature_manifest_sha256(signal_store)
    assert a == feature_manifest_sha256(signal_store) and len(a) == 64
    import shutil

    other = tmp_path / "other"
    shutil.copytree(signal_store, other)
    m = json.loads((other / "_manifest.json").read_text())
    m["tables"]["claim_features"]["sha256"] = "0" * 64
    (other / "_manifest.json").write_text(json.dumps(m))
    assert feature_manifest_sha256(other) != a
    # a different manifest means a different input key: scoring is NOT skipped
    out = tmp_path / "s"
    score(signal_store, trained[0], out)
    assert score(other, trained[0], out)["status"] == "written"


def test_scorer_date_filter_and_registry_mismatch(trained, signal_store, tmp_path):
    out = tmp_path / "s"
    r = score(signal_store, trained[0], out, as_of_from="2024-09-01", as_of_to="2024-09-30")
    t = pq.read_table(out / "part-00000.parquet")
    d = np.array(t.column("as_of").to_numpy()).astype("datetime64[D]")
    assert (
        0 < r["rows"] < 5000
        and d.min() >= np.datetime64("2024-09-01")
        and d.max() <= np.datetime64("2024-09-30")
    )
    import shutil

    bad = tmp_path / "bad"
    shutil.copytree(signal_store, bad)
    m = json.loads((bad / "_manifest.json").read_text())
    m["registry_sha256"] = "f" * 64
    (bad / "_manifest.json").write_text(json.dumps(m))
    with pytest.raises(ValueError, match="registry"):
        score(bad, trained[0], tmp_path / "x")
