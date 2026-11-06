import json

import pytest

from lakehouse.ml import cli, sagemaker_env
from lakehouse.ml.hyperparams import Hyperparameters


def _sm_root(tmp_path, hp=None, store=None):
    root = tmp_path / "opt_ml"
    (root / "input" / "config").mkdir(parents=True)
    (root / "input" / "data").mkdir(parents=True)
    if hp is not None:
        (root / "input" / "config" / "hyperparameters.json").write_text(json.dumps(hp))
    if store is not None:
        (root / "input" / "data" / "training").symlink_to(store, target_is_directory=True)
    return root


def test_hyperparameters_coerce_sagemaker_strings():
    hp = Hyperparameters.from_mapping(
        {
            "seed": "7",
            "lr_c": "0.5",
            "include_quasi": "true",
            "model": '"hgb"',
            "verify_inputs": "false",
            "sagemaker_program": "x",
            "sagemaker_job_name": "j",
            "_tuning_objective_metric": "m",
        }
    )
    assert (hp.seed, hp.lr_c, hp.include_quasi, hp.model, hp.verify_inputs) == (
        7,
        0.5,
        True,
        "hgb",
        False,
    )


@pytest.mark.parametrize(
    "bad", [{"sede": "1"}, {"seed": "x"}, {"model": "xgb"}, {"include_quasi": "maybe"},
            {"train_frac": "0.95"}, {"calibration": "magic"}],
)  # fmt: skip
def test_bad_hyperparameters_fail_loudly(bad):
    with pytest.raises(ValueError):
        Hyperparameters.from_mapping(bad)


def test_context_reads_fixed_paths_hyperparameters_and_channels(tmp_path, feature_store):
    root = _sm_root(tmp_path, {"seed": "3", "model": "logreg"}, feature_store)
    ctx = sagemaker_env.from_env({}, root=root)
    assert ctx.model_dir == root / "model" and ctx.output_data_dir == root / "output" / "data"
    assert ctx.channel("training") == root / "input" / "data" / "training"
    assert ctx.hp().seed == 3 and ctx.hp().model == "logreg"
    with pytest.raises(FileNotFoundError, match="validation"):
        ctx.channel("validation")


def test_sm_environment_overrides_win(tmp_path):
    root = _sm_root(tmp_path, {"seed": "3"})
    env = {
        "SM_MODEL_DIR": str(tmp_path / "m"),
        "SM_OUTPUT_DATA_DIR": str(tmp_path / "o"),
        "SM_CHANNEL_TRAINING": str(tmp_path / "chan"),
        "SM_HP_SEED": "9",
        "SM_HP_NOT_A_PARAM": "ignored",
    }
    ctx = sagemaker_env.from_env(env, root=root)
    assert ctx.model_dir == tmp_path / "m" and ctx.output_data_dir == tmp_path / "o"
    assert ctx.channel() == tmp_path / "chan"
    assert ctx.hp().seed == 9


def test_failure_file_is_written_and_truncated(tmp_path):
    ctx = sagemaker_env.from_env({}, root=_sm_root(tmp_path))
    p = ctx.write_failure("x" * 5000)
    assert p == tmp_path / "opt_ml" / "output" / "failure" and len(p.read_text()) == 1024


def test_cli_train_follows_the_container_contract(tmp_path, signal_store, capsys):
    root = _sm_root(
        tmp_path, {"verify_inputs": "false", "hgb_max_iter": "30", "model": "logreg"}, signal_store
    )
    assert cli.main(["train", "--sm-root", str(root)]) == 0
    assert (root / "model" / "model.joblib").is_file() and (
        root / "model" / "model_meta.json"
    ).is_file()
    assert (root / "output" / "data" / "metrics.json").is_file()
    assert not (root / "output" / "failure").exists()
    out = capsys.readouterr().out
    assert "test:auc_pr=" in out and "test:brier=" in out  # scrapeable by MetricDefinitions


def test_cli_train_failure_writes_failure_file_and_exits_nonzero(tmp_path):
    root = _sm_root(tmp_path, {"seed": "1"})  # no training channel
    assert cli.main(["train", "--sm-root", str(root)]) == 1
    assert "training" in (root / "output" / "failure").read_text()


def test_cli_rejects_unknown_hyperparameter_in_container_mode(tmp_path, feature_store):
    root = _sm_root(tmp_path, {"seeed": "1"}, feature_store)
    assert cli.main(["train", "--sm-root", str(root)]) == 1
    assert "seeed" in (root / "output" / "failure").read_text()
