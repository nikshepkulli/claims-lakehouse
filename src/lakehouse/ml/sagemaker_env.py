"""SageMaker container contract, resolved against a configurable root (default ``/opt/ml``).

Training container contract (what SageMaker mounts, no AWS call is made here):

* ``<root>/input/data/<channel>/``        input channels (we use ``training`` = the feature store)
* ``<root>/input/config/hyperparameters.json``   all values are strings
* ``<root>/model/``                        whatever is written here is tarred to S3 as model.tar.gz
* ``<root>/output/data/``                  extra artifacts, tarred to S3 as output.tar.gz
* ``<root>/output/failure``                failure reason shown by DescribeTrainingJob

Environment overrides honoured when present (set by the sagemaker-training-toolkit; plain BYO
containers get only the fixed paths): ``SM_MODEL_DIR``, ``SM_OUTPUT_DATA_DIR``,
``SM_INPUT_CONFIG_DIR``, ``SM_CHANNEL_<NAME>``, ``SM_HP_<NAME>``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from .hyperparams import Hyperparameters

DEFAULT_ROOT = "/opt/ml"
FAILURE_MAX = 1024  # SageMaker truncates the failure message


@dataclass(frozen=True)
class SMContext:
    root: Path
    model_dir: Path
    output_dir: Path  # <root>/output ; the failure file lives here
    output_data_dir: Path
    config_dir: Path
    channels: dict[str, Path] = field(default_factory=dict)
    hyperparameters: dict = field(default_factory=dict)

    def channel(self, name: str = "training") -> Path:
        if name not in self.channels:
            raise FileNotFoundError(
                f"input channel {name!r} not found; have {sorted(self.channels)} "
                f"(expected {self.root}/input/data/{name})"
            )
        return self.channels[name]

    def hp(self) -> Hyperparameters:
        return Hyperparameters.from_mapping(self.hyperparameters)

    def write_failure(self, message: str) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        p = self.output_dir / "failure"
        p.write_text(message[:FAILURE_MAX])
        return p


def from_env(environ: dict | None = None, root: str | Path | None = None) -> SMContext:
    env = os.environ if environ is None else environ
    root = Path(root if root is not None else DEFAULT_ROOT)
    model_dir = Path(env.get("SM_MODEL_DIR", root / "model"))
    output_data = Path(env.get("SM_OUTPUT_DATA_DIR", root / "output" / "data"))
    config_dir = Path(env.get("SM_INPUT_CONFIG_DIR", root / "input" / "config"))
    channels: dict[str, Path] = {}
    data = root / "input" / "data"
    if data.is_dir():
        channels = {p.name: p for p in sorted(data.iterdir()) if p.is_dir()}
    for k, v in env.items():
        if k.startswith("SM_CHANNEL_"):
            channels[k[len("SM_CHANNEL_") :].lower()] = Path(v)
    hp: dict = {}
    hp_file = config_dir / "hyperparameters.json"
    if hp_file.is_file():
        hp = json.loads(hp_file.read_text())
        if not isinstance(hp, dict):
            raise ValueError(f"{hp_file}: expected a JSON object")
    fields_lc = set(Hyperparameters.__dataclass_fields__)
    for k, v in env.items():
        if k.startswith("SM_HP_") and k[len("SM_HP_") :].lower() in fields_lc:
            hp[k[len("SM_HP_") :].lower()] = v
    return SMContext(
        root=root,
        model_dir=model_dir,
        output_dir=root / "output",
        output_data_dir=output_data,
        config_dir=config_dir,
        channels=channels,
        hyperparameters=hp,
    )
