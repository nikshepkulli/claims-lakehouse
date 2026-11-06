"""Structural lint of src/lakehouse/ml/Dockerfile. CI never builds the image."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DF = ROOT / "src" / "lakehouse" / "ml" / "Dockerfile"


def _lines():
    text = re.sub(r"\\\n\s*", " ", DF.read_text())  # join continuations
    return [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]


def test_pinned_base_image_no_latest():
    froms = [ln for ln in _lines() if ln.upper().startswith("FROM ")]
    assert len(froms) == 1
    img = froms[0].split()[1]
    assert ":" in img and not img.endswith(":latest") and re.search(r":\d+\.\d+\.\d+", img)


def test_runs_unprivileged_and_uses_exec_form_entrypoint():
    ln = _lines()
    users = [x for x in ln if x.upper().startswith("USER ")]
    assert users and users[-1].split()[1] not in {"root", "0"}
    ep = [x for x in ln if x.upper().startswith("ENTRYPOINT ")]
    assert len(ep) == 1 and ep[0].split(" ", 1)[1].startswith('["python", "-m", "lakehouse.ml"]')


def test_copy_sources_exist_and_requirements_installed_from_the_pinned_file():
    ln = _lines()
    for x in ln:
        if x.upper().startswith("COPY "):
            *srcs, _dst = x.split()[1:]
            for s in srcs:
                assert (ROOT / s).exists(), f"COPY source missing: {s}"
    assert any(
        "pip install" in x and "-r requirements-ml.txt" in x and "--no-cache-dir" in x for x in ln
    )
    assert (ROOT / "requirements-ml.txt").is_file()


def test_no_secrets_or_remote_script_execution():
    text = DF.read_text()
    assert not re.search(r"AKIA[0-9A-Z]{16}|AWS_SECRET|aws_access_key|PASSWORD", text, re.I)
    assert not re.search(r"curl[^\n]*\|\s*(ba)?sh|wget[^\n]*\|\s*(ba)?sh", text)
    assert "ADD http" not in text


def test_registry_and_package_are_inside_the_image_layout():
    # lakehouse.features resolves governance/*.yaml as <root>/governance, package at <root>/src
    text = DF.read_text()
    assert "COPY governance ./governance" in text and "COPY src/lakehouse ./src/lakehouse" in text
    assert "PYTHONPATH=/opt/program/src" in text
