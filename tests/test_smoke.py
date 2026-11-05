"""Smoke test so the pytest job is green until real tests land."""


def test_smoke() -> None:
    assert 1 + 1 == 2
