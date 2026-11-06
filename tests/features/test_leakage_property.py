"""Leakage test 1: removing, shuffling or rewriting FUTURE rows never changes features at as_of.

Property-style: 12 seeded random datasets, a random as_of each, three perturbations of everything
the as_of cannot know (see ``features_helpers._variant``). Seeded loops instead of a property
library keep the dependency list unchanged and every failure reproducible by seed.
"""

import pytest
from features_helpers import future_invariance_violations


@pytest.mark.parametrize("seed", range(12))
def test_future_rows_never_change_features(seed, tmp_path):
    assert future_invariance_violations(seed, tmp_path) == []
