"""claimsgen: seeded, 100% synthetic healthcare-claims data generator.

All data produced by this package is fabricated. No real patients, providers
or PHI are used or derived from anything real.
"""

__version__ = "1.0.0"

from .config import DefectRates, GenConfig  # noqa: E402
from .generator import generate  # noqa: E402

__all__ = ["DefectRates", "GenConfig", "generate"]
