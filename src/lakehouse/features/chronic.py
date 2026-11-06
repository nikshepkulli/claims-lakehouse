"""Chronic-condition flags from PRIMARY ICD-10 codes (gold keeps only the primary diagnosis).

One regex per condition, shared by the SQL engine (DuckDB RE2) and the online implementation
(Python ``re``); the patterns use only syntax both accept. A code must be well formed, so junk
such as ``I10-9`` never sets a flag.
"""

from __future__ import annotations

_SUFFIX = r"(\.[0-9A-Z]{1,4})?$"

CONDITIONS: dict[str, str] = {
    "diabetes": rf"^E1[0-4]{_SUFFIX}",
    "hypertension": rf"^I1[0-6]{_SUFFIX}",
    "coronary_artery_disease": rf"^I2[0-5]{_SUFFIX}",
    "heart_failure": rf"^I50{_SUFFIX}",
    "copd": rf"^J4[0-4]{_SUFFIX}",
    "asthma": rf"^J4[56]{_SUFFIX}",
    "ckd": rf"^N18{_SUFFIX}",
    "depression": rf"^F3[23]{_SUFFIX}",
}
