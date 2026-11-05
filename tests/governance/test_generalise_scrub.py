"""Generalisation of quasi-identifiers and free-text scrubbing."""

import json
import re
from datetime import date

import pytest
from governance_helpers import TEST_KEY

from lakehouse.governance.generalise import age_band, birth_year_band, bucket_start, year_of
from lakehouse.governance.scrub import scrub_text
from lakehouse.governance.tokenise import Tokeniser


@pytest.mark.parametrize(
    "year, start, label",
    [
        (1985, 1985, "1985-1989"),
        (1989, 1985, "1985-1989"),
        (1990, 1990, "1990-1994"),
        (None, None, None),
    ],
)
def test_birth_year_bucket(year, start, label):
    assert bucket_start(year) == start
    assert birth_year_band(year) == label


@pytest.mark.parametrize(
    "age, label",
    [(0, "0-4"), (37, "35-39"), (89, "85-89"), (90, "90+"), (104, "90+"), (None, None)],
)
def test_age_band_is_top_coded_at_90(age, label):
    assert age_band(age) == label


def test_date_generalised_to_year():
    assert year_of(date(2024, 7, 19)) == date(2024, 1, 1)
    assert year_of(None) is None


def test_scrub_removes_each_obvious_identifier_kind():
    text = (
        "Call 555-0142 or (415) 555-0199 or +1 415-555-0100. Mail jane.doe@example.org. "
        "SSN 123-45-6789. NPI 1234567890. Member M000000123 saw PRV0000042 on 2024-03-09 "
        "and 3/9/2024."
    )
    out, counts = scrub_text(text)
    for leaked in ["555-0142", "555-0199", "555-0100", "@example.org", "123-45-6789", "1234567890",
                   "M000000123", "PRV0000042", "2024-03-09", "3/9/2024"]:  # fmt: skip
        assert leaked not in out
    assert counts["PHONE"] == 3 and counts["EMAIL"] == 1 and counts["SSN"] == 1
    assert counts["MEMBER_ID"] == 1 and counts["PROVIDER_ID"] == 1 and counts["DATE"] == 2
    assert counts["ID_NUMBER"] == 1


def test_scrub_with_tokeniser_links_notes_to_structured_columns():
    t = Tokeniser(TEST_KEY)
    out, _ = scrub_text("Member M000000123 returned.", t)
    assert t.token("M000000123", "member") in out and "M000000123" not in out


def test_scrub_does_not_corrupt_tokens_or_clinical_text():
    t = Tokeniser(TEST_KEY)
    out, counts = scrub_text("Member M000000123, ICD E11.9, 3 months, 2 weeks.", t)
    again, again_counts = scrub_text(out, t)
    assert again == out and not again_counts  # idempotent: tokens are not re-scrubbed
    assert "E11.9" in out and "3 months" in out


def test_scrub_handles_none_and_empty():
    assert scrub_text(None)[0] is None
    assert scrub_text("")[0] == ""


def test_synthetic_notes_phones_are_scrubbed(raw_dir):
    notes = [
        json.loads(line)
        for f in sorted((raw_dir / "notes").rglob("*.jsonl"))
        for line in f.read_text().splitlines()
    ]
    phone = re.compile(r"555-01\d\d")
    with_phone = [n for n in notes if phone.search(n["text"])]
    assert with_phone, "fixture should contain phone-like strings; generator changed?"
    for n in notes:
        out, _ = scrub_text(n["text"], Tokeniser(TEST_KEY))
        assert not phone.search(out)
        assert n["member_id"] not in out
