"""Chunked, seeded generation of claims data with injected defects.

Determinism: every chunk draws from its own RNG (seed, chunk index), so output
is a pure function of (seed, rows, chunk_size, config) - no dependence on wall
clock, hash ordering or process state. Memory is bounded by ``chunk_size``.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from . import __version__
from . import reference as ref
from .config import (STREAM_CHUNK, STREAM_DIMS, STREAM_DRIFT, STREAM_VOLUME, GenConfig,
                     rng_for)
from .writers import Drift, Sink, date32, epoch_days, iso

SAMPLE_N = 5
DRIFT_COLUMN = "prior_auth_flag"
LATE_MIN_DAYS, LATE_MAX_DAYS = 31, 90  # normal lag is 0-2 days


@dataclass
class Dims:
    n_members: int
    n_providers: int
    birth_year: np.ndarray
    sex: np.ndarray
    prov_mult: np.ndarray
    prov_cdf: np.ndarray
    tables: dict[str, pa.Table]


def fmt_ids(prefix: str, ints: np.ndarray, width: int) -> pa.Array:
    s = pc.utf8_lpad(pc.cast(pa.array(ints), pa.string()), width, "0")
    return pc.binary_join_element_wise(pa.scalar(prefix), s, pa.scalar(""))


def _round2(a: np.ndarray) -> np.ndarray:
    return np.round(a, 2)


def build_dims(cfg: GenConfig) -> Dims:
    r = rng_for(cfg.seed, STREAM_DIMS)
    m, p = cfg.n_members, cfg.n_providers
    birth_year = r.integers(1935, 2024, m).astype(np.int16)
    sex = r.integers(0, 2, m).astype(np.int8)
    state = r.integers(0, len(ref.STATES), m)
    plan = r.choice(len(ref.PLANS), m, p=ref.PLAN_P)
    enrolled = epoch_days(cfg.start_date) - r.integers(0, 3000, m)
    members = pa.table({
        "member_id": fmt_ids("M", np.arange(m), 9),
        "birth_year": pa.array(birth_year),
        "sex": pa.array(np.where(sex == 0, "F", "M")),
        "state": pa.array(np.array(ref.STATES)[state]),
        "plan_type": pa.array(np.array(ref.PLANS)[plan]),
        "enrollment_date": date32(enrolled),
    })
    # ~80% standard, ~18% elevated, ~2% very-high-cost providers.
    tier = r.choice(3, p, p=[0.80, 0.18, 0.02])
    mult = np.select(
        [tier == 0, tier == 1, tier == 2],
        [r.uniform(0.8, 1.1, p), r.uniform(1.2, 2.0, p), r.uniform(4.0, 8.0, p)])
    w = ref.zipf_weights(p, 0.7)[r.permutation(p)]  # heavy-tailed claim volume per provider
    providers = pa.table({
        "provider_id": fmt_ids("PRV", np.arange(p), 7),
        "specialty": pa.array(np.array(ref.SPECIALTIES)[r.integers(0, len(ref.SPECIALTIES), p)]),
        "state": pa.array(np.array(ref.STATES)[r.integers(0, len(ref.STATES), p)]),
        "network_status": pa.array(np.where(r.random(p) < 0.9, "IN_NETWORK", "OUT_OF_NETWORK")),
        "cost_tier": pa.array(np.array(["STANDARD", "ELEVATED", "HIGH_COST"])[tier]),
    })
    return Dims(m, p, birth_year, sex, mult, np.cumsum(w), {"members": members, "providers": providers})


def daily_plan(cfg: GenConfig) -> tuple[np.ndarray, np.ndarray]:
    """Claims per service day: winter respiratory-season peak, weekday/weekend, noise."""
    r = rng_for(cfg.seed, STREAM_VOLUME)
    off = np.arange(cfg.days)
    dates = [cfg.start_date + timedelta(days=int(i)) for i in off]
    doy = np.array([d.timetuple().tm_yday for d in dates])
    wkend = np.array([d.weekday() >= 5 for d in dates])
    w = (1 + 0.25 * np.cos(2 * math.pi * (doy - 15) / 365)) * np.where(wkend, 0.35, 1.0)
    w = w * r.lognormal(0, 0.05, cfg.days)
    counts = r.multinomial(cfg.rows, w / w.sum())
    return counts, np.cumsum(counts)


def pick_drift(cfg: GenConfig, counts: np.ndarray) -> Drift:
    r = rng_for(cfg.seed, STREAM_DRIFT)
    ok = [i for i in range(min(100, cfg.days - 1), min(250, cfg.days))
          if (cfg.start_date + timedelta(days=i)).weekday() < 5 and counts[i] > 0]
    if not ok:
        ok = [int(np.argmax(counts))]
    day = int(r.choice(ok))
    return Drift("claims", epoch_days(cfg.start_date) + day, DRIFT_COLUMN)


class Tally:
    """Counts distinct injected defects and keeps a few sample ids per type."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.samples: dict[str, list[str]] = {}

    def add(self, kind: str, mask_or_idx: np.ndarray, ids: pa.Array) -> None:
        idx = np.flatnonzero(mask_or_idx) if mask_or_idx.dtype == bool else mask_or_idx
        self.counts[kind] += int(len(idx))
        s = self.samples.setdefault(kind, [])
        if len(s) < SAMPLE_N and len(idx):
            s.extend(ids.take(pa.array(idx[: SAMPLE_N - len(s)])).to_pylist())


def _code_array(base: np.ndarray, valid_codes: np.ndarray, invalid: list[str],
                r: np.random.Generator, null_rate: float, invalid_rate: float,
                ) -> tuple[pa.Array, np.ndarray, np.ndarray]:
    """Dictionary-encode codes, replacing some with invalid strings and some with NULL."""
    n = len(base)
    idx = base.copy()
    inv = r.random(n) < invalid_rate
    idx[inv] = len(valid_codes) + r.integers(0, len(invalid), int(inv.sum()))
    nul = (r.random(n) < null_rate) & ~inv
    dictionary = pa.array(list(valid_codes) + invalid)
    arr = pa.DictionaryArray.from_arrays(pa.array(idx, mask=nul), dictionary).cast(pa.string())
    return arr, inv, nul


_NOTE_OPEN = ["Patient seen in clinic", "Follow-up visit", "Urgent visit", "Telehealth encounter",
              "Outpatient evaluation"]
_NOTE_PLAN = ["Continue current medications and recheck in 3 months.",
              "Ordered labs and imaging; return if symptoms worsen.",
              "Referred to specialist; discussed lifestyle modification.",
              "Started short course of treatment; follow up in 2 weeks.",
              "Education provided; no medication changes today."]
_NOTE_EXTRA = ["", "", "Patient reports good adherence.", "Denies fever or weight change.",
               "Family history reviewed; non-contributory.", "Callback number on file: 555-01{:02d}."]


def _make_notes(sel: np.ndarray, r: np.random.Generator, gidx: np.ndarray, svc: np.ndarray,
                midx: np.ndarray, member_ids: list[str], claim_ids: list[str], prim_icd: np.ndarray,
                dims: Dims) -> list[str]:
    k = len(sel)
    o, pl, ex, ph = (r.integers(0, len(_NOTE_OPEN), k), r.integers(0, len(_NOTE_PLAN), k),
                     r.integers(0, len(_NOTE_EXTRA), k), r.integers(0, 100, k))
    out = []
    for j, i in enumerate(sel.tolist()):
        mi = int(midx[i]) % dims.n_members
        age = 2024 - int(dims.birth_year[mi])
        who = "woman" if dims.sex[mi] == 0 else "man"
        c = int(prim_icd[i])
        extra = _NOTE_EXTRA[ex[j]].format(int(ph[j])) if "{" in _NOTE_EXTRA[ex[j]] else _NOTE_EXTRA[ex[j]]
        text = (f"{_NOTE_OPEN[o[j]]}. {age}-year-old {who} presenting with {ref.ICD_COMPLAINT[c]}. "
                f"Assessment: {ref.ICD_DESC[c]} ({ref.ICD_CODES[c]}). {_NOTE_PLAN[pl[j]]} "
                f"{extra} [SYNTHETIC NOTE - generated by claimsgen; not real patient data]").replace("  ", " ")
        out.append(json.dumps({
            "note_id": f"NTE{int(gidx[i]):012d}", "claim_id": claim_ids[i], "member_id": member_ids[i],
            "note_date": iso(int(svc[i])), "text": text.strip()}))
    return out


def _events(claim_ids, member_ids, prov_ids, ctype, billed, paid_total, denied, recv, gidx, r) -> list[str]:
    n = len(claim_ids)
    sub_ts = recv.astype("int64") * 86400 + r.integers(6 * 3600, 20 * 3600, n)
    out_ts = sub_ts + r.integers(1, 10, n) * 86400 + r.integers(0, 3600, n)
    ts = np.concatenate([sub_ts, out_ts])
    order = np.argsort(ts, kind="stable")
    iso_ts = (ts.astype("datetime64[s]").astype(str)).tolist()
    lines = []
    for e in order.tolist():
        i = e % n
        if e < n:
            lines.append('{"event_id":"EVT%013d","event_type":"claim_submitted","event_ts":"%sZ",'
                         '"claim_id":"%s","member_id":"%s","provider_id":"%s","claim_type":"%s",'
                         '"amount":%.2f}' % (2 * gidx[i], iso_ts[e], claim_ids[i], member_ids[i],
                                             prov_ids[i], ref.CLAIM_TYPES[ctype[i]], billed[i]))
        else:
            et, amt = ("claim_denied", 0.0) if denied[i] else ("claim_paid", paid_total[i])
            lines.append('{"event_id":"EVT%013d","event_type":"%s","event_ts":"%sZ","claim_id":"%s",'
                         '"member_id":"%s","provider_id":"%s","claim_type":"%s","amount":%.2f}'
                         % (2 * gidx[i] + 1, et, iso_ts[e], claim_ids[i], member_ids[i], prov_ids[i],
                            ref.CLAIM_TYPES[ctype[i]], amt))
    return lines


def gen_chunk(cfg: GenConfig, dims: Dims, cum: np.ndarray, ci: int, a: int, b: int,
              sink: Sink, tally: Tally) -> None:
    r = rng_for(cfg.seed, STREAM_CHUNK, ci)
    rates, M, P = cfg.rates, dims.n_members, dims.n_providers
    n = b - a
    gidx = np.arange(a, b, dtype=np.int64)
    claim_ids = fmt_ids("CLM", gidx, 12)

    # --- dates (+ late arrivals) ---
    svc = (epoch_days(cfg.start_date) + np.searchsorted(cum, gidx, side="right")).astype(np.int32)
    lag = r.integers(0, 3, n).astype(np.int32)
    late = r.random(n) < rates.late_arrival
    lag[late] = r.integers(LATE_MIN_DAYS, LATE_MAX_DAYS + 1, int(late.sum()))
    recv = svc + lag
    tally.add("late_arrival", late, claim_ids)

    # --- parties (skewed: ~u^2 member utilisation, zipf provider volume) ---
    midx = np.minimum((M * r.random(n) ** 2).astype(np.int64), M - 1)
    orphan = r.random(n) < rates.orphan_member
    midx_out = midx.copy()
    midx_out[orphan] = M + r.integers(0, 1_000_000, int(orphan.sum()))  # ids never in members
    tally.add("orphan_member_id", orphan, claim_ids)
    pidx = np.minimum(np.searchsorted(dims.prov_cdf, r.random(n) * dims.prov_cdf[-1]), P - 1)
    ctype = r.choice(3, n, p=ref.CLAIM_TYPE_P)
    pos = np.empty(n, dtype=object)
    for t, codes in ref.POS_BY_TYPE.items():
        m = ctype == t
        pos[m] = np.array(codes)[r.integers(0, len(codes), int(m.sum()))]
    denied = r.random(n) < 0.08

    # --- lines ---
    nl = np.minimum(1 + r.poisson(1.4, n), 6)
    L = int(nl.sum())
    line_c = np.repeat(np.arange(n), nl)
    line_no = np.arange(L) - np.repeat(np.cumsum(nl) - nl, nl) + 1
    proc = r.choice(len(ref.PROCEDURES), L, p=ref.PROC_W)
    units = 1 + r.poisson(0.3, L)
    billed = _round2(ref.PROC_BASE[proc] * units * dims.prov_mult[pidx][line_c] * r.lognormal(0, 0.3, L))
    allowed = _round2(billed * r.uniform(0.4, 0.8, L))
    paid = _round2(allowed * 0.8) * (~denied[line_c])
    line_ids = pc.binary_join_element_wise(claim_ids.take(pa.array(line_c)),
                                           fmt_ids("", line_no, 2), pa.scalar("-"))
    neg = r.random(L) < rates.negative_amount
    billed, allowed, paid = (np.where(neg, -x, x) + 0.0 for x in (billed, allowed, paid))
    tally.add("negative_amount", neg, line_ids)
    proc_arr, inv_p, nul_p = _code_array(proc, ref.PROC_CODES, ref.INVALID_PROC, r,
                                         rates.null_procedure_code, rates.invalid_procedure_code)
    tally.add("invalid_procedure_code", inv_p, line_ids)
    tally.add("null_procedure_code", nul_p, line_ids)
    tot = {k: _round2(np.bincount(line_c, v, n)) for k, v in
           (("b", billed), ("a", allowed), ("p", paid))}

    # --- diagnoses ---
    nd = np.minimum(1 + r.poisson(1.0, n), 4)
    D = int(nd.sum())
    dx_c = np.repeat(np.arange(n), nd)
    dx_seq = np.arange(D) - np.repeat(np.cumsum(nd) - nd, nd) + 1
    icd = r.choice(len(ref.DIAGNOSES), D, p=ref.ICD_W)
    prim_icd = icd[np.cumsum(nd) - nd]
    icd_arr, inv_i, nul_i = _code_array(icd, ref.ICD_CODES, ref.INVALID_ICD, r,
                                        rates.null_icd10, rates.invalid_icd10)
    dx_ids = pc.binary_join_element_wise(claim_ids.take(pa.array(dx_c)), fmt_ids("", dx_seq, 1), pa.scalar("-"))
    tally.add("invalid_icd10", inv_i, dx_ids)
    tally.add("null_icd10", nul_i, dx_ids)

    member_ids = fmt_ids("M", midx_out, 9)
    prov_ids = fmt_ids("PRV", pidx, 7)
    header = pa.table({
        "claim_id": claim_ids, "member_id": member_ids, "provider_id": prov_ids,
        "claim_type": pa.array(np.array(ref.CLAIM_TYPES)[ctype]),
        "place_of_service": pa.array(pos.tolist()),
        "service_date": date32(svc), "received_date": date32(recv),
        "claim_status": pa.array(np.where(denied, "DENIED", "PAID")),
        "line_count": pa.array(nl.astype(np.int16)),
        "total_billed_amount": pa.array(tot["b"]), "total_allowed_amount": pa.array(tot["a"]),
        "total_paid_amount": pa.array(tot["p"]),
    })
    lines = pa.table({
        "line_id": line_ids, "claim_id": claim_ids.take(pa.array(line_c)),
        "line_number": pa.array(line_no.astype(np.int16)), "procedure_code": proc_arr,
        "units": pa.array(units.astype(np.int16)), "service_date": date32(svc[line_c]),
        "billed_amount": pa.array(billed), "allowed_amount": pa.array(allowed),
        "paid_amount": pa.array(paid),
    })
    dx = pa.table({
        "claim_id": claim_ids.take(pa.array(dx_c)), "dx_seq": pa.array(dx_seq.astype(np.int16)),
        "icd10_code": icd_arr,
        "diagnosis_type": pa.array(np.where(dx_seq == 1, "PRIMARY", "SECONDARY")),
    })

    # --- duplicates: exact re-sends of whole claims (header + lines + dx) in the same batch ---
    dup_mask = r.random(n) < rates.duplicate_claim
    dup = np.flatnonzero(dup_mask)
    tally.add("duplicate_claim", dup, claim_ids)
    c_take = np.concatenate([np.arange(n), dup])
    l_take = np.concatenate([np.arange(L), np.flatnonzero(dup_mask[line_c])])
    d_take = np.concatenate([np.arange(D), np.flatnonzero(dup_mask[dx_c])])
    sink.write_partitioned("claims", header.take(pa.array(c_take)), recv[c_take], ci)
    sink.write_partitioned("claim_lines", lines.take(pa.array(l_take)), recv[line_c][l_take], ci)
    sink.write_partitioned("diagnoses", dx.take(pa.array(d_take)), recv[dx_c][d_take], ci)

    # --- unstructured + streaming sources (built from unique claims) ---
    cid_l, mid_l, pid_l = claim_ids.to_pylist(), member_ids.to_pylist(), prov_ids.to_pylist()
    sink.write_events(_events(cid_l, mid_l, pid_l, ctype, tot["b"], tot["p"], denied, recv, gidx, r))
    sel = np.flatnonzero(r.random(n) < cfg.notes_rate)
    if len(sel):
        sink.write_notes(_make_notes(sel, r, gidx, svc, midx, mid_l, cid_l, prim_icd, dims),
                         recv[sel], ci)


def generate(cfg: GenConfig, log: Callable[[str], None] = lambda msg: None) -> dict:
    """Generate all outputs under ``cfg.out``; returns (and writes) the defect manifest."""
    dims = build_dims(cfg)
    counts, cum = daily_plan(cfg)
    drift = pick_drift(cfg, counts)
    sink = Sink(cfg.out, cfg.formats, cfg.overwrite, drift)
    tally = Tally()
    try:
        sink.write_snapshot("members", dims.tables["members"], cfg.start_date)
        sink.write_snapshot("providers", dims.tables["providers"], cfg.start_date)
        n_chunks = math.ceil(cfg.rows / cfg.chunk_size)
        for ci in range(n_chunks):
            a, b = ci * cfg.chunk_size, min((ci + 1) * cfg.chunk_size, cfg.rows)
            gen_chunk(cfg, dims, cum, ci, a, b, sink, tally)
            log(f"chunk {ci + 1}/{n_chunks} done ({b:,}/{cfg.rows:,} claims)")
    finally:
        sink.close()
    manifest = build_manifest(cfg, drift, tally, sink, counts)
    (cfg.out / "defects_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def build_manifest(cfg: GenConfig, drift: Drift, tally: Tally, sink: Sink, counts: np.ndarray) -> dict:
    c = tally.counts
    r = cfg.rates
    spec = {
        "duplicate_claim": ("claims, claim_lines, diagnoses",
                            "Whole claim re-sent in the same ingest_date batch; extra rows share claim_id "
                            "(distinct claims duplicated = count). Detect: count(*) > count(distinct claim_id).",
                            r.duplicate_claim),
        "late_arrival": ("claims", f"received_date - service_date is {LATE_MIN_DAYS}-{LATE_MAX_DAYS} days "
                         "(normal lag is 0-2); row lands in a much later ingest_date partition.", r.late_arrival),
        "orphan_member_id": ("claims", "member_id has no row in members (id range outside the member dimension).",
                             r.orphan_member),
        "null_procedure_code": ("claim_lines", "procedure_code is NULL.", r.null_procedure_code),
        "invalid_procedure_code": ("claim_lines", "procedure_code fails ^(\\d{5}|[A-V]\\d{4})$.",
                                   r.invalid_procedure_code),
        "negative_amount": ("claim_lines", "billed/allowed/paid amounts negated (header totals follow the lines).",
                            r.negative_amount),
        "null_icd10": ("diagnoses", "icd10_code is NULL.", r.null_icd10),
        "invalid_icd10": ("diagnoses", "icd10_code fails ^[A-TV-Z]\\d[0-9A-Z](\\.[0-9A-Z]{1,4})?$.",
                          r.invalid_icd10),
    }
    defects = {k: {"count": int(c[k]), "table": t, "description": d, "rate": rate,
                   "sample_ids": tally.samples.get(k, [])} for k, (t, d, rate) in sorted(spec.items())}
    return {
        "synthetic_notice": "All data is 100% synthetic. No real PHI/PII.",
        "generator_version": __version__,
        "seed": cfg.seed, "claims": cfg.rows, "chunk_size": cfg.chunk_size,
        "n_members": cfg.n_members, "n_providers": cfg.n_providers,
        "window": {"start": cfg.start_date.isoformat(), "days": cfg.days},
        "formats": list(cfg.formats),
        "counting_note": "Counts are DISTINCT injected records (claim_id / line_id / '<claim_id>-<dx_seq>'); "
                         "duplicate_claim copies replicate rows including any other defect they carry.",
        "defects": defects,
        "schema_drift": {
            "table": drift.table, "ingest_date": iso(drift.day), "added_column": drift.column,
            "description": "Only the claims files of this ingest_date carry the extra column."},
        "rows_written": dict(sorted(sink.rows.items())),
        "files_written": dict(sorted(sink.files.items())),
        "claims_per_service_day": {"min": int(counts.min()), "max": int(counts.max()),
                                   "mean": round(float(counts.mean()), 1)},
    }
