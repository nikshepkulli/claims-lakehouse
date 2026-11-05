# Synthetic claims data generator (`claimsgen`)

> **All data produced by this generator is 100% synthetic. It contains no real PHI or PII, is not derived from any real patient, member, provider or payer, and must never be mixed with real data.** Member and provider identifiers are sequential fabricated tokens, notes are templated text carrying a `[SYNTHETIC NOTE ...]` marker, and phone-like strings use the reserved fictional `555-01xx` range. Code *formats* (CPT-shaped procedure codes, ICD-10 codes) are real public vocabularies; their pairing with people is random.

## Quick start

```bash
pip install -r requirements-claimsgen.txt          # numpy + pyarrow
PYTHONPATH=src python -m claimsgen --rows 100000 --seed 42 --out data/raw
```

| Flag | Default | Meaning |
|---|---|---|
| `--rows` | 100000 | distinct **claims** (headers, min 1000). Lines are about 2.4x, diagnoses about 2x |
| `--seed` | 42 | whole output is a pure function of seed + settings |
| `--out` | `data/raw` | output root (gitignored). Refuses to overwrite unless `--overwrite` |
| `--chunk-size` | 250000 | claims per chunk; bounds memory (part of the determinism contract) |
| `--formats` | `parquet,csv` | drop `csv` to halve write time and disk |
| `--start-date`, `--days` | 2024-01-01, 366 | service-date window |
| `--notes-rate` | 0.25 | share of claims that get a clinical note |

Output layout (Hive-style date partitions; the date is the **ingest/batch date**, i.e. `received_date`):

```
data/raw/
  members/snapshot_date=2024-01-01/part-00000.{parquet,csv}
  providers/snapshot_date=2024-01-01/part-00000.{parquet,csv}
  claims/ingest_date=YYYY-MM-DD/part-<chunk>.{parquet,csv}
  claim_lines/ingest_date=.../part-<chunk>.{parquet,csv}
  diagnoses/ingest_date=.../part-<chunk>.{parquet,csv}
  events/claim_events.jsonl            # JSON-lines claim event stream (time-ordered within a chunk)
  notes/ingest_date=.../part-<chunk>.jsonl   # free-text clinical notes
  defects_manifest.json                # ground truth for every injected defect
```

## Schema

```mermaid
erDiagram
    MEMBERS ||--o{ CLAIMS : "member_id (orphans injected)"
    PROVIDERS ||--o{ CLAIMS : provider_id
    CLAIMS ||--|{ CLAIM_LINES : claim_id
    CLAIMS ||--|{ DIAGNOSES : claim_id
    CLAIMS ||--o{ NOTES : claim_id
    CLAIMS ||--|{ CLAIM_EVENTS : claim_id
    MEMBERS { string member_id PK "M000000000" 
              int16 birth_year
              string sex
              string state
              string plan_type
              date enrollment_date }
    PROVIDERS { string provider_id PK "PRV0000000"
                string specialty
                string state
                string network_status
                string cost_tier "STANDARD|ELEVATED|HIGH_COST" }
    CLAIMS { string claim_id PK "CLM000000000000"
             string member_id FK
             string provider_id FK
             string claim_type "PROFESSIONAL|INSTITUTIONAL|PHARMACY"
             string place_of_service
             date service_date
             date received_date "= ingest_date partition"
             string claim_status "PAID|DENIED"
             int16 line_count
             double total_billed_amount
             double total_allowed_amount
             double total_paid_amount }
    CLAIM_LINES { string line_id PK "claim_id-NN"
                  string claim_id FK
                  int16 line_number
                  string procedure_code
                  int16 units
                  date service_date
                  double billed_amount
                  double allowed_amount
                  double paid_amount }
    DIAGNOSES { string claim_id FK
                int16 dx_seq "PK with claim_id"
                string icd10_code
                string diagnosis_type "PRIMARY|SECONDARY" }
    NOTES { string note_id PK
            string claim_id FK
            string member_id
            date note_date
            string text }
    CLAIM_EVENTS { string event_id PK
                   string event_type "claim_submitted|claim_paid|claim_denied"
                   string event_ts
                   string claim_id FK
                   string member_id
                   string provider_id
                   string claim_type
                   double amount }
```

Amounts are `float64` rounded to cents (raw layer); downstream layers are expected to cast to `DECIMAL(12,2)`. Header totals equal the sum of their lines (after negative-amount injection).

## Realism

- **Cost skew:** ~80% of providers standard, ~18% elevated (1.2-2x), ~2% `HIGH_COST` (4-8x prices). Claim volume per provider is Zipf-distributed. Procedure and diagnosis frequencies are Zipf-weighted; billed amounts are log-normal around a per-code base price.
- **Member utilisation:** skewed (`id ~ N * u^2`), so a minority of members generate most claims.
- **Seasonality:** daily volume = winter respiratory-season cosine (+/-25%, peak mid-January), weekends at 35%, plus noise; the per-day counts are drawn from one multinomial so they sum exactly to `--rows`.
- **Lifecycle:** each claim emits `claim_submitted` then `claim_paid` or `claim_denied` (8% denied, paid = 0).

## Defect catalogue

Every defect is injected at a configurable rate (`DefectRates` in `config.py`) and counted in `defects_manifest.json` (`count`, `rate`, `table`, `description`, 5 `sample_ids`). Counts are of **distinct** injected records; duplicate copies replicate rows including any other defect they carry.

| Defect | Table | How it looks | Default rate | Detect with |
|---|---|---|---|---|
| `duplicate_claim` | claims, claim_lines, diagnoses | whole claim re-sent in the same batch, byte-identical rows | 0.5% of claims | `count(*) > count(distinct claim_id)` |
| `late_arrival` | claims (and children) | `received_date` is 31-90 days after `service_date` (normal 0-2) so the claim lands in a far-later partition | 2% of claims | lag > 30 days; watermark / late-data handling |
| `orphan_member_id` | claims | `member_id` absent from `members` | 0.3% of claims | anti-join to members |
| `null_procedure_code` | claim_lines | NULL | 0.4% of lines | not-null test |
| `invalid_procedure_code` | claim_lines | e.g. `ABCDE`, `99-99`, `0000` | 0.4% of lines | `^(\d{5}\|[A-V]\d{4})$` |
| `negative_amount` | claim_lines | billed/allowed/paid negated | 0.3% of lines | amount >= 0 test |
| `null_icd10` | diagnoses | NULL | 0.4% of dx rows | not-null test |
| `invalid_icd10` | diagnoses | e.g. `123.45`, `??.1`, `i1o` | 0.4% of dx rows | `^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$` |
| `schema_drift` | claims | **one** weekday partition (chosen by seed, days 100-250) has an extra column `prior_auth_flag` in both Parquet and CSV | 1 day | schema-evolution / contract check |

Notes and events are built from the clean claim data (notes show the true diagnosis even where the diagnosis row was corrupted), which gives later reconciliation checks something real to find.

## Determinism and scale

Each chunk draws from `SeedSequence([seed, stream, chunk_index])`, so output depends only on `(seed, rows, chunk_size, config)`: no clock, no iteration-order effects. Tests assert byte-identical trees for repeated runs. Changing `--chunk-size` changes the sampled data (documented, deliberate). Memory is bounded by the chunk (dimension tables are held in memory: members = rows/6 capped at 5M, providers = rows/400 capped at 100k).

### Measured performance

Apple-silicon laptop, Python 3.13, pyarrow 26, numpy 2.5, single process.

| Claims (`--rows`) | Table rows written (claims+lines+dx) | Formats | Wall time | Peak RSS | Disk |
|---|---|---|---|---|---|
| 100,000 | 540k (+200k events) | parquet+csv | 1.3 s | 0.44 GB | 107 MB |
| 1,000,000 | 5.4M (+2M events) | parquet+csv | 7.8 s | 1.07 GB | 992 MB |
| 5,000,000 | 27M (+10M events) | parquet only | 31 s | 1.23 GB | 2.9 GB (7.9k files) |

Memory is flat in volume (about 1.2 GB at 5M); extrapolating linearly, 50M claims is roughly 5-6 minutes and ~30 GB (parquet only, ~80k small files, which is deliberately realistic for a landing zone).

## Tests

`pytest tests/claimsgen` covers: byte-level determinism, seed sensitivity, manifest counts vs counts recomputed from the data for every defect, referential integrity of non-injected rows (and that orphans are exactly the injected set), header/line reconciliation, duplicates being exact re-sends, schema drift confined to one partition, partition/`received_date` agreement, cost and seasonal skew, events/notes parseability and synthetic marker, CSV/Parquet row parity, and a CLI smoke test.
