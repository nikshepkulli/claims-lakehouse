# Model card: claim denial risk (`claim_denial_risk`)

> **Synthetic-data caveat, read first.** Every record is synthetic. The labels come from the data generator, which marks each claim denied with an **independent 8% probability, unrelated to any feature** (`src/claimsgen/generator.py`: `denied = r.random(n) < 0.08`). The metrics below therefore measure the pipeline, not a claims model: they say **nothing** about real-world performance, and the right result on this data is "no better than chance", which is what is reported. Do not cite these numbers as model quality.

## Model details

| | |
| --- | --- |
| Name | `claim_denial_risk` |
| Task | Binary classification: probability that a claim will be denied, scored when the claim is received |
| Algorithms | Candidate 1: logistic regression (baseline, one-hot categoricals, median-imputed and scaled numerics). Candidate 2: `HistGradientBoostingClassifier` (native categoricals). Both class-weighted, then Platt-calibrated. The better raw AUC-PR on the calibration block is kept |
| Version | `v` + 12 hex of a sha256 over features, hyperparameters, feature-manifest hash, training rows and library version; stored in `model_meta.json` and on every scored row |
| Code | `src/lakehouse/ml/`; how to run: [ml.md](ml.md) |
| Owner | Nikshep Kulli (portfolio project) |

## Intended use

* Demonstrating an MLOps workflow (feature store to training to calibrated batch scores to registry) on synthetic data.
* **Not** for any real claims decision, prior authorization, provider or member action, or pricing. There is no real-world validation, no fairness assessment on real populations, and no clinical or regulatory review. Not a HIPAA compliance claim.

## Data

* Source: `claimsgen` synthetic claims (seed 42, 20,000 requested rows, 19,906 claims after cleaning) converted to a gold-shaped export, then the point-in-time feature store ([features.md](features.md)). Label: `is_denied` (`claim_status = DENIED`), stored apart from features and joined only in `training_set()`.
* Features: the guarded allow-list from `governance/feature_registry.yaml` (claim-table columns: claim fields, member `m_*` and provider `p_*` as-of features, 30-day outcome lag). Excluded: all ids and `as_of`, the label, anything PHI_LIKE or a direct identifier, and (default) quasi-identifiers `m_state`, `m_age_band`. The exact list is in `model_meta.json`.
* Split by time (`as_of` = receipt day), purge gap 30 days: train 2024-01-01..2024-07-07 (10,712 rows), calibration 2024-08-07..2024-09-30 (2,447), test 2024-10-31..2025-03-27 (3,983); 2,764 rows dropped by the purge gaps. Test prevalence 8.99% (358 denied).

## Metrics (test block, calibrated; `make ml-demo`, reproducible byte for byte)

| Model | AUC-PR | ROC-AUC | Brier | ECE |
| --- | --- | --- | --- | --- |
| No skill (constant training prevalence) | 0.0899 | 0.5 | 0.0820 | n/a |
| Logistic regression (**selected**, calibration-block AUC-PR 0.0937) | 0.0872 | 0.4896 | 0.0821 | 0.0192 |
| Gradient boosting (calibration-block AUC-PR 0.0750) | 0.0897 | 0.4949 | 0.0821 | 0.0197 |

Reading: AUC-PR equals the prevalence and ROC-AUC is at 0.5, so there is no skill. The calibration is "good" only because the calibrated output collapses to about the base rate. Precision at the top 1% / 5% / 10% of risk (selected model): 7.5% / 8.0% / 7.8% against 9.0% overall (lift 0.83 / 0.89 / 0.86, i.e. at or below chance).

Calibration table (equal-frequency deciles, mean predicted vs observed denial rate): predictions span only 6.0% to 8.3% while observed rates range 6.8% to 11.6% with no monotone trend, which is noise at ~400 rows per bin.

Per-segment slices (claim type, place of service, plan type, provider cost tier, specialty) are in `metrics.json`. Segments below 200 rows are listed but not scored. Individual slices swing widely by chance (for example Radiology ROC-AUC 0.66 on 212 rows, OB/GYN 0.41 on 337); with random labels these are noise, not findings, and show why slices need confidence intervals before anyone reads them.

## Validation that the pipeline can learn

Because the demo labels carry no signal, the tests replace them with a planted rule (billed amount above 5,000, or 4+ lines in a pseudo-random subset). On that data the same code reaches ROC-AUC above 0.8, Brier below the no-skill reference, ECE below 0.05 and lift above 2 at the top 5% (asserted in `tests/ml/test_ml_train.py`). That validates the code path, not a claims model.

## Limitations and risks

* No evidence of real-world performance (see the caveat above). Real denials depend on coverage rules, authorization, coding errors and payer policy that the generator does not model.
* Outcome-dependent features (trailing denial rates, paid sums) use only outcomes known 30 days after receipt, an assumed lag (gold has no adjudication date). A real adjudication lag distribution would change them.
* Proxy and fairness risk: even without state or age band, features such as plan type, specialty or place of service can proxy protected attributes in real data. No fairness analysis was done and none is possible on random labels.
* Calibration is fitted on one time block; it will drift if denial behaviour shifts. No drift monitoring exists.
* Pickled scikit-learn artifacts: load only from a trusted location.
* Re-identification: scores carry `claim_id` only and no member or provider identifier, but `claim_id` is a quasi-identifier in the classification registry; treat the scores table as CONFIDENTIAL.

## Ethical and governance notes

The model is gated by the feature registry's classification vocabulary: a PHI_LIKE or direct-identifier feature cannot enter the feature list (the guard raises, and a test fails if one does). Any production use would need a documented purpose, human review of adverse actions, and a fairness and impact assessment, none of which exist here.
