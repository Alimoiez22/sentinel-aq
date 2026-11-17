# Limitations (honest list, kept current)

## Data
* **Two Lahore locations only, chosen without the API** (ADR-020): the reference monitor
  8664 (US Diplomatic Post, 2019-05-23..2025-03-04) and one AirGradient low-cost sensor
  (4568423, from 2025-05-30). About 40 other air-sensor markers in Lahore were not
  enumerated. A working API key would make discovery complete.
* **The reference record stops on 2025-03-04.** News reports say the US State Department
  stopped publishing embassy monitoring data in 2025 (context, not evidence). No
  reference-grade Lahore data newer than that is in this study.
* **8664's licence is not stated** on its OpenAQ page.
* **PM10 is out of scope:** neither chosen location measures it.
* **Low-cost sensor bias is not corrected** (Barkjohn et al. 2021 report ~40 % over-reading
  for PurpleAir in the US). Values are published as reported. This project's claims are
  about pipeline correctness, not exposure science.
* **End-of-hour stamps** were checked on 3 real locations; rule Q06 never fired on the
  2,437 Lahore days (DATA_CONTRACT §3).
* **`-999.0` read as a missing-value code** on the evidence of the data itself; no source
  documenting it was found (ADR-021). Small negatives are still quarantined, more
  strictly than AirNow, which keeps values above about −5.
* **Licences vary by provider**; no raw data is committed, and attribution is recorded per location.

## Method
* **Synthetic Track A**: 2 locations × 3 days. Faults are realistic in mechanism, not in
  frequency.
* **n = 5 seeds per cell**; descriptive statistics only.
* **The same person (and AI assistant) wrote the faults, the classifier and the
  evaluation.** The classifier was frozen and evaluated on held-out seeds (POLICY.md),
  but the fault catalogue itself reflects what the author anticipated.
* **The manual-mode baseline is not measured** (needs a human operator).
* **Track B has a baseline only:** one snapshot (2026-11-17, 2,437 keys). Revisions need
  later snapshots; the daily schedule is the author's step.
* **The real-data Track A window is small:** 3 days per location (6 location-days).
* **"Inappropriate action"**: the "moved output further from golden" criterion is not
  measured per action.

## Engineering
* **Day-local flags**: the Hampel and stuck-sensor rules see one day only, with no
  cross-day context.
* **The "revised" status is compared as "complete"** in reconciliation, because golden
  has no publication history (ADR-017). Revisions are audited separately in
  `ops.revision_diff`.
* **Metadata change detection**: snapshots detect changes through ETag, size or
  LastModified. A change touching none of these would be missed (ADR-012).
* **Arrival lag** uses LastModified (for a patched file, the patch time).
* **dbt replaced by SQL files** (ADR-014): no dbt lineage or docs site.
* **Large Airflow image** (~5 GB) and a 4 GB Docker VM on the development laptop (ADR-008).
* **Only one location timezone per location**; a location that changed time zone is not
  modelled.
