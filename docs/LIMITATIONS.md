# Limitations (honest list, kept current)

## Data
* **No real Lahore data processed yet.** M1 discovery and coverage need an OpenAQ API key;
  until then every pipeline result is on clearly labelled synthetic fixtures.
* **Low-cost sensor bias is not corrected** (Barkjohn et al. 2021 report ~40 % over-reading
  for PurpleAir in the US). Values are published as reported. This project's claims are
  about pipeline correctness, not exposure science.
* **End-of-hour stamps** are assumed from one real file (DATA_CONTRACT §3).
* **Licences vary by provider**; no raw data is committed, and attribution is recorded per location.

## Method
* **Synthetic Track A**: 2 locations × 3 days. Faults are realistic in mechanism, not in
  frequency.
* **n = 5 seeds per cell**; descriptive statistics only.
* **The same person (and AI assistant) wrote the faults, the classifier and the
  evaluation.** The classifier was frozen and evaluated on held-out seeds (POLICY.md),
  but the fault catalogue itself reflects what the author anticipated.
* **The manual-mode baseline is not measured** (needs a human operator).
* **Track B is not measured** (no snapshot window yet).
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
