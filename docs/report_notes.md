# Report notes (bullets only; the author writes the prose)

## M0 — environment and skeleton (2026-11-06)

**Evidence file:** `results/m0_environment.txt` (hardware, Docker VM, versions, memory).

- Host: Windows 11 Pro, i7-1165G7, 15.7 GB RAM. Docker VM capped at 4 GB / 8 CPUs.
- Stack: PostgreSQL 18.6 + PostGIS 3.6.4 (reported by `sentinel check-db` from inside
  an Airflow task), Airflow 3.3.2 (LocalExecutor), dbt-core 1.12.6 + dbt-postgres 1.11.0.
- Memory, Airflow profile idle after one smoke run: api-server 294 MiB, scheduler
  377 MiB, dag-processor 212 MiB, db 68 MiB (one `docker stats` sample; not a benchmark).
- `make test`: ruff clean, mypy strict clean, 26 passed (5 integration, 21 unit).
- Build problems met and fixed (ADR-008):
  - four parallel builds of the same image;
  - a slow link, about 50–200 KB/s, measured with curl;
  - one build that "succeeded" with 0-byte source files, now guarded by an import
    check in both Dockerfiles.
- dbt v2 exists but has no Postgres adapter, so we stay on v1 (ADR-007).
- Incident: under cmd.exe the `make destroy` guard failed open and deleted the (empty)
  Compose volumes. Fixed with make-level guards and a POSIX-shell check (ADR-009).
  Possible discussion point: why safety checks must fail closed.
- Fresh-volume check after the incident: `make test` 26 passed; Airflow migrated a new
  `airflow_meta` and the smoke DAG run succeeded again.

**Figures/tables to make later:** none for M0.

## M1 — discovery, fetch, snapshots, coverage (in progress, 2026-11-11)

- Built and tested offline: archive client, OpenAQ API client, raw store, fetch,
  snapshot, coverage. `make test`: 80 passed (5 integration, 75 unit), 1 network test
  excluded.
- `make test-network`: 1 passed. The real bucket listed 31 files for location 2178,
  Jan 2024, and the downloaded file has a known header variant.
- Real coverage numbers: **not measured yet**. `make discover` needs OPENAQ_API_KEY.
- Possible table: per-location coverage (`results/m1_coverage_summary.md`, generated).
- Possible discussion point: why snapshots compare metadata and then confirm with
  hashes (ADR-012), and what that misses.

**Open points for the author:**
- Review ADR-001…019 and mark each accepted or changed (ADR-014 and ADR-018 change the brief).
- The CI workflow is written but has not run (no GitHub remote yet).

## M2-M13 — pipeline, recovery, experiments (2026-11-11)

**Evidence files:** `experiments/results/results.csv`, `metadata.json`,
`summary_by_mode.csv`, `summary_by_fault_mode.csv`, `results_table.md`, `docs/figures/*.svg`.

- Track A: 160 trials (16 faults × 2 automated modes × 5 held-out seeds), 304.6 s total,
  commit 087feed, PostgreSQL 18.6 (metadata.json). Synthetic data only.
- Faulty trials (F1–F15, n = 75 per mode), from summary_by_mode.csv:
  - outputs equal golden: guarded 70/75, retries_only 60/75;
  - handled as expected: guarded 70/75, retries_only 40/75;
  - recovered without a human: guarded 50/75, retries_only 60/75 (guarded escalates the
    25 trials whose faults require a human decision: F3, F5, F6, F13, F15);
  - wrong hourly or daily values published: 0 in both modes;
  - missing location-days: guarded 5 (all F5, waiting for a human), retries_only 15
    (F2, F5, F7);
  - inappropriate automatic actions: 0 in both modes;
  - time to verified recovery, when needed, median [IQR]: guarded 0.176 s
    [0.047, 0.251], retries_only 1.314 s [1.159, 1.379]. Backoffs were scaled to 0 and
    the dataset is tiny, so this is relative cost only.
- Overhead vs F0 (summary_by_fault_mode.csv): most cells lie within about ±0.3 s, which
  is the noise level on this laptop. The exception is F12 under guarded, median 0.541 s
  [0.539, 0.637], because it retries every location-day skipped after the dropped
  connection. No other overhead difference is claimed.
- Classifier clf-1: 80/80 correct on held-out seeds (column `classified_correctly`).
- Not measured: manual mode (needs a human operator), Track B (needs the API key and
  a snapshot window), real Lahore data.
- `make demo`: truncated transfer → P03 refetch → verified; revision → P06 → outputs
  `revised`; all tables equal golden.

**Possible tables/figures for the report:** summary_by_mode (table),
handled_as_expected.svg (figure), the per-fault table (appendix), the demo transcript
(incident → decision → verification).

**Possible discussion points:**
- Why `recovered_verified` alone would favour retries_only (silent blocking), and why
  `handled_as_expected` is needed.
- Where guarded loses: F5 (unknown schema) stays unpublished until a human acts; this
  is a design choice, not a failure of correctness.
- The autocommit bug found by the experiment runner: why "the tests passed" was not
  evidence of correct transactions.
- Threats: synthetic data, n = 5, the same author wrote the faults and the classifier,
  no manual baseline yet.
