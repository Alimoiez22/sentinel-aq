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
- CI: private repo github.com/Alimoiez22/sentinel-aq. The first run (42973212590,
  manual dispatch on `main`, commit f172582) passed: lint, mypy, 134 tests (1 network
  test deselected), on ubuntu-24.04 with the same PostGIS digest as Compose. Note:
  GitHub did not start runs for the initial pushes, so the workflow now also allows
  manual runs. Push events still started no runs during this session; runs were
  started by hand.
- Fresh-clone check (definition of done): CI job `fresh-clone-demo`, run 42985928208,
  commit 9299589, on a clean ubuntu-24.04 machine. `make setup && make demo` passed and
  ended in "verified recovery, outputs equal golden".

## M2-M13 — pipeline, recovery, experiments (2026-11-11; rerun 2026-11-17)

**Evidence files:** `experiments/results/` (synthetic) and `experiments/results_real/`
(real Lahore files): `results.csv`, `metadata.json`, `summary_by_mode.csv`,
`summary_by_fault_mode.csv`, `results_table.md`; figures in `docs/figures/`.

- Both experiments were rerun on commit 4f42feb (quality rules v2, injector v2;
  classifier clf-1 unchanged): 160 trials each, 235.6 s (synthetic) and 228.2 s (real),
  PostgreSQL 18.6 (metadata.json). The first synthetic run (087feed) is superseded.
- Faulty trials (F1-F15, n = 75 per mode), from summary_by_mode.csv. **Identical counts
  on synthetic and real data:**
  - outputs equal golden: guarded 70/75, retries_only 60/75;
  - handled as expected: guarded 70/75, retries_only 40/75;
  - recovered without a human: guarded 50/75, retries_only 60/75 (guarded escalates the
    25 trials whose faults need a human decision: F3, F5, F6, F13, F15);
  - wrong hourly or daily values published: 0 in both modes, both datasets;
  - missing location-days: guarded 5 (all F5, waiting for a human), retries_only 15
    (F2, F5, F7);
  - inappropriate automatic actions: 0 in both modes;
  - time to verified recovery when needed, median [IQR]: guarded 0.152 s [0.042, 0.182]
    synthetic, 0.146 s [0.042, 0.168] real; retries_only 0.991 s [0.826, 1.012]
    synthetic, 0.964 s [0.935, 1.002] real. Backoffs were scaled to 0 and the datasets
    are tiny, so this is relative cost only.
- Overhead vs F0 (summary_by_fault_mode.csv): F12 under guarded has a median of 0.411 s
  synthetic and 0.301 s real, because it retries each location-day skipped after the
  dropped connection. Every other median is within 0.365 s (synthetic) and 0.287 s
  (real), which is the noise level here. **No overhead difference is claimed.**
- Classifier clf-1: 80/80 correct on held-out seeds, in both datasets.
- Found and fixed with real data (ADR-022): F13 and F14 first mutated rows in file order.
  On the multi-parameter AirGradient file they never touched PM2.5, so F13 "missed"
  escalation 5/5 in a real-data run that has since been replaced. Lesson: check that
  faults manifest on production-shaped data.
- Not measured: manual mode (needs a human operator); Track B revisions (baseline
  snapshot only).
- `make demo`: truncated transfer → P03 refetch → verified; revision → P06 → outputs
  `revised`; all tables equal golden.

## M1 completed with real Lahore data (2026-11-17)

**Evidence files:** `config/locations_lahore.csv`, `results/m1_locations.csv`,
`results/m1_coverage_summary.md`, `results/quality_rules_v1_vs_v2.csv`,
`results/quality_rules_v1_vs_v2_by_rule.csv`, `results/lahore_pm25.md`,
`results/lahore_pm25_by_month.csv`, `results/lahore_pm25_by_season.csv`,
`docs/figures/lahore_pm25_monthly.svg`, `data/snapshots/20261117T152712Z/meta.json`
(not committed).

- The API key was rejected (HTTP 401). Locations came from the public Explorer, with
  every detail verified from archive files (ADR-020).
- Coverage (m1_coverage_summary.md):
  - 8664: a file on 1909/2113 days of its active span (90%);
  - 4568423: a file on 528/533 days (99%);
  - median arrival lag: 72.0 h for 4568423, matching OpenAQ's documented ~72 h;
    15706.1 h for 8664, whose files were written long after their dates (a
    historical backfill).
- All 2,437 real files were variant B, and rule Q06 never fired, which supports
  end-of-hour stamps.
- Rules v1 → v2 (quality_rules_v1_vs_v2.csv), recomputed from the raw files: 8664
  blocked days 392 → 16. Cause: 7,879 rows of `-999.0` (a missing-value code).
- After the run, reprocessing and the golden rebuild, every published table equals
  golden on real data (results/reconcile_latest.json): 48,267 hourly facts, 2,437 daily
  rows, 88 monthly rows; 0 missing, 0 extra, 0 wrong, checksums equal.
- Lahore PM2.5 (lahore_pm25.md): 8664 has 1,448 exceedance days out of 1,464 complete
  days; the winter mean of daily means is 179.8 µg/m³ (2019/20) to 254.2 µg/m³
  (2023/24). 4568423 (uncorrected low-cost): 485 of 488. Rule Q12 flags 646 hours at
  8664 above 500 µg/m³ (quality_rules_v1_vs_v2_by_rule.csv).
- Track B: first snapshot 20261117T152712Z, 2,437 keys (baseline only).

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
