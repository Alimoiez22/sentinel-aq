# PROJECT 2: Sentinel-AQ
## Correctness-Verified Recovery for an Incremental Air-Quality Pipeline (Lahore & Pakistan)

(The author's project brief, kept in the repo so every working session starts from it.)

## 1. Context and motivation
Winter smog in Lahore is a major public-health problem. Project 1 (AIS warehouse) is at
`D:\projects\ais-mobility-warehouse`. This project uses OpenAQ air-quality sensor data.
The scientific contribution is the RECOVERY ENGINEERING. The analytics (daily PM2.5,
guideline exceedances) exist so there is a meaningful output whose correctness can be checked.

## 2. Research question
On (a) a controlled catalogue of injected failures and (b) real data problems observed in
the OpenAQ archive, how does GUARDED AUTOMATIC RECOVERY compare with a MANUAL RUNBOOK
and RETRIES ONLY, on: verified-recovery rate, time to verified recovery, data damage
(duplicate/missing/wrong rows vs a golden rebuild), escalation rate, inappropriate
automatic actions, and processing overhead?
Success = correct published outputs. Finishing with wrong numbers is a FAILURE.

## 3. Scope
IN: incremental daily ingestion, data contracts, sensor quality rules, idempotent loads,
checkpoints/replay/backfill, retroactive revisions, quarantine, golden-rebuild
reconciliation, deterministic policy engine, fault injector, controlled experiments,
real-world observational study, dashboards.
OUT (for now): LLM components (optional final milestone), cloud, streaming, forecasting.

## 4. Data source
- OpenAQ daily gzipped CSV files per location in the public AWS bucket
  `openaq-data-archive` (`--no-sign-request`), Hive-style partitions by location/year/month.
- Files are written about 72 hours after the local day ends and MAY BE PATCHED LATER.
- Docs are inconsistent on column names (sensor_id vs sensors_id, unit vs units): read the
  REAL headers and map variants explicitly in docs/DATA_CONTRACT.md.
- scripts/fetch_openaq.sh and scripts/snapshot.sh fill data/landing_drop/ and data/snapshots/.
- config/scope.yaml: OpenAQ location ids (Lahore first; optional other Pakistani/South
  Asian cities), date range, parameters (pm25, pm10 by default).
- A coverage helper reports days with files and hours per day per location. If Lahore
  coverage is thin, say so and propose widening scope. Never pad with invented data.
- Must run on a 16 GB laptop.

## 5. Architecture
Daily file → Register (manifest: location, date, sha256, size, rows, header variant) →
Contract validation → Idempotent staging load (per location-day) → Quality rules
(flag / quarantine / block) → Hourly fact → Incremental daily marts → Reconcile →
Publication gate (complete / partial / blocked / revised).
Revision watcher: re-scan the last N days (default 14); same location-date with a new
checksum = REVISION → supersede, replay affected location-days, re-verify, mark outputs
revised with old-vs-new audit.
Recovery loop: Detect → Evidence → Deterministic classify → Policy lookup → ONE
allow-listed bounded action OR escalate → Verify correctness → Record.

## 6. Tech stack
PostgreSQL + PostGIS (monthly partitions), Python 3.12 (uv, psycopg 3, polars/pyarrow),
dbt-core + dbt-postgres, Apache Airflow (Docker Compose, LocalExecutor), Pandera,
pydantic-validated YAML, pytest + hypothesis, ruff, mypy, Grafana or Metabase (justify),
GitHub Actions, Makefile.

## 7. Warehouse model
dim_location (id, name, city, country, point geometry, provider); dim_sensor (id,
location, parameter, first/last seen, position history); dim_parameter (code, name,
canonical unit); dim_date + dim_hour (local and UTC; document timezone handling);
fact_measurement_hourly (grain: sensor × UTC hour; normalized value, original value+unit,
quality flags, batch_id, revision_no); mart_daily_location (daily mean, hours observed,
completeness %, guideline exceedance, publication status); mart_exceedance_summary
(exceedance days per location per month). Guidelines in config/guidelines.yaml with
cited source; daily means only when completeness meets a configurable, cited minimum.

## 8. Quality rules (config/quality_rules.yaml; versioned ids; FLAG / QUARANTINE / BLOCK)
Missing/unparseable value or timestamp → quarantine. Negative value → quarantine.
Above per-parameter ceiling (cited) → quarantine. Unit mismatch → convert only via a
documented safe conversion, else quarantine + escalate. Stuck sensor (same value ≥ N
hours) → flag. Spike vs rolling median (Hampel) → flag, keep. Timestamp outside the
file's local day, future, or suspicious offset → quarantine. Duplicate (sensor, datetime)
→ keep one by documented rule, count it. Sensor relocation → flag + new position version.
Completeness below threshold → mart "partial". Batch reject rate above threshold →
BLOCK publication for that location-day.

## 9. Operational metadata (ops schema)
ops.batch_manifest (batch_id, location_id, source_date, file_name, sha256, size_bytes,
raw_row_count, header_variant, arrival_ts, status, revision_no, superseded_by,
attempt_count); ops.checkpoint; ops.run_log (mode: manual/retries_only/guarded);
ops.incident (failure_class, evidence jsonb, classifier_version, decision,
policy_rule_id, action, attempts, outcome, verified, escalated,
time_to_verified_recovery_s, origin: injected/real); ops.action_log (immutable audit);
ops.publication_status; ops.revision_diff (old vs new values); quarantine.aq_rejects.

## 10. Correctness mechanisms
Immutable content-addressed raw files (sha256). Idempotent loads: per-batch temp table
→ validate → replace the location-day in ONE transaction (or MERGE on a documented key).
Advisory locks per location-day. Checkpoints per stage for resume. Incremental mart
refresh only for affected keys, equal to a full rebuild. `make golden` rebuilds
everything from the latest revisions into a separate schema; comparators check row
counts, order-independent checksums per partition, and mart value equality.
Hypothesis property tests: random arrival orders, duplicates, replays and revisions
always converge to the golden state.

## 11. Fault catalogue (seeded; docs/FAULTS.md)
F1 duplicate file; F2 truncated/corrupt gzip; F3 corrupt rows (%); F4 header variant /
renamed column; F5 added or reordered columns; F6 type drift; F7 missing day; F8 late
file; F9 retroactive revision; F10 crash after staging before commit; F11 crash mid-mart
refresh; F12 DB connection drop during load; F13 unit change mid-series; F14 stuck-sensor
run or spike burst; F15 timezone offset error.

## 12. Policy engine (deterministic; NO LLM in the decision path)
config/policy.yaml: failure_class → allowed action, max attempts, backoff, risk tier,
verification required, escalation condition. Classifier uses evidence only; unknown →
escalate. Allowed automatic actions: bounded retry (transient only), skip duplicate,
resume from checkpoint, map a KNOWN header variant, apply revision, quarantine-and-continue
below the reject threshold (else block), bounded backfill, block publication.
NEVER automatic: accept unknown schemas, invent unit conversions, edit SQL/models, delete
raw data, disable tests, change thresholds. These escalate with an evidence bundle and
the RUNBOOK section. Every decision logs its policy_rule_id.

## 13. Evaluation
TRACK A (controlled): modes manual (pipeline halts; a person recovers via RUNBOOK + CLI;
real elapsed time; labelled human-operated with operator and trial count) /
retries_only / guarded. Faults F1–F15 × modes × 5 seeds (config/experiments.yaml).
`make experiment` resets between trials and writes results.csv + metadata.json.
Metrics: recovered_verified, time_to_verified_recovery_s, duplicate/missing/wrong rows
vs golden, escalated, inappropriate_action (defined precisely), overhead_s.
TRACK B (real world): `make snapshot` run daily from as early as possible;
`make revisions-report` measures real revisions (count, lateness, values changed), real
quality-rule hits, and how the guarded pipeline handled each. Report zero if none.
Analysis: tables with denominators ("5/5 verified"), medians + IQR, figures generated
by code, separate Track B section, negative results reported honestly.

## 14. Observability
Dashboards over ops tables (batch timeline, incidents and decisions, publication status,
reject rates per rule, revision history, recovery times) + SQL views.

## 16. Milestones
R   Research brief (done 2026-11-05, approved)
M0  Compose (Postgres/PostGIS, Airflow), Makefile, CI, fixtures with both header variants
M1  Fetch + snapshot scripts + coverage report → STOP, author chooses locations
M2  Manifest, checksums, contract from real headers, explicit variant mapping
M3  Idempotent staging + checkpoints + locks; property test replay N == once
M4  Quality rules + quarantine + flags + reject stats
M5  Warehouse + dbt marts + completeness + exceedances
M6  Revision handling (supersede, replay, diff, "revised")
M7  Golden rebuild + comparators; incremental == golden
M8  Airflow DAGs (daily, revision watch, backfill, recovery) + publication gate
M9  Fault injector F1–F15 + tests that each fault manifests
M10 Evidence, classifier, policy engine, actions, audit log
M11 Experiment runner (Track A) + revisions report (Track B)
M12 Dashboards + analysis notebook + figures
M13 Docs complete (README quickstart ≤ 10 commands, generated results table, RUNBOOK,
    POLICY, FAULTS, LIMITATIONS, report_notes.md)
M14 OPTIONAL after applications: AI incident assistant (suggests class + runbook section,
    may abstain, evaluated offline vs the deterministic classifier; never executes)

## 17. Definition of done
`make setup && make demo` shows a 3-day run with an injected fault AND a revision:
incident → decision → verified recovery. `make experiment` reproduces results.csv;
README table generated from it. CI green. Every number traceable. Honest LIMITATIONS.
