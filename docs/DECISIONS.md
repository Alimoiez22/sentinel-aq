# Decision log (ADR style)

Each decision records its context, the options, the decision and its consequences.
**Status** says who decided: *proposed by Claude* means the AI assistant suggested it.
The author reviews each one and marks it *accepted by author* or *changed*. Reasons
for accepted decisions are rewritten by the author in his own words.

---

## ADR-001 — Native PostgreSQL partitioning instead of TimescaleDB
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** The hourly fact is partitioned by month. The data is small: a handful of
  locations × 2 parameters × 24 hours a day. The project's central claim is that
  incremental mart refreshes equal a golden rebuild (M7), so every refresh step must be
  explicit and testable.
- **Options.** (a) PostgreSQL 18 declarative range partitioning. (b) TimescaleDB
  hypertables with continuous aggregates and compression.
- **Decision.** (a) native partitioning.
- **Consequences.** One fewer extension to pin, and every mechanism (partition swap,
  incremental refresh) is code we write and test. TimescaleDB's main benefits do not
  apply at this size. Continuous aggregates would hide the refresh logic we need to
  verify. Monthly partitions are created by our own DDL; this is a small amount of extra code.

## ADR-002 — One PostgreSQL server, two databases, least-privilege roles
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Airflow needs a metadata database. Experiments (M11) reset the warehouse
  between trials and must never damage Airflow's state. Docker's VM is capped at 4 GB
  (Project 1, decision 2).
- **Options.** (a) One server, separate databases. (b) Two PostgreSQL containers.
  (c) One database, separate schemas.
- **Decision.** (a). `warehouse` is owned by role `sentinel`, `airflow_meta` by role
  `airflow`. Neither role is a superuser, `CONNECT` is revoked from `PUBLIC` on both
  databases, and the image superuser is used only by `docker/initdb/00_init.sh`.
- **Consequences.** About 100–200 MB less memory than option (b) (not measured; an
  estimate). A reset can drop the warehouse's schemas without touching Airflow. The
  integration tests check that the pipeline role cannot connect to `airflow_meta`.
  Because the init script runs only on an empty volume, changing roles later needs
  `make destroy CONFIRM=yes`.

## ADR-003 — Airflow runs our CLI from an isolated virtual environment
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Airflow installs with a constraints file that pins hundreds of libraries.
  dbt, polars and pandera bring their own pins. Mixing them in one environment risks
  conflicts or silent downgrades.
- **Options.** (a) Install everything into Airflow's environment. (b) A separate venv
  inside the Airflow image, built from our `uv.lock`, with tasks calling
  `/opt/sentinel-venv/bin/sentinel`. (c) `DockerOperator`, one container per task.
- **Decision.** (b).
- **Consequences.** A single lockfile defines our versions in both images (app and
  Airflow). DAGs stay thin (they call CLI commands), so every step can also be run by
  hand, which the manual recovery mode needs. Option (c) was rejected because it needs
  the Docker socket inside Airflow, which is heavier and riskier on Windows.

## ADR-004 — Compose profiles: Airflow is optional
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Airflow 3 runs an api-server, a scheduler and a dag-processor, in a 4 GB VM.
  Tests and CI do not need it.
- **Decision.** Default `make up` starts only `db`. `make test` adds a one-off `app`
  container. `make up-airflow` starts the `airflow` profile.
- **Consequences.** Fast tests and a small CI. Measured memory use of the Airflow
  profile is recorded in `docs/report_notes.md` (M0 notes).

## ADR-005 — Synthetic, byte-reproducible fixtures
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Tests need files in the OpenAQ archive format with both header variants.
  OpenAQ licences vary by provider, and rule 4 forbids presenting invented data as real.
- **Options.** (a) Commit a few real files. (b) Generate synthetic files with a fixed
  seed.
- **Decision.** (b). Locations 99000001 (variant A) and 99000002 (variant B) are named
  `SYNTHETIC-*`. Gzip is written with `mtime=0` and no stored file name, so output is
  byte-identical. `MANIFEST.csv` holds the sha256 of each file. Only the real header
  line is recorded, with its source (`data/fixtures/HEADERS.md`).
- **Consequences.** Tests cannot surprise us with real-data quirks. Those come from M1
  onwards, from real files that are kept out of git.

## ADR-006 — Exact pins in uv.lock, ranges in pyproject.toml
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Rule 5 asks for pinned versions. Docker images also drift when pinned by tag.
- **Decision.** `pyproject.toml` states compatible ranges. `uv.lock` pins exact versions
  and hashes, and images build with `uv sync --frozen`, which refuses to run if the
  lock is out of date. Base images are pinned by sha256 digest, with the human-readable
  tag in a comment.
- **Consequences.** Upgrades are explicit: run `uv lock --upgrade-package X`, then rerun
  the tests. dbt-core 1.12.6 is paired with dbt-postgres 1.11.0, the newest adapter;
  M0 checks that the pair runs (`dbt --version`).

## ADR-007 — Stay on dbt Core v1 (1.12), not dbt v2
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Running `dbt --version` in our image printed that "dbt v2 is available, and
  is the default for new installations". PyPI has `dbt` 2.0.11 alongside `dbt-core`
  1.12.6. The dbt upgrade guide (docs.getdbt.com, checked 2026-11-06) says v2 is a Rust
  rewrite, connects through ADBC drivers, and lists BigQuery, Databricks, Redshift,
  Snowflake, ClickHouse (beta), Spark (beta) and DuckDB as adapters. **Postgres is not
  listed.** The same page says dbt v1.x "remains fully supported" but gives no end date.
- **Options.** (a) dbt-core 1.12 + dbt-postgres 1.11. (b) dbt v2 (no Postgres adapter).
  (c) Plain SQL files with our own runner.
- **Decision.** (a). Checked in the built image: `dbt --version` reports core 1.12.6
  and postgres plugin 1.11.0, both "Up to date".
- **Consequences.** New dbt features will land in v2 only. Our needs (incremental models
  with `delete+insert`/`merge`, tests) exist in v1. Risk: v1 end-of-support. Re-check the
  dbt product lifecycle page before M5 and record the date in LIMITATIONS if one is announced.

## ADR-008 — Build hygiene on a slow link and a 4 GB Docker VM
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context.** Observed during M0 on this laptop:
  1. Compose built the same Airflow image four times in parallel, because every Airflow
     service inherited `build:`. The four builds ran for over 20 minutes.
  2. The download rate was about 200 KB/s (one 12 MB wheel in 60 s on the host, measured
     while other builds were running).
  3. One build finished "successfully" with every source file copied as **0 bytes**.
     The image would have run nothing.
- **Decision.** Only `airflow-init` builds the Airflow image; other services reuse the
  tag with `pull_policy: never`. Both Dockerfiles share a BuildKit cache mount
  (`id=sentinel-uv`) for downloaded wheels. Both end with a step that runs
  `sentinel --version` and imports the package, so an empty or broken package fails
  the build.
- **Consequences.** Rebuilds after a failure reuse downloaded wheels. A corrupted build
  context can no longer produce an image that looks healthy. The Airflow image is large
  (base 3.2 GB; our venv measured 695 MB in the first build). This is acceptable for
  local use and is listed for M13 LIMITATIONS. The Airflow base image sets
  `UV_CACHE_DIR=/tmp/.cache/uv`; our build overrides it so the cache mount is shared,
  and the venv is installed as root, so tasks can run our code but cannot modify it.

## ADR-009 — Safety guards must fail closed; the Makefile requires a POSIX shell
- **Date:** 2026-11-06 · **Milestone:** M0 · **Status:** proposed by Claude
- **Context (incident).** `make destroy` was run from PowerShell to test its guard. With no
  `sh` on PATH, GNU make on Windows ran recipes in `cmd.exe`. The guard
  `test ... || (echo ...; exit 1)` broke there: `test` does not exist, and `;` is not a
  separator, so `exit 1` was printed as text and the exit code was 0. The next recipe
  line removed the Compose volumes, **deleting `sentinel_pgdata` and
  `sentinel_airflow_logs`**. What was lost: an empty warehouse, Airflow metadata from one
  smoke run, and its logs. No raw or downloaded data existed yet. Project 1's
  `ais_pgdata` was not affected.
- **Options.** (a) Keep shell guards and document "use Git Bash". (b) Evaluate guards
  in make itself (`ifneq` + `$(error ...)`), and stop the Makefile when the shell is not
  POSIX.
- **Decision.** (b). The Makefile stops at parse time unless `echo $$((1+1))` prints 2
  (a POSIX shell). `destroy` checks `CONFIRM=yes` with `ifneq`/`$(error)`, so no shell is
  involved. Tested: from PowerShell every target stops with a clear message (exit 2).
  From Git Bash, `make destroy` and `make destroy CONFIRM=no` stop (exit 2), and only
  `CONFIRM=yes` reaches the volume removal (checked with `make -n`).
- **Consequences.** Run make from Git Bash or WSL. Every future destructive target
  (e.g. experiment resets in M11) must use a make-level guard and must never touch
  `data/raw` (rule 8). The deleted volumes were recreated by `make test` and
  `make up-airflow`, which also showed that the init path works on an empty volume.

## ADR-010 — Read the archive over plain HTTPS, without an AWS SDK
- **Date:** 2026-11-11 · **Milestone:** M1 · **Status:** proposed by Claude
- **Context.** We need two operations on a public bucket: list a prefix and download an
  object. The R probe showed both work anonymously over HTTPS.
- **Options.** (a) boto3 with unsigned requests. (b) The AWS CLI with `--no-sign-request`.
  (c) Python's standard `urllib` calling ListObjectsV2 and GET directly.
- **Decision.** (c), in `sentinel/openaq/archive.py` and `sentinel/http.py`.
- **Consequences.** No extra dependency to pin, and the HTTP layer is small enough to
  test against an in-process fake server (pagination, 5xx retry, 429 wait, no retry on
  404). Retries are bounded: at most 5 attempts with exponential backoff, and permanent
  errors (4xx except 429) are never retried. We parse S3's XML ourselves; the parser is
  tested on a real response. Checked on the real bucket by `make test-network`.

## ADR-011 — Content-addressed raw store, latest-version landing mirror, append-only index
- **Date:** 2026-11-11 · **Milestone:** M1 · **Status:** proposed by Claude
- **Context.** The bucket keeps no old versions (RESEARCH §5.2). A patched file replaces
  the old one, so the old bytes exist only if we kept them. M6 needs both versions to
  compute old-vs-new differences.
- **Decision.**
  - `data/raw/sha256/ab/<sha256>.csv.gz`: every version we ever downloaded, named by
    its hash. It is written once, via a temp file and an atomic rename, set read-only,
    and re-verified on every read.
  - `data/landing_drop/<archive key>`: the latest version only, the inbox for M2.
  - `data/raw/index.csv`: an append-only list of (key, ETag) → sha256. It makes fetching
    idempotent (a version already stored is never downloaded again) and resumable (one
    row per file, written as soon as the file is stored).
  - A download whose byte count differs from the listing is rejected before anything
    is written.
- **Consequences.** Revisions keep both versions automatically. Disk use grows with
  revisions, which is small for daily files of a few kB. Nothing in `data/raw` is ever
  deleted (rule 8).

## ADR-012 — Snapshots detect changes by ETag, size and LastModified; SHA-256 confirms
- **Date:** 2026-11-11 · **Milestone:** M1 · **Status:** proposed by Claude
- **Context.** Track B must find real revisions. Hashing every file every day would mean
  downloading everything daily, which is not possible on this connection (ADR-008).
- **Options.** (a) Download and hash everything each day. (b) Compare listing metadata,
  then download only what changed.
- **Decision.** (b). A key counts as changed if its ETag, size or LastModified differs
  from the previous snapshot. Changed and new versions are downloaded and hashed, and
  `changes.csv` records the old and new SHA-256. A snapshot is written to a hidden temp
  directory and renamed when complete, so a crash never leaves a partial snapshot for
  the next run to compare against. An existing run is never overwritten.
- **Consequences.** A daily run costs one listing request per location-month plus the
  changed files. Risk: a change that alters no metadata would be missed. That is
  unlikely, since S3 sets LastModified on every write, but it is listed as a threat to
  validity for Track B. Property tests check that an unchanged listing yields no diff
  and that the diff does not depend on listing order.

## ADR-013 — Two-level coverage: listing first, hours only for a shortlist
- **Date:** 2026-11-11 · **Milestone:** M1 · **Status:** proposed by Claude
- **Context.** Hours per day need the file contents, and downloading every Pakistani
  location for two years over a ~50–200 KB/s link is too slow and mostly wasted.
- **Decision.** `make coverage` uses listings only: days with a file, sizes, and arrival
  lag. `make fetch` prints the planned download size, and downloads only with
  `CONFIRM=yes`. Hours per day (`make coverage-hours`) are computed only for fetched
  locations. The report writes "not measured" where hours were not computed.
  - A day counts towards ">=18h" with at least 18 distinct hourly timestamps for the
    parameter, from any of the location's sensors for it (EU 2008/50/EC).
  - "Winter" is Nov–Feb, the smog season. This is a project choice, not a cited
    standard.
  - Only the two known header variants are read. Any other header is reported with its
    text, never guessed.
- **Consequences.** The location choice is made with explicit denominators. Hours-based
  numbers exist only for the shortlist. Arrival lag uses LastModified, which for a
  patched file is the patch time, so the lag of revised files is overstated. This is
  noted in the report.

## ADR-014 — Marts as plain SQL files run on both schemas, instead of dbt
- **Date:** 2026-11-11 · **Milestone:** M5 · **Status:** proposed by Claude (changes the brief: author to accept or reject)
- **Context.** The brief lists dbt for the marts. Three needs pull the other way:
  1. A mart refresh must be scoped to the exact location-days a batch touched, and must
     commit in the SAME transaction as the publication gate. Otherwise a crash between
     them (F11) leaves marts and gate out of step.
  2. "Incremental == golden" is only meaningful if both are produced by identical logic.
  3. The experiments run hundreds of trials, and dbt starts a new process (several
     seconds) on every invocation.
- **Options.** (a) dbt incremental models with key variables, run as a subprocess.
  (b) SQL files run by our Python inside the stage transaction.
- **Decision.** (b). `sql/marts/refresh_daily.sql` runs with `search_path` set to `wh`
  (live) or `golden` (rebuild): the same file produces both. dbt-core stays installed
  but unused. Removing it is a follow-up for the author to decide.
- **Consequences.** We lose dbt's lineage graph and its documentation site. We gain an
  atomic gate + mart refresh, and an exact comparison against golden. Tested:
  `test_crash_mid_marts_resumes_from_checkpoint` and `test_incremental_equals_golden`.

## ADR-015 — One transaction per stage, under a transaction-level advisory lock
- **Date:** 2026-11-11 · **Milestone:** M3 · **Status:** proposed by Claude
- **Decision.** register, stage, quality+fact and marts+gate each run in one
  transaction. Each takes `pg_advisory_xact_lock(namespace, hash(location-day))` and
  writes its checkpoint inside the same transaction. Every write replaces the
  location-day (delete + insert).
- **Why.** A crash anywhere rolls back the whole stage, checkpoint included, so a rerun
  repeats it. Replacing the location-day makes a repeat produce the same rows
  (idempotence). The transaction-level lock is released on rollback too; a
  session-level lock would survive it (RESEARCH §7.6).
- **Evidence.** Integration tests cover a crash after staging, a crash mid-mart, and a
  dropped connection. A Hypothesis property test runs random arrivals, duplicates,
  replays and revisions, and always converges to golden. A deliberate mutation (process
  the oldest revision instead of the newest) made that property test fail, which shows
  the test can detect real errors.

## ADR-016 — Separate databases for tests and experiments
- **Date:** 2026-11-11 · **Milestone:** M3 · **Status:** proposed by Claude
- **Decision.** `warehouse_test` (pytest) and `warehouse_exp` (experiments) are owned by
  the pipeline role, created by `docker/initdb/10_aux_databases.sh` (or `make db-aux`
  on an existing volume). `reset_database` refuses any other database name.
- **Consequences.** A test or experiment reset can never empty the live warehouse.
  Tested: `test_reset_refuses_live_warehouse`.

## ADR-017 — Order-independent definitions where incremental must equal golden
- **Date:** 2026-11-11 · **Milestone:** M4/M7 · **Status:** proposed by Claude
- **Context.** Anything that depends on processing order breaks "incremental == golden"
  for reasons that are not bugs.
- **Decisions.**
  - **Relocation (Q15)** compares coordinates with the location's registered position,
    not with the last position seen. `wh.dim_sensor` still records position history.
  - **Daily means** use `avg()` over `numeric`, rounded to 6 decimals, because
    floating-point sums depend on row order.
  - **Duplicates (Q11)** keep the first row in file order, which is deterministic.
  - **Status** `revised` is compared as `complete`: golden cannot know publication
    history, and the revision itself is audited in `ops.revision_diff`.
- **Consequences.** The checksums and key comparisons in `make reconcile` are exact.
  Limitation: the Hampel and stuck-sensor flags look at one day only (no cross-day
  context). This is listed for LIMITATIONS.

## ADR-018 — Static HTML dashboard over SQL views instead of Grafana or Metabase
- **Date:** 2026-11-11 · **Milestone:** M12 · **Status:** proposed by Claude
- **Context.** The brief asks for dashboards in Grafana or Metabase. On this laptop the
  connection measured about 50–200 KB/s and Docker runs in a 4 GB VM (ADR-008). Either
  tool would add a multi-hundred-MB image and another long-running service. Project 1
  shipped a self-contained HTML dashboard successfully.
- **Decision.** Six SQL views in `ops` (`sql/ddl/006_views.sql`) are the observability
  layer. `make dashboard` renders them into one static, self-contained
  `docs/dashboard.html`. `make dashboard DB=warehouse_exp` shows experiment data.
- **Consequences.** There is no live refresh and no alerting; the page is regenerated
  on demand. Any BI tool can later read the same views. The figures in `docs/figures/`
  are SVG files generated without a plotting library, so there is no extra dependency
  to download.

## ADR-019 — Evaluation design choices made during M11
- **Date:** 2026-11-11 · **Milestone:** M11 · **Status:** proposed by Claude
- **Held-out seeds.** clf-1 was revised while developing with seeds 1–3. It was frozen
  in commit 087feed, and evaluation used seeds 11–15 (different targets and fault
  parameters). On those 80 guarded trials the classifier was correct in 80/80
  (`results.csv`, column `classified_correctly`).
- **`handled_as_expected`.** `recovered_verified` alone rewards a pipeline that blocks bad
  data silently: retries_only scores 60/75 on it, higher than guarded's 50/75, because
  guarded escalates by design. The added metric requires correct outputs AND the
  escalation the fault calls for (FAULTS.md). Both metrics are reported; neither is
  hidden.
- **Damage split.** "Wrong" monthly summary rows come only from missing days.
  `wrong_values_published` (hourly or daily values) is reported separately, and it was
  0 in every trial.
- **Bug found by the runner.** Non-autocommit connections turned every stage
  "transaction" into a savepoint, so nothing committed until the end of the session.
  Fixed (autocommit connections), with a regression test reading through a second
  connection. Integration tests had missed it because they read through the same
  connection.
