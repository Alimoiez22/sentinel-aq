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
