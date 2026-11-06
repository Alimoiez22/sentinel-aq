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

**Open points for the author:**
- Review ADR-001…009 and mark each accepted or changed.
- The CI workflow is written but has not run (no GitHub remote yet).
