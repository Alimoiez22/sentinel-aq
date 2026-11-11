# Architecture

```
OpenAQ S3 archive ──snapshot/fetch──▶ data/raw (sha256, write-once) + data/landing_drop (latest)
                                          │
              register (ops.batch_manifest: sha256, variant, revision_no)
                                          │
        stage (staging.measurement)  ──▶  quality + fact (quarantine.aq_rejects, wh.fact_*)
                                          │
              marts + gate (wh.mart_*, ops.publication_status)  ──▶  verify
                                          │
    golden rebuild (golden.*) ◀── same parse, rules, gate and mart SQL ──▶ reconcile
                                          │
  recovery engine: evidence → clf-1 → policy.yaml → one bounded action | escalate → verify
```

## Schemas
| Schema | Holds | Written by |
|---|---|---|
| ops | manifest, checkpoints, run log, publication status, revision diff, incidents, action log (append-only), quality stats, views | pipeline, recovery engine |
| staging | parsed rows of the current batch of each location-day | stage |
| quarantine | rejected rows + rule id/version | quality |
| wh | dim_location, dim_sensor (position history), dim_parameter, guideline, fact_measurement_hourly (monthly partitions), mart_daily_location, mart_exceedance_summary | quality+fact, marts |
| golden | the same published structures, rebuilt from scratch | `make golden` |

Databases: `warehouse` (live), `warehouse_test` (pytest), `warehouse_exp` (experiments),
`airflow_meta` (Airflow), all on one PostgreSQL 18 server (ADR-002, ADR-016).

## Grain choices (explain in an interview)
* **Fact: one sensor × one UTC hour.** This is the finest grain the source has. Using
  UTC avoids daylight-saving ambiguity. `source_date` and `location_id` identify the
  file a row came from, so a location-day can be replaced atomically.
* **Mart: one location × local date × parameter.** Health guidelines are daily means in
  local time. Several sensors of one parameter are averaged per hour first, so every
  hour counts once.
* **Local date** = date of (ts_utc − 1 h) in the location's time zone, because stamps
  mark the end of the hour (DATA_CONTRACT §3).
* **Exceedance summary: location × local month × parameter**, counting complete days
  and days above the WHO 24-hour guideline. These are not called breaches (RESEARCH §2.10).

## Correctness mechanisms
1. **Content-addressed raw files.** A file is stored under its SHA-256 and written once,
   so revisions keep both versions (ADR-011).
2. **One transaction per stage.** Each stage runs on an autocommit connection
   (`with conn.transaction()` is then a real transaction), takes an advisory lock on the
   location-day, and writes its checkpoint inside the same transaction (ADR-015).
3. **Replace, don't append.** Every write replaces the location-day, which makes stages
   idempotent.
4. **The same logic for live and golden.** The mart SQL runs with `search_path` set to
   `wh` or `golden` (ADR-014). Definitions are order-independent (ADR-017).
5. **Comparators.** Key-based comparison gives missing, extra and wrong rows, plus
   order-independent checksums per month.
6. **Property test.** Any sequence of arrivals, duplicates, replays and revisions
   converges to golden. This was mutation-checked (ADR-015).

## Orchestration
Airflow 3.3 (LocalExecutor) is optional (`make up-airflow`). DAGs call the CLI only:
`sentinel_daily` runs snapshot → daily (guarded, 14-day window) → golden → reconcile;
`sentinel_backfill` is triggered manually with a date range. Recovery is the policy
engine's job, so Airflow retries are off (one recovery layer, auditable).
