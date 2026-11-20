# RUNBOOK

Each escalation records an evidence bundle (`ops.incident.evidence`) and the RUNBOOK
section of its policy rule (`config/policy.yaml`, field `runbook`). Run `make` from Git Bash
or WSL (ADR-009). All commands below are safe to repeat: every stage is idempotent (ADR-015).

## 0. First look (every incident)
```bash
make psql
```
```sql
SELECT * FROM ops.v_incidents ORDER BY incident_id DESC LIMIT 20;
SELECT evidence FROM ops.incident WHERE incident_id = <id>;
SELECT * FROM ops.v_batch_timeline WHERE location_id = <loc> AND source_date = '<day>';
```
After any fix: `make golden && make reconcile`. Exit code 0 means the published tables
equal the golden rebuild. Never edit `data/raw/` (rule 8); it is write-once by design.

## RB-01 Transient database error (P01)
Symptom: `OperationalError` / `AdminShutdown` in the evidence. The engine already retried
up to 3 times. Check that the database is up (`make status`), then rerun:
`docker compose run --rm app sentinel daily --start <day> --end <day>`.

## RB-02 Process crash mid-pipeline (P02)
Symptom: run error with some stages checkpointed. Rerunning resumes from the last
checkpoint (stages are transactional). Rerun as in RB-01. If it crashes again at the same
stage, look at the log line `stage`, and do not skip stages by hand.

## RB-03 Transfer corruption (P03)
Symptom: batch `rejected` with `unreadable`, and the source checksum differs from the
landing checksum. The engine already re-fetched twice. Check connectivity, then:
`make fetch LOCATIONS="<loc>" CONFIRM=yes` and rerun.

## RB-04 Missing file (P04)
Symptom: no batch, and the file exists upstream. The engine backfilled once. If it is
still missing, check `data/landing_drop/records/...` and the fetch index
`data/raw/index.csv`, then fetch as in RB-03.

## RB-05 Late file (P05)
Symptom: no batch, not upstream yet, inside the 96 h grace window. Nothing to do; the next
daily run rechecks. After 3 runs it escalates: check the location on OpenAQ (is it
still reporting?) and record the outage in LIMITATIONS if it is permanent.

## RB-06 Revision (P06)
Informational. The pipeline superseded the old batch, replayed the day and marked
outputs `revised`. Review `SELECT * FROM ops.v_revisions WHERE source_date = '<day>';`.

## RB-07 Too many rejected rows: publication blocked (P07)
Symptom: batch `blocked`; `ops.quality_stats` shows which rules fired. Decide:
* the data really is bad upstream: leave it blocked (correct outcome), note it;
* a rule is wrong for this data (e.g. a new unit spelling): change the rule in
  `config/quality_rules.yaml` with a NEW version and an ADR, then rerun. Never lower a
  threshold just to publish one day.

## RB-08 Unknown schema (P08)
Symptom: batch `rejected` with `unknown_header: <columns>`. Compare the header with
`data/fixtures/HEADERS.md`. If it is a genuine new variant, add it to `KNOWN_HEADERS`
and `TO_CANONICAL` in `src/sentinel/contract.py`, add a fixture and a test, record it
in `docs/DATA_CONTRACT.md`, then rerun. Never map columns by guessing.

## RB-09 Unknown unit (P09)
Symptom: blocked, top rule `Q05_unknown_unit`; evidence lists the units seen. PM mass
concentrations cannot be converted from ppm. Leave the day blocked and report the issue
upstream. Add a conversion only if it is an exact, documented one (ADR required).

## RB-10 Time-zone offset error (P10)
Symptom: blocked, top rule `Q08_offset_mismatch`. Timestamps do not match the
location's time zone. Do not shift them; leave the day blocked and report upstream.

## RB-11 Source itself unreadable (P11)
Symptom: rejected `unreadable`, and the source has the same checksum. The upstream file is
broken; wait for a revision (the snapshot will detect it).

## RB-12 Verification failed (P12)
Published rows do not match a recomputation from the raw file. Run
`make golden && make reconcile` and read the examples in the output. Rerun the day.
If it persists, this is a bug: write a failing test first.

## RB-99 Unknown
Collect the evidence, the log lines for the location-day, and the batch timeline. Open
an issue with them. Do not act until the failure is understood.

## Running a manual trial (Track A, mode = manual)
1. `make manual-start FAULT=F2 SEED=11 OPERATOR=<you>`: the pipeline runs without
   recovery and halts. The clock starts.
2. `make manual-shell`: a shell on `warehouse_exp`. Use only this RUNBOOK. Useful:
   `psql` is not in the app image; use `sentinel` commands and, from another
   terminal, `make psql` then `\c warehouse_exp`.
   Re-run: `sentinel run --locations "99000001 99000002" --landing data/manual_trial/landing --raw data/manual_trial/raw`.
   The upstream copy of every file is in `data/manual_trial/source/` (re-fetch = copy
   the file from there into `data/manual_trial/landing/` at the same path).
3. `make manual-finish`: records elapsed time and damage in
   `experiments/results/manual_trials.csv`, labelled human-operated, with your trial number.

## Track B snapshots (manual, optional)
Track B measures how often OpenAQ revises old files. It needs a snapshot per day over
several weeks. No schedule is installed (a daily task ran on 2026-11-20 and was removed
the same day at the author's request). To collect snapshots, run by hand:
```bash
bash scripts/snapshot.sh          # one snapshot; appends to data/snapshots/_runs.log
make revisions-report             # results/trackb_report.md
```
