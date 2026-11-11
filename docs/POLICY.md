# Recovery policy

Source of truth: `config/policy.yaml` (validated at load; any non-escalate action must be
in `allowed_actions`, or loading fails). Engine: `src/sentinel/recovery.py`.

## The loop
Detect (evidence from the run summary and the database) -> classify (clf-1, a pure
function of the evidence) -> look up ONE policy rule -> take ONE bounded action or
escalate -> verify (recompute the location-day from its raw file and compare) -> record
(`ops.incident`, append-only `ops.action_log`, both with `policy_rule_id`).

No LLM is in this path (RESEARCH §4: LLM root-cause accuracy is moderate on narrow
classification and low on open-ended analysis, and it varies between runs).

## Evidence -> class (clf-1)
| Evidence | Condition | Class |
|---|---|---|
| run error | psycopg OperationalError/InterfaceError family (by hierarchy) | transient_db |
| run error | anything else, after some stages were checkpointed | process_crash |
| rejected file | `unknown_header` | unknown_schema |
| rejected file | unreadable/empty AND upstream checksum differs | transfer_corruption |
| rejected file | unreadable/empty AND upstream has the same bytes (or none) | source_unreadable |
| no file | file exists upstream | missing_file |
| no file | not upstream, <= 96 h after the day ended | late_file |
| no file | not upstream, later than that | unknown |
| blocked | most frequent quarantine rule Q05 | unknown_unit |
| blocked | most frequent quarantine rule Q08 | timezone_offset |
| blocked | otherwise | rows_over_threshold |
| published but not verified | - | verification_failed |
| newer revision processed | - | revision |

## Rules (P01-P99)
| Rule | Class | Action | Max attempts | Escalates |
|---|---|---|---|---|
| P01 | transient_db | retry (reconnect, reprocess) | 3 | when exhausted |
| P02 | process_crash | resume_from_checkpoint | 2 | when exhausted |
| P03 | transfer_corruption | refetch_and_reload | 2 | when exhausted / source empty |
| P04 | missing_file | bounded_backfill (max 3 days) | 1 | when exhausted / source empty |
| P05 | late_file | wait_and_recheck (once per scheduled run) | 3 | when exhausted |
| P06 | revision | apply_revision (already done by the pipeline; verified) | 1 | no |
| P07 | rows_over_threshold | block_publication, then escalate | 1 | always |
| P08-P12, P99 | unknown_schema, unknown_unit, timezone_offset, source_unreadable, verification_failed, unknown | escalate | 0 | always |

Never automatic (escalate instead): accept unknown schemas, invent unit conversions, shift
timestamps, edit SQL/models, delete raw data, disable tests, change thresholds.

## Development note (honest record)
clf-1 was revised twice while developing with seeds 1-3. Transient errors are now
recognised by exception hierarchy, because psycopg raises `AdminShutdown`, a subclass of
`OperationalError`. Location-days skipped after a lost connection are now reported with
that cause. The classifier was then frozen (commit 087feed) before the evaluation seeds
11-15 were run.
