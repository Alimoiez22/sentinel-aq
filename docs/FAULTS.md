# Fault catalogue (F1-F15)

Implemented in `src/sentinel/faults.py`, seeded and deterministic (`build_trial(fault,
seed, clean)`). Each seed picks the target location-day and the fault's parameters, so
evaluation seeds (11-15) are variants not seen during development (seeds 1-3).

**Where the fault happens decides the truth** used by the golden rebuild:
* transfer or pipeline faults -> truth = the clean upstream file;
* faults in the upstream data itself -> truth = that faulty file run through the same
  rules (a correct pipeline quarantines or blocks it; it must not repair it).

| Fault | What is injected | Where | Truth | Expected handling | Escalate? |
|---|---|---|---|---|---|
| F0 | nothing (baseline for overhead) | - | clean | publish | no |
| F1 | identical file delivered again in run 1 | transfer | clean | content-addressed no-op | no |
| F2 | landing copy truncated to 30-70 % of its bytes | transfer | clean | `transfer_corruption` -> refetch (P03) | no |
| F3 | 30-45 % of values replaced by `#ERR` | upstream | faulty file | rows quarantined, day blocked (P07) | yes |
| F4 | header switched to the other KNOWN variant | upstream | faulty file | mapped by the contract, no incident | no |
| F5 | extra column `qc_flag` (unknown schema) | upstream | clean content | `unknown_schema` -> escalate (P08) | yes |
| F6 | decimal comma in every value | upstream | faulty file | all rows quarantined, blocked (P07) | yes |
| F7 | file never reaches landing; it exists upstream | transfer | clean | `missing_file` -> bounded backfill (P04) | no |
| F8 | newest day not published upstream until run 1 | upstream timing | clean | `late_file` -> wait and recheck (P05) | no |
| F9 | retroactive patch: 3-6 values x1.1-1.5 in run 1 | upstream | revised file | `revision` -> apply (P06) | no |
| F10 | crash after staging, before commit (run 0) | pipeline | clean | `process_crash` -> resume (P02) | no |
| F11 | crash in the middle of the mart refresh | pipeline | clean | `process_crash` -> resume (P02) | no |
| F12 | database backend killed (pg_terminate_backend) | infrastructure | clean | `transient_db` -> retry (P01) | no |
| F13 | the PM2.5 series switches to `ppm` part-way through the day (injector v2, ADR-022) | upstream | faulty file | blocked; `unknown_unit` -> escalate (P09) | yes |
| F14 | 8-hour stuck run + 3 spikes (x6) in the PM2.5 series (v2) | upstream | faulty file | flagged (Q13, Q14) and kept | no |
| F15 | local times written with `+00:00` | upstream | faulty file | blocked; `timezone_offset` -> escalate (P10) | yes |

Tests: `tests/unit/test_faults_recovery.py` checks that every trial is deterministic,
that each fault really manifests (e.g. F2 is unreadable, F5 has an unknown header,
F6 leaves no parseable value), and that truth follows the table above.

"Expected handling" is a project definition. It is used to judge *inappropriate
automatic actions* (RESEARCH §8) and `handled_as_expected` (docs/EXPERIMENTS.md).
