# Quality rules

Source of truth: `config/quality_rules.yaml` (validated by `src/sentinel/quality.py`).
Each quarantined row stores the rule id and version that rejected it
(`quarantine.aq_rejects`); per-batch counts are in `ops.quality_stats`.

| Rule | Action | Condition | Basis |
|---|---|---|---|
| Q16 out of scope | info (dropped, counted) | parameter not in scope (pm25, pm10) | scope |
| Q10 bad sensor | quarantine | sensor id missing / not a number | contract |
| Q09 wrong location | quarantine | row location != file location | contract |
| Q01 bad timestamp | quarantine | datetime does not parse | contract |
| Q07 future | quarantine | timestamp after processing time | sanity |
| Q08 offset mismatch | quarantine | file offset != time-zone offset of the location | IANA tz (tzdata) |
| Q06 outside local day | quarantine | (ts - 1 h) not on the file's local date | end-of-hour stamps (DATA_CONTRACT §3) |
| Q02 bad value | quarantine | missing, not a number, NaN, inf | contract |
| Q05 unknown unit | quarantine (+ escalate if it blocks) | unit not in the safe-conversion table | RESEARCH §7 |
| Q03 negative | quarantine | value < 0 | physical |
| Q04 above ceiling | quarantine | > 1000 µg/m³ after conversion | PMS5003 max range, BAM-1020 default (RESEARCH §7.14) |
| Q11 duplicate | quarantine (2nd+ copy) | same sensor and hour; first row in file order kept | deterministic rule (ADR-017) |
| Q12 high band | flag | 500 < value <= 1000 µg/m³ | PMS5003 effective range (RESEARCH §7.14) |
| Q13 stuck | flag | same value >= 6 consecutive hours | project choice |
| Q14 spike | flag | Hampel, window ±3 h, 3 x 1.4826 x MAD | pearson2016hampel, hampel1974influence |
| Q15 relocated | flag | coords > 0.001° from the registered site | ADR-017 |
| B01 reject rate | block the location-day | quarantined share > 20 % | project choice (tested) |
| completeness | mart `partial` | < 18 of 24 hours | EU 2008/50/EC Annex XI |

Evaluation order is fixed. The first failing rule is recorded (one quarantine row per
file row), so per-rule counts add up to the reject total.

**Not cited, project choices:** the 20 % block threshold, the 6-hour stuck run and the
Hampel window. They are configurable, and changing them requires a new rule version and
an ADR (RUNBOOK RB-07).

**Limitations:** the Hampel and stuck-sensor flags look at one day only. Low-cost sensor
bias is not corrected (RESEARCH §7.15).
