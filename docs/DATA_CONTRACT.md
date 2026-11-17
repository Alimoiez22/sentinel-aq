# Data contract: OpenAQ daily files

Implemented in `src/sentinel/contract.py` (version 1). Facts and sources: RESEARCH §5.

## 1. File level (decides whether a file can be read)
| Check | On failure |
|---|---|
| gzip-compressed UTF-8 text | batch `rejected`, reason `unreadable` |
| a non-empty header line | `rejected`, reason `empty` |
| header column SET equals a known variant | `rejected`, reason `unknown_header` (escalate) |

Columns are read by NAME, so a reordered file with a known column set is accepted. An
added, missing or renamed column is an unknown schema and is never guessed (F5).

## 2. Known header variants (explicit mapping)
| Canonical column | Variant A (docs table) | Variant B (docs example; real file 2026-11-05) |
|---|---|---|
| file_location_id | location_id | location_id |
| sensor_id | sensor_id | sensors_id |
| location_name | location | location |
| datetime_raw | datetime | datetime |
| lat_raw / lon_raw | lat / lon | lat / lon |
| parameter | parameter | parameter |
| unit_raw | unit | units |
| value_raw | value | value |

Provenance of each header: `data/fixtures/HEADERS.md`. Variant A has not been seen in a
real file. All 2,437 real Lahore files fetched on 2026-11-17 (locations 8664 and 4568423)
parsed as variant B.

## 3. Typed canonical frame (Pandera schema `CANONICAL_SCHEMA`)
* `ts_utc`: `datetime` parsed as ISO-8601 with offset (`%Y-%m-%dT%H:%M:%S%:z`), then
  converted to UTC. NULL if it does not parse (quarantined by Q01).
* `utc_offset_s`: the offset written in the file, in seconds (checked by Q08).
* `value`: float; NULL if missing, not a number, NaN or infinite (quarantined by Q02).
* OpenAQ hourly stamps are treated as the END of the hour (01:00 .. 00:00 next day).
  Checked on real files: location 2178 (2026-11-05), and 8664 and 4568423 (2026-11-17).
  Across all 2,437 Lahore location-days, rule Q06 (outside local day) did not fire
  (results/quality_rules_v1_vs_v2_by_rule.csv).
* Missing-value code: real 8664 files contain `-999.0` for hours with no measurement
  (7,879 rows). Rules v2 treat it as a missing hour (ADR-021).
* Cosmetic quirk: the Model Town file's `location` column reads `Model Town-4538361`, a
  different number from its location id 4568423. The row `location_id` matches the key,
  so Q09 is not affected.

## 4. Keys and identity
* A file is identified by its archive key `records/csv.gz/locationid={id}/year=/month=/
  location-{id}-{YYYYMMDD}.csv.gz`; the location and source date come from the key.
* A file VERSION is identified by its SHA-256. Same bytes = same batch (duplicate);
  new bytes for the same location-day = a revision.
* Rows whose `location_id` differs from the key's location are quarantined (Q09).
