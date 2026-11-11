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
real file yet; M1/M2 on real Lahore files will report which variants occur.

## 3. Typed canonical frame (Pandera schema `CANONICAL_SCHEMA`)
* `ts_utc`: `datetime` parsed as ISO-8601 with offset (`%Y-%m-%dT%H:%M:%S%:z`), then
  converted to UTC. NULL if it does not parse (quarantined by Q01).
* `utc_offset_s`: the offset written in the file, in seconds (checked by Q08).
* `value`: float; NULL if missing, not a number, NaN or infinite (quarantined by Q02).
* OpenAQ hourly stamps are treated as the END of the hour: the real file read on
  2026-11-05 started at 01:00. This rests on n = 1 file and must be re-checked on
  real Lahore data (Q06 rule hits will show it).

## 4. Keys and identity
* A file is identified by its archive key `records/csv.gz/locationid={id}/year=/month=/
  location-{id}-{YYYYMMDD}.csv.gz`; the location and source date come from the key.
* A file VERSION is identified by its SHA-256. Same bytes = same batch (duplicate);
  new bytes for the same location-day = a revision.
* Rows whose `location_id` differs from the key's location are quarantined (Q09).
