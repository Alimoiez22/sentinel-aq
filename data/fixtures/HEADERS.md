# Header variants: provenance

Only header lines are recorded here. No real data rows are committed, because OpenAQ
licences vary by provider.

| Variant | Header | Where it was seen | Date checked |
|---|---|---|---|
| A | `location_id,sensor_id,location,datetime,lat,lon,parameter,unit,value` | OpenAQ docs, column table, https://docs.openaq.org/aws/about | 2026-11-05 |
| B | `location_id,sensors_id,location,datetime,lat,lon,parameter,units,value` | OpenAQ docs, example header, and the real file `records/csv.gz/locationid=2178/year=2022/month=05/location-2178-20220503.csv.gz` (bucket `openaq-data-archive`) | 2026-11-05 |

Variant A has not yet been seen in a real file. M1 and M2 will report which variants
the chosen Lahore files actually use. The `units` values in the real file were the
literal string `µg/m³` (UTF-8, non-ASCII).

The synthetic fixtures in `synthetic/` use variant A for location 99000001 and
variant B for location 99000002.
