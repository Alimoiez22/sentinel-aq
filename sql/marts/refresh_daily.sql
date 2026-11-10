-- Refresh mart_daily_location for one location and a set of local dates.
-- Run with search_path = wh (live) or golden (rebuild): the SAME file produces both,
-- which is what makes "incremental == golden" a meaningful check (M7).
-- Parameters: %(location_id)s int, %(dates)s date[], %(min_hours)s int.
--
-- Grain notes:
--   * OpenAQ hourly stamps mark the END of the hour (DATA_CONTRACT §3), so the local date
--     of a reading is the date of (ts_utc - 1 hour) in the location's time zone.
--   * Several sensors of the same parameter at one location are averaged per hour first,
--     then the daily mean is the mean of hourly values (each hour counts once).
--   * avg() over numeric (not double) and rounding to 6 decimals make the result
--     independent of row order, so the live and golden marts can be compared exactly.

DELETE FROM mart_daily_location
WHERE location_id = %(location_id)s AND local_date = ANY (%(dates)s::date[]);

INSERT INTO mart_daily_location (
    location_id, local_date, parameter, daily_mean, hours_observed, completeness_pct,
    exceeds_guideline, guideline_value, publication_status)
WITH hourly AS (
    SELECT f.location_id,
           f.parameter,
           ((f.ts_utc - interval '1 hour') AT TIME ZONE l.timezone)::date AS local_date,
           f.ts_utc,
           avg(f.value::numeric) AS v
    FROM fact_measurement_hourly f
    JOIN wh.dim_location l ON l.location_id = f.location_id
    WHERE f.location_id = %(location_id)s
      -- prefilter on the batch day (indexed); local dates can only shift by one day
      AND f.source_date BETWEEN (SELECT min(d) FROM unnest(%(dates)s::date[]) d) - 1
                            AND (SELECT max(d) FROM unnest(%(dates)s::date[]) d) + 1
    GROUP BY f.location_id, f.parameter, local_date, f.ts_utc
),
daily AS (
    SELECT location_id, local_date, parameter, count(*)::int AS hours, avg(v) AS mean
    FROM hourly
    WHERE local_date = ANY (%(dates)s::date[])
    GROUP BY location_id, local_date, parameter
),
keys AS (   -- days with data, plus gated days without any accepted row (e.g. blocked)
    SELECT location_id, local_date, parameter FROM daily
    UNION
    SELECT location_id, local_date, parameter FROM publication_status
    WHERE location_id = %(location_id)s AND local_date = ANY (%(dates)s::date[])
)
SELECT k.location_id,
       k.local_date,
       k.parameter,
       CASE WHEN s.status IN ('complete', 'revised') AND coalesce(d.hours, 0) >= %(min_hours)s
            THEN round(d.mean, 6)::double precision END,
       coalesce(d.hours, 0),
       round(100.0 * coalesce(d.hours, 0) / 24, 2)::double precision,
       CASE WHEN s.status IN ('complete', 'revised') AND coalesce(d.hours, 0) >= %(min_hours)s
            THEN round(d.mean, 6) > g.value_ugm3::numeric END,
       g.value_ugm3,
       coalesce(s.status, 'partial')
FROM keys k
LEFT JOIN daily d USING (location_id, local_date, parameter)
LEFT JOIN publication_status s USING (location_id, local_date, parameter)
JOIN wh.guideline g ON g.parameter = k.parameter AND g.averaging = '24h';

-- Monthly exceedance summary for the months touched.
DELETE FROM mart_exceedance_summary
WHERE location_id = %(location_id)s
  AND month IN (SELECT DISTINCT date_trunc('month', d)::date FROM unnest(%(dates)s::date[]) d);

INSERT INTO mart_exceedance_summary (location_id, month, parameter, complete_days, exceedance_days)
SELECT location_id,
       date_trunc('month', local_date)::date,
       parameter,
       count(*) FILTER (WHERE daily_mean IS NOT NULL)::int,
       count(*) FILTER (WHERE exceeds_guideline)::int
FROM mart_daily_location
WHERE location_id = %(location_id)s
  AND date_trunc('month', local_date)::date IN
      (SELECT DISTINCT date_trunc('month', d)::date FROM unnest(%(dates)s::date[]) d)
GROUP BY location_id, date_trunc('month', local_date), parameter;
