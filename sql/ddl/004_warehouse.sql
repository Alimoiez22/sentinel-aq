-- Warehouse (PROJECT_BRIEF §7). Grain choices are explained in docs/ARCHITECTURE.md.

CREATE TABLE IF NOT EXISTS wh.dim_parameter (
    parameter      text PRIMARY KEY,               -- OpenAQ code, e.g. pm25
    name           text NOT NULL,
    canonical_unit text NOT NULL
);
INSERT INTO wh.dim_parameter VALUES
    ('pm25', 'Particulate matter < 2.5 um', 'µg/m³'),
    ('pm10', 'Particulate matter < 10 um',  'µg/m³')
ON CONFLICT (parameter) DO NOTHING;

CREATE TABLE IF NOT EXISTS wh.dim_location (
    location_id  integer PRIMARY KEY,
    name         text,
    city         text,
    country      text,
    timezone     text NOT NULL,
    provider     text,
    geom         geometry(Point, 4326)
);

-- Position history: a relocation opens a new version (valid_from) instead of overwriting.
CREATE TABLE IF NOT EXISTS wh.dim_sensor (
    sensor_id    integer     NOT NULL,
    valid_from   timestamptz NOT NULL,
    valid_to     timestamptz,                      -- NULL = current version
    location_id  integer     NOT NULL REFERENCES wh.dim_location (location_id),
    parameter    text        NOT NULL REFERENCES wh.dim_parameter (parameter),
    geom         geometry(Point, 4326),
    first_seen   timestamptz NOT NULL,
    last_seen    timestamptz NOT NULL,
    PRIMARY KEY (sensor_id, valid_from)
);

-- Hourly fact. Grain: one sensor x one UTC hour. Partitioned by month on ts_utc.
-- source_date + location_id identify the batch a row came from, so a location-day can
-- be replaced atomically (delete + insert in one transaction).
CREATE TABLE IF NOT EXISTS wh.fact_measurement_hourly (
    sensor_id    integer          NOT NULL,
    ts_utc       timestamptz      NOT NULL,        -- end of the measured hour, UTC
    location_id  integer          NOT NULL,
    source_date  date             NOT NULL,        -- local calendar day of the source file
    parameter    text             NOT NULL,
    value        double precision NOT NULL,        -- canonical unit (µg/m³)
    value_orig   double precision NOT NULL,
    unit_orig    text             NOT NULL,
    flags        text[]           NOT NULL DEFAULT '{}',
    batch_id     bigint           NOT NULL,
    revision_no  integer          NOT NULL,
    PRIMARY KEY (sensor_id, ts_utc)
) PARTITION BY RANGE (ts_utc);
CREATE TABLE IF NOT EXISTS wh.fact_measurement_hourly_default
    PARTITION OF wh.fact_measurement_hourly DEFAULT;
CREATE INDEX IF NOT EXISTS fact_locday ON wh.fact_measurement_hourly (location_id, source_date);

-- Monthly partitions are created on demand by the loader before it inserts, so the
-- DEFAULT partition stays empty (PostgreSQL refuses to add a partition whose range
-- already has rows in DEFAULT).
CREATE OR REPLACE FUNCTION wh.ensure_month_partition(month_start date) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part text := format('fact_measurement_hourly_%s', to_char(month_start, 'YYYYMM'));
BEGIN
    IF to_regclass('wh.' || part) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE wh.%I PARTITION OF wh.fact_measurement_hourly
               FOR VALUES FROM (%L) TO (%L)',
            part,
            -- timestamp (no zone) interpreted AS UTC -> exact UTC month boundaries,
            -- independent of the session's TimeZone setting.
            month_start::timestamp AT TIME ZONE 'UTC',
            (month_start + interval '1 month')::timestamp AT TIME ZONE 'UTC');
    END IF;
END $$;

-- Published marts. Same DDL is created in the golden schema by the golden rebuild.
CREATE TABLE IF NOT EXISTS wh.mart_daily_location (
    location_id        integer NOT NULL,
    local_date         date    NOT NULL,
    parameter          text    NOT NULL,
    daily_mean         double precision,          -- NULL when completeness is insufficient
    hours_observed     integer NOT NULL,
    completeness_pct   double precision NOT NULL,
    exceeds_guideline  boolean,                   -- NULL when daily_mean is NULL
    guideline_value    double precision NOT NULL,
    publication_status text    NOT NULL,
    PRIMARY KEY (location_id, local_date, parameter)
);

CREATE TABLE IF NOT EXISTS wh.mart_exceedance_summary (
    location_id      integer NOT NULL,
    month            date    NOT NULL,             -- first day of the local month
    parameter        text    NOT NULL,
    complete_days    integer NOT NULL,
    exceedance_days  integer NOT NULL,
    PRIMARY KEY (location_id, month, parameter)
);
