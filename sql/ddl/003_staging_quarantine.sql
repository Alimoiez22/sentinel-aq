-- Parsed rows of the current batch for each location-day. Replaced as a whole, in one
-- transaction, so a reader never sees half of a location-day (M3).
CREATE TABLE IF NOT EXISTS staging.measurement (
    batch_id      bigint      NOT NULL REFERENCES ops.batch_manifest (batch_id),
    location_id   integer     NOT NULL,
    source_date   date        NOT NULL,
    row_no        integer     NOT NULL,           -- line number in the file (1 = first data row)
    sensor_id     integer,
    location_name text,
    datetime_raw  text,
    ts_utc        timestamptz,                    -- NULL when datetime_raw does not parse
    utc_offset_s  integer,                        -- offset written in the file, in seconds
    lat           double precision,
    lon           double precision,
    parameter     text,
    unit_raw      text,
    value_raw     text,
    value         double precision,               -- NULL when value_raw does not parse
    PRIMARY KEY (location_id, source_date, row_no)
);

-- Rows rejected by a quality rule, with the rule (id + version) that rejected them.
CREATE TABLE IF NOT EXISTS quarantine.aq_rejects (
    batch_id     bigint      NOT NULL REFERENCES ops.batch_manifest (batch_id),
    location_id  integer     NOT NULL,
    source_date  date        NOT NULL,
    row_no       integer     NOT NULL,
    rule_id      text        NOT NULL,
    rule_version integer     NOT NULL,
    reason       text        NOT NULL,
    row_payload  jsonb       NOT NULL,
    rejected_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, row_no)
);
CREATE INDEX IF NOT EXISTS aq_rejects_locday ON quarantine.aq_rejects (location_id, source_date);

-- Per-batch quality statistics (reject counts per rule, flag counts per rule).
CREATE TABLE IF NOT EXISTS ops.quality_stats (
    batch_id    bigint  NOT NULL REFERENCES ops.batch_manifest (batch_id),
    rule_id     text    NOT NULL,
    action      text    NOT NULL CHECK (action IN ('flag', 'quarantine', 'block', 'info')),
    n_rows      integer NOT NULL,
    PRIMARY KEY (batch_id, rule_id)
);
