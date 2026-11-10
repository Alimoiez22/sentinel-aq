-- Guideline values, loaded from config/guidelines.yaml by `sentinel migrate` (cited there).
CREATE TABLE IF NOT EXISTS wh.guideline (
    parameter   text             NOT NULL REFERENCES wh.dim_parameter (parameter),
    averaging   text             NOT NULL CHECK (averaging IN ('24h', 'annual')),
    value_ugm3  double precision NOT NULL CHECK (value_ugm3 > 0),
    source      text             NOT NULL,
    PRIMARY KEY (parameter, averaging)
);

-- The mart SQL (sql/marts/*.sql) is written against unqualified table names and run
-- with search_path set to either `wh` (live) or `golden` (rebuild). This view gives the
-- live schema the same name the golden schema uses for its gate table.
CREATE OR REPLACE VIEW wh.publication_status AS SELECT * FROM ops.publication_status;

-- Golden copies of the published structures (filled by `make golden`, M7). The golden
-- fact is not partitioned: it is rebuilt from scratch every time.
CREATE TABLE IF NOT EXISTS golden.fact_measurement_hourly
    (LIKE wh.fact_measurement_hourly INCLUDING ALL);
CREATE TABLE IF NOT EXISTS golden.mart_daily_location (LIKE wh.mart_daily_location INCLUDING ALL);
CREATE TABLE IF NOT EXISTS golden.mart_exceedance_summary
    (LIKE wh.mart_exceedance_summary INCLUDING ALL);
-- LIKE never copies foreign keys, so golden rows do not reference live batches.
CREATE TABLE IF NOT EXISTS golden.publication_status (LIKE ops.publication_status INCLUDING ALL);
