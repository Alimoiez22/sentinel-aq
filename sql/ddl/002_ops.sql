-- Operational metadata (PROJECT_BRIEF §9).

-- One row per distinct file version (content) for a location-day.
CREATE TABLE IF NOT EXISTS ops.batch_manifest (
    batch_id        bigserial PRIMARY KEY,
    location_id     integer     NOT NULL,
    source_date     date        NOT NULL,
    file_key        text        NOT NULL,
    sha256          char(64)    NOT NULL,
    size_bytes      integer     NOT NULL CHECK (size_bytes >= 0),
    raw_row_count   integer,                       -- NULL when the file is unreadable
    header_variant  text,                          -- A, B, or NULL when unknown/unreadable
    arrival_ts      timestamptz NOT NULL,
    registered_at   timestamptz NOT NULL DEFAULT now(),
    status          text        NOT NULL CHECK (status IN
                      ('registered', 'loaded', 'published', 'blocked', 'superseded', 'rejected')),
    status_reason   text,
    revision_no     integer     NOT NULL CHECK (revision_no >= 0),
    superseded_by   bigint REFERENCES ops.batch_manifest (batch_id),
    attempt_count   integer     NOT NULL DEFAULT 0,
    -- The same bytes for the same location-day are one batch (F1 duplicate = no-op).
    UNIQUE (location_id, source_date, sha256),
    UNIQUE (location_id, source_date, revision_no)
);
CREATE INDEX IF NOT EXISTS batch_manifest_locday
    ON ops.batch_manifest (location_id, source_date, revision_no DESC);

-- Stage-level checkpoints: resume skips stages already done for a batch.
CREATE TABLE IF NOT EXISTS ops.checkpoint (
    batch_id    bigint      NOT NULL REFERENCES ops.batch_manifest (batch_id),
    stage       text        NOT NULL CHECK (stage IN ('staged', 'quality', 'fact', 'marts', 'verified')),
    done_at     timestamptz NOT NULL DEFAULT now(),
    detail      jsonb,
    PRIMARY KEY (batch_id, stage)
);

CREATE TABLE IF NOT EXISTS ops.run_log (
    run_id      bigserial PRIMARY KEY,
    mode        text        NOT NULL CHECK (mode IN ('manual', 'retries_only', 'guarded', 'plain')),
    command     text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz,
    outcome     text,
    git_commit  text,
    detail      jsonb
);

-- Publication gate state per location-day-parameter (what readers may trust).
CREATE TABLE IF NOT EXISTS ops.publication_status (
    location_id  integer     NOT NULL,
    local_date   date        NOT NULL,
    parameter    text        NOT NULL,
    status       text        NOT NULL CHECK (status IN ('complete', 'partial', 'blocked', 'revised')),
    reason       text,
    batch_id     bigint REFERENCES ops.batch_manifest (batch_id),
    revision_no  integer,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (location_id, local_date, parameter)
);

-- Old vs new values when a revision replaces a location-day (M6).
CREATE TABLE IF NOT EXISTS ops.revision_diff (
    location_id  integer     NOT NULL,
    source_date  date        NOT NULL,
    old_batch_id bigint      NOT NULL REFERENCES ops.batch_manifest (batch_id),
    new_batch_id bigint      NOT NULL REFERENCES ops.batch_manifest (batch_id),
    sensor_id    integer     NOT NULL,
    ts_utc       timestamptz NOT NULL,
    change       text        NOT NULL CHECK (change IN ('added', 'removed', 'value_changed')),
    old_value    double precision,
    new_value    double precision,
    PRIMARY KEY (new_batch_id, sensor_id, ts_utc)
);

-- Incidents and decisions (M10). One row per detected failure.
CREATE TABLE IF NOT EXISTS ops.incident (
    incident_id                  bigserial PRIMARY KEY,
    detected_at                  timestamptz NOT NULL DEFAULT now(),
    location_id                  integer,
    source_date                  date,
    batch_id                     bigint,
    stage                        text,
    failure_class                text        NOT NULL,
    evidence                     jsonb       NOT NULL,
    classifier_version           text        NOT NULL,
    decision                     text        NOT NULL CHECK (decision IN ('act', 'escalate', 'none')),
    policy_rule_id               text,
    action                       text,
    attempts                     integer     NOT NULL DEFAULT 0,
    outcome                      text,
    verified                     boolean,
    escalated                    boolean     NOT NULL DEFAULT false,
    time_to_verified_recovery_s  double precision,
    origin                       text        NOT NULL CHECK (origin IN ('injected', 'real')),
    run_id                       bigint REFERENCES ops.run_log (run_id)
);

-- Immutable audit trail of every automatic or manual action.
CREATE TABLE IF NOT EXISTS ops.action_log (
    action_id      bigserial PRIMARY KEY,
    incident_id    bigint REFERENCES ops.incident (incident_id),
    at             timestamptz NOT NULL DEFAULT now(),
    actor          text        NOT NULL,           -- 'policy-engine', 'retry-loop', 'operator'
    action         text        NOT NULL,
    policy_rule_id text,
    params         jsonb,
    result         text        NOT NULL
);

-- Make the audit trail append-only: updates and deletes are rejected.
CREATE OR REPLACE FUNCTION ops.reject_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ops.action_log is append-only';
END $$;
DROP TRIGGER IF EXISTS action_log_append_only ON ops.action_log;
CREATE TRIGGER action_log_append_only BEFORE UPDATE OR DELETE ON ops.action_log
    FOR EACH ROW EXECUTE FUNCTION ops.reject_change();
