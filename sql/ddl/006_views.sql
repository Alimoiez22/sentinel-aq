-- Observability views (PROJECT_BRIEF §14). The dashboard and the RUNBOOK query these.

CREATE OR REPLACE VIEW ops.v_batch_timeline AS
SELECT b.batch_id, b.location_id, b.source_date, b.revision_no, b.status, b.status_reason,
       b.header_variant, b.raw_row_count, b.arrival_ts, b.registered_at,
       (SELECT max(done_at) FROM ops.checkpoint c WHERE c.batch_id = b.batch_id) AS last_stage_at,
       (SELECT string_agg(stage, ',' ORDER BY done_at) FROM ops.checkpoint c
         WHERE c.batch_id = b.batch_id) AS stages_done
FROM ops.batch_manifest b;

CREATE OR REPLACE VIEW ops.v_incidents AS
SELECT i.incident_id, i.detected_at, i.location_id, i.source_date, i.failure_class,
       i.decision, i.policy_rule_id, i.action, i.attempts, i.outcome, i.verified, i.escalated,
       i.time_to_verified_recovery_s, i.origin,
       (SELECT count(*) FROM ops.action_log a WHERE a.incident_id = i.incident_id) AS n_actions
FROM ops.incident i;

CREATE OR REPLACE VIEW ops.v_publication AS
SELECT status, count(*) AS location_days,
       min(local_date) AS first_day, max(local_date) AS last_day
FROM ops.publication_status GROUP BY status;

CREATE OR REPLACE VIEW ops.v_reject_rates AS
SELECT q.rule_id, q.action, count(DISTINCT q.batch_id) AS batches, sum(q.n_rows) AS rows_hit,
       round(100.0 * sum(q.n_rows) / nullif(sum(b.raw_row_count), 0), 2) AS pct_of_rows
FROM ops.quality_stats q JOIN ops.batch_manifest b USING (batch_id)
WHERE q.n_rows > 0
GROUP BY q.rule_id, q.action;

CREATE OR REPLACE VIEW ops.v_revisions AS
SELECT d.location_id, d.source_date, d.old_batch_id, d.new_batch_id, d.change,
       count(*) AS rows, round(avg(abs(d.new_value - d.old_value))::numeric, 3) AS mean_abs_change
FROM ops.revision_diff d
GROUP BY d.location_id, d.source_date, d.old_batch_id, d.new_batch_id, d.change;

CREATE OR REPLACE VIEW ops.v_recovery_times AS
SELECT failure_class, count(*) AS incidents,
       count(*) FILTER (WHERE verified AND NOT escalated) AS recovered,
       count(*) FILTER (WHERE escalated) AS escalated,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY time_to_verified_recovery_s)
         AS median_ttr_s
FROM ops.incident GROUP BY failure_class;
