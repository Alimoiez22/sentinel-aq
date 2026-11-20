-- v_reject_rates v2: the percentage in 006 divided by the rows of AFFECTED batches only,
-- which read like a share of all data (e.g. "Q00 60.99 %"). Both denominators are now
-- explicit. (006 is applied and hash-guarded, so the change is a new migration.)
DROP VIEW IF EXISTS ops.v_reject_rates;
CREATE VIEW ops.v_reject_rates AS
WITH totals AS (SELECT sum(raw_row_count) AS all_rows FROM ops.batch_manifest
                WHERE status <> 'rejected')
SELECT q.rule_id, q.action,
       count(DISTINCT q.batch_id) AS batches_hit,
       sum(q.n_rows) AS rows_hit,
       round(100.0 * sum(q.n_rows) / nullif((SELECT all_rows FROM totals), 0), 2)
         AS pct_of_all_rows,
       round(100.0 * sum(q.n_rows) / nullif(sum(b.raw_row_count), 0), 2)
         AS pct_of_rows_in_batches_hit
FROM ops.quality_stats q JOIN ops.batch_manifest b USING (batch_id)
WHERE q.n_rows > 0
GROUP BY q.rule_id, q.action;
