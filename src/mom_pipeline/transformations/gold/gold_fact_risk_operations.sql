CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.gold_fact_risk_operations
AS
SELECT
  CAST(ro.created_at AS DATE) AS operation_date,
  ro.operation_type,
  COUNT(*) AS operation_count,
  COUNT(CASE WHEN ro.operation_type = 'dispute' THEN 1 END) AS dispute_count,
  SUM(COALESCE(ro.amount, 0)) AS total_disputed_amount,
  AVG(COALESCE(ro.amount, 0)) AS avg_disputed_amount
FROM ${silver_schema}.silver_risk_operations ro
GROUP BY CAST(ro.created_at AS DATE), ro.operation_type;
