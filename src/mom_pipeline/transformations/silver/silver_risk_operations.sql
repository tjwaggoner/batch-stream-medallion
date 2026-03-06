CREATE OR REFRESH MATERIALIZED VIEW ${silver_schema}.silver_risk_operations
AS
WITH disputes AS (
  SELECT
    dispute_id AS operation_id,
    'dispute' AS operation_type,
    account_id,
    amount,
    status,
    reason AS detail,
    created_at
  FROM ${bronze_schema}.bronze_dispute_records
),
dispute_changes AS (
  SELECT
    CONCAT('dsc_', dispute_id, '_', CAST(change_ts AS STRING)) AS operation_id,
    'dispute_status_change' AS operation_type,
    NULL AS account_id,
    NULL AS amount,
    new_status AS status,
    CONCAT(old_status, ' -> ', new_status) AS detail,
    change_ts AS created_at
  FROM ${bronze_schema}.bronze_dispute_status_changes
),
cases AS (
  SELECT
    case_id AS operation_id,
    'case' AS operation_type,
    account_id,
    NULL AS amount,
    status,
    outcome AS detail,
    created_at
  FROM ${bronze_schema}.bronze_case_records
),
risk_ops AS (
  SELECT
    op_id AS operation_id,
    op_type AS operation_type,
    NULL AS account_id,
    amount,
    decision AS status,
    CONCAT('risk_score: ', CAST(risk_score AS STRING)) AS detail,
    op_date AS created_at
  FROM ${bronze_schema}.bronze_risk_operations
),
rules AS (
  SELECT
    CONCAT('rule_', rule_id, '_', CAST(evaluation_date AS STRING)) AS operation_id,
    'rule_evaluation' AS operation_type,
    NULL AS account_id,
    NULL AS amount,
    CASE WHEN false_positive_rate > 0.1 THEN 'review' ELSE 'active' END AS status,
    CONCAT(rule_name, ' | FPR: ', CAST(false_positive_rate AS STRING)) AS detail,
    CAST(evaluation_date AS TIMESTAMP) AS created_at
  FROM ${bronze_schema}.bronze_rule_performance
)
SELECT * FROM disputes
UNION ALL SELECT * FROM dispute_changes
UNION ALL SELECT * FROM cases
UNION ALL SELECT * FROM risk_ops
UNION ALL SELECT * FROM rules;
