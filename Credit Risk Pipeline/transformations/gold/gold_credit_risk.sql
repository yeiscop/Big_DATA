-- Gold Layer: Aggregated credit risk metrics
CREATE OR REFRESH MATERIALIZED VIEW gold_credit_risk
COMMENT "Gold layer - Aggregated credit risk metrics by loan grade and penalty status"
AS
SELECT 
  loan_grade,
  high_debt_penalty,
  COUNT(*) AS total_applications,
  AVG(person_age) AS avg_age,
  AVG(loan_amnt) AS avg_loan_amount,
  AVG(loan_int_rate) AS avg_interest_rate,
  SUM(CASE WHEN loan_status = 1 THEN 1 ELSE 0 END) AS default_count,
  ROUND(SUM(CASE WHEN loan_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_pct
FROM silver_credit_risk
GROUP BY ALL;
