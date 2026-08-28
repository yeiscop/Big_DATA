-- Silver Layer: Clean data and apply debt penalization logic
CREATE OR REFRESH MATERIALIZED VIEW silver_credit_risk
COMMENT "Silver layer - Cleaned data with debt penalty flag for amounts > 200"
AS
SELECT 
  *,
  CASE 
    WHEN loan_amnt > 200 THEN 1
    ELSE 0
  END AS high_debt_penalty
FROM bronze_credit_risk
WHERE person_age IS NOT NULL;
