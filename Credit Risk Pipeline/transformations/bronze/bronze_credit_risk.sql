-- Bronze Layer: Ingest credit risk data with age filter
CREATE OR REFRESH MATERIALIZED VIEW bronze_credit_risk
COMMENT "Bronze layer - Credit risk data filtered for adults (age > 18)"
AS
SELECT *
FROM workspace.bronze.credit_risk_raw
WHERE person_age > 18;