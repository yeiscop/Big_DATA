Act as a senior data engineer. We are building a credit-risk data pipeline in Databricks Free Edition using PySpark, Delta Lake, and Unity Catalog.

We must implement Bronze, Silver, and Gold layers using the Kaggle Credit Risk Dataset.

Follow Spec-Driven Development (SDD):
1. Inspect the actual source schema and identify assumptions.
2. Write a technical specification for each layer.
3. Define data-quality rules, edge cases, acceptance criteria, and test cases.
4. Define the deterministic risk-segmentation rule supplied by the instructor.
5. Do not generate implementation code until you have presented the specification for review.

The path load my file is: /Volumes/workspace/bronze/landing_zone/credit_risk_dataset.csv
Preserve source data in Bronze (Make EDA, feature selection and proportional analisys with LOANS), clean and standardize data in Silver, and create the risk_segment column in Gold.

Do not invent columns or business rules. Clearly identify anything that needs clarification.