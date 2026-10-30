# Beginner Vibe Coding Exercise: Credit Risk Data Pipeline

**Level:** Beginner  
**Platform:** Databricks Free Edition  
**Tools:** PySpark, Delta Lake, Unity Catalog, and Databricks Genie (if available)  
**Dataset:** [Kaggle Credit Risk Dataset](https://www.kaggle.com/datasets/laotse/credit-risk-dataset/data)

## 1. Goal

Use an AI coding assistant to help build a simple data pipeline for loan application data. You will describe what you want in everyday language, ask AI to generate code, run the code, inspect the results, and improve it when needed.

The pipeline has three layers:

- **Bronze — Raw data:** preserve the original source data.
- **Silver — Clean data:** apply the cleaning rules agreed with the instructor.
- **Gold — Analysis-ready data:** create a risk category using the instructor's rule.

You will also explore the data (EDA), identify useful columns, and analyze loan counts and percentages.

> **Important:** This is a beginner learning exercise. The risk categories are a simple classroom rule, not a validated credit-risk prediction model.

## 2. What is Vibe Coding?

For this exercise, Vibe Coding means describing a task in natural language and using an AI assistant to generate and improve code through repeated attempts.

A simple cycle is:

1. Describe the task.
2. Ask AI to generate code.
3. Read the explanation and run the code.
4. Inspect the output or error.
5. Ask AI to fix or improve the code.
6. Check that the result is correct.

Even when AI generates the code, **you are responsible for checking it**. Do not assume code is correct just because it runs.

## 3. Prepare the CSV file

Download the dataset from Kaggle and upload the CSV file to a Unity Catalog Volume that your Databricks workspace can access.

Use a path template like this:

```text
/Volumes/<catalog_name>/<schema_name>/<volume_name>/credit_risk_dataset.csv
```

Replace the placeholders with the real names in your workspace. This is only a template; do not assume that this path already exists.

Before coding, confirm:

- [ ] The CSV file is downloaded.
- [ ] The file is uploaded to a Unity Catalog Volume.
- [ ] The exact path and filename are known.
- [ ] The notebook can access the file.

## 4. Rules for working with AI

Use the following rules throughout the exercise:

- Give AI one small task at a time.
- Tell AI to inspect the actual schema instead of guessing column names.
- Ask AI to explain generated code in simple language.
- Run the code and inspect the result after each step.
- If an error appears, share the full error message with AI and ask for a correction.
- Ask AI to explain what changed and why.
- Do not allow AI to invent business rules or silently remove data.
- Do not claim a test passed unless you ran it and checked the result.

Unlike the SDD group, you do not need to complete a full specification before starting to code. However, you must still use the same instructor-provided requirements and risk rule as the SDD group.

## 5. Prompt 1 — Inspect the dataset

Paste this prompt into your AI coding assistant in Databricks:

> Act as a patient senior data engineer helping a beginner.
>
> I am using Databricks Free Edition and PySpark. I want to analyze the Kaggle Credit Risk Dataset.
>
> My CSV path is: `PASTE_YOUR_REAL_SOURCE_PATH_HERE`
>
> First, help me inspect the file. Generate only the small amount of code needed to read the CSV and display:
>
> 1. The number of rows and columns.
> 2. The actual column names and data types.
> 3. Five sample rows.
> 4. The number of missing values in each column.
> 5. A short explanation of what we learned.
>
> Do not assume that any column exists before checking the actual schema. Explain the code in beginner-friendly language. Do not build the full pipeline yet.

Run the code and save the schema and profiling results.

## 6. Prompt 2 — Build the Bronze layer

> Help me build the Bronze layer in Databricks using PySpark and Delta Lake.
>
> Use the actual CSV path and schema that I inspected. Preserve the original source columns and values as much as possible. Add simple ingestion metadata, such as source filename and ingestion timestamp, without replacing the original fields.
>
> Save the result as a Delta table registered in Unity Catalog. Ask me for the catalog, schema, and table names if I have not provided them.
>
> Before writing the table, explain the steps in simple language. Then provide the code for this step only.
>
> After I run it, help me verify the source row count, Bronze row count, and original columns. Do not claim that the checks passed until I share the results.

## 7. Prompt 3 — Build the Silver layer

> Help me create the Silver layer from the Bronze table using PySpark.
>
> First, summarize the cleaning rules that the instructor has approved. Do not invent extra rules.
>
> Apply only the approved rules. These may include handling missing values, checking ages greater than 100 if an age column exists, and standardizing employment length if that column exists.
>
> Before coding, tell me if any decision is still missing—for example, whether a zero interest rate is valid or what to do with missing age. Ask me to clarify instead of guessing.
>
> Generate beginner-friendly code for the Silver layer only. Explain each transformation.
>
> After I run the code, help me compare row counts before and after cleaning and check whether the approved rules were applied.

## 8. Prompt 4 — Create the Gold layer

> Help me create the Gold layer from the Silver table using PySpark and Delta Lake.
>
> Create a column named `risk_segment` using exactly this instructor-provided rule:
>
> `PASTE_THE_INSTRUCTOR_APPROVED_RISK_RULE_HERE`
>
> If I have not supplied the rule, stop and ask me for it. Do not invent the rule or its thresholds.
>
> Ask for the Gold table name if it is missing. Explain the logic in simple language, then generate code for the Gold layer only.
>
> Include a rule-version field if the instructor requires it. After I run the code, help me check the segment counts and verify sample records against the rule.
>
> Do not describe this simple rule as a validated predictive model.

## 9. Prompt 5 — Perform exploratory data analysis (EDA)

> Help me perform beginner-friendly exploratory data analysis on the cleaned Silver table.
>
> Use only columns that exist in the actual dataset. Generate code in small sections to show:
>
> 1. Row and column counts.
> 2. Missing values per column.
> 3. Basic numerical statistics, including mean, median, minimum, and maximum where appropriate.
> 4. Counts for important categorical columns.
> 5. A histogram of loan amount if that column exists.
> 6. A histogram of interest rate if that column exists.
> 7. Historical loan-status counts if that field exists and its meaning is confirmed.
>
> Explain each result in simple language. Do not claim a relationship is meaningful just because two variables appear correlated.

Run each section and inspect the output before moving to the next one.

## 10. Prompt 6 — Identify useful features

> Help me identify useful columns for understanding loan applications.
>
> Inspect the actual Silver schema and create a simple table with:
>
> - Column name.
> - Meaning, if known from the dataset documentation.
> - Missing-value or data-quality concerns.
> - Why the column might be useful.
> - Whether to include it, exclude it, or investigate it further.
>
> Do not invent meanings or claim that a feature predicts default without evidence.
>
> If we later build a model to predict historical `loan_status`, warn me about target leakage. The outcome being predicted must not be used as an input feature for that same prediction.
>
> Keep the explanation suitable for beginners.

## 11. Prompt 7 — Analyze loan counts and proportions

> Help me calculate loan counts and percentages from the Silver and Gold tables using only confirmed columns.
>
> Where the required columns exist, calculate:
>
> 1. Number and percentage of records in each historical loan-status category.
> 2. Historical default proportion, only if the meaning of the status field is confirmed.
> 3. Average and median loan amount.
> 4. Number and percentage of records in each Gold risk segment.
>
> Use this formula:
>
> \[
> \text{Percentage}=\frac{\text{Count in category}}{\text{Total eligible records}}\times100\%
> \]
>
> State the denominator for each percentage. Help me verify that the category counts sum to the total number of eligible records. Explain that historical proportions do not guarantee future outcomes.

## 12. Prompt 8 — Test and improve the pipeline

> Help me test the pipeline we have built so far.
>
> Create simple checks for:
>
> - Source and Bronze row-count comparison.
> - Expected columns in each layer.
> - Approved Silver cleaning rules.
> - Missing values that should have been handled.
> - Gold risk categories following the instructor's rule.
> - Counts and percentages reconciling with their denominators.
>
> Generate tests in small steps. Explain what each test checks and what a failure means.
>
> Do not claim that a test passed until I run it and share the result. If a test fails, help me diagnose the cause and make the smallest appropriate correction.

## 13. Optional — Ask Genie business questions

If AI/BI Genie is available in the workspace and the Gold table is ready, ask questions such as:

- How many applications are in each risk segment?
- What percentage of applications belongs to each segment?
- What is the average loan amount by segment?
- What is the historical loan-status distribution?

Check answers against a separate SQL query or calculation. If AI/BI Genie is unavailable, perform the analysis in the notebook.

**Note:** Genie Code helps with coding; AI/BI Genie is designed for asking questions about data. They are different capabilities.

## 14. Final checklist

- [ ] The real CSV path was used.
- [ ] The actual source schema was inspected.
- [ ] Bronze preserves the source data according to the agreed policy.
- [ ] Silver follows only the approved cleaning rules.
- [ ] Gold uses the instructor's exact risk rule.
- [ ] EDA tables and charts were reviewed.
- [ ] Feature choices were explained.
- [ ] Loan counts and percentages were checked.
- [ ] Tests were executed and results recorded.
- [ ] AI-generated code was reviewed and understood.
- [ ] Assumptions and limitations were documented.

## 15. What to submit

1. Notebook containing the code and outputs.
2. Bronze, Silver, and Gold tables.
3. Two or three EDA charts with short explanations.
4. Feature-selection table.
5. Loan-status and risk-segment counts and percentages, where applicable.
6. Test results.
7. A short reflection answering:
   - What did AI help you do?
   - What errors or incorrect assumptions did you find?
   - How did you verify the results?
   - What would you improve next time?

## Remember

Vibe Coding makes it easy to start generating code quickly. It does **not** remove the need to understand requirements, inspect results, test the code, or verify business rules. Use the same dataset, requirements, risk rule, and acceptance tests as the SDD group so that the class can compare the two development approaches fairly.
