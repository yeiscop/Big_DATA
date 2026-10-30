# Beginner SDD Exercise: Credit Risk Data Pipeline

**Level:** Beginner  
**Platform:** Databricks Free Edition  
**Tools:** PySpark, Delta Lake, Unity Catalog, and Databricks Genie (if available)  
**Dataset:** [Kaggle Credit Risk Dataset](https://www.kaggle.com/datasets/laotse/credit-risk-dataset/data)

## 1. Goal

Plan a simple data pipeline that takes loan application data and prepares it for analysis.

We will use three layers:

- **Bronze — Raw data:** keep the original data.
- **Silver — Clean data:** fix agreed data-quality issues.
- **Gold — Analysis-ready data:** add a simple risk category using the instructor's rule.

We will also explore the data (EDA), look at loan distributions, and identify useful columns.

> **Important:** This is a beginner learning exercise. The risk categories are a simple classroom rule, not a validated credit-risk prediction model.

## 2. What is Spec-Driven Development (SDD)?

Spec-Driven Development means **deciding what the program should do before asking AI to write the code**.

Follow these steps:

1. **Look at the data.**
2. **Write down what the pipeline must do.**
3. **Check the plan with the instructor.**
4. **Ask AI to write the code only after approval.**
5. **Run the code and check the results.**
6. **Fix problems and document what you learned.**

**Do not generate implementation code until the instructor or team has approved the specification.** During the first step, AI may help inspect and describe the source data, but should not build the pipeline yet.

## 3. Find the CSV file

Download the dataset from Kaggle and upload the CSV file to a Unity Catalog Volume that your Databricks workspace can access.

Use a configurable path like this:

```text
/Volumes/workspace/bronze/landing_zone/credit_risk_dataset.csv
```

Replace the placeholders with the real names from your workspace. This is an example template, not a guaranteed existing path.

Before continuing, confirm:

- [ ] The CSV file has been downloaded.
- [ ] The file has been uploaded to a Unity Catalog Volume.
- [ ] The exact path and filename are known.
- [ ] The notebook has permission to read the file.

If the file is not available, ask the instructor for help. Do not guess the path.

## 4. Step 1 — Discover the data

Before designing the pipeline, inspect the CSV file.

Find out:

- How many rows and columns are there?
- What are the actual column names?
- What data type does each column appear to have?
- Are there missing values?
- Are there duplicate rows?
- What do a few sample records look like?

Do not assume that a column exists just because it appears in an example. Confirm the real schema first.

**Deliverable:** A short data overview with the row count, column names, data types, missing values, and any questions.

## 5. Step 2 — Write the simple pipeline specification

### Bronze: keep the original data

**What it should do**

- Read the CSV using the agreed source path.
- Keep the original columns and values.
- Add simple metadata, such as the source filename and ingestion time, if appropriate.
- Save the data as a Delta table in Unity Catalog.

**How we will check it**

- The file can be read.
- The original columns are present.
- The Bronze row count matches the number of data rows in the CSV, accounting for any documented parsing errors.

### Silver: clean the data

**What it should do**

Apply only the cleaning rules approved by the instructor. The initial exercise may include:

- Check missing values.
- If the source has an age column, identify records where age is greater than 100 and apply the instructor-approved rule.
- If the source has an employment-length column, standardize its values.
- If the source has an interest-rate column, agree on how to handle missing values.

**Questions to answer before coding**

- Should rows with missing age be kept or removed?
- Should a zero interest rate be treated as valid?
- How should unexpected values be handled?
- Should duplicate rows be removed or only reported?

Do not make these decisions silently. Record the agreed answers.

**How we will check it**

- The approved cleaning rules have been applied.
- Missing values are handled as agreed.
- The number of rows before and after cleaning is recorded.

### Gold: create a risk category

**What it should do**

Create a column named `risk_segment` using the exact rule provided by the instructor.

If the instructor has not provided the rule, ask for it. Do not invent one.

Record the categories, the meaning of each category, and how records with missing required values are handled.

**How we will check it**

- Each row follows the approved rule.
- Only the approved category labels appear, except for any explicitly allowed `Unclassified` category.
- The counts and percentages for all categories add up to the total number of Gold records.

## 6. Step 3 — Explore the data (EDA)

EDA means **Exploratory Data Analysis**: looking at the data to understand it before drawing conclusions.

Start with these simple analyses:

1. Count the rows and columns.
2. Count missing values in each column.
3. Calculate basic statistics for numerical columns, such as average, median, minimum, and maximum.
4. Count values in categorical columns.
5. Plot a histogram of loan amounts, if that field exists.
6. Plot a histogram of interest rates, if that field exists.
7. Compare historical loan-status counts, if the field exists and its meaning is confirmed.

**Deliverable:** A short report with a few tables, two or three charts, and observations supported by the data.

## 7. Step 4 — Identify useful features

A **feature** is a column that may help us understand or analyze loan applications.

First, confirm which columns actually exist. Then create a simple table:

| Column | What does it mean? | Is it useful for our analysis? | Why? |
|---|---|---|---|
| Actual column name | Confirm from the dataset | Yes / No / Unsure | Brief explanation |

Consider data quality, missing values, and whether a column is relevant to the question.

If you later build a model to predict historical `loan_status`, do not use that same outcome as an input feature. This would leak the answer into the model. For this beginner exercise, focus on understanding the columns rather than building a predictive model.

## 8. Step 5 — Analyze loan proportions

Use counts and percentages to describe the dataset. Only use fields confirmed in the source data.

For a category \(k\):

\[
\text{Percentage}_k =
\frac{\text{Number of records in category } k}
{\text{Total eligible records}} \times 100
\]

Calculate, where the required columns are available:

- Number and percentage of applications in each historical loan-status category.
- Historical default proportion, if the status meaning is confirmed.
- Loan amount average and median.
- Number and percentage of applications in each Gold risk category.

Always state the denominator used. A historical default proportion describes the dataset being analyzed; it does not guarantee the default rate of future loans.

## 9. Step 6 — Review and approve the specification

Before asking AI to implement the pipeline, make sure the team and instructor agree on the following:

- [ ] The actual CSV path is known.
- [ ] The source schema has been inspected.
- [ ] The Bronze requirements are clear.
- [ ] The Silver cleaning rules are agreed.
- [ ] The EDA questions and charts are selected.
- [ ] The candidate features have been documented.
- [ ] The loan proportion calculations are defined.
- [ ] The instructor's risk-segmentation rule is available and understood.
- [ ] The acceptance checks are clear.
- [ ] Open questions have been answered or explicitly left unresolved.

**Approval gate:** Stop here. Do not ask AI to generate the pipeline implementation until the specification has been reviewed and approved.

## 10. Step 7 — Implement and test after approval

Only after approval, ask AI to implement one layer at a time:

1. Bronze ingestion.
2. Silver cleaning.
3. Gold segmentation.
4. EDA and charts.
5. Loan proportion analysis.
6. Validation checks.

Run each step and inspect the output before moving on.

Ask AI to explain unfamiliar code in simple language. Do not assume that generated code works just because it looks correct.

### Basic acceptance checklist

- [ ] Bronze contains the expected source records and columns.
- [ ] Silver follows the approved cleaning rules.
- [ ] Gold uses the approved risk rule.
- [ ] Counts and percentages can be independently checked.
- [ ] Charts use the correct data layer.
- [ ] Results are reproducible when the same source and rules are used.
- [ ] Tests are marked as passed only after they have actually been run.

## 11. Optional: Ask Genie business questions

If AI/BI Genie is available in your workspace and the Gold table is ready, try questions such as:

- How many applications are in each risk segment?
- What percentage of applications belongs to each segment?
- What is the average loan amount by segment?
- What is the historical loan-status distribution?

Check the answers against a separate query or calculation. Genie may not be available in every workspace, so this part can be replaced with instructor-provided SQL or notebook analysis.

**Note:** Genie Code helps with coding; AI/BI Genie is designed for asking questions about data. They are different capabilities.

## 12. What to submit

1. The approved short specification.
2. The source-data overview.
3. Bronze, Silver, and Gold tables.
4. Two or three EDA charts with brief explanations.
5. A feature table with reasons for selecting candidate columns.
6. Loan-status and risk-segment counts and percentages, where applicable.
7. Test results.
8. A short reflection: What did the AI do well? What did the team need to verify or correct?

## 13. Important reminders

- Do not invent column names, file paths, or business rules.
- Ask the instructor when a decision is unclear.
- Keep the first version simple.
- Verify AI-generated code and answers.
- Document assumptions and limitations.
- A simple risk-segmentation rule is not the same as a validated credit-risk model.
