---
name: data_quality_validation
description: Framework, procedures, and quality criteria for validating data integrity, enforcing constraints, and executing tests across the Lakehouse.
---

# Data Quality Validation Skill

This skill defines the framework, validation procedures, quality dimensions, and result verification methods for enforcing data integrity across all Lakehouse layers.

---

## When to Use This Skill

Activate this skill when:
- Establishing data quality requirements during Assessment Sub-stage 3 (`03_governance_readiness_and_design_decisions`).
- Verifying source table integrity during Assessment Sub-stage 2 (`02_workload_and_source_profiling`).
- Building automated validation gates between medallion layers (Bronze to Silver, Silver to Gold).
- Writing and executing automated test suites in `03_execution/04_testing_and_validation/`.
- Conducting post-load reconciliation (row counts, primary key integrity, referential checks).

---

## Data Quality Dimensions & Assertion Patterns

### 1. Completeness & Nullability
- **Objective:** Verify that mandatory business keys and critical metrics contain no `NULL` or blank values.
- **Assertion:**
  ```python
  def assert_no_nulls(df, column_name):
      null_count = df.filter(df[column_name].isNull()).count()
      assert null_count == 0, f"Integrity Failure: Found {null_count} nulls in '{column_name}'."
  ```

### 2. Uniqueness & Primary Key Integrity
- **Objective:** Ensure that table records are unique along their designated primary key.
- **Assertion:**
  ```python
  def assert_unique_key(df, key_column):
      total_rows = df.count()
      distinct_rows = df.select(key_column).distinct().count()
      assert total_rows == distinct_rows, (
          f"Integrity Failure: Primary key '{key_column}' contains "
          f"{total_rows - distinct_rows} duplicate values."
      )
  ```

### 3. Referential Integrity (Foreign Key Verification)
- **Objective:** Ensure child records have matching parent keys without orphaned rows.
- **Assertion:**
  ```python
  def assert_referential_integrity(child_df, child_fk, parent_df, parent_pk):
      orphans = child_df.join(
          parent_df,
          child_df[child_fk] == parent_df[parent_pk],
          "left_anti"
      ).count()
      assert orphans == 0, f"Integrity Failure: Found {orphans} orphaned records in '{child_fk}'."
  ```

### 4. Validity & Domain Range
- **Objective:** Ensure dates are not in the future, prices/quantities are positive, and categorical status values match allowed sets.
- **Assertion:**
  ```python
  from pyspark.sql.functions import col, current_date

  def assert_valid_dates(df, date_column):
      future_dates = df.filter(col(date_column) > current_date()).count()
      assert future_dates == 0, f"Validity Failure: Found {future_dates} records with future dates."
  ```

### 5. Reconciliation & Row Count Parity
- **Objective:** Verify that source batch records are accounted for in downstream layers after filtering and deduplication.

---

## Layer-by-Layer Quality Gates

| Layer | Validation Focus | Action on Failure |
| :--- | :--- | :--- |
| **Bronze** | Schema conformity, decodable payload, non-empty batches. | Log warning, abort batch or route raw record to landing quarantine. |
| **Silver** | Primary key uniqueness, mandatory non-nulls, valid data types. | Route non-conforming records to quarantine table; alert on threshold breach. |
| **Gold** | Referential integrity, metric bounds, aggregation reconciliation. | Block downstream consumption / dashboard refresh until investigated. |

---

## How to Validate Results

Validate data quality test suites with these steps:
1. **Assertion Execution Check:** Run the validation suite against known valid data to confirm a 100% pass rate.
2. **Failure Detection Check:** Run test assertions against intentionally corrupted sample records to ensure assertions correctly trigger and fail.
3. **Quarantine Routing Check:** Verify that records failing Silver validation rules are cleanly isolated into the quarantine table with failure metadata (`_failed_rule`, `_quarantine_timestamp`).
4. **Deliverable Check:** Verify test results, execution logs, and reconciliation summaries are documented under `03_execution/04_testing_and_validation/`.
