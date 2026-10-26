---
name: pyspark_delta
description: Engineering standards, optimization patterns, and best practices for developing PySpark and Delta Lake pipelines.
---

# PySpark and Delta Lake Skill

This skill provides practical standards, idiomatic patterns, and performance guidelines for writing PySpark and Delta Lake pipelines within the Execution phase of the Lakehouse Build methodology.

---

## When to Use This Skill

Activate this skill when:
- Developing pipeline scripts and notebooks in the Execution phase (`03_execution/`).
- Building reusable ingestion and transformation functions in `notebooks/includes/`.
- Implementing Delta Lake table writes, merges, time-travel queries, and schema evolution.
- Optimizing PySpark transformations, joins, and memory utilization on Databricks Free Edition.
- Enforcing idempotent batch or streaming pipeline runs.

> **Methodology Phase Gate:** Ingestion pipelines and transformation notebooks must **not** be developed until the Assessment phase has been completed, documented, and approved.

---

## Step-by-Step Engineering Guidelines

### 1. Delta Table Writes & Idempotency
- **Idempotent Ingestion:** Ensure that re-running an ingestion job against the same source data does not duplicate records or create inconsistent state.
- **Merge Logic (Upserts):** Always use explicit match and insert conditions:
  ```python
  from delta.tables import DeltaTable

  target_table = DeltaTable.forName(spark, "silver.orders")
  target_table.alias("tgt").merge(
      source_df.alias("src"),
      "tgt.order_id = src.order_id"
  ).whenMatchedUpdate(set={
      "customer_id": "src.customer_id",
      "order_status": "src.order_status",
      "_updated_at": "current_timestamp()"
  }).whenNotMatchedInsert(values={
      "order_id": "src.order_id",
      "customer_id": "src.customer_id",
      "order_status": "src.order_status",
      "_ingested_at": "current_timestamp()",
      "_updated_at": "current_timestamp()"
  }).execute()
  ```
- **Schema Enforcement:** Rely on Delta Lake's native schema validation. Never pass `.option("mergeSchema", "true")` without documented architectural approval.

### 2. PySpark Performance & Memory Standards
- **Explicit Schema Declaration:** Always provide an explicit `StructType` schema when reading raw files; never rely on distributed schema inference.
- **Avoid Driver Bottlenecks:**
  - Never invoke `.collect()` or `.toPandas()` on unaggregated or large DataFrames.
  - Avoid Python UDFs when standard `pyspark.sql.functions` are available.
  - Avoid Python-side loops over DataFrame rows.
- **Join Best Practices:**
  - Broadcast lookup/dimension tables under 10 MB: `from pyspark.sql.functions import broadcast; df.join(broadcast(dim_df), ...)`.
  - Ensure join key data types match exactly across joined DataFrames.

### 3. Delta Table Maintenance & Sizing
- **Table Optimization Properties:**
  ```sql
  ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
  );
  ```
- **Compaction:** Run `OPTIMIZE <table>` after large batch loads or frequent small-file merges.

---

## How to Validate Results

Validate PySpark and Delta implementations with these steps:
1. **Idempotency Test:** Execute the pipeline script twice consecutively with the exact same input batch. Verify that row counts, keys, and table checksums remain identical.
2. **Schema & Null Assertion:** Verify that target tables conform to the expected schema without unexpected nulls in required columns.
3. **Execution Resource Check:** Ensure jobs run within the single-node compute limits of Databricks Free Edition without out-of-memory (OOM) or spill-to-disk warnings.
4. **Audit Trail Verification:** Query the Delta table history (`DESCRIBE HISTORY <table>`) to confirm that transaction operations, operation metrics, and isolation levels are properly recorded.
