# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Build: Module 3 - Execution: Bronze Layer Ingestion
# MAGIC 
# MAGIC This notebook ingests raw data from `samples.bakehouse` into `lakehouse_build.bronze` as append-only Delta tables with standard audit metadata columns:
# MAGIC - `sales_transactions`
# MAGIC - `sales_customers`
# MAGIC - `sales_franchises`
# MAGIC - `sales_suppliers`
# MAGIC 
# MAGIC **Catalog:** `lakehouse_build`  
# MAGIC **Schema:** `bronze`  
# MAGIC **Standards:** `.agents/skills/pyspark_delta/SKILL.md`

# COMMAND ----------

import sys
import os

# Add includes and repo root to sys.path
for p in [os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), "..")), os.path.abspath(os.path.join(os.getcwd(), "includes"))]:
    if p not in sys.path:
        sys.path.append(p)

try:
    from notebooks.includes.configuration import config
    from notebooks.includes.common_functions import add_bronze_audit_columns
except ImportError:
    from includes.configuration import config
    from includes.common_functions import add_bronze_audit_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ensure Target Catalog and Schema Exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.TARGET_CATALOG}.{config.BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Execute Bronze Ingestion

# COMMAND ----------

BRONZE_TABLES_MAP = {
    config.SRC_TRANSACTIONS: config.BRONZE_TRANSACTIONS,
    config.SRC_CUSTOMERS: config.BRONZE_CUSTOMERS,
    config.SRC_FRANCHISES: config.BRONZE_FRANCHISES,
    config.SRC_SUPPLIERS: config.BRONZE_SUPPLIERS
}

results = {}
for src_name, tgt_name in BRONZE_TABLES_MAP.items():
    src_table = config.get_source_table(src_name)
    tgt_table = config.get_bronze_table(tgt_name)
    
    print(f"[BRONZE INGEST] Ingesting from {src_table} -> {tgt_table}...")
    raw_df = spark.table(src_table)
    
    audited_df = add_bronze_audit_columns(
        df=raw_df,
        source_identifier=src_table,
        batch_id="orchestrated_batch"
    )
    
    row_count = audited_df.count()
    (
        audited_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(tgt_table)
    )
    results[tgt_name] = row_count
    print(f"[BRONZE INGEST] Successfully loaded {tgt_table} ({row_count} rows).\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Ingestion Summary

# COMMAND ----------

for table_name, count in results.items():
    print(f"Table: {table_name:25} | Rows: {count:,}")
