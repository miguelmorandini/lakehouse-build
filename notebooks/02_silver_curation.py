# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Build: Module 3 - Execution: Silver Layer Curation
# MAGIC 
# MAGIC This notebook transforms, cleanses, and conforms raw Bronze tables into enterprise Silver Delta tables:
# MAGIC 1. `dim_suppliers`: 27 raw suppliers + conformed unknown member (`supplier_key = -1`).
# MAGIC 2. `dim_franchises`: 48 franchises linked to `supplier_key` with fallback `-1`.
# MAGIC 3. `dim_customers`: 300 conformed customer master records (snake_case).
# MAGIC 4. `fct_sales_transactions`: 3,333 sales with `cardNumber` strictly dropped, `transaction_date` derived, and Liquid Clustering enabled.
# MAGIC 
# MAGIC **Catalog:** `lakehouse_build`  
# MAGIC **Schema:** `silver`  
# MAGIC **Standards:** `.agents/skills/lakehouse_design/SKILL.md`

# COMMAND ----------

import sys
import os

for p in [os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), "..")), os.path.abspath(os.path.join(os.getcwd(), "includes"))]:
    if p not in sys.path:
        sys.path.append(p)

from pyspark.sql import functions as F

try:
    from notebooks.includes.configuration import config
    from notebooks.includes.common_functions import (
        sanitize_silver_transactions,
        build_unknown_supplier_df
    )
except ImportError:
    from includes.configuration import config
    from includes.common_functions import (
        sanitize_silver_transactions,
        build_unknown_supplier_df
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ensure Silver Schema Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.TARGET_CATALOG}.{config.SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Curate `silver.dim_suppliers` (Conformed Unknown Member: -1)

# COMMAND ----------

bronze_sup = spark.table(config.get_bronze_table(config.BRONZE_SUPPLIERS))
unknown_df = build_unknown_supplier_df(spark)

curated_sup = (
    bronze_sup
    .select(
        F.col("supplierID").cast("long").alias("supplier_key"),
        F.col("supplierID").cast("long").alias("supplier_id"),
        F.col("name").alias("supplier_name"),
        F.col("ingredient"),
        F.col("continent"),
        F.col("city"),
        F.col("district"),
        F.col("size").alias("supplier_size"),
        F.col("longitude").cast("double"),
        F.col("latitude").cast("double"),
        F.col("approved"),
        F.lit("BRONZE_SOURCE").alias("_record_source")
    )
    .unionByName(unknown_df)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_updated_at", F.current_timestamp())
)

curated_sup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.get_silver_table(config.SILVER_DIM_SUPPLIERS))
print(f"Loaded {curated_sup.count()} rows into {config.get_silver_table(config.SILVER_DIM_SUPPLIERS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Curate `silver.dim_franchises` (FK Fallback -1)

# COMMAND ----------

f_df = spark.table(config.get_bronze_table(config.BRONZE_FRANCHISES)).alias("f")
s_df = spark.table(config.get_silver_table(config.SILVER_DIM_SUPPLIERS)).alias("s")

curated_fran = (
    f_df.join(
        s_df,
        F.col("f.supplierID") == F.col("s.supplier_id"),
        "left"
    )
    .select(
        F.col("f.franchiseID").cast("long").alias("franchise_id"),
        F.col("f.name").alias("franchise_name"),
        F.col("f.city"),
        F.col("f.district"),
        F.col("f.zipcode"),
        F.col("f.country"),
        F.col("f.size").alias("franchise_size"),
        F.col("f.longitude").cast("double"),
        F.col("f.latitude").cast("double"),
        F.coalesce(F.col("s.supplier_key"), F.lit(config.UNKNOWN_SUPPLIER_KEY)).alias("supplier_key"),
        F.current_timestamp().alias("_ingested_at"),
        F.current_timestamp().alias("_updated_at")
    )
)

curated_fran.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.get_silver_table(config.SILVER_DIM_FRANCHISES))
print(f"Loaded {curated_fran.count()} rows into {config.get_silver_table(config.SILVER_DIM_FRANCHISES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Curate `silver.dim_customers` (Standard Snake Case)

# COMMAND ----------

bronze_cust = spark.table(config.get_bronze_table(config.BRONZE_CUSTOMERS))

curated_cust = (
    bronze_cust
    .select(
        F.col("customerID").cast("long").alias("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email_address"),
        F.col("phone_number"),
        F.col("address"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("continent"),
        F.col("postal_zip_code").cast("long"),
        F.col("gender"),
        F.current_timestamp().alias("_ingested_at"),
        F.current_timestamp().alias("_updated_at")
    )
)

curated_cust.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.get_silver_table(config.SILVER_DIM_CUSTOMERS))
print(f"Loaded {curated_cust.count()} rows into {config.get_silver_table(config.SILVER_DIM_CUSTOMERS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Curate `silver.fct_sales_transactions` (PCI-DSS Sanitized & Clustered)

# COMMAND ----------

bronze_tx = spark.table(config.get_bronze_table(config.BRONZE_TRANSACTIONS))
sanitized_tx = sanitize_silver_transactions(
    df=bronze_tx,
    prohibited_columns=config.PROHIBITED_PCI_COLUMNS
)

(
    sanitized_tx.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy(*config.CLUSTERING_KEYS_SILVER_SALES)
    .saveAsTable(config.get_silver_table(config.SILVER_FCT_SALES))
)
print(f"Loaded {sanitized_tx.count()} rows into {config.get_silver_table(config.SILVER_FCT_SALES)} with Liquid Clustering.")
