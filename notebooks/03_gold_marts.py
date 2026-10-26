# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Build: Module 3 - Execution: Gold Layer Business Marts
# MAGIC 
# MAGIC This notebook builds business consumption star-schema marts in `lakehouse_build.gold`:
# MAGIC 1. `fct_daily_franchise_sales`: Daily store aggregate performance mart with Liquid Clustering (`sales_date`, `franchise_id`).
# MAGIC 2. `fct_product_performance`: Product mix velocity, volume share, and revenue mart.
# MAGIC 
# MAGIC **Catalog:** `lakehouse_build`  
# MAGIC **Schema:** `gold`  
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
except ImportError:
    from includes.configuration import config

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ensure Gold Schema Exists

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.TARGET_CATALOG}.{config.GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Build `gold.fct_daily_franchise_sales`

# COMMAND ----------

tx_df = spark.table(config.get_silver_table(config.SILVER_FCT_SALES)).alias("tx")
f_df = spark.table(config.get_silver_table(config.SILVER_DIM_FRANCHISES)).alias("f")
s_df = spark.table(config.get_silver_table(config.SILVER_DIM_SUPPLIERS)).alias("s")

daily_agg = (
    tx_df
    .groupBy("transaction_date", "franchise_id")
    .agg(
        F.sum("total_price").cast("long").alias("total_sales_revenue_units"),
        F.sum("quantity").cast("long").alias("total_quantity_sold"),
        F.count("transaction_id").cast("long").alias("total_transactions"),
        F.round(F.avg("total_price"), 2).alias("average_ticket_revenue_units"),
        F.round(F.avg("quantity"), 2).alias("average_units_per_order"),
        F.round(F.avg("unit_price"), 2).alias("reference_unit_price")
    )
    .withColumnRenamed("transaction_date", "sales_date")
).alias("agg")

gold_daily_df = (
    daily_agg
    .join(f_df, F.col("agg.franchise_id") == F.col("f.franchise_id"), "inner")
    .join(s_df, F.col("f.supplier_key") == F.col("s.supplier_key"), "left")
    .select(
        F.col("agg.sales_date"),
        F.col("agg.franchise_id"),
        F.col("f.franchise_name"),
        F.col("f.city").alias("store_city"),
        F.col("f.country").alias("store_country"),
        F.col("f.franchise_size").alias("store_size"),
        F.col("f.supplier_key"),
        F.col("s.supplier_name"),
        F.col("s.ingredient").alias("primary_ingredient"),
        F.col("agg.total_sales_revenue_units"),
        F.col("agg.total_quantity_sold"),
        F.col("agg.total_transactions"),
        F.col("agg.average_ticket_revenue_units"),
        F.col("agg.average_units_per_order"),
        F.col("agg.reference_unit_price"),
        F.current_timestamp().alias("_refreshed_at")
    )
)

(
    gold_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .clusterBy(*config.CLUSTERING_KEYS_GOLD_SALES)
    .saveAsTable(config.get_gold_table(config.GOLD_FCT_DAILY_SALES))
)
print(f"Loaded {gold_daily_df.count()} rows into {config.get_gold_table(config.GOLD_FCT_DAILY_SALES)} with Liquid Clustering.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Build `gold.fct_product_performance`

# COMMAND ----------

totals = tx_df.agg(
    F.sum("total_price").alias("network_revenue"),
    F.sum("quantity").alias("network_units")
).collect()[0]

network_revenue = float(totals["network_revenue"])
network_units = float(totals["network_units"])

product_df = (
    tx_df
    .groupBy("product_name")
    .agg(
        F.sum("total_price").cast("long").alias("total_sales_revenue_units"),
        F.sum("quantity").cast("long").alias("total_quantity_sold"),
        F.count("transaction_id").cast("long").alias("total_transactions"),
        F.round(F.avg("total_price"), 2).alias("average_ticket_revenue_units"),
        F.round(F.avg("quantity"), 2).alias("average_units_per_order"),
        F.round(F.avg("unit_price"), 2).alias("reference_unit_price")
    )
    .withColumn(
        "revenue_share_pct",
        F.round((F.col("total_sales_revenue_units") * 100.0) / F.lit(network_revenue), 2)
    )
    .withColumn(
        "unit_volume_share_pct",
        F.round((F.col("total_quantity_sold") * 100.0) / F.lit(network_units), 2)
    )
    .withColumn("_refreshed_at", F.current_timestamp())
    .orderBy(F.col("total_sales_revenue_units").desc())
)

(
    product_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF))
)
print(f"Loaded {product_df.count()} rows into {config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF)}.")
