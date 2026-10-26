"""
Execution Module: Gold Layer Marts & Dimensional Modeling Pipeline
------------------------------------------------------------------
Builds curated consumption-ready star schema marts in `lakehouse_build.gold`:
  1. gold.fct_daily_franchise_sales: Pre-aggregated daily store performance mart
     formulated in abstract monetary units with Liquid Clustering.
  2. gold.fct_product_performance: Product mix velocity, volume share, and revenue mart.

Layers: Silver (lakehouse_build.silver) -> Gold (lakehouse_build.gold)
Standards: .agents/skills/lakehouse_design/SKILL.md, .agents/skills/pyspark_delta/SKILL.md
"""

import sys
import os
from typing import Dict, Any

# Ensure repository root is on sys.path for local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from notebooks.includes.configuration import config


def build_gold_fct_daily_franchise_sales(spark: SparkSession) -> int:
    """
    Builds gold.fct_daily_franchise_sales aggregating transaction facts by
    store and date, enriched with store dimensions and abstract revenue units.
    """
    silver_tx_table = config.get_silver_table(config.SILVER_FCT_SALES)
    silver_fran_table = config.get_silver_table(config.SILVER_DIM_FRANCHISES)
    silver_sup_table = config.get_silver_table(config.SILVER_DIM_SUPPLIERS)
    gold_table = config.get_gold_table(config.GOLD_FCT_DAILY_SALES)
    
    print(f"[GOLD] Building {gold_table}...")
    
    tx_df = spark.table(silver_tx_table).alias("tx")
    f_df = spark.table(silver_fran_table).alias("f")
    s_df = spark.table(silver_sup_table).alias("s")
    
    # Pre-aggregate sales facts by date and franchise
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
    
    # Join with franchise store dimensions and conformed supplier dimensions
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
    
    # Save as Liquid Clustered Delta table
    (
        gold_daily_df.write
        .format("delta")
        .mode("overwrite")
        .clusterBy(*config.CLUSTERING_KEYS_GOLD_SALES)
        .saveAsTable(gold_table)
    )
    
    count = spark.table(gold_table).count()
    print(f"[GOLD] Successfully built {gold_table} ({count} rows) with Liquid Clustering.\n")
    return count


def build_gold_fct_product_performance(spark: SparkSession) -> int:
    """
    Builds gold.fct_product_performance analyzing product mix, velocity, and
    overall commercial contribution across the 17-day operational window.
    """
    silver_tx_table = config.get_silver_table(config.SILVER_FCT_SALES)
    gold_table = config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF)
    
    print(f"[GOLD] Building {gold_table}...")
    tx_df = spark.table(silver_tx_table)
    
    # Calculate network-wide totals for share calculation
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
        .saveAsTable(gold_table)
    )
    
    count = spark.table(gold_table).count()
    print(f"[GOLD] Successfully built {gold_table} ({count} rows).\n")
    return count


def run_gold_pipeline(spark: SparkSession) -> Dict[str, int]:
    """Orchestrates end-to-end Gold layer execution."""
    print("=== STARTING GOLD MARTS PIPELINE ===")
    results = {
        "fct_daily_franchise_sales": build_gold_fct_daily_franchise_sales(spark),
        "fct_product_performance": build_gold_fct_product_performance(spark)
    }
    print("=== GOLD PIPELINE COMPLETED SUCCESSFULLY ===")
    return results


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("LakehouseBuild_Gold_Pipeline").getOrCreate()
    run_gold_pipeline(spark_session)
