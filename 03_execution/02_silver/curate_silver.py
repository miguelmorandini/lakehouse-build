"""
Execution Module: Silver Curation & Conformance Pipeline
--------------------------------------------------------
Curates, conforms, and sanitizes raw Bronze tables into enterprise Silver Delta tables:
  1. silver.dim_suppliers: 27 source suppliers + conformed unknown member (supplier_key = -1).
  2. silver.dim_franchises: 48 stores linked to supplier_key with -1 fallback.
  3. silver.dim_customers: 300 conformed customer master records.
  4. silver.fct_sales_transactions: 3,333 sales with `cardNumber` strictly dropped,
     `transaction_date` derived, and Liquid Clustering enabled.

Layers: Bronze (lakehouse_build.bronze) -> Silver (lakehouse_build.silver)
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
from notebooks.includes.common_functions import (
    sanitize_silver_transactions,
    build_unknown_supplier_df,
    merge_silver_entity
)


def load_silver_dim_suppliers(spark: SparkSession) -> int:
    """Loads silver.dim_suppliers uniting 27 raw suppliers with conformed member (-1)."""
    bronze_table = config.get_bronze_table(config.BRONZE_SUPPLIERS)
    silver_table = config.get_silver_table(config.SILVER_DIM_SUPPLIERS)
    
    print(f"[SILVER] Processing {silver_table} from {bronze_table}...")
    bronze_sup = spark.table(bronze_table)
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
    
    (
        curated_sup.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
    
    count = spark.table(silver_table).count()
    print(f"[SILVER] Successfully saved {silver_table} ({count} rows).\n")
    return count


def load_silver_dim_franchises(spark: SparkSession) -> int:
    """Loads silver.dim_franchises resolving supplier_key with fallback -1."""
    bronze_fran = config.get_bronze_table(config.BRONZE_FRANCHISES)
    silver_sup = config.get_silver_table(config.SILVER_DIM_SUPPLIERS)
    silver_table = config.get_silver_table(config.SILVER_DIM_FRANCHISES)
    
    print(f"[SILVER] Processing {silver_table} from {bronze_fran}...")
    f_df = spark.table(bronze_fran).alias("f")
    s_df = spark.table(silver_sup).alias("s")
    
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
    
    (
        curated_fran.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
    
    count = spark.table(silver_table).count()
    print(f"[SILVER] Successfully saved {silver_table} ({count} rows).\n")
    return count


def load_silver_dim_customers(spark: SparkSession) -> int:
    """Loads silver.dim_customers conforming columns to standard snake_case."""
    bronze_table = config.get_bronze_table(config.BRONZE_CUSTOMERS)
    silver_table = config.get_silver_table(config.SILVER_DIM_CUSTOMERS)
    
    print(f"[SILVER] Processing {silver_table} from {bronze_table}...")
    bronze_cust = spark.table(bronze_table)
    
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
    
    (
        curated_cust.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )
    
    count = spark.table(silver_table).count()
    print(f"[SILVER] Successfully saved {silver_table} ({count} rows).\n")
    return count


def load_silver_fct_sales(spark: SparkSession) -> int:
    """Loads silver.fct_sales_transactions dropping cardNumber and deriving transaction_date."""
    bronze_table = config.get_bronze_table(config.BRONZE_TRANSACTIONS)
    silver_table = config.get_silver_table(config.SILVER_FCT_SALES)
    
    print(f"[SILVER] Processing {silver_table} from {bronze_table}...")
    bronze_tx = spark.table(bronze_table)
    
    # Apply standard sanitization (drops cardNumber, derives transaction_date)
    sanitized_tx = sanitize_silver_transactions(
        df=bronze_tx,
        prohibited_columns=config.PROHIBITED_PCI_COLUMNS
    )
    
    # Save as clustered Delta table
    (
        sanitized_tx.write
        .format("delta")
        .mode("overwrite")
        .clusterBy(*config.CLUSTERING_KEYS_SILVER_SALES)
        .saveAsTable(silver_table)
    )
    
    count = spark.table(silver_table).count()
    print(f"[SILVER] Successfully saved {silver_table} ({count} rows) with Liquid Clustering.\n")
    return count


def run_silver_pipeline(spark: SparkSession) -> Dict[str, int]:
    """Orchestrates end-to-end Silver layer execution."""
    print("=== STARTING SILVER CURATION PIPELINE ===")
    results = {
        "dim_suppliers": load_silver_dim_suppliers(spark),
        "dim_franchises": load_silver_dim_franchises(spark),
        "dim_customers": load_silver_dim_customers(spark),
        "fct_sales_transactions": load_silver_fct_sales(spark)
    }
    print("=== SILVER PIPELINE COMPLETED SUCCESSFULLY ===")
    return results


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("LakehouseBuild_Silver_Pipeline").getOrCreate()
    run_silver_pipeline(spark_session)
