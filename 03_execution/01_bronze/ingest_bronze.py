"""
Execution Module: Bronze Ingestion Pipeline
-------------------------------------------
Ingests raw transactional, customer, franchise, and supplier data from
`samples.bakehouse` into `lakehouse_build.bronze` as append-only Delta tables
with standard audit metadata columns.

Layers: Source (samples.bakehouse) -> Bronze (lakehouse_build.bronze)
Standards: .agents/skills/pyspark_delta/SKILL.md
"""

import sys
import os
from typing import List, Dict

# Ensure repository root is on sys.path for local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from notebooks.includes.configuration import config
from notebooks.includes.common_functions import add_bronze_audit_columns


# Mapping of source tables to their target Bronze table names
BRONZE_TABLES_MAP: Dict[str, str] = {
    config.SRC_TRANSACTIONS: config.BRONZE_TRANSACTIONS,
    config.SRC_CUSTOMERS: config.BRONZE_CUSTOMERS,
    config.SRC_FRANCHISES: config.BRONZE_FRANCHISES,
    config.SRC_SUPPLIERS: config.BRONZE_SUPPLIERS
}


def ingest_bronze_layer(spark, batch_id: str = "initial_load") -> Dict[str, int]:
    """
    Ingests all candidate tables from samples.bakehouse into the Bronze layer.
    
    Args:
        spark: Active SparkSession
        batch_id: Batch identifier for audit tracking
        
    Returns:
        Dict mapping table names to their ingested row count
    """
    results = {}
    
    for src_name, tgt_name in BRONZE_TABLES_MAP.items():
        src_table = config.get_source_table(src_name)
        tgt_table = config.get_bronze_table(tgt_name)
        
        print(f"[BRONZE INGEST] Reading from {src_table}...")
        raw_df = spark.table(src_table)
        
        # Enrich with standard audit columns
        audited_df = add_bronze_audit_columns(
            df=raw_df,
            source_identifier=src_table,
            batch_id=batch_id
        )
        
        row_count = audited_df.count()
        print(f"[BRONZE INGEST] Writing {row_count} rows to {tgt_table} (append mode)...")
        
        (
            audited_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(tgt_table)
        )
        
        results[tgt_name] = row_count
        print(f"[BRONZE INGEST] Successfully loaded {tgt_table} ({row_count} rows).\n")
        
    return results


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("LakehouseBuild_Bronze_Ingestion").getOrCreate()
    ingest_bronze_layer(spark)
