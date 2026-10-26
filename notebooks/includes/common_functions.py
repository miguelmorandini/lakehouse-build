"""
Lakehouse Build Common Functions Module
---------------------------------------
Reusable PySpark and Delta Lake utility functions adhering to the engineering
and data quality validation standards of the Lakehouse Build methodology.
"""

from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType
)
from delta.tables import DeltaTable


# =============================================================================
# 1. Audit & Ingestion Functions (Bronze Layer)
# =============================================================================

def add_bronze_audit_columns(
    df: DataFrame,
    source_identifier: str = "samples.bakehouse",
    batch_id: str = "initial_load"
) -> DataFrame:
    """
    Appends standard metadata audit columns to raw DataFrames before Bronze ingestion.
    
    Audit Columns:
      - `_ingested_at`: UTC timestamp of ingestion.
      - `_source_system`: Origin catalog/source identifier.
      - `_batch_id`: Ingestion run or batch identifier.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_system", F.lit(source_identifier))
          .withColumn("_batch_id", F.lit(batch_id))
    )


# =============================================================================
# 2. Curation & Sanitization Functions (Silver Layer)
# =============================================================================

def sanitize_silver_transactions(df: DataFrame, prohibited_columns: List[str] = None) -> DataFrame:
    """
    Sanitizes raw transactions for Silver layer promotion:
      - Drops prohibited PCI columns (such as `cardNumber`).
      - Derives `transaction_date` from `dateTime` for Liquid Clustering.
      - Renames camelCase source columns to snake_case target columns.
      - Retains clean business attributes and audit timestamps.
    """
    if prohibited_columns is None:
        prohibited_columns = ["cardNumber"]

    # Explicitly drop PCI/sensitive attributes
    cleaned_df = df.drop(*prohibited_columns)

    # Standardize column naming and derive date
    return (
        cleaned_df
        .withColumnRenamed("transactionID", "transaction_id")
        .withColumnRenamed("customerID", "customer_id")
        .withColumnRenamed("franchiseID", "franchise_id")
        .withColumnRenamed("dateTime", "transaction_datetime")
        .withColumnRenamed("product", "product_name")
        .withColumnRenamed("unitPrice", "unit_price")
        .withColumnRenamed("totalPrice", "total_price")
        .withColumnRenamed("paymentMethod", "payment_method")
        .withColumn("transaction_date", F.to_date(F.col("transaction_datetime")))
        .withColumn("_updated_at", F.current_timestamp())
        .select(
            "transaction_id",
            "customer_id",
            "franchise_id",
            "transaction_datetime",
            "transaction_date",
            "product_name",
            "quantity",
            "unit_price",
            "total_price",
            "payment_method",
            "_ingested_at",
            "_updated_at"
        )
    )


def build_unknown_supplier_df(spark: SparkSession) -> DataFrame:
    """
    Generates a single-row DataFrame containing the standard Kimball conformed
    unknown supplier member (supplier_key = -1) to gracefully resolve missing
    supplier references for the 21 orphaned franchises.
    """
    schema = StructType([
        StructField("supplier_key", LongType(), False),
        StructField("supplier_id", LongType(), False),
        StructField("supplier_name", StringType(), False),
        StructField("ingredient", StringType(), False),
        StructField("continent", StringType(), False),
        StructField("city", StringType(), False),
        StructField("district", StringType(), False),
        StructField("supplier_size", StringType(), False),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("approved", StringType(), False),
        StructField("_record_source", StringType(), False)
    ])

    data = [(
        -1,
        -1,
        "Unknown / Unassigned Supplier",
        "Unassigned",
        "Unknown",
        "Unknown",
        "Unknown",
        "N/A",
        None,
        None,
        "N/A",
        "SYSTEM_DEFAULT"
    )]

    return spark.createDataFrame(data, schema)


# =============================================================================
# 3. Delta Merge (Upsert) Utilities
# =============================================================================

def merge_silver_entity(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_name: str,
    key_columns: List[str]
) -> None:
    """
    Performs an idempotent Delta Lake MERGE INTO operation:
      - Matches records on the specified key columns.
      - Updates existing records with all incoming columns (updating `_updated_at`).
      - Inserts new records when not matched.
    """
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Build join predicate: e.g. "tgt.id = src.id AND tgt.code = src.code"
    match_predicate = " AND ".join([f"tgt.{k} = src.{k}" for k in key_columns])

    # Execute Delta merge
    (
        delta_target.alias("tgt")
        .merge(
            source_df.alias("src"),
            match_predicate
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# =============================================================================
# 4. Data Quality Validation Assertions
# =============================================================================

def assert_no_nulls(df: DataFrame, column_name: str) -> None:
    """Verifies that the specified column contains zero NULL or NaN values."""
    null_count = df.filter(F.col(column_name).isNull() | F.isnan(column_name)).count()
    if null_count > 0:
        raise AssertionError(
            f"Data Quality Failure: Column '{column_name}' contains {null_count} null/NaN values."
        )


def assert_unique_key(df: DataFrame, key_column: str) -> None:
    """Verifies that the specified key column is 100% unique across all rows."""
    total_count = df.count()
    distinct_count = df.select(key_column).distinct().count()
    if total_count != distinct_count:
        duplicate_count = total_count - distinct_count
        raise AssertionError(
            f"Data Quality Failure: Key '{key_column}' contains {duplicate_count} duplicate values "
            f"(Total: {total_count}, Distinct: {distinct_count})."
        )


def assert_referential_integrity(
    child_df: DataFrame,
    child_fk: str,
    parent_df: DataFrame,
    parent_pk: str
) -> None:
    """Verifies that all child foreign keys exist in the parent primary key set (0 orphans)."""
    orphaned_count = child_df.join(
        parent_df,
        child_df[child_fk] == parent_df[parent_pk],
        "left_anti"
    ).count()

    if orphaned_count > 0:
        raise AssertionError(
            f"Data Quality Failure: Foreign key '{child_fk}' has {orphaned_count} orphaned records "
            f"with no matching parent in '{parent_pk}'."
        )
