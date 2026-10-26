# Ingestion & Transformation Patterns Guide

**Methodology Module:** Module 2: Preparation  
**Document:** `02_ingestion_and_transformation_patterns.md`  
**Reference Standards:** PySpark & Delta Lake Skill (`.agents/skills/pyspark_delta/`)

---

## 1. Overview

This guide defines the standardized engineering patterns for moving data through the Medallion layers (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) during Module 3: Execution.

---

## 2. Bronze Ingestion Pattern (Raw Landing)

### Principles
- **Source:** Direct 1-to-1 extraction from `samples.bakehouse`.
- **Mode:** Append-only (`append`).
- **Schema:** Preserve raw column names and types without transformation or filtering.
- **Audit:** Mandatory addition of `_ingested_at`, `_source_system`, and `_batch_id`.

### Code Pattern
```python
from notebooks.includes.configuration import config
from notebooks.includes.common_functions import add_bronze_audit_columns

def ingest_raw_table(spark, table_name: str, batch_id: str):
    source_path = config.get_source_table(table_name)
    target_path = config.get_bronze_table(table_name)
    
    raw_df = spark.table(source_path)
    audited_df = add_bronze_audit_columns(raw_df, source_identifier=source_path, batch_id=batch_id)
    
    audited_df.write.format("delta").mode("append").saveAsTable(target_path)
```

---

## 3. Silver Curation & Conformance Pattern

### Principles
- **Idempotency:** Driven by Delta Lake `MERGE INTO` on designated primary keys.
- **Sanitization:** Strict omission of `cardNumber` from `fct_sales_transactions`.
- **Conformed Members:** Union of the conformed unknown supplier (`supplier_key = -1`) into `silver.dim_suppliers`.
- **Relationship Safety:** Use `LEFT JOIN` on `franchises.supplierID = suppliers.supplierID` with `COALESCE(supplierID, -1)`.

### Code Pattern: Conformed Unknown Supplier Injection
```python
def load_silver_suppliers(spark):
    bronze_sup = spark.table(config.get_bronze_table(config.BRONZE_SUPPLIERS))
    unknown_df = build_unknown_supplier_df(spark)
    
    curated_sup = (
        bronze_sup
        .select(
            F.col("supplierID").alias("supplier_key"),
            F.col("supplierID").alias("supplier_id"),
            F.col("name").alias("supplier_name"),
            F.col("ingredient"),
            F.col("continent"),
            F.col("city"),
            F.col("district"),
            F.col("size").alias("supplier_size"),
            F.col("longitude"),
            F.col("latitude"),
            F.col("approved"),
            F.lit("BRONZE_SOURCE").alias("_record_source")
        )
        .unionByName(unknown_df)
    )
    
    merge_silver_entity(
        spark=spark,
        source_df=curated_sup,
        target_table_name=config.get_silver_table(config.SILVER_DIM_SUPPLIERS),
        key_columns=["supplier_key"]
    )
```

### Code Pattern: Sanitized Sales Fact Merge
```python
def load_silver_sales(spark):
    bronze_tx = spark.table(config.get_bronze_table(config.BRONZE_TRANSACTIONS))
    sanitized_tx = sanitize_silver_transactions(bronze_tx)
    
    merge_silver_entity(
        spark=spark,
        source_df=sanitized_tx,
        target_table_name=config.get_silver_table(config.SILVER_FCT_SALES),
        key_columns=["transaction_id"]
    )
```

---

## 4. Gold Aggregation & Star Schema Pattern

### Principles
- **Granularity:** Pre-aggregated daily store sales and product performance marts.
- **Financial Units:** Formulated in abstract monetary units (`total_sales_revenue_units`).
- **Optimization:** Liquid Clustering on `sales_date` and `franchise_id`.

### Code Pattern: Daily Franchise Sales Mart
```python
def build_gold_daily_sales(spark):
    sales_df = spark.table(config.get_silver_table(config.SILVER_FCT_SALES))
    
    daily_sales = (
        sales_df
        .groupBy("transaction_date", "franchise_id")
        .agg(
            F.sum("total_price").alias("total_sales_revenue_units"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.count("transaction_id").alias("total_transactions"),
            F.avg("total_price").alias("average_ticket_revenue_units"),
            F.avg("unit_price").alias("reference_unit_price")
        )
        .withColumnRenamed("transaction_date", "sales_date")
        .withColumn("_refreshed_at", F.current_timestamp())
    )
    
    target_table = config.get_gold_table(config.GOLD_FCT_DAILY_SALES)
    daily_sales.write.format("delta").mode("overwrite").saveAsTable(target_table)
```

---

## 5. Transition to Module 3: Execution Checklist

With these patterns documented and utility functions established, the preparation foundation is complete:
- [x] Ingestion patterns specified for Bronze raw append.
- [x] Curation and Delta merge patterns defined for Silver entities.
- [x] Sensitive `cardNumber` omission policy hardcoded into transformation utility.
- [x] Conformed fallback supplier rule (`-1`) codified into DataFrame builder.
- [x] Abstract financial aggregation metrics standardized for Gold.
- [x] Foundation cleared for Module 3: Execution pipeline development.
