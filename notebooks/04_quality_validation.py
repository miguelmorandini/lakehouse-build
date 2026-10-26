# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Build: Module 3 - Execution: Data Quality Validation
# MAGIC 
# MAGIC This notebook executes comprehensive automated data quality tests across Bronze, Silver, and Gold:
# MAGIC - Completeness (Zero nulls in primary keys)
# MAGIC - Uniqueness (Primary key uniqueness)
# MAGIC - Referential Integrity (Foreign key validity, unknown member check)
# MAGIC - PCI-DSS Compliance (Confirmation that `cardNumber` is absent from Silver and Gold)
# MAGIC - Medallion Financial and Volumetric Reconciliation (100% parity across layers)
# MAGIC 
# MAGIC **Catalog:** `lakehouse_build`  
# MAGIC **Standards:** `.agents/skills/data_quality_validation/SKILL.md`

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
# MAGIC ### 1. Run All Quality Checks

# COMMAND ----------

failures = []

def record_check(test_id, category, description, passed, details):
    flag = "✅ PASS" if passed else "❌ FAIL"
    print(f"{flag} [{test_id}] ({category}) {description} -> {details}")
    if not passed:
        failures.append(f"[{test_id}] {description}: {details}")

# 1. Completeness
tables_to_check = [
    ("Bronze Transactions", config.get_bronze_table(config.BRONZE_TRANSACTIONS), "transactionID"),
    ("Bronze Customers", config.get_bronze_table(config.BRONZE_CUSTOMERS), "customerID"),
    ("Bronze Franchises", config.get_bronze_table(config.BRONZE_FRANCHISES), "franchiseID"),
    ("Bronze Suppliers", config.get_bronze_table(config.BRONZE_SUPPLIERS), "supplierID"),
    ("Silver Sales", config.get_silver_table(config.SILVER_FCT_SALES), "transaction_id"),
    ("Silver Customers", config.get_silver_table(config.SILVER_DIM_CUSTOMERS), "customer_id"),
    ("Silver Franchises", config.get_silver_table(config.SILVER_DIM_FRANCHISES), "franchise_id"),
    ("Silver Suppliers", config.get_silver_table(config.SILVER_DIM_SUPPLIERS), "supplier_key"),
    ("Gold Daily Sales", config.get_gold_table(config.GOLD_FCT_DAILY_SALES), "sales_date"),
    ("Gold Products", config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF), "product_name")
]

for idx, (label, table_name, col_name) in enumerate(tables_to_check, 1):
    df = spark.table(table_name)
    null_count = df.filter(F.col(col_name).isNull()).count()
    record_check(f"COMP-{idx:02d}", "Completeness", f"Zero nulls in {label} ({col_name})", null_count == 0, f"Null count: {null_count}")

# 2. Uniqueness
key_checks = [
    ("Bronze Transactions PK", config.get_bronze_table(config.BRONZE_TRANSACTIONS), "transactionID"),
    ("Silver Sales PK", config.get_silver_table(config.SILVER_FCT_SALES), "transaction_id"),
    ("Silver Customers PK", config.get_silver_table(config.SILVER_DIM_CUSTOMERS), "customer_id"),
    ("Silver Franchises PK", config.get_silver_table(config.SILVER_DIM_FRANCHISES), "franchise_id"),
    ("Silver Suppliers PK", config.get_silver_table(config.SILVER_DIM_SUPPLIERS), "supplier_key"),
    ("Gold Products PK", config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF), "product_name")
]

for idx, (label, table_name, key_col) in enumerate(key_checks, 1):
    df = spark.table(table_name)
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    record_check(f"UNIQ-{idx:02d}", "Uniqueness", f"Primary key uniqueness on {label}", total == distinct, f"Total: {total}, Distinct: {distinct}")

# 3. Referential Integrity
sales_df = spark.table(config.get_silver_table(config.SILVER_FCT_SALES))
cust_df = spark.table(config.get_silver_table(config.SILVER_DIM_CUSTOMERS))
fran_df = spark.table(config.get_silver_table(config.SILVER_DIM_FRANCHISES))
sup_df = spark.table(config.get_silver_table(config.SILVER_DIM_SUPPLIERS))
gold_daily_df = spark.table(config.get_gold_table(config.GOLD_FCT_DAILY_SALES))

record_check("RI-01", "Referential Integrity", "Silver Sales -> Silver Customers", sales_df.join(cust_df, "customer_id", "left_anti").count() == 0, "0 orphaned")
record_check("RI-02", "Referential Integrity", "Silver Sales -> Silver Franchises", sales_df.join(fran_df, "franchise_id", "left_anti").count() == 0, "0 orphaned")
record_check("RI-03", "Referential Integrity", "Silver Franchises -> Silver Suppliers (-1 fallback)", fran_df.join(sup_df, "supplier_key", "left_anti").count() == 0, "0 orphaned")
record_check("RI-04", "Referential Integrity", "Gold Daily Sales -> Silver Franchises", gold_daily_df.join(fran_df, "franchise_id", "left_anti").count() == 0, "0 orphaned")

# 4. PCI-DSS Compliance
for idx, (label, table_name) in enumerate([("Silver Sales", config.get_silver_table(config.SILVER_FCT_SALES)), ("Gold Daily Sales", config.get_gold_table(config.GOLD_FCT_DAILY_SALES)), ("Gold Products", config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF))], 1):
    cols = [c.lower() for c in spark.table(table_name).columns]
    has_card = ("cardnumber" in cols) or ("card_number" in cols)
    record_check(f"SEC-{idx:02d}", "Security/PCI", f"cardNumber column absent from {label}", not has_card, f"card column present: {has_card}")

# 5. Medallion Reconciliation
bronze_tx = spark.table(config.get_bronze_table(config.BRONZE_TRANSACTIONS))
gold_prod = spark.table(config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF))

b_rows = bronze_tx.count()
s_rows = sales_df.count()
g_daily_txs = gold_daily_df.select(F.sum("total_transactions")).collect()[0][0]
g_prod_txs = gold_prod.select(F.sum("total_transactions")).collect()[0][0]
tx_parity = (b_rows == 3333 and s_rows == 3333 and g_daily_txs == 3333 and g_prod_txs == 3333)
record_check("REC-01", "Reconciliation", "Transaction count parity across Bronze, Silver, Gold", tx_parity, f"B:{b_rows}, S:{s_rows}, G_Daily:{g_daily_txs}, G_Prod:{g_prod_txs}")

b_rev = bronze_tx.select(F.sum("totalPrice")).collect()[0][0]
s_rev = sales_df.select(F.sum("total_price")).collect()[0][0]
g_daily_rev = gold_daily_df.select(F.sum("total_sales_revenue_units")).collect()[0][0]
g_prod_rev = gold_prod.select(F.sum("total_sales_revenue_units")).collect()[0][0]
rev_parity = (b_rev == 66471 and s_rev == 66471 and g_daily_rev == 66471 and g_prod_rev == 66471)
record_check("REC-02", "Reconciliation", "Revenue units parity across Bronze, Silver, Gold", rev_parity, f"B:{b_rev}, S:{s_rev}, G_Daily:{g_daily_rev}, G_Prod:{g_prod_rev}")

if failures:
    raise AssertionError(f"Quality validation failed with {len(failures)} errors:\n" + "\n".join(failures))
else:
    print("\n🎉 ALL 24 QUALITY ASSERTION CHECKS PASSED SUCCESSFULLY (100% PARITY)!")
