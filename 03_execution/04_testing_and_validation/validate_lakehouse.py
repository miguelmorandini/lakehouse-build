"""
Execution Module: Data Quality & Lakehouse Validation Test Suite
----------------------------------------------------------------
Comprehensive automated test suite validating data completeness, primary key uniqueness,
referential integrity, PCI-DSS compliance, and volumetric reconciliation across all
Medallion layers (Bronze, Silver, Gold).

Standards: .agents/skills/data_quality_validation/SKILL.md
"""

import sys
import os
from typing import Dict, List, Tuple
from dataclasses import dataclass

# Ensure repository root is on sys.path for local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from notebooks.includes.configuration import config
from notebooks.includes.common_functions import (
    assert_no_nulls,
    assert_unique_key,
    assert_referential_integrity
)


@dataclass
class TestResult:
    test_id: str
    category: str
    description: str
    status: str
    details: str


class LakehouseValidator:
    """Executes automated quality checks across Bronze, Silver, and Gold layers."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: List[TestResult] = []

    def _record(self, test_id: str, category: str, description: str, passed: bool, details: str):
        status = "PASSED" if passed else "FAILED"
        self.results.append(TestResult(test_id, category, description, status, details))
        flag = "[PASS]" if passed else "[FAIL]"
        print(f"{flag} {test_id} ({category}): {description} -> {details}")

    def test_completeness_and_nulls(self):
        """Verifies mandatory primary keys have zero NULL values."""
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
            df = self.spark.table(table_name)
            null_count = df.filter(F.col(col_name).isNull()).count()
            self._record(
                test_id=f"COMP-{idx:02d}",
                category="Completeness",
                description=f"Zero nulls in {label} ({col_name})",
                passed=(null_count == 0),
                details=f"Null count: {null_count}"
            )

    def test_uniqueness_primary_keys(self):
        """Verifies primary key uniqueness across dimensions and facts."""
        key_checks = [
            ("Bronze Transactions PK", config.get_bronze_table(config.BRONZE_TRANSACTIONS), "transactionID"),
            ("Silver Sales PK", config.get_silver_table(config.SILVER_FCT_SALES), "transaction_id"),
            ("Silver Customers PK", config.get_silver_table(config.SILVER_DIM_CUSTOMERS), "customer_id"),
            ("Silver Franchises PK", config.get_silver_table(config.SILVER_DIM_FRANCHISES), "franchise_id"),
            ("Silver Suppliers PK", config.get_silver_table(config.SILVER_DIM_SUPPLIERS), "supplier_key"),
            ("Gold Products PK", config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF), "product_name")
        ]

        for idx, (label, table_name, key_col) in enumerate(key_checks, 1):
            df = self.spark.table(table_name)
            total = df.count()
            distinct = df.select(key_col).distinct().count()
            is_unique = (total == distinct)
            self._record(
                test_id=f"UNIQ-{idx:02d}",
                category="Uniqueness",
                description=f"Primary key uniqueness on {label}",
                passed=is_unique,
                details=f"Total: {total}, Distinct: {distinct}, Duplicates: {total - distinct}"
            )

    def test_referential_integrity(self):
        """Verifies all foreign key relationships have zero orphaned child records."""
        # Check 1: Silver Sales -> Silver Customers
        sales_df = self.spark.table(config.get_silver_table(config.SILVER_FCT_SALES))
        cust_df = self.spark.table(config.get_silver_table(config.SILVER_DIM_CUSTOMERS))
        orphaned_cust = sales_df.join(cust_df, sales_df["customer_id"] == cust_df["customer_id"], "left_anti").count()
        self._record(
            test_id="RI-01",
            category="Referential Integrity",
            description="Silver Sales -> Silver Customers integrity",
            passed=(orphaned_cust == 0),
            details=f"Orphaned sales: {orphaned_cust}"
        )

        # Check 2: Silver Sales -> Silver Franchises
        fran_df = self.spark.table(config.get_silver_table(config.SILVER_DIM_FRANCHISES))
        orphaned_fran = sales_df.join(fran_df, sales_df["franchise_id"] == fran_df["franchise_id"], "left_anti").count()
        self._record(
            test_id="RI-02",
            category="Referential Integrity",
            description="Silver Sales -> Silver Franchises integrity",
            passed=(orphaned_fran == 0),
            details=f"Orphaned sales: {orphaned_fran}"
        )

        # Check 3: Silver Franchises -> Silver Suppliers (including -1 fallback)
        sup_df = self.spark.table(config.get_silver_table(config.SILVER_DIM_SUPPLIERS))
        orphaned_sup = fran_df.join(sup_df, fran_df["supplier_key"] == sup_df["supplier_key"], "left_anti").count()
        self._record(
            test_id="RI-03",
            category="Referential Integrity",
            description="Silver Franchises -> Silver Suppliers (with -1 fallback)",
            passed=(orphaned_sup == 0),
            details=f"Orphaned stores: {orphaned_sup}"
        )

        # Check 4: Gold Daily Sales -> Silver Franchises
        gold_daily_df = self.spark.table(config.get_gold_table(config.GOLD_FCT_DAILY_SALES))
        orphaned_gold_fran = gold_daily_df.join(fran_df, gold_daily_df["franchise_id"] == fran_df["franchise_id"], "left_anti").count()
        self._record(
            test_id="RI-04",
            category="Referential Integrity",
            description="Gold Daily Sales -> Silver Franchises integrity",
            passed=(orphaned_gold_fran == 0),
            details=f"Orphaned gold rows: {orphaned_gold_fran}"
        )

    def test_pci_dss_compliance(self):
        """Verifies prohibited cardNumber column is absent from Silver and Gold layers."""
        sanitized_tables = [
            ("Silver Sales", config.get_silver_table(config.SILVER_FCT_SALES)),
            ("Gold Daily Sales", config.get_gold_table(config.GOLD_FCT_DAILY_SALES)),
            ("Gold Products", config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF))
        ]

        for idx, (label, table_name) in enumerate(sanitized_tables, 1):
            cols = [c.lower() for c in self.spark.table(table_name).columns]
            has_card = ("cardnumber" in cols) or ("card_number" in cols)
            self._record(
                test_id=f"SEC-{idx:02d}",
                category="Security/PCI",
                description=f"cardNumber column absent from {label}",
                passed=(not has_card),
                details=f"Prohibited card column present: {has_card}"
            )

    def test_medallion_reconciliation(self):
        """Verifies exact volumetric and financial parity across all layers."""
        bronze_tx = self.spark.table(config.get_bronze_table(config.BRONZE_TRANSACTIONS))
        silver_tx = self.spark.table(config.get_silver_table(config.SILVER_FCT_SALES))
        gold_daily = self.spark.table(config.get_gold_table(config.GOLD_FCT_DAILY_SALES))
        gold_prod = self.spark.table(config.get_gold_table(config.GOLD_FCT_PRODUCT_PERF))

        b_rows = bronze_tx.count()
        s_rows = silver_tx.count()
        g_daily_txs = gold_daily.select(F.sum("total_transactions")).collect()[0][0]
        g_prod_txs = gold_prod.select(F.sum("total_transactions")).collect()[0][0]

        tx_parity = (b_rows == 3333 and s_rows == 3333 and g_daily_txs == 3333 and g_prod_txs == 3333)
        self._record(
            test_id="REC-01",
            category="Reconciliation",
            description="Transaction count parity across Bronze, Silver, Gold",
            passed=tx_parity,
            details=f"Bronze: {b_rows}, Silver: {s_rows}, Gold Daily: {g_daily_txs}, Gold Prod: {g_prod_txs}"
        )

        b_rev = bronze_tx.select(F.sum("totalPrice")).collect()[0][0]
        s_rev = silver_tx.select(F.sum("total_price")).collect()[0][0]
        g_daily_rev = gold_daily.select(F.sum("total_sales_revenue_units")).collect()[0][0]
        g_prod_rev = gold_prod.select(F.sum("total_sales_revenue_units")).collect()[0][0]

        rev_parity = (b_rev == 66471 and s_rev == 66471 and g_daily_rev == 66471 and g_prod_rev == 66471)
        self._record(
            test_id="REC-02",
            category="Reconciliation",
            description="Revenue units parity across Bronze, Silver, Gold",
            passed=rev_parity,
            details=f"Bronze: {b_rev}, Silver: {s_rev}, Gold Daily: {g_daily_rev}, Gold Prod: {g_prod_rev}"
        )

    def run_all_tests(self) -> List[TestResult]:
        """Runs the entire Lakehouse validation suite."""
        print("=== EXECUTING LAKEHOUSE BUILD QUALITY VALIDATION SUITE ===")
        self.test_completeness_and_nulls()
        self.test_uniqueness_primary_keys()
        self.test_referential_integrity()
        self.test_pci_dss_compliance()
        self.test_medallion_reconciliation()

        passed_count = sum(1 for r in self.results if r.status == "PASSED")
        total_count = len(self.results)
        print(f"\n=== VALIDATION SUMMARY: {passed_count}/{total_count} TESTS PASSED ===")
        return self.results


if __name__ == "__main__":
    spark_sess = SparkSession.builder.appName("LakehouseBuild_Quality_Validation").getOrCreate()
    validator = LakehouseValidator(spark_sess)
    validator.run_all_tests()
