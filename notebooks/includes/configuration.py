"""
Lakehouse Build Configuration Module
------------------------------------
Centralized environment parameters, table naming conventions, catalog schemas,
and audit standards for the Lakehouse Build project.
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass(frozen=True)
class LakehouseConfig:
    """Immutable configuration container for the Lakehouse Medallion layers."""

    # -------------------------------------------------------------------------
    # 1. Environment & Catalogs
    # -------------------------------------------------------------------------
    SOURCE_CATALOG: str = "samples"
    SOURCE_SCHEMA: str = "bakehouse"

    # Target catalog can be overridden via environment variable LAKEHOUSE_CATALOG
    TARGET_CATALOG: str = os.getenv("LAKEHOUSE_CATALOG", "lakehouse_build")

    # Layer Schema Names
    BRONZE_SCHEMA: str = "bronze"
    SILVER_SCHEMA: str = "silver"
    GOLD_SCHEMA: str = "gold"

    # -------------------------------------------------------------------------
    # 2. Source Table Names (samples.bakehouse)
    # -------------------------------------------------------------------------
    SRC_TRANSACTIONS: str = "sales_transactions"
    SRC_CUSTOMERS: str = "sales_customers"
    SRC_FRANCHISES: str = "sales_franchises"
    SRC_SUPPLIERS: str = "sales_suppliers"

    # -------------------------------------------------------------------------
    # 3. Target Table Identifiers
    # -------------------------------------------------------------------------
    # Bronze Tables (Raw replica + audit metadata)
    BRONZE_TRANSACTIONS: str = "sales_transactions"
    BRONZE_CUSTOMERS: str = "sales_customers"
    BRONZE_FRANCHISES: str = "sales_franchises"
    BRONZE_SUPPLIERS: str = "sales_suppliers"

    # Silver Tables (Curated & sanitized conformed entities)
    SILVER_FCT_SALES: str = "fct_sales_transactions"
    SILVER_DIM_CUSTOMERS: str = "dim_customers"
    SILVER_DIM_FRANCHISES: str = "dim_franchises"
    SILVER_DIM_SUPPLIERS: str = "dim_suppliers"

    # Gold Tables (Business marts & aggregates)
    GOLD_FCT_DAILY_SALES: str = "fct_daily_franchise_sales"
    GOLD_FCT_PRODUCT_PERF: str = "fct_product_performance"

    # -------------------------------------------------------------------------
    # 4. Governance & PII Policies
    # -------------------------------------------------------------------------
    # Columns strictly prohibited from entering Silver or Gold layers
    PROHIBITED_PCI_COLUMNS: List[str] = field(default_factory=lambda: ["cardNumber"])

    # -------------------------------------------------------------------------
    # 5. Conformed Unknown Supplier Default Member (-1)
    # -------------------------------------------------------------------------
    UNKNOWN_SUPPLIER_KEY: int = -1
    UNKNOWN_SUPPLIER_RECORD: Dict[str, Any] = field(default_factory=lambda: {
        "supplier_key": -1,
        "supplier_id": -1,
        "name": "Unknown / Unassigned Supplier",
        "ingredient": "Unassigned",
        "continent": "Unknown",
        "city": "Unknown",
        "district": "Unknown",
        "size": "N/A",
        "longitude": None,
        "latitude": None,
        "approved": "N/A",
        "_record_source": "SYSTEM_DEFAULT"
    })

    # -------------------------------------------------------------------------
    # 6. Delta Table Layout & Clustering
    # -------------------------------------------------------------------------
    CLUSTERING_KEYS_SILVER_SALES: List[str] = field(default_factory=lambda: ["transaction_date", "franchise_id"])
    CLUSTERING_KEYS_GOLD_SALES: List[str] = field(default_factory=lambda: ["sales_date", "franchise_id"])

    # -------------------------------------------------------------------------
    # 7. Helper Methods for Fully Qualified Table Names (3-Level Namespace)
    # -------------------------------------------------------------------------
    def get_source_table(self, table_name: str) -> str:
        return f"{self.SOURCE_CATALOG}.{self.SOURCE_SCHEMA}.{table_name}"

    def get_bronze_table(self, table_name: str) -> str:
        return f"{self.TARGET_CATALOG}.{self.BRONZE_SCHEMA}.{table_name}"

    def get_silver_table(self, table_name: str) -> str:
        return f"{self.TARGET_CATALOG}.{self.SILVER_SCHEMA}.{table_name}"

    def get_gold_table(self, table_name: str) -> str:
        return f"{self.TARGET_CATALOG}.{self.GOLD_SCHEMA}.{table_name}"


# Default singleton instance
config = LakehouseConfig()
