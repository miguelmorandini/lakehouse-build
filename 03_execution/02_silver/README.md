# Silver Layer Execution & Quality Verification Report

**Layer:** Silver (Curated & Conformed)  
**Catalog / Schema:** `lakehouse_build.silver`  
**Format:** Delta Lake (Liquid Clustered / Conformed)  
**Execution Status:** **COMPLETED & VERIFIED**

---

## 1. Overview & Objectives

The Silver layer acts as the enterprise "single version of the truth." It transforms raw Bronze assets into conformed, standardized, deduplicated, and sanitized dimensional entities:
- Strict **PCI-DSS sanitization** by completely omitting `cardNumber`.
- Resolving the 21 unmapped franchise stores by injecting the **conformed Unknown Supplier member** (`supplier_key = -1`).
- Enabling **Liquid Clustering** on `(transaction_date, franchise_id)` for high-performance downstream querying.

---

## 2. Silver Tables & Validation Metrics

| Table Name | Entity Type | Row Count | Null Counts (Key Cols) | Distinct PKs | Governance & Design Applied |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `silver.dim_suppliers` | Dimension | **28** | 0 on `supplier_key` | 28 | Includes 27 raw suppliers + 1 conformed unknown member (`supplier_key = -1`). |
| `silver.dim_franchises` | Dimension | **48** | 0 on `franchise_id`, 0 on `supplier_key` | 48 | 21 stores mapped to `supplier_key = -1`; 27 stores mapped to active suppliers. |
| `silver.dim_customers` | Dimension | **300** | 0 on `customer_id` | 300 | Cleaned column types, snake_case attributes. |
| `silver.fct_sales_transactions` | Fact | **3,333** | 0 on `transaction_id`, `customer_id`, `franchise_id` | 3,333 | `cardNumber` dropped. Clustered by `(transaction_date, franchise_id)`. |

---

## 3. Data Quality & Referential Integrity Verifications

1. **Card Number Elimination (PCI-DSS):**
   - Verified that `cardNumber` is absent from `silver.fct_sales_transactions`.
   - `payment_method` (`mastercard`, `amex`, `visa`) retained for commercial analysis.
2. **Referential Integrity Proof:**
   - Transactions $\rightarrow$ Customers: **3,333 / 3,333 matched (0 orphaned records)**.
   - Transactions $\rightarrow$ Franchises: **3,333 / 3,333 matched (0 orphaned records)**.
   - Franchises $\rightarrow$ Suppliers: **48 / 48 valid keys (27 active suppliers, 21 mapped to `-1`)**.
3. **Auditability:**
   - All tables track `_ingested_at` and `_updated_at`.

---

## 4. Pipeline Artifacts

- **Curation Script:** [curate_silver.py](file:///c:/Miguel/Projects/lakehouse-build/03_execution/02_silver/curate_silver.py)
- **Configuration:** [configuration.py](file:///c:/Miguel/Projects/lakehouse-build/notebooks/includes/configuration.py)
- **Common Functions:** [common_functions.py](file:///c:/Miguel/Projects/lakehouse-build/notebooks/includes/common_functions.py)
