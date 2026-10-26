# Bronze Layer Execution & Reconciliation Report

**Layer:** Bronze (Raw Landing)  
**Catalog / Schema:** `lakehouse_build.bronze`  
**Source Catalog:** `samples.bakehouse`  
**Format:** Delta Lake (Append-Only)  
**Execution Status:** **COMPLETED & VERIFIED**

---

## 1. Overview & Objectives

The Bronze layer serves as the raw, high-fidelity historical replica of source data from `samples.bakehouse`. It preserves all original column names, types, and raw payloads (including raw `cardNumber` for audit purposes), while appending mandatory system audit metadata.

---

## 2. Ingested Tables & Reconciliation

| Source Table | Bronze Table | Source Rows | Bronze Rows | Row Variance | Audit Status | Ingestion Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `samples.bakehouse.sales_transactions` | `lakehouse_build.bronze.sales_transactions` | 3,333 | **3,333** | 0 (100.0%) | Verified | **SUCCEEDED** |
| `samples.bakehouse.sales_customers` | `lakehouse_build.bronze.sales_customers` | 300 | **300** | 0 (100.0%) | Verified | **SUCCEEDED** |
| `samples.bakehouse.sales_franchises` | `lakehouse_build.bronze.sales_franchises` | 48 | **48** | 0 (100.0%) | Verified | **SUCCEEDED** |
| `samples.bakehouse.sales_suppliers` | `lakehouse_build.bronze.sales_suppliers` | 27 | **27** | 0 (100.0%) | Verified | **SUCCEEDED** |

---

## 3. Audit Metadata Columns

Every Bronze table is enriched with standard lineage metadata:
- `_ingested_at` (`timestamp`): UTC timestamp of load execution.
- `_source_system` (`string`): Fully qualified origin path (e.g. `samples.bakehouse.sales_transactions`).
- `_batch_id` (`string`): Batch identifier (`initial_load`).

---

## 4. Pipeline Artifacts

- **Ingestion Script:** [ingest_bronze.py](file:///c:/Miguel/Projects/lakehouse-build/03_execution/01_bronze/ingest_bronze.py)
- **Configuration:** [configuration.py](file:///c:/Miguel/Projects/lakehouse-build/notebooks/includes/configuration.py)
- **Audit Utilities:** [common_functions.py](file:///c:/Miguel/Projects/lakehouse-build/notebooks/includes/common_functions.py)
