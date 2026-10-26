# Gold Layer Execution & Analytical Marts Report

**Layer:** Gold (Consumption & Dimensional Marts)  
**Catalog / Schema:** `lakehouse_build.gold`  
**Format:** Delta Lake (Liquid Clustered / Aggregated)  
**Execution Status:** **COMPLETED & VERIFIED**

---

## 1. Overview & Objectives

The Gold layer delivers curated, high-performance dimensional star schema marts designed for business intelligence, executive KPI monitoring, and franchise operational analytics.

Key Architectural Highlights:
- **Abstract Monetary Units:** Formulated as `total_sales_revenue_units` and `average_ticket_revenue_units` in strict alignment with governance decisions.
- **Liquid Clustering:** Configured on `(sales_date, franchise_id)` to optimize date-range and store-level reporting.
- **Supply-Chain Integrity:** Seamlessly surfaces conformed store attributes and unknown supplier descriptions (`Unknown / Unassigned Supplier`) without dropping unmapped stores.

---

## 2. Gold Tables & Metric Specifications

### 2.1 Table: `gold.fct_daily_franchise_sales`
Pre-aggregated daily store performance mart containing store geographical metadata, store tier, and daily commercial metrics.

| Metric / Attribute | Data Type | Semantics / Formulation |
| :--- | :--- | :--- |
| `sales_date` | `date` | Transaction date (Cluster key). |
| `franchise_id` | `bigint` | Store identifier (Cluster key). |
| `franchise_name` | `string` | Store commercial name. |
| `store_city` | `string` | Store city. |
| `store_country` | `string` | Store country (9 distinct countries). |
| `store_size` | `string` | Store operational size tier (`S`, `M`, `L`, `XL`, `XXL`). |
| `supplier_key` | `bigint` | Foreign key to `silver.dim_suppliers` (conformed `-1` for unmapped stores). |
| `supplier_name` | `string` | Assigned vendor name or `'Unknown / Unassigned Supplier'`. |
| `primary_ingredient` | `string` | Primary supplied ingredient or `'Unassigned'`. |
| `total_sales_revenue_units` | `bigint` | `SUM(total_price)` — Daily revenue in abstract currency units. |
| `total_quantity_sold` | `bigint` | `SUM(quantity)` — Total product units sold. |
| `total_transactions` | `bigint` | `COUNT(transaction_id)` — Total transaction count. |
| `average_ticket_revenue_units` | `double` | `ROUND(AVG(total_price), 2)` — Average transaction ticket. |
| `average_units_per_order` | `double` | `ROUND(AVG(quantity), 2)` — Basket size. |
| `reference_unit_price` | `double` | Reference baseline price (= 3.0). |
| `_refreshed_at` | `timestamp` | Mart build timestamp. |

### 2.2 Table: `gold.fct_product_performance`
Product mix velocity and commercial contribution mart across all 6 core bakery items.

| Product Name | Total Sales Revenue Units | Revenue Share % | Total Quantity Sold | Unit Share % | Total Transactions | Avg Basket Size |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Golden Gate Ginger** | **11,595** | 17.44% | 3,865 | 17.44% | 586 | 6.60 |
| **Outback Oatmeal** | **11,199** | 16.85% | 3,733 | 16.85% | 561 | 6.65 |
| **Austin Almond Biscotti** | **11,148** | 16.77% | 3,716 | 16.77% | 530 | 7.01 |
| **Tokyo Tidbits** | **10,986** | 16.53% | 3,662 | 16.53% | 583 | 6.28 |
| **Pearly Pies** | **10,785** | 16.22% | 3,595 | 16.22% | 550 | 6.54 |
| **Orchard Oasis** | **10,758** | 16.18% | 3,586 | 16.18% | 523 | 6.86 |
| **Network Total** | **66,471** | **100.00%** | **22,157** | **100.00%** | **3,333** | - |

---

## 3. End-to-End Medallion Reconciliation Proof

| Layer | Total Transactions | Total Units Sold | Total Revenue (Units) | Variance vs Source |
| :--- | :--- | :--- | :--- | :--- |
| **Raw Source (`samples.bakehouse`)** | 3,333 | 22,157 | 66,471 | Baseline |
| **Bronze (`lakehouse_build.bronze`)** | 3,333 | 22,157 | 66,471 | **0.00% (Exact match)** |
| **Silver (`lakehouse_build.silver`)** | 3,333 | 22,157 | 66,471 | **0.00% (Exact match)** |
| **Gold (`lakehouse_build.gold`)** | 3,333 | 22,157 | 66,471 | **0.00% (Exact match)** |

---

## 4. Pipeline Artifacts

- **Pipeline Script:** [build_gold.py](file:///c:/Miguel/Projects/lakehouse-build/03_execution/03_gold/build_gold.py)
- **Configuration:** [configuration.py](file:///c:/Miguel/Projects/lakehouse-build/notebooks/includes/configuration.py)
