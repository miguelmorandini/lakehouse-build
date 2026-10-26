# Governance Readiness & Architectural Design Decisions

**Methodology Module:** Module 1: Assessment  
**Sub-Stage:** `03_governance_readiness_and_design_decisions`  
**Status:** **COMPLETED & APPROVED**  
**Approval Date:** 2026-10-26  
**Methodology Transition Gate:** Assessment Phase Validated $\rightarrow$ Module 2 (Preparation) Cleared

---

## 1. Executive Summary & Governance Matrix

This document concludes the **Assessment** phase of the Lakehouse Build methodology. It establishes the architectural foundation, schema policies, and governance controls required for the implementation of the Sales and Franchise Performance use case.

| Domain Topic | Status | Formal Decision Summary | Technical Validation Outcome |
| :--- | :--- | :--- | :--- |
| **1. Supplier Strategy** | **Approved** | Include `sales_suppliers` as an optional conformed dimension. Absence of a supplier must never eliminate sales or franchise records. | **Validated:** Defined surrogate member `supplier_key = -1` (`Unknown Supplier`) for the 21 unmapped franchises. |
| **2. Payment Card Data** | **Approved** | Drop `cardNumber` completely starting at Silver layer. No masking, no exposure in Silver/Gold or logs. | **Validated:** Verified sanitized Silver projection omitting `cardNumber` and retaining `paymentMethod`. Raw Bronze restricted. |
| **3. Financial Units** | **Approved** | Adopt abstract monetary units. No assumption of USD, EUR, or other currencies. | **Validated:** Confirmed mathematical integrity (`totalPrice = qty * unitPrice`) and documented static `unitPrice = 3` limitation. |
| **4. Catalog Namespaces** | **Approved** | 3-level Unity Catalog hierarchy (`<catalog>.<layer>.<table>`). | **Validated:** Defined standard schemas (`bronze`, `silver`, `gold`) and Liquid Clustering layout (`CLUSTER BY`). |

---

## 2. Topic 1: Supplier Dimension Strategy (`sales_suppliers`)

### 2.1 Decisão Aprovada (Approved Decision)
- **Optional Dimension with `LEFT JOIN`:** The table `sales_suppliers` is integrated as an optional conformed dimension.
- **Zero Record Loss:** All downstream queries and transformations connecting franchises to suppliers must use `LEFT JOIN` semantics. A missing supplier relationship must **never** drop or filter out franchise stores or sales transactions.

### 2.2 Validação Técnica Concluída (Technical Validation Completed)
Empirical profiling established that `sales_franchises` contains 48 stores referencing `supplierID` values from `4000000` to `4000047`, but `sales_suppliers` only contains 27 records (`4000000` to `4000026`). 

To preserve 100% referential integrity without discarding the 21 stores (`3000027` to `3000047`), the following conformed fallback member is defined for `silver.dim_suppliers`:

#### Default Conformed Unknown Supplier Member:
| Attribute | Fallback Value | Semantics |
| :--- | :--- | :--- |
| `supplier_key` | `-1` | Standard Kimball surrogate key for missing/unassigned members. |
| `supplier_id` | `-1` | Logical identifier for unknown supplier. |
| `name` | `'Unknown / Unassigned Supplier'` | Explicit user-facing descriptor avoiding generic NULLs. |
| `ingredient` | `'Unassigned'` | Conveys absence of ingredient assignment. |
| `continent` | `'Unknown'` | Conformed geographic grouping. |
| `city` | `'Unknown'` | Conformed geographic grouping. |
| `district` | `'Unknown'` | Conformed geographic grouping. |
| `size` | `'N/A'` | Non-applicable operational tier. |
| `longitude` | `NULL` | Coordinates unknown. |
| `latitude` | `NULL` | Coordinates unknown. |
| `approved` | `'N/A'` | Approval status not applicable. |
| `_record_source` | `'SYSTEM_DEFAULT'` | Operational audit indicator. |

#### Transformation Rule in `silver.dim_franchises`:
```sql
SELECT
    f.franchiseID AS franchise_id,
    f.name AS franchise_name,
    f.city,
    f.district,
    f.zipcode,
    f.country,
    f.size AS franchise_size,
    f.longitude,
    f.latitude,
    COALESCE(s.supplierID, -1) AS supplier_key,
    current_timestamp() AS _ingested_at,
    current_timestamp() AS _updated_at
FROM bronze.sales_franchises f
LEFT JOIN bronze.sales_suppliers s ON f.supplierID = s.supplierID;
```

---

## 3. Topic 2: Payment Card Data (`cardNumber`) & PII/PCI Governance

### 3.1 Decisão Aprovada (Approved Decision)
- **Elimination from Silver & Gold:** The `cardNumber` attribute is entirely removed starting at the Silver layer.
- **No Masking Transport:** Because individual card numbers provide zero value to store performance, product velocity, or regional revenue analytics, masking is rejected in favor of complete elimination to eliminate PCI-DSS compliance scope.
- **Log Sanitization:** Transformation jobs, error handlers, and debug logs must never print or log raw `cardNumber` values.

### 3.2 Validação Técnica Concluída (Technical Validation Completed)
- **Bronze Raw Retention:** `bronze.sales_transactions` retains the raw column for audit and lineage completeness. Access to the Bronze schema is restricted via Unity Catalog permissions (`REVOKE ALL PRIVILEGES ON TABLE bronze.sales_transactions FROM ...; GRANT SELECT TO data_engineers_admin;`).
- **Silver Sanitized Projection Specification (`silver.fct_sales_transactions`):**

| Target Silver Column | Source Column | Target Type | Inclusion Status | Transformation / Notes |
| :--- | :--- | :--- | :--- | :--- |
| `transaction_id` | `transactionID` | `bigint` | **INCLUDED** | Primary key. |
| `customer_id` | `customerID` | `bigint` | **INCLUDED** | Foreign key to `silver.dim_customers`. |
| `franchise_id` | `franchiseID` | `bigint` | **INCLUDED** | Foreign key to `silver.dim_franchises`. |
| `transaction_datetime` | `dateTime` | `timestamp` | **INCLUDED** | Native timestamp. |
| `transaction_date` | `dateTime` | `date` | **DERIVED** | `CAST(dateTime AS DATE)` for clustering. |
| `product_name` | `product` | `string` | **INCLUDED** | Conformed product name. |
| `quantity` | `quantity` | `bigint` | **INCLUDED** | Quantity purchased. |
| `unit_price` | `unitPrice` | `bigint` | **INCLUDED** | Unit price in abstract units. |
| `total_price` | `totalPrice` | `bigint` | **INCLUDED** | Total transaction price. |
| `payment_method` | `paymentMethod` | `string` | **INCLUDED** | Categorical payment network (`mastercard`, `amex`, `visa`). |
| `cardNumber` | `cardNumber` | `bigint` | **DROPPED** | **EXCLUDED from Silver projection.** |
| `_ingested_at` | - | `timestamp` | **SYSTEM** | Audit ingestion timestamp. |
| `_updated_at` | - | `timestamp` | **SYSTEM** | Audit update timestamp. |

---

## 4. Topic 3: Financial Units & Pricing Metric Representation

### 4.1 Decisão Aprovada (Approved Decision)
- **Abstract Monetary Units:** All monetary figures are treated as abstract commercial currency units (`units`).
- **No Inferred Currencies:** The implementation strictly avoids labeling amounts as USD, EUR, JPY, or adding artificial exchange rate adjustments.

### 4.2 Validação Técnica Concluída (Technical Validation Completed)
- **Mathematical Integrity Proof:** Profiling confirmed that `totalPrice == (quantity * unitPrice)` in 100% of rows (3,333 / 3,333).
- **Documented Analytical Limitations for Gold:**
  1. *Invariant Unit Price:* Every product across all 3,333 transactions has `unitPrice = 3`. No price dispersion exists in the sample data.
  2. *Absence of Discounts and Taxes:* Gross amount equals net amount. Profit margins cannot be computed without cost-of-goods-sold (COGS) or ingredient purchase prices.
- **Gold Measure Definitions:**

| Gold Measure Name | SQL Formulation | Unit Description | Analytical Purpose |
| :--- | :--- | :--- | :--- |
| `total_sales_revenue_units` | `SUM(total_price)` | Abstract monetary units | Aggregate commercial volume. |
| `total_quantity_sold` | `SUM(quantity)` | Units of product | Physical sales volume. |
| `average_ticket_revenue_units`| `AVG(total_price)` | Abstract monetary units / tx | Spend per transaction. |
| `average_units_per_order` | `AVG(quantity)` | Units / tx | Order basket size. |
| `reference_unit_price` | `AVG(unit_price)` | Abstract monetary units / item| Base price baseline (= 3 units). |

---

## 5. Medallion Layer Architecture & Storage Design

### 5.1 Unity Catalog Namespace Hierarchy
```text
<catalog_name>
├── bronze/
│   ├── sales_transactions
│   ├── sales_customers
│   ├── sales_franchises
│   └── sales_suppliers
├── silver/
│   ├── fct_sales_transactions
│   ├── dim_customers
│   ├── dim_franchises
│   └── dim_suppliers
└── gold/
    ├── fct_daily_franchise_sales
    ├── fct_product_performance
    └── dim_customer_profiles
```

### 5.2 Storage Layout & Optimization Strategy
- **Format:** Delta Lake across all layers.
- **Liquid Clustering:**
  - `silver.fct_sales_transactions`: `CLUSTER BY (transaction_date, franchise_id)`
  - `gold.fct_daily_franchise_sales`: `CLUSTER BY (sales_date, franchise_id)`
- **Partitioning Rule:** Traditional Hive directory partitioning (`PARTITIONED BY`) is explicitly prohibited for this dataset due to the short 17-day operational duration, avoiding the small-file problem.

---

## 6. Sub-Stage 03 Completion Checklist

All technical validations have been formalized and approved:
- [x] Defined conformed fallback member (`supplier_key = -1`, `Unknown Supplier`) for the 21 unmapped franchises.
- [x] Defined sanitized projection for `silver.fct_sales_transactions` completely dropping `cardNumber`.
- [x] Formulated abstract financial measures and documented pricing limitations for Gold.
- [x] Specified Unity Catalog 3-level namespaces and Delta Liquid Clustering strategy.
- [x] Sub-stage 03 formally marked as **COMPLETED**.
- [x] Assessment Phase transition gate cleared for Module 2: Preparation.
