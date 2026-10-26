# Governance Readiness & Architectural Design Decisions

**Methodology Module:** Module 1: Assessment  
**Sub-Stage:** `03_governance_readiness_and_design_decisions`  
**Status:** **IN PROGRESS (NOT COMPLETED)**  
*Note: In accordance with methodology standards, this sub-stage remains active and open until all technical validations are documented and formally reviewed.*

---

## 1. Governance & Architectural Decisions Matrix

Each architectural item below is classified into one of three governing categories:
- **Decisão Aprovada (Approved Decision):** Formally accepted design choice governing data layers and pipeline logic.
- **Validação Técnica Pendente (Pending Technical Validation):** Empirical or technical verification required before final sign-off.
- **Premissa (Premise / Assumption):** Foundational operational assumption underpinning the design.

---

## 2. Topic 1: Supplier Dimension Strategy (`sales_suppliers`)

### 2.1 Decisão Aprovada (Approved Decision)
- **Optional Dimension with `LEFT JOIN`:** The table `sales_suppliers` is approved for inclusion as an **optional conformed dimension**.
- **Sales Record Preservation:** Ingestion and transformation into Silver and Gold layers must strictly use `LEFT JOIN` semantics between franchises and suppliers. The absence of a matched supplier in the master table must **never** eliminate, filter out, or drop sales transactions or franchise records.

### 2.2 Validação Técnica Pendente (Pending Technical Validation)
- **Cardinality Validation:** Validate the exact cardinality and mapping between `sales_franchises.supplierID` and `sales_suppliers.supplierID`. Profiling identified that 21 of 48 franchises reference `supplierID` values (`4000027` to `4000047`) that do not exist in `sales_suppliers` (`4000000` to `4000026`).
- **Fallback Rule Documentation:** Prior to locking in the schema, document the exact surrogate key substitution rule (e.g., assigning surrogate key `-1` or `UNKNOWN` with descriptive attributes like `name: 'Unknown Supplier'`, `ingredient: 'Unassigned'`, `approved: 'N/A'`) to maintain referential integrity in downstream dimensional models.

### 2.3 Premissa (Premise)
- Commercial store sales and revenue performance can be analyzed independently of supplier allocations. Franchises operate as standalone retail entities even when supply chain metadata is incomplete in the sample source.

---

## 3. Topic 2: Payment Card Data (`cardNumber`) & PII/PCI Governance

### 3.1 Decisão Aprovada (Approved Decision)
- **Drop from Silver and Gold:** The column `cardNumber` is approved for complete removal starting at the **Silver layer**.
- **No Downstream Exposure:** The column will **not** be masked and will **not** be carried forward into Silver or Gold entities. The field is not required for the defined commercial and franchise analytics use case.
- **Log and Report Sanitization:** Ingestion notebooks, audit logs, error messages, and profiling reports must strictly avoid printing or logging raw `cardNumber` values.

### 3.2 Validação Técnica Pendente (Pending Technical Validation)
- **Exclusion Verification:** In the Silver transformation specification, ensure the projection list explicitly omits `cardNumber` while retaining `paymentMethod` (`mastercard`, `visa`, `amex`) for commercial payment breakdown.
- **Raw Layer Retention Check:** Verify that `cardNumber` remains sealed inside the append-only Bronze raw table (`bronze.sales_transactions`) with access strictly restricted to elevated administrative roles (complying with PCI-DSS retention boundaries).

### 3.3 Premissa (Premise)
- Payment method categorization (`paymentMethod`) provides sufficient commercial insight for business users without requiring individual card account identifiers.

---

## 4. Topic 3: Financial Units & Pricing Metric Representation

### 4.1 Decisão Aprovada (Approved Decision)
- **Abstract Monetary Units:** Approved the adoption of **abstract, unspecified monetary units**.
- **No Currency Assumptions:** The Lakehouse implementation must **not** assume USD, EUR, JPY, or any other specific currency, as no currency attribute exists in the source data.

### 4.2 Validação Técnica Pendente (Pending Technical Validation)
- **Mathematical Integrity Validation:** Empirical profiling proved that `totalPrice == (quantity * unitPrice)` across 100% of the 3,333 records.
- **Document Pricing Limitations:** Document analytical limitations before defining Gold financial metrics:
  - `unitPrice` is invariant at `3` across all 6 products and all transactions.
  - Absence of tax, discount, surcharge, or exchange rate columns.
  - Gold business metrics must be labeled as `revenue_units`, `unit_price`, and `total_amount` rather than using currency symbols (`$`, `€`, `¥`).

### 4.3 Premissa (Premise)
- Transaction values represent consistent commercial units across all geographic locations in the dataset, enabling direct arithmetic aggregation across stores and products without currency conversion.

---

## 5. Medallion Layer Architectural Blueprint (Draft Baseline)

```
[ Raw Source: samples.bakehouse ]
               │
               ▼
┌────────────────────────────────────────────────────────┐
│ Bronze Layer (Raw Append-Only Storage)                 │
│ - bronze.sales_transactions (includes raw cardNumber) │
│ - bronze.sales_customers                               │
│ - bronze.sales_franchises                              │
│ - bronze.sales_suppliers                               │
│ Audit Columns: _ingested_at, _source_file, _batch_id   │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ Silver Layer (Cleaned, Conformed, Sanitized)           │
│ - silver.fct_sales_transactions (cardNumber DROPPED)   │
│ - silver.dim_customers                                 │
│ - silver.dim_franchises                                │
│ - silver.dim_suppliers (LEFT JOIN, fallback -1 UNKNOWN)│
│ Write Mode: MERGE / UPSERT, Delta Lake                 │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ Gold Layer (Curated Dimensional Star Schema)           │
│ - gold.fct_daily_franchise_sales                       │
│ - gold.dim_products                                    │
│ - gold.dim_franchises                                  │
│ - gold.dim_customers                                   │
│ Metrics: Abstract financial revenue units              │
└────────────────────────────────────────────────────────┘
```

---

## 6. Sub-Stage 03 Completion Checklist (Outstanding Items)

To declare Sub-stage 03 complete and advance to Module 2 (Preparation), the following validations must be finalized:
- [ ] Complete formal schema definition for the Silver fallback record for unassigned suppliers (`supplierID = -1`).
- [ ] Define Unity Catalog namespace and access control lists (ACLs) separating Bronze raw access from Silver/Gold consumption.
- [ ] Document final data dictionary for all Gold measures with abstract financial units.
- [ ] Technical sign-off from project lead on governance decisions.
