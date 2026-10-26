# Source Workload & Data Profiling Report: Bakehouse Sales Domain

**Profiling Date:** 2026-10-26  
**Catalog / Schema:** `samples.bakehouse`  
**Execution Environment:** Databricks Free Edition (Serverless Starter Warehouse `13a04a10a74d24cc`)  
**Methodology Sub-Stage:** Module 1: Assessment — `02_workload_and_source_profiling`  
**Access Mode:** Strictly Read-Only (SQL Statement Execution API)

---

## 1. Executive Summary

This report documents the empirical profiling of the four candidate source tables in `samples.bakehouse`. All metrics, distributions, and relationship tests were executed against live tables in the Databricks workspace.

### Summary Metrics Across Sources

| Source Table | Physical Format | Total Rows | Column Count | Null Count (All Columns) | Duplicate PKs | Observed Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sales_transactions` | Delta Lake | **3,333** | 10 | 0 (0.00%) | 0 | Highly consistent transactional dataset across a 17-day operational window. |
| `sales_customers` | Delta Lake | **300** | 12 | 0 (0.00%) | 0 | Master customer dimension; 100% active in transactions. |
| `sales_franchises` | Delta Lake | **48** | 10 | 0 (0.00%) | 0 | Master store dimension; 100% active in transactions; contains supply-chain anomaly. |
| `sales_suppliers` | Delta Lake | **27** | 10 | 0 (0.00%) | 0 | Specialized ingredient supplier master; 100% approved. |

---

## 2. Deep-Dive Table Profiles

### 2.1 Table: `samples.bakehouse.sales_transactions`
The transactional fact table recording purchases across the franchise network.

#### Structural & Quality Profile
| Column Name | Physical Type | Null Count | Null % | Distinct Values | Min Value | Max Value | Observed Semantics |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `transactionID` | `bigint` | 0 | 0.00% | 3,333 | 1,000,000 | 1,003,332 | **Primary Key** (100% unique, zero duplicates). |
| `customerID` | `bigint` | 0 | 0.00% | 300 | 2,000,000 | 2,000,299 | Foreign key to `sales_customers`. |
| `franchiseID` | `bigint` | 0 | 0.00% | 48 | 3,000,000 | 3,000,047 | Foreign key to `sales_franchises`. |
| `dateTime` | `timestamp` | 0 | 0.00% | 3,333 | 2024-05-01 00:03:28 | 2024-05-17 11:48:38 | Purchase timestamp (17-day operational window). |
| `product` | `string` | 0 | 0.00% | 6 | Austin Almond Biscotti | Tokyo Tidbits | Product name (6 distinct bakery items). |
| `quantity` | `bigint` | 0 | 0.00% | 25 | 1 | 60 | Units purchased (mean: 6.65 units/tx). |
| `unitPrice` | `bigint` | 0 | 0.00% | 1 | 3 | 3 | Constant integer value of 3 across all rows. |
| `totalPrice` | `bigint` | 0 | 0.00% | 26 | 3 | 180 | Transaction total (mean: 19.94). |
| `paymentMethod` | `string` | 0 | 0.00% | 3 | amex | visa | Payment card network (`mastercard`, `amex`, `visa`). |
| `cardNumber` | `bigint` | 0 | 0.00% | 3,333 | 340007480537384 | 5599856254505085 | Unmasked 15-16 digit payment card number. |

#### Product Distribution Breakdown
| Product Name | Transactions | Total Units Sold | Total Revenue | Min Unit Price | Max Unit Price |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Golden Gate Ginger** | 586 (17.58%) | 3,865 | 11,595 | 3 | 3 |
| **Tokyo Tidbits** | 583 (17.49%) | 3,662 | 10,986 | 3 | 3 |
| **Outback Oatmeal** | 561 (16.83%) | 3,733 | 11,199 | 3 | 3 |
| **Pearly Pies** | 550 (16.50%) | 3,595 | 10,785 | 3 | 3 |
| **Austin Almond Biscotti** | 530 (15.90%) | 3,716 | 11,148 | 3 | 3 |
| **Orchard Oasis** | 523 (15.69%) | 3,586 | 10,758 | 3 | 3 |
| **Total** | **3,333** | **22,157** | **66,471** | - | - |

#### Payment Method Distribution
| Payment Method | Transaction Count | % of Volume | Total Revenue | % of Revenue |
| :--- | :--- | :--- | :--- | :--- |
| **Mastercard** | 1,144 | 34.32% | 22,263 | 33.49% |
| **Amex** | 1,106 | 33.18% | 22,434 | 33.75% |
| **Visa** | 1,083 | 32.49% | 21,774 | 32.76% |

---

### 2.2 Table: `samples.bakehouse.sales_customers`
The master dimension for customer profiles and geographic segmentation.

#### Structural & Quality Profile
| Column Name | Physical Type | Null Count | Null % | Distinct Values | Min / Max Value | Observed Semantics |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `customerID` | `bigint` | 0 | 0.00% | 300 | 2,000,000 / 2,000,299 | **Primary Key** (100% unique sequence). |
| `first_name` | `string` | 0 | 0.00% | 226 | - | Customer first name. |
| `last_name` | `string` | 0 | 0.00% | 271 | - | Customer surname. |
| `email_address` | `string` | 0 | 0.00% | 300 | - | Customer email (100% unique). |
| `phone_number` | `string` | 0 | 0.00% | 300 | - | Customer contact phone. |
| `address` | `string` | 0 | 0.00% | 300 | - | Street address. |
| `city` | `string` | 0 | 0.00% | 158 | - | City of residence. |
| `state` | `string` | 0 | 0.00% | 107 | - | State/region. |
| `country` | `string` | 0 | 0.00% | 3 | Australia, Japan, US | 3 operating countries. |
| `continent` | `string` | 0 | 0.00% | 3 | Asia, North America, Oceania | 3 operating continents. |
| `postal_zip_code` | `bigint` | 0 | 0.00% | 290 | - | Postal zip code. |
| `gender` | `string` | 0 | 0.00% | 2 | female, male | Female: 151 (50.3%), Male: 149 (49.7%). |

---

### 2.3 Table: `samples.bakehouse.sales_franchises`
The store master table detailing franchise properties and locations.

#### Structural & Quality Profile
| Column Name | Physical Type | Null Count | Null % | Distinct Values | Min / Max Value | Observed Semantics |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `franchiseID` | `bigint` | 0 | 0.00% | 48 | 3,000,000 / 3,000,047 | **Primary Key** (100% unique sequence). |
| `name` | `string` | 0 | 0.00% | 48 | - | Store name (e.g., "Golden Gate Goodies"). |
| `city` | `string` | 0 | 0.00% | 48 | - | Store city (1 store per city). |
| `district` | `string` | 0 | 0.00% | 48 | - | District / neighborhood. |
| `zipcode` | `string` | 0 | 0.00% | 48 | - | Store postal code. |
| `country` | `string` | 0 | 0.00% | 9 | Japan (20), US (16), Australia (6), Europe (6) | Country breakdown. |
| `size` | `string` | 0 | 0.00% | 5 | XL (10), M (10), S (10), L (9), XXL (9) | Store size tier. |
| `longitude` | `double` | 0 | 0.00% | 48 | - | Geographic longitude. |
| `latitude` | `double` | 0 | 0.00% | 48 | - | Geographic latitude. |
| `supplierID` | `bigint` | 0 | 0.00% | 48 | 4,000,000 / 4,000,047 | Candidate FK to suppliers (**Anomaly detected**). |

---

### 2.4 Table: `samples.bakehouse.sales_suppliers`
The supplier master table representing primary ingredient vendors.

#### Structural & Quality Profile
| Column Name | Physical Type | Null Count | Null % | Distinct Values | Min / Max Value | Observed Semantics |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `supplierID` | `bigint` | 0 | 0.00% | 27 | 4,000,000 / 4,000,026 | **Primary Key** (100% unique sequence). |
| `name` | `string` | 0 | 0.00% | 27 | - | Supplier business name. |
| `ingredient` | `string` | 0 | 0.00% | 27 | cacao, vanilla, almonds, etc. | 27 distinct ingredients (1 per supplier). |
| `continent` | `string` | 0 | 0.00% | 5 | Africa, Asia, Europe, North America, South America | Global supplier sourcing. |
| `city` | `string` | 0 | 0.00% | 27 | - | Supplier headquarters city. |
| `district` | `string` | 0 | 0.00% | 27 | - | Supplier district. |
| `size` | `string` | 0 | 0.00% | 5 | L, M, S, XL, XXL | Supplier operational size. |
| `longitude` | `double` | 0 | 0.00% | 27 | - | Supplier longitude. |
| `latitude` | `double` | 0 | 0.00% | 27 | - | Supplier latitude. |
| `approved` | `string` | 0 | 0.00% | 1 | Y | 100% approved status (`Y`). |

---

## 3. Pricing Math Investigation (`totalPrice` vs `quantity * unitPrice`)

### Empirical Test Execution
We verified whether `totalPrice` is an exact mathematical product of `quantity` and `unitPrice`:

```sql
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN totalPrice = (quantity * unitPrice) THEN 1 ELSE 0 END) as price_matches_math,
    SUM(CASE WHEN totalPrice != (quantity * unitPrice) THEN 1 ELSE 0 END) as price_mismatches_math,
    COUNT(DISTINCT unitPrice) as distinct_unit_prices
FROM samples.bakehouse.sales_transactions;
```

### Results
- `total_rows`: **3,333**
- `price_matches_math`: **3,333** (100.00%)
- `price_mismatches_math`: **0** (0.00%)
- `distinct_unit_prices`: **1** (every row has `unitPrice = 3`)

### Finding & Limitation
- **Mathematical Integrity:** In 100% of the transactions, `totalPrice == quantity * unitPrice`.
- **Pricing Reality:** `unitPrice` is uniformly `3` across all 6 products and all 3,333 transactions.
- **Strict Boundary:** We do **not** assume a monetary currency (e.g., USD, EUR, JPY) as no currency column exists. Currency must be confirmed or treated as abstract currency units.

---

## 4. Empirical Key & Relationship Verification

### Test 1: `sales_transactions.customerID` $\leftrightarrow$ `sales_customers.customerID`
```sql
SELECT 
    COUNT(t.customerID) as total_tx_rows,
    COUNT(c.customerID) as matched_cust_rows,
    SUM(CASE WHEN c.customerID IS NULL THEN 1 ELSE 0 END) as orphaned_cust_txs
FROM samples.bakehouse.sales_transactions t
LEFT JOIN samples.bakehouse.sales_customers c ON t.customerID = c.customerID;
```
- **Total Transactions:** 3,333
- **Matched Customer Rows:** 3,333
- **Orphaned Transactions:** **0** (100.00% match rate)
- **Active Customers:** All 300 customers in `sales_customers` have at least 1 transaction in `sales_transactions`.

### Test 2: `sales_transactions.franchiseID` $\leftrightarrow$ `sales_franchises.franchiseID`
```sql
SELECT 
    COUNT(t.franchiseID) as total_tx_rows,
    COUNT(f.franchiseID) as matched_franchise_rows,
    SUM(CASE WHEN f.franchiseID IS NULL THEN 1 ELSE 0 END) as orphaned_franchise_txs
FROM samples.bakehouse.sales_transactions t
LEFT JOIN samples.bakehouse.sales_franchises f ON t.franchiseID = f.franchiseID;
```
- **Total Transactions:** 3,333
- **Matched Franchise Rows:** 3,333
- **Orphaned Transactions:** **0** (100.00% match rate)
- **Active Franchises:** All 48 franchises in `sales_franchises` have at least 1 transaction in `sales_transactions`.

### Test 3: `sales_franchises.supplierID` $\leftrightarrow$ `sales_suppliers.supplierID` (Critical Anomaly)
```sql
SELECT 
    COUNT(f.supplierID) as total_franchise_rows,
    COUNT(s.supplierID) as matched_supplier_rows,
    SUM(CASE WHEN s.supplierID IS NULL THEN 1 ELSE 0 END) as orphaned_supplier_franchises,
    MIN(f.supplierID) as min_f_supplier_id,
    MAX(f.supplierID) as max_f_supplier_id
FROM samples.bakehouse.sales_franchises f
LEFT JOIN samples.bakehouse.sales_suppliers s ON f.supplierID = s.supplierID;
```
- **Total Franchises:** 48
- **Matched Supplier Rows:** **27**
- **Orphaned Franchises:** **21** (43.75% failure rate / 56.25% match rate)
- **Observed Anomaly Detail:**
  - `sales_franchises.supplierID` was generated sequentially from `4000000` to `4000047` (48 distinct IDs).
  - `sales_suppliers` only contains 27 records (`4000000` through `4000026`).
  - Franchises `3000027` through `3000047` reference non-existent suppliers (`4000027` to `4000047`).
  - **Impact:** An inner join between franchises and suppliers loses 21 out of 48 stores. A left join produces NULL supplier attributes for 43.75% of the stores.

---

## 5. Temporal & Operational Distribution

- **Total Operational Span:** 16.5 days (`2024-05-01 00:03:28` to `2024-05-17 11:48:38`).
- **Cadence:** Uniform distribution of ~180 to ~229 transactions daily from May 1 to May 16, ending at midday on May 17 with 106 transactions.
- **Date Gaps:** None. Continuous daily transaction flow.
- **Implication:** The dataset represents a focused historical sample window rather than multi-year historical depth. Partitioning by year/month would result in a single partition, confirming that Databricks **Liquid Clustering** (`CLUSTER BY`) is vastly superior to Hive-style directory partitioning.

---

## 6. Governance, Security & Quality Observations

1. **Card Number Exposure (PCI-DSS Risk):**
   - The column `cardNumber` contains raw 15-to-16-digit payment card numbers for all 3,333 transactions without masking (e.g., `343808569426192`, `2244626981238094`).
   - *Recommendation:* In Silver/Gold layers, this column must be masked (e.g., `**** **** **** 1234`) or tokenized/hashed to prevent compliance exposure.
2. **Customer PII Exposure:**
   - Full names, physical addresses, email addresses, and phone numbers are present in plain text in `sales_customers`.
   - *Recommendation:* Define access restrictions or role-based column masking in Unity Catalog during Sub-stage 3.

---

## 7. Observed Facts vs. Hypotheses & Open Decisions

### Observed Facts (Empirically Proven)
1. `sales_transactions` has 3,333 rows, with 100% non-null values across all 10 columns.
2. `transactionID`, `customerID`, `franchiseID`, and `supplierID` are unique primary keys in their respective tables.
3. Every transaction maps cleanly to an existing customer (300/300) and franchise (48/48).
4. `totalPrice` exactly equals `quantity * unitPrice` in 100% of transactions.
5. `unitPrice` is invariant at `3` across all records and products.
6. `sales_franchises.supplierID` has 21 orphaned records when joined against `sales_suppliers`.

### Hypotheses & Open Decisions Requiring Approval
1. **Currency Representation:** No currency code exists. Should we document revenue as generic currency units or assume USD?
2. **Supplier Dimension Inclusion:** Given that 21 of 48 franchises have no matching supplier record, should we:
   - *Option A:* Model `sales_suppliers` as an optional left-joined dimension with a conformed `Unknown / Unassigned Supplier` record for missing IDs?
   - *Option B:* Exclude `sales_suppliers` from the core commercial sales star schema and keep it purely as an operational diagnostic view?
3. **Card Number Handling:** Should `cardNumber` be masked (retaining only last 4 digits) or completely dropped in the Silver/Gold layers?
