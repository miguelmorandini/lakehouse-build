# Use Case Definition & Scope Charter: Sales & Franchise Performance Analytics

This document defines the analytical charter, business objectives, consumer personas, source scoping, and acceptance criteria for the initial implementation of the **Lakehouse Build** project.

---

## 1. Executive Summary & Business Context

The primary goal of this use case is to deliver an end-to-end analytical foundation for evaluating **Sales Performance and Franchise Operations** across the Bakehouse franchise network. 

By analyzing transaction data alongside customer, franchise, and supplier dimensions, the Lakehouse will enable leadership and operational teams to monitor revenue, volume, customer concentration, and geographic performance.

---

## 2. Business Objectives & Analytical Questions

### 2.1 Core Objectives
- Establish a single source of truth for sales transactions and commercial performance.
- Evaluate franchise efficiency, sales volume, and geographical footprint.
- Analyze customer purchasing behavior, payment method distribution, and demographic patterns.
- Investigate supply-chain dependency and supplier coverage across operating franchises.

### 2.2 Key Analytical Questions
1. **Franchise Performance:**
   - What are the top and bottom performing franchises by total revenue and transaction volume?
   - How does sales performance vary across franchise store sizes (`S`, `M`, `L`, `XL`, `XXL`) and geographical locations (countries, cities, districts)?
2. **Product & Revenue Analysis:**
   - What is the sales volume and revenue contribution of each of the 6 core bakery products?
   - What is the daily trend in sales volume and revenue over the observed operational period?
3. **Customer & Payment Insights:**
   - What is the distribution of transactions and revenue across customer geographic regions (continents, states, cities) and genders?
   - How do payment methods (`mastercard`, `visa`, `amex`) distribute across sales volume?
4. **Supply Chain & Supplier Footprint:**
   - Which suppliers supply which franchises, and what ingredients are tied to active store networks?
   - What are the operational impacts of supplier coverage gaps identified during profiling?

---

## 3. Data Source Scoping

### 3.1 In-Scope Candidate Sources (Catalog: `samples.bakehouse`)
The following 4 source tables have been selected for this use case:

| Source Table | Business Domain | Role in Use Case |
| :--- | :--- | :--- |
| `samples.bakehouse.sales_transactions` | Commercial Transactions | Central transactional fact table containing order details, timestamp, product, quantities, prices, and payment methods. |
| `samples.bakehouse.sales_customers` | Customer Master | Conformed customer dimension providing demographic, geographic, and contact attributes. |
| `samples.bakehouse.sales_franchises` | Franchise Operations | Franchise store dimension providing location coordinates, store size, and assigned supplier reference. |
| `samples.bakehouse.sales_suppliers` | Supply Chain | Supplier master providing ingredient specialty, location, and approval status. |

### 3.2 Out-of-Scope Sources
The following tables are explicitly excluded from this initial use case:
- `samples.bakehouse.media_customer_reviews`: Unstructured text sentiment and media feedback are deferred to a subsequent AI/NLP iteration.
- `samples.bakehouse.media_gold_reviews_chunked`: Pre-chunked embeddings/vector-search tables are outside the scope of commercial and dimensional sales analytics.

---

## 4. Target Personas & Delivery Model

| Persona | Primary Needs | Consumption Model |
| :--- | :--- | :--- |
| **Franchise Operations Manager** | Store-level sales KPIs, store size performance, regional benchmarks. | Curated Gold dimensional models and BI dashboards. |
| **Commercial / Financial Analyst** | Revenue trends, product mix velocity, daily cash-flow and payment splits. | SQL queries against Gold marts and ad-hoc aggregations. |
| **Supply Chain Coordinator** | Supplier allocations, ingredient distribution, franchise dependency. | Silver-level conformed reports and supplier integrity views. |

- **Delivery Cadence:** Daily batch refresh (simulated historical batch processing).
- **Latency Expectation:** T+1 reporting availability upon pipeline completion.

---

## 5. Proposed Acceptance Criteria

The implementation will be accepted when:
1. **Scope Adherence:** All analytical queries for the defined business questions execute successfully against the Gold layer without requiring raw table joins.
2. **Lineage Completeness:** 100% of the metrics in the Gold layer trace back cleanly through Silver conformed entities to Bronze raw audit records.
3. **Data Integrity & Reconciliation:**
   - Bronze-to-Silver reconciliation shows zero unexplained record loss from `sales_transactions` (3,333 rows).
   - Referential integrity checks between transactions and dimensions pass at 100% (valid customer and franchise IDs).
   - Known supplier integrity gaps (21 orphaned franchises) are gracefully handled in Silver without pipeline termination.
4. **Governance Conformance:** Sensitive customer and payment fields (such as `cardNumber`) are appropriately masked/protected before reaching Gold analytics.
