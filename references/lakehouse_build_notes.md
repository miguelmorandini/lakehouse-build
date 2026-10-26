# Lakehouse Build Methodology Notes & Implementation Guidelines

This reference document maintains the official notes, verified principles, and architectural guidelines derived from the **Lakehouse Build** methodology, while cleanly separating methodology rules from project-specific implementation choices.

---

## 1. Official Methodology vs. Project Implementation Boundaries

To preserve methodological integrity:
- **Official Methodology Grounding:** All conceptual definitions, phase names, and lifecycle workflows must strictly follow confirmed notes from the Lakehouse Build methodology. No official concepts, sub-stages, or certification criteria may be invented.
- **Project Decisions Separation:** Decisions regarding tooling (Databricks Free Edition), sample catalogs (`samples.bakehouse`), and repository subdirectories (`01_bronze`, `02_silver`, etc.) are operational choices specific to this project and must not be confused with official methodology standards.

---

## 2. Confirmed Lakehouse Build Methodology Modules

The methodology prescribes three sequential modules:

### Module 1: Assessment (Active Module)
- **Status:** Officially confirmed with three mandatory sub-stages:
  1. `01_use_case_definition_and_scope`: Clarifying business problems, consumer personas, analytical goals, delivery SLAs, and explicit in-scope/out-of-scope boundaries.
  2. `02_workload_and_source_profiling`: Performing deep empirical inspection of real source assets—evaluating physical schemas, record volumes, null rates, cardinality, and detecting candidate keys and joins without assumptions.
  3. `03_governance_readiness_and_design_decisions`: Establishing catalog namespaces, medallion architecture layer roles (Bronze, Silver, Gold), access boundaries, and Delta storage formats.
- **Gatekeeper Rule:** Assessment must be fully completed, documented, and approved before proceeding to Module 2.

### Module 2: Preparation (Pending Assessment Sign-off)
- **Status:** Confirmed high-level module.
- **Sub-stage Status:** Specific sub-stages and naming conventions have **not** been officially confirmed. The repository uses `02_preparation/` as a working directory. Official sub-stages will be documented once confirmed.

### Module 3: Execution (Pending Preparation Sign-off)
- **Status:** Confirmed high-level module.
- **Sub-stage Status:** The folders (`01_bronze`, `02_silver`, `03_gold`, `04_testing_and_validation`) under `03_execution/` represent practical development organization for this project. They must **not** be presented as official Lakehouse Build methodology sub-stages unless confirmed.

---

## 3. Project-Specific Implementation Decisions & Operational Rules

### 3.1 Data Exploration & Catalog Strategy
- **Platform:** Databricks Free Edition.
- **Initial Catalog Target:** `samples.bakehouse`.
- **Selective Ingestion:** Do **not** assume all tables in `samples.bakehouse` will be ingested. Table selection is strictly determined by the scoped use case defined during Assessment.

### 3.2 Anti-Hallucination & Empirical Verification Rules
- **No Inferred Schemas or Keys:** Never declare a column as a primary key or foreign key based on naming convention alone. Uniqueness and referential integrity must be tested with SQL/Spark assertions against real data.
- **No Inferred Business Rules:** Do not invent metrics, conversion logic, status meanings, or formulas without verification against source data or explicit user confirmation.
- **No Synthetic Data:** Do not generate mock, synthetic, or fictitious data files.
- **Safety Controls:** No destructive `DROP`, `DELETE`, or `ALTER` statements against remote catalogs, and no automated resource publishing without prior user approval.

---

## 4. Methodology Decision Log

| Date | Topic | Decision / Finding | Methodology vs Project | Status |
| :--- | :--- | :--- | :--- | :--- |
| 2026-10-26 | Module Sequencing | Strictly enforce Assessment $\rightarrow$ Preparation $\rightarrow$ Execution. No code before Assessment sign-off. | Methodology | Confirmed |
| 2026-10-26 | Assessment Sub-stages | Use confirmed 3 sub-stages: scope, profiling, and governance readiness. | Methodology | Confirmed |
| 2026-10-26 | Target Catalog | Start exploration with `samples.bakehouse` in Databricks Free Edition; tables selected selectively. | Project | Confirmed |
| 2026-10-26 | Execution Folders | Use `01_bronze`, `02_silver`, `03_gold`, `04_testing_and_validation` as working folders only. | Project | Working Baseline |
| 2026-10-26 | Domain Scope | Prioritize Sales & Franchise Performance (`sales_*` tables). Defer `media_*` to later iterations. | Project | Confirmed |
| 2026-10-26 | Pricing Math | Confirmed `totalPrice = quantity * unitPrice` across 100% of rows (3,333/3,333). Unit price is uniformly 3. | Project | Empirically Proven |
| 2026-10-26 | Referential Integrity | Transactions $\rightarrow$ Customers (100%), Transactions $\rightarrow$ Franchises (100%). | Project | Empirically Proven |
| 2026-10-26 | Supplier Anomaly | Franchises $\rightarrow$ Suppliers has 21 orphaned records (43.75% missing suppliers). Requires handling strategy. | Project | Identified Anomaly |
| 2026-10-26 | Governance / Security | `cardNumber` in `sales_transactions` is unmasked 15-16 digit numbers. Masking required in Silver/Gold. | Project | Flagged for Design |
| 2026-10-26 | Supplier Inclusion | Approved optional dimension via `LEFT JOIN` (no sales dropped). Validation pending for `-1` fallback rule. | Project | Decisão Aprovada |
| 2026-10-26 | Card Data Removal | Approved dropping `cardNumber` from Silver/Gold entirely. Do not mask or log values. | Project | Decisão Aprovada |
| 2026-10-26 | Monetary Unit | Approved abstract/unspecified units (no USD assumption). Document limitations before Gold measures. | Project | Decisão Aprovada |
| 2026-10-26 | Supplier Fallback Rule | Formalized `supplier_key = -1` ('Unknown Supplier') to resolve 21 orphaned franchises. | Project | Validado Tecnicamente |
| 2026-10-26 | Sanitized Projection | Specified `silver.fct_sales_transactions` schema dropping `cardNumber` and keeping `paymentMethod`. | Project | Validado Tecnicamente |
| 2026-10-26 | Financial Metrics | Formulated Gold measures in abstract units (`total_sales_revenue_units`) with documented price limits. | Project | Validado Tecnicamente |
| 2026-10-26 | Assessment Completion | Sub-stage 03 and Module 1 (Assessment) completed and signed off. Cleared for Module 2: Preparation. | Methodology | Concluído |
| 2026-10-26 | Shared Configuration | Built `notebooks/includes/configuration.py` with immutable parameters, table names, and PCI rules. | Project | Concluído |
| 2026-10-26 | Common Utilities | Built `notebooks/includes/common_functions.py` with audit enrichment, Delta merge, and quality tests. | Project | Concluído |
| 2026-10-26 | Preparation Guides | Established catalog DDLs, compute specs, and Medallion patterns in `02_preparation/`. | Project | Concluído |
| 2026-10-26 | Preparation Completion | Module 2 (Preparation) completed and signed off. Cleared for Module 3: Execution. | Methodology | Concluído |
| 2026-10-26 | Target Catalog | Created managed catalog `lakehouse_build` with schemas `bronze`, `silver`, and `gold`. | Project | Concluído |
| 2026-10-26 | Bronze Execution | Ingested all 4 tables into `lakehouse_build.bronze` with audit columns and 100% row reconciliation. | Project | Concluído |
| 2026-10-26 | Silver Execution | Curated and conformed 4 Silver tables; dropped cardNumber, resolved 21 orphaned franchises with -1 fallback. | Project | Concluído |
| 2026-10-26 | Gold Execution | Built 2 Gold marts with abstract monetary units and Liquid Clustering; 100% reconciliation (66,471 units). | Project | Concluído |
