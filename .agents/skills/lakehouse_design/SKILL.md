---
name: lakehouse_design
description: Architectural patterns and design standards for Medallion Lakehouse layers (Bronze, Silver, Gold), dimensional modeling, and table layouts.
---

# Lakehouse Design Skill

This skill defines architectural standards, medallion layer patterns, and data modeling guidelines for designing a Databricks Lakehouse grounded strictly in empirical profiling evidence.

---

## When to Use This Skill

Activate this skill when:
- Designing medallion architecture boundaries during Sub-stage 3 (`03_governance_readiness_and_design_decisions`) of the Assessment phase.
- Specifying schemas, constraints, and audit metadata for Bronze, Silver, and Gold layers.
- Formulating dimensional models (Star Schema, Fact and Dimension tables) or consumption marts based on confirmed use cases.
- Defining table layout strategies (Liquid Clustering, partitioning, compaction) based on observed data volume and query patterns.
- Validating architectural specifications before initiating the Preparation phase.

---

## Medallion Architecture Layer Standards

### 1. Bronze Layer (Raw Ingestion / Landing)
- **Role:** High-fidelity, append-only historical replica of source data.
- **Rules:**
  - Preserve source column names, structures, and native types.
  - Do not apply business transformations, deduplication, or filtering.
  - Mandatory audit columns added to every Bronze table:
    - `_ingested_at`: UTC timestamp of ingestion.
    - `_source_system` / `_source_file`: Identifier of origin.
    - `_batch_id`: Run or execution identifier.
- **Write Mode:** Append-only.

### 2. Silver Layer (Cleaned, Conformed, Enriched)
- **Role:** Enterprise-wide curated and conformed entities ("single version of the truth").
- **Rules:**
  - Standardize data types, string trimming, and date/timestamp casting.
  - Enforce primary key uniqueness and deduplication via Delta `MERGE INTO`.
  - Isolate records that fail referential integrity or schema constraints into quarantine/dead-letter destinations.
  - Standardize business keys and generate surrogate keys where required.
- **Write Mode:** Merge/Upsert (`merge`), overwrite, or append depending on entity dynamics.

### 3. Gold Layer (Business Aggregates / Consumption)
- **Role:** Purpose-built, highly performant tables optimized for business intelligence, reporting, and analytical queries.
- **Modeling Paradigms:**
  - Dimensional Models: Conformed dimensions and fact tables adhering to Kimball design.
  - Analytical Marts: Pre-aggregated KPI tables and wide reporting views.
- **Write Mode:** Overwrite or targeted partition updates.

---

## Storage & Layout Optimization Guidelines

- **Liquid Clustering:** Standard recommendation for Databricks Lakehouses (`CLUSTER BY (col1, col2)`). Avoid legacy Hive-style multi-column partitioning.
- **Partitioning Thresholds:** Only consider physical partitioning on tables exceeding 10–20 GB. Avoid partitioning small sample datasets.
- **File Sizing & Maintenance:**
  - Target file size: 128 MB to 256 MB.
  - Define periodic `OPTIMIZE` and `VACUUM` schedules.

---

## How to Validate Results

Validate design deliverables before concluding Sub-stage 3:
1. **End-to-End Lineage Traceability:** Every Gold metric must trace directly back through Silver transformations to Bronze audit columns and verified source attributes.
2. **Schema & Constraint Completeness:** Each table design in Bronze, Silver, and Gold must have a documented data dictionary listing column names, physical types, nullability, primary key constraints, and default values.
3. **Anti-Hallucination Audit:** Verify that all dimensions, facts, and join keys reflect real relationships proven in Sub-stage 2 profiling—not assumed or arbitrary entities.
4. **Deliverable Check:** Verify that design specifications are saved as markdown documents under `01_assessment/03_governance_readiness_and_design_decisions/`.
