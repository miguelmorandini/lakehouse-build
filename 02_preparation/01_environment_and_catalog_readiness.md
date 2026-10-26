# Environment & Catalog Readiness Guide

**Methodology Module:** Module 2: Preparation  
**Document:** `01_environment_and_catalog_readiness.md`  
**Target Platform:** Databricks Free Edition (Unity Catalog enabled)

---

## 1. Overview & Objectives

The goal of this preparation step is to prepare the Databricks Unity Catalog environment, define the 3-level namespace schemas, and establish access controls prior to executing data pipelines.

---

## 2. Target Unity Catalog Architecture

In compliance with the Lakehouse Build governance model, tables are organized under a 3-level namespace:

```
<catalog_name>
├── bronze/    # Raw ingestion landing layer (append-only, audit-enriched)
├── silver/    # Conformed, curated, deduplicated enterprise entities
└── gold/      # Consumption-ready dimensional star schemas & reporting marts
```

> **Target Catalog:** Configurable via `LAKEHOUSE_CATALOG` (e.g. `lakehouse_build` or existing workspace catalog).

---

## 3. Schema DDL Definitions

To initialize the environment, execute the following DDL statements in the Databricks SQL Warehouse or interactive notebook:

```sql
-- 1. Create Bronze Schema
CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
COMMENT 'Landing schema for raw, append-only replicas with audit metadata from samples.bakehouse';

-- 2. Create Silver Schema
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
COMMENT 'Enterprise curated schema: cleansed, conformed, deduplicated entities with PCI data omitted';

-- 3. Create Gold Schema
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
COMMENT 'Consumption-ready dimensional models and analytical marts with abstract financial units';
```

---

## 4. Compute & Cluster Specifications

| Component | Target Spec | Purpose |
| :--- | :--- | :--- |
| **Databricks SQL Warehouse** | Serverless Starter Warehouse (`13a04a10a74d24cc`) | Interactive profiling, schema verification, and analytical ad-hoc queries. |
| **Spark Compute** | Single-Node or Serverless Compute | PySpark execution for Bronze ingestion and Silver/Gold Delta merge pipelines. |
| **Runtime Version** | Databricks Runtime (DBR) 14.3 LTS+ or Serverless | Native support for Delta Lake Liquid Clustering (`CLUSTER BY`). |

---

## 5. Security & Access Boundaries

1. **Bronze Schema Isolation:**
   - Contains raw payment card numbers (`cardNumber`).
   - Access restricted to data engineering pipeline service principals and administrators.
   - Prohibited from general analyst or BI tool read access.
2. **Silver & Gold Schemas:**
   - Sanitized (no card numbers, conformed dimensions).
   - Open to analysts, reporting roles, and dashboard service principals.

---

## 6. Readiness Verification Checklist

Before advancing to Module 3 (Execution), verify that:
- [ ] Target catalog and schemas (`bronze`, `silver`, `gold`) exist in Unity Catalog.
- [ ] Read access to `samples.bakehouse` is verified.
- [ ] The `configuration.py` and `common_functions.py` scripts are accessible in `notebooks/includes/`.
- [ ] The compute cluster/warehouse is available and responsive.
