---
name: lakehouse_assessment
description: Guidance for executing the Assessment module of the Lakehouse Build methodology, covering use case scoping, source profiling, and governance design.
---

# Lakehouse Assessment Skill

This skill provides step-by-step procedures, practical workflows, quality criteria, and result validation procedures for executing **Module 1: Assessment** of the Lakehouse Build methodology.

---

## When to Use This Skill

Activate this skill when:
- Initiating a Lakehouse project under the Lakehouse Build methodology.
- Scoping or refining analytical use cases, target personas, and delivery SLAs.
- Coordinating source data profiling across candidate catalogs or tables.
- Formulating governance standards, catalog namespace conventions, and layer role boundaries prior to any pipeline implementation.
- Validating whether the Assessment phase deliverables are complete and approved to transition into Preparation.

---

## Sub-stages and Step-by-Step Workflow

Assessment consists of three officially confirmed sub-stages that must be executed sequentially:

```
01_use_case_definition_and_scope
               ↓
02_workload_and_source_profiling
               ↓
03_governance_readiness_and_design_decisions
```

### Sub-stage 1: Use Case Definition & Scope (`01_use_case_definition_and_scope`)
1. **Identify Business Objectives:** Document the concrete analytical problem, business decisions to support, or KPIs required.
2. **Identify Target Personas & Consumers:** Define end consumers (e.g., operational managers, business analysts, downstream applications).
3. **Specify SLAs & Delivery Cadence:** Define update frequency (daily, hourly), data latency requirements, and availability expectations.
4. **Define Explicit Boundaries:** Maintain two explicit lists:
   - *In-Scope:* Specifically identified business questions, tables, and metrics.
   - *Out-of-Scope:* Deferred questions, future data sources, or non-essential features to prevent scope creep.

### Sub-stage 2: Workload & Source Profiling (`02_workload_and_source_profiling`)
1. **Source Discovery:** Inspect the target catalog (e.g., `samples.bakehouse` in Databricks Free Edition).
2. **Empirical Data Inspection:** Engage the `source_profiling` skill to obtain schemas, row counts, null rates, cardinality, and distributions from actual data.
3. **Key and Relationship Discovery:** Test candidate keys and joins with empirical queries. **Never assume or extrapolate keys, joins, or business meanings.**
4. **Selective Table Scoping:** Select only the subset of source tables necessary to fulfill the in-scope use case. Do not ingest unused tables.

### Sub-stage 3: Governance Readiness & Design Decisions (`03_governance_readiness_and_design_decisions`)
1. **Catalog & Namespace Strategy:** Define the 3-level namespace (`<catalog>.<schema>.<table>`).
2. **Medallion Layer Responsibilities:** Engage the `lakehouse_design` skill to specify exact roles for Bronze (raw/audit), Silver (curated/conformed), and Gold (aggregates/marts).
3. **Access Controls & Data Protection:** Identify sensitive fields, anonymization needs, and access boundaries.
4. **Table Properties & Storage Standards:** Establish Delta Lake table configurations, Liquid Clustering or partitioning strategies, and retention settings.

---

## How to Validate Results

Validate the Assessment module before seeking sign-off:
1. **Scope Validation:** Verify that every metric and analytical requirement in Sub-stage 1 maps directly to an empirically verified column documented in Sub-stage 2.
2. **Profiling Completeness:** Verify that each in-scope table has a profiling markdown file in `01_assessment/02_workload_and_source_profiling/` with row counts, physical schema, null percentages, and uniqueness test outcomes.
3. **Anti-Hallucination Audit:** Check every join condition and foreign key in the design documentation: confirm that each is backed by an empirical query proving referential consistency, not by naming assumptions.
4. **Gate Review:** Confirm that no pipeline code or transformation notebooks exist in `02_preparation/` or `03_execution/` before the Assessment documentation is completed and validated.

---

## Prohibitions & Anti-Patterns

- **No Premature Implementation:** Do not write ingestion scripts, transformations, or Delta table creations during Assessment.
- **No Hallucinated Schemas or Keys:** Never infer relationships, keys, or business logic without data evidence.
- **No Blanket Inclusion:** Never assume all tables in a sample catalog (`samples.bakehouse`) must be used.
