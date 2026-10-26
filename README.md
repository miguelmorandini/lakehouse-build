# Lakehouse Build Project

A practical implementation of modern lakehouse architecture following the three core modules of the **Lakehouse Build** methodology:
1. **Assessment** (Current Active Module)
2. **Preparation**
3. **Execution**

---

## 1. Project Overview & Operating Principles

The objective of this project is to implement a robust, scalable, and governed Databricks Lakehouse architecture guided by the structured Lakehouse Build methodology. 

### Core Operating Rules
- **Phased Execution:** Work strictly in sequence (Assessment $\rightarrow$ Preparation $\rightarrow$ Execution). Do not begin ingestion, transformation, or deployment code for Bronze, Silver, or Gold layers until the Assessment phase is fully defined, documented, and approved.
- **Inspect Before Modifying:** Always inspect existing schemas, catalog metadata, sample records, and directory contents before proposing models or transformations.
- **Empirical Grounding (Zero Hallucination):** Never invent, assume, or extrapolate schemas, data types, primary keys, foreign keys, table relationships, or business rules. Every relationship and business rule must be supported by empirical data inspection or explicit user confirmation.
- **Document All Decisions:** Record architectural choices, schema interpretations, dimensional model decisions, and trade-offs under `01_assessment/` or `references/`.
- **Validate Results:** Validate outputs empirically using explicit criteria (row counts, uniqueness, null ratios, referential integrity).
- **Environment & Safety Constraints:** The target environment is **Databricks Free Edition** utilizing sample datasets (initially exploring `samples.bakehouse`). Do not create synthetic/mock data, do not run destructive or unauthorized write/alter operations on Databricks, and do not publish cloud resources. Do not assume all tables in `samples.bakehouse` will be used.

---

## 2. Tools & Environment

| Tool / Technology | Role in Project | Classification |
| :--- | :--- | :--- |
| **Lakehouse Build Methodology** | Phased framework providing structured assessment, preparation, and execution steps. | Official Methodology |
| **Databricks Free Edition** | Managed compute and workspace for exploration and execution. | Project Environment |
| **Unity Catalog / Sample Catalog** | Catalog exploration, initially targeting `samples.bakehouse`. | Project Data Source |
| **Delta Lake** | Storage format offering ACID transactions, time travel, and schema enforcement. | Architectural Standard |
| **Apache Spark / PySpark** | Distributed engine for data profiling, transformation, and testing. | Processing Engine |

---

## 3. Directory Organization

The repository follows a clean `snake_case` directory structure organized by methodology modules:

```text
lakehouse_build/
├── README.md                                             # Project overview, operating rules, and repository map
├── .agents/
│   └── skills/                                           # Operational skills with practical procedures and validation
│       ├── lakehouse_assessment/SKILL.md                 # Assessment module execution, use case scoping, and governance
│       ├── source_profiling/SKILL.md                     # Data inspection, statistics, and empirical relationship checks
│       ├── lakehouse_design/SKILL.md                     # Medallion layer boundaries and dimensional modeling patterns
│       ├── pyspark_delta/SKILL.md                        # PySpark and Delta Lake coding standards and optimization
│       └── data_quality_validation/SKILL.md              # Quality expectations, test suites, and gate criteria
├── 01_assessment/                                        # Module 1: Assessment (Active Phase)
│   ├── 01_use_case_definition_and_scope/                 # Confirmed sub-stage: Business goals, personas, SLAs, boundaries
│   ├── 02_workload_and_source_profiling/                 # Confirmed sub-stage: Physical schemas, volumes, profiling metrics
│   └── 03_governance_readiness_and_design_decisions/     # Confirmed sub-stage: Catalog hierarchy, layer roles, conventions
├── 02_preparation/                                       # Module 2: Preparation (Pending Assessment sign-off)
├── 03_execution/                                         # Module 3: Execution (Working implementation folders)
│   ├── 01_bronze/                                        # Raw ingestion pipelines (to be developed in Execution)
│   ├── 02_silver/                                        # Cleansed, conformed entities (to be developed in Execution)
│   ├── 03_gold/                                          # Business aggregates and marts (to be developed in Execution)
│   └── 04_testing_and_validation/                        # Automated validation suites and test runs
├── notebooks/                                            # Interactive Databricks notebooks
│   └── includes/                                         # Shared utilities, configuration, and helpers
└── references/
    └── lakehouse_build_notes.md                          # Methodology notes and project design log
```

> **Important Note on Methodology Sub-Stages:**
> - The three sub-stages under `01_assessment/` (`01_use_case_definition_and_scope`, `02_workload_and_source_profiling`, `03_governance_readiness_and_design_decisions`) are **confirmed official sub-stages** of the Lakehouse Build methodology.
> - The sub-stages under `02_preparation/` and `03_execution/` (such as `01_bronze`, `02_silver`, etc.) represent **functional, working directories for this project implementation** and are not declared as official methodology sub-stages unless officially confirmed.

---

## 4. Current Status & Next Steps

1. **Current Status:** 
   - Sub-stage 1 (`01_use_case_definition_and_scope`) completed: charter documented in [use_case_charter.md](file:///c:/Miguel/Projects/lakehouse-build/01_assessment/01_use_case_definition_and_scope/use_case_charter.md).
   - Sub-stage 2 (`02_workload_and_source_profiling`) completed: empirical profiling report across 4 tables in `samples.bakehouse` documented in [source_workload_profiling_report.md](file:///c:/Miguel/Projects/lakehouse-build/01_assessment/02_workload_and_source_profiling/source_workload_profiling_report.md).
   - Sub-stage 3 (`03_governance_readiness_and_design_decisions`) **IN PROGRESS**: architectural choices, classifications, and governance rules registered in [governance_and_design_decisions.md](file:///c:/Miguel/Projects/lakehouse-build/01_assessment/03_governance_readiness_and_design_decisions/governance_and_design_decisions.md). Sub-stage remains open until pending technical validations are formalized.
2. **Immediate Step:** Execute and document the technical validations (cardinality/fallback rules for suppliers, Silver projection exclusion of `cardNumber`, and Gold measures naming with abstract currency units) to finalize Assessment.
3. **Implementation Hold:** Pipeline development and notebook implementation remain blocked until all three Assessment sub-stages are completed and approved.
