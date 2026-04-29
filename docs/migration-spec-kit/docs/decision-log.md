# Decision Log

Record all architecture and design decisions made during migration.

## Template

**Decision:** [What was decided]
**Date:** [When]
**Context:** [Why this decision was needed]
**Options Considered:** [What alternatives were evaluated]
**Rationale:** [Why this option was chosen]
**Consequences:** [What trade-offs or implications]

---

## Decisions

### DEC-001: Use Medallion Architecture (Bronze/Silver/Gold)

**Date:** 2024-xx-xx
**Context:** Need to decide target data architecture for migrated SSIS pipelines.
**Options Considered:**
1. Direct migration — replicate SSIS staging/dim/fact structure as-is
2. Medallion — Bronze (raw) → Silver (cleansed) → Gold (business)
3. Data Vault 2.0

**Rationale:** Medallion aligns with Databricks best practices, provides clear data lineage,
and the three-layer approach maps naturally to SSIS's staging → dimension/fact pattern.
Data Vault was considered but adds complexity not justified for this scope.

**Consequences:** Additional Silver layer adds processing time but improves data quality
and reprocessability.

---

### DEC-002: SCD Type 2 via Hash Comparison

**Date:** 2024-xx-xx
**Context:** Original sp_MergeCustomerDim uses HASHBYTES for change detection. Need to decide
how to implement in Databricks.
**Options Considered:**
1. Column-by-column comparison
2. SHA-256 hash comparison (matching source pattern)
3. Delta Change Data Feed (CDF)

**Rationale:** Hash comparison preserves parity with source system, making validation easier.
Uses `F.sha2(F.concat_ws(...), 256)` which produces identical logic to SQL Server HASHBYTES.

**Consequences:** Hash collisions are theoretically possible but negligible with SHA-256.

---

### DEC-003: Unity Catalog for Governance

**Date:** 2024-xx-xx
**Context:** Need centralized governance, access control, and lineage tracking.
**Options Considered:**
1. Hive Metastore (legacy)
2. Unity Catalog

**Rationale:** Unity Catalog provides row/column-level security, audit logging, and
cross-workspace governance. Future-proof choice aligned with Databricks roadmap.

**Consequences:** Requires Unity Catalog-enabled workspace. Some older cluster types
not compatible.
