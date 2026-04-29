---
name: ssis-migration
description: >
  Analyzes SSIS packages (.dtsx) and SQL Server stored procedures, then generates
  Databricks notebooks using Medallion architecture (Bronze/Silver/Gold) with Unity Catalog.
  Use when user says "migrate SSIS", "convert DTSX", "SSIS to Databricks",
  "analyze this package", "convert stored procedure to Spark", or uploads .dtsx files.
  Do NOT use for ADF pipeline migration or general SQL conversion without SSIS context.
license: MIT
metadata:
  author: yasarkocyigit
  version: 1.0.0
  category: data-engineering
  tags: [ssis, databricks, migration, etl, medallion, unity-catalog]
---

# SSIS to Databricks Migration Skill

## Overview

This skill converts SSIS (SQL Server Integration Services) ETL packages and associated
stored procedures into Databricks notebooks following the Medallion architecture pattern
with Unity Catalog.

## Instructions

### Step 1: Analyze the DTSX Package

Parse the DTSX XML file and extract:

1. **Connection Managers** — identify all data sources and destinations
2. **Control Flow** — list all tasks in execution order (follow PrecedenceConstraints)
3. **Data Flow** — for each Pipeline task, extract:
   - Source components (OLE DB, Flat File, etc.) and their SQL queries
   - Transformations (Derived Column, Conditional Split, Lookup, Aggregate, etc.)
   - Destinations and their target tables
   - Error outputs
4. **Variables and Parameters** — list all package/project variables with data types
5. **Precedence Constraints** — map execution order and conditions (Success/Failure/Expression)

When analyzing, consult `references/dtsx-structure-guide.md` for DTSX XML element mappings.

### Step 2: Analyze Stored Procedures

For each `.sql` file referenced by the DTSX:

1. Identify the merge/upsert pattern (INSERT, MERGE, SCD Type 1/2)
2. Extract source and target tables
3. Note any HASHBYTES comparisons (map to `sha2()` in Spark)
4. Identify transaction handling (BEGIN TRAN / COMMIT / ROLLBACK)
5. Extract audit/logging logic

### Step 3: Generate Migration Output

For each SSIS package, produce THREE Databricks notebooks:

**Bronze notebook** (`notebooks/bronze/`):
- JDBC read from source with query pushdown
- Add `_etl_*` audit columns
- Append to Bronze Delta table
- Replace SSIS variables with `dbutils.widgets`
- Replace connection managers with `dbutils.secrets`

**Silver notebook** (`notebooks/silver/`):
- Read from Bronze table
- Apply dedup (ROW_NUMBER window)
- Apply data quality rules
- Implement Conditional Split as `.filter()` branches
- MERGE into Silver table

**Gold notebook** (`notebooks/gold/`):
- Implement stored procedure logic as Spark SQL / PySpark
- SCD Type 2 → hash comparison + expire/insert pattern
- Fact merge → surrogate key lookups + MERGE INTO
- Late-arriving dimension handling
- Audit logging

### Step 4: Generate Supporting Artifacts

- **DDL** (`ddl/bronze/`, `ddl/silver/`, `ddl/gold/`): CREATE TABLE statements for Unity Catalog
- **Workflow JSON** (`workflows/`): Databricks job definition linking notebooks in correct order
- **Validation queries** (`05-validation/`): Row count and data quality comparison queries

## Component Mapping Reference

Use this quick reference for translating SSIS components. For the complete mapping,
consult `references/databricks-patterns.md`.

| SSIS Component | Databricks Equivalent |
|---|---|
| Execute SQL Task | `spark.sql()` or `%sql` cell |
| Data Flow Task | PySpark DataFrame pipeline |
| OLE DB Source | `spark.read.format("jdbc")` |
| Flat File Source | `spark.read.csv()` |
| Derived Column | `.withColumn()` + `F.when()` / `F.upper()` etc. |
| Conditional Split | `.filter()` per branch |
| Lookup | `.join()` with broadcast hint for small tables |
| Aggregate | `.groupBy().agg()` |
| OLE DB Destination | `.write.format("delta").saveAsTable()` |
| Package Variable | `dbutils.widgets` |
| Connection Manager | `dbutils.secrets` + JDBC URL |
| HASHBYTES | `F.sha2(F.concat_ws(...), 256)` |
| MERGE (T-SQL) | Delta `MERGE INTO` |

## Data Type Mapping

| SQL Server | Spark SQL |
|---|---|
| int | INT |
| bigint | BIGINT |
| varchar/nvarchar | STRING |
| decimal(p,s) | DECIMAL(p,s) |
| money | DECIMAL(19,4) |
| datetime/datetime2 | TIMESTAMP |
| date | DATE |
| bit | BOOLEAN |
| uniqueidentifier | STRING |

## CRITICAL Rules

- Always preserve the original SSIS execution order from PrecedenceConstraints
- Never hardcode credentials — use `dbutils.secrets.get()` for all connection details
- Always add `_etl_load_timestamp`, `_etl_source`, `_etl_pipeline` audit columns to Bronze
- Use `DECIMAL` (not `DOUBLE`) for financial amounts — precision matters
- Handle NULL values explicitly — Spark NULLs behave differently than SQL Server NULLs
- Add comments in notebooks referencing the original SSIS component name for traceability
- For SCD Type 2, always use hash comparison (SHA-256), never compare column-by-column

## Examples

**Example 1: Simple ETL Package**
User says: "Convert ETL_Load_Customers.dtsx to Databricks"
Actions:
1. Parse DTSX XML → identify 5 tasks, 1 data flow with 4 transforms
2. Generate bronze/load_raw_customers.py (JDBC → Delta)
3. Generate silver/cleanse_customers.py (dedup, validate, split)
4. Generate gold/merge_dim_customer.py (SCD Type 2)
5. Generate DDL for all three layers
6. Generate validation queries

**Example 2: Complex Package with Error Handling**
User says: "Migrate ETL_Load_Orders.dtsx — it has error outputs and sends email on failure"
Actions:
1. Parse DTSX → identify sequence container, multi-output data flow, failure path
2. Map error output → separate error Delta table (not flat file)
3. Map Send Mail Task → Databricks workflow notification config
4. Generate all Bronze/Silver/Gold notebooks with try/except error handling

## Troubleshooting

**Error: Cannot parse DTSX**
Cause: File may be encrypted or use non-standard XML
Solution: Ask user to export without encryption from SSDT/Visual Studio

**Error: Unknown SSIS component**
Cause: Third-party component not in mapping table
Solution: Ask user what the component does, then map to equivalent PySpark logic

**Error: Connection Manager references missing**
Cause: Project-level connection managers not included in DTSX
Solution: Ask user for the connection details or project parameter values
