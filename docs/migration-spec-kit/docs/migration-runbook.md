# Migration Runbook

## Pre-Migration Checklist

- [ ] Source SQL Server access confirmed
- [ ] Databricks workspace provisioned with Unity Catalog
- [ ] Secret scope created and populated (sql-server-host, sql-user, sql-password)
- [ ] Network connectivity: Databricks → SQL Server (firewall/VNet peering)
- [ ] JDBC driver available on Databricks cluster
- [ ] All DTSX packages collected in `01-source/dtsx/`
- [ ] All stored procedures collected in `01-source/stored-procedures/`
- [ ] Analysis completed (`02-analysis/` populated)
- [ ] Mapping reviewed and approved (`03-mapping/`)

## Execution Steps

### Step 1: Create Unity Catalog Objects
```bash
# Run DDL scripts in order
databricks workspace import_dir 04-target/ddl/ /Repos/migration/ddl/
# Execute in Databricks:
# 1. ddl/bronze/create_bronze_tables.sql
# 2. ddl/silver/create_silver_tables.sql
# 3. ddl/gold/create_gold_tables.sql
```

### Step 2: Deploy Notebooks
```bash
databricks workspace import_dir 04-target/notebooks/ /Repos/migration/notebooks/
databricks workspace import_dir 04-target/shared/ /Repos/migration/shared/
```

### Step 3: Initial Full Load
Run notebooks manually in order (first run = full load):
1. `bronze/load_raw_customers` (set incremental_start = "1900-01-01")
2. `silver/cleanse_customers`
3. `gold/merge_dim_customer`
4. `bronze/load_raw_orders`
5. `gold/merge_fact_orders`

### Step 4: Validate
Run all queries in `05-validation/` and fill in `reconciliation-report.md`.

### Step 5: Deploy Workflow
```bash
databricks jobs create --json @04-target/workflows/daily_etl_workflow.json
```

### Step 6: Parallel Run (recommended 1-2 weeks)
- Keep SSIS packages running alongside Databricks
- Compare outputs daily using validation queries
- Resolve discrepancies before cutover

### Step 7: Cutover
- Disable SSIS packages in SQL Server Agent
- Unpause Databricks workflow schedule
- Monitor first 3 automated runs

## Post-Migration

- [ ] SSIS packages disabled
- [ ] Databricks workflow running on schedule
- [ ] Alerting configured (email on failure)
- [ ] Documentation updated
- [ ] Stakeholders notified
