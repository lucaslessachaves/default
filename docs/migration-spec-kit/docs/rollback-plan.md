# Rollback Plan

## When to Rollback

Trigger rollback if ANY of the following occur during parallel run or after cutover:

- Row count discrepancy > 1% between source and target
- Revenue/amount discrepancy > $0.01
- Databricks workflow fails 3+ consecutive runs
- Data quality checks report critical violations
- Business users report incorrect data in downstream reports

## Rollback Steps

### Immediate (< 1 hour)

1. **Pause Databricks workflow**
   ```bash
   databricks jobs update --job-id <JOB_ID> --json '{"schedule": {"pause_status": "PAUSED"}}'
   ```

2. **Re-enable SSIS packages**
   - SQL Server Agent → Jobs → Enable ETL_Load_Customers
   - SQL Server Agent → Jobs → Enable ETL_Load_Orders

3. **Notify stakeholders**
   - Email data-team@company.com with rollback reason
   - Update decision-log.md

### Post-Rollback Investigation

1. Check Databricks workflow run history for errors
2. Compare audit logs: `bronze._audit_log` and `gold._audit_log`
3. Run validation queries to identify specific discrepancies
4. Review Delta table history: `DESCRIBE HISTORY migration_prod.gold.fact_order`

### Data Recovery (if needed)

Delta Lake supports time travel — restore to a known good state:

```sql
-- Check table history
DESCRIBE HISTORY migration_prod.gold.dim_customer;

-- Restore to specific version
RESTORE TABLE migration_prod.gold.dim_customer TO VERSION AS OF <version_number>;

-- Or restore to timestamp
RESTORE TABLE migration_prod.gold.dim_customer TO TIMESTAMP AS OF '2024-06-15T10:00:00';
```

## Prevention

After fixing the issue:
1. Add test case to `05-validation/`
2. Update `06-docs/decision-log.md`
3. Re-run parallel for minimum 3 days before re-cutover
