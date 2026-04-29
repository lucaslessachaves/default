# Migration Reconciliation Report

> Fill in after running validation queries against source and target.

## Summary

| Check | Source Count | Target Count | Difference | Status |
|---|---|---|---|---|
| Total Customers | ___ | ___ | ___ | PASS / FAIL |
| Active Customers | ___ | ___ | ___ | PASS / FAIL |
| Current Dim Customers (SCD2) | ___ | ___ | ___ | PASS / FAIL |
| Total Orders | ___ | ___ | ___ | PASS / FAIL |
| Total Revenue ($) | ___ | ___ | ___ | PASS / FAIL |

## Data Quality Results

| Check | Result | Notes |
|---|---|---|
| NULL checks (required fields) | ___ nulls found | |
| Duplicate current dim records | ___ dupes found | |
| Duplicate fact records | ___ dupes found | |
| Orphan foreign keys | ___ orphans found | |
| Negative amounts | ___ found | |
| Invalid SCD dates | ___ found | |
| Date range check | Min: ___ Max: ___ | |
| SCD2 history gaps | ___ gaps found | |

## Acceptance Criteria

- Row count difference: < 0.01% (allowance for in-flight records)
- Revenue difference: < $0.01 (rounding tolerance)
- Zero NULL violations on required fields
- Zero duplicate primary keys
- Zero orphan foreign keys
- Zero SCD date violations

## Sign-off

| Role | Name | Date | Approved |
|---|---|---|---|
| Data Engineer | ___ | ___ | [ ] |
| Data Analyst / QA | ___ | ___ | [ ] |
| Business Owner | ___ | ___ | [ ] |
