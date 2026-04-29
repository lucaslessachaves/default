# Package Dependency Map

## Execution Order

```
ETL_Load_Customers.dtsx ──→ ETL_Load_Orders.dtsx
        │                           │
        ▼                           ▼
  sp_MergeCustomerDim         sp_MergeOrderFact
        │                           │
        ▼                           ├──→ dim.Customer (lookup)
  dim.Customer                 ├──→ dim.Date (lookup)
                               ├──→ dim.Region (lookup)
                               └──→ ref.CurrencyRate (lookup)
```

## Why This Order?

ETL_Load_Orders depends on ETL_Load_Customers because:
- sp_MergeOrderFact performs a surrogate key lookup against dim.Customer
- If customers are not loaded first, orders will either fail the lookup or create late-arriving dimension members (placeholder records)

## Table Dependencies

### Source Tables (SQL Server - AdventureWorks)
| Table | Used By |
|---|---|
| dbo.Customer | ETL_Load_Customers.dtsx |
| dbo.Orders | ETL_Load_Orders.dtsx |

### Staging Tables (SQL Server - DW_Staging)
| Table | Written By | Read By |
|---|---|---|
| staging.Customer | ETL_Load_Customers | sp_MergeCustomerDim |
| staging.Orders | ETL_Load_Orders | sp_MergeOrderFact |
| staging.OrderRegionSummary | ETL_Load_Orders | sp_MergeOrderFact |

### Dimension Tables
| Table | Written By | Read By |
|---|---|---|
| dim.Customer | sp_MergeCustomerDim | sp_MergeOrderFact (lookup) |
| dim.Date | Pre-populated | sp_MergeOrderFact (lookup) |
| dim.Region | Pre-populated | sp_MergeOrderFact (lookup) |

### Fact Tables
| Table | Written By |
|---|---|
| fact.Orders | sp_MergeOrderFact |
| fact.DailyRegionSummary | sp_MergeOrderFact |

### Reference Tables
| Table | Used By |
|---|---|
| ref.CurrencyRate | sp_MergeOrderFact |

### Audit Tables
| Table | Written By |
|---|---|
| audit.PackageExecution | ETL_Load_Customers |
| audit.MergeLog | sp_MergeCustomerDim, sp_MergeOrderFact |

## External Dependencies

| Resource | Type | Used By |
|---|---|---|
| \\FileServer\ETL\Logs\OrderErrors.csv | File Share | ETL_Load_Orders (error output) |
| \\FileServer\ETL\Archive\ | File Share | ETL_Load_Orders (archive) |
| SMTP Server | Email | ETL_Load_Orders (failure notification) |
