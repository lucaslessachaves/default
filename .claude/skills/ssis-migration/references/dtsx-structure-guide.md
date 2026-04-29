# DTSX XML Structure Guide

## Overview

A `.dtsx` file is XML following the Microsoft DTS (Data Transformation Services) schema.
Understanding this structure is essential for automated parsing and migration.

## Root Element

```xml
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="PackageName"
  DTS:ExecutableType="Microsoft.Package">
```

Key attributes:
- `DTS:ObjectName` — Package name
- `DTS:DTSID` — Unique GUID
- `DTS:CreationDate` — When created

## Major Sections

### 1. Connection Managers
Location: `DTS:Executable/DTS:ConnectionManagers/DTS:ConnectionManager`

```xml
<DTS:ConnectionManager
  DTS:refId="Package.ConnectionManagers[Name]"
  DTS:CreationName="OLEDB|FLATFILE|ADO.NET|..."
  DTS:ObjectName="ConnectionName">
  <DTS:ObjectData>
    <DTS:ConnectionManager DTS:ConnectionString="..." />
  </DTS:ObjectData>
</DTS:ConnectionManager>
```

CreationName values:
- `OLEDB` — SQL Server, Oracle via OLE DB
- `FLATFILE` — CSV, TXT files
- `ADO.NET` — .NET managed connections
- `SMTP` — Email
- `FTP` — File transfer

### 2. Variables
Location: `DTS:Executable/DTS:Variables/DTS:Variable`

```xml
<DTS:Variable DTS:Namespace="User" DTS:ObjectName="VariableName">
  <DTS:VariableValue DTS:DataType="3">0</DTS:VariableValue>
</DTS:Variable>
```

DataType codes:
- `2` = Int16
- `3` = Int32
- `7` = DateTime
- `8` = String
- `11` = Boolean
- `13` = Object
- `20` = Int64

### 3. Package Parameters
Location: `DTS:Executable/DTS:PackageParameters/DTS:PackageParameter`

Similar to variables but designed for external configuration.

### 4. Executables (Tasks)
Location: `DTS:Executable/DTS:Executables/DTS:Executable`

Each task has a `DTS:CreationName` identifying its type:

| CreationName | Task Type |
|---|---|
| `Microsoft.ExecuteSQLTask` | Execute SQL Task |
| `Microsoft.Pipeline` | Data Flow Task |
| `Microsoft.SendMailTask` | Send Mail Task |
| `Microsoft.FileSystemTask` | File System Task |
| `Microsoft.ExecuteProcess` | Execute Process Task |
| `STOCK:SEQUENCE` | Sequence Container |
| `STOCK:FORLOOP` | For Loop Container |
| `STOCK:FOREACHLOOP` | For Each Loop Container |

### 5. Data Flow (Pipeline)
Location: Inside `Microsoft.Pipeline` executable → `DTS:ObjectData/pipeline`

#### Components
```xml
<component name="ComponentName" componentClassID="Microsoft.OLEDBSource">
  <properties>
    <property name="SqlCommand">SELECT ... FROM ...</property>
  </properties>
  <outputs>
    <output name="Output Name">
      <outputColumns>
        <outputColumn name="ColName" dataType="i4" />
      </outputColumns>
    </output>
  </outputs>
</component>
```

componentClassID values:
- `Microsoft.OLEDBSource` — OLE DB Source
- `Microsoft.OLEDBDestination` — OLE DB Destination
- `Microsoft.FlatFileSource` — Flat File Source
- `Microsoft.FlatFileDestination` — Flat File Destination
- `Microsoft.DerivedColumn` — Derived Column
- `Microsoft.ConditionalSplit` — Conditional Split
- `Microsoft.Lookup` — Lookup
- `Microsoft.Aggregate` — Aggregate
- `Microsoft.DataConvert` — Data Conversion
- `Microsoft.Sort` — Sort
- `Microsoft.MergeJoin` — Merge Join
- `Microsoft.UnionAll` — Union All
- `Microsoft.Multicast` — Multicast
- `Microsoft.RowCount` — Row Count
- `Microsoft.ScriptComponent` — Script Component (C#/VB.NET)

#### Paths (Data Flow connections)
```xml
<paths>
  <path startId="Source.OutputName" endId="Dest.InputName" />
</paths>
```

### 6. Precedence Constraints
Location: `DTS:Executable/DTS:PrecedenceConstraints/DTS:PrecedenceConstraint`

```xml
<DTS:PrecedenceConstraint
  DTS:From="Package\Task1"
  DTS:To="Package\Task2"
  DTS:Value="0"        <!-- 0=Success, 1=Failure, 2=Completion -->
  DTS:EvalOp="2" />    <!-- 1=Constraint, 2=Expression, 3=Both -->
```

## SSIS Expression Syntax (for Derived Columns)

| SSIS Expression | Description |
|---|---|
| `GETDATE()` | Current timestamp |
| `YEAR(col)` | Extract year |
| `UPPER(col)` | Uppercase |
| `TRIM(col)` | Trim whitespace |
| `(bool) ? val1 : val2` | Ternary/conditional |
| `col1 + " " + col2` | String concatenation |
| `(DT_WSTR,50)col` | Type cast |
| `ISNULL(col)` | Null check |
| `REPLACENULL(col, "default")` | Null replacement |
