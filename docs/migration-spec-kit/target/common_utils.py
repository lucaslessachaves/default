# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utilities
# MAGIC Shared functions used across Bronze, Silver, and Gold notebooks.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from datetime import datetime


def add_audit_columns(df: DataFrame, source: str, pipeline: str) -> DataFrame:
    """Add standard ETL audit columns to a DataFrame.
    Replaces SSIS Derived Column transform for audit fields.
    """
    return (df
        .withColumn("_etl_load_timestamp", F.current_timestamp())
        .withColumn("_etl_source", F.lit(source))
        .withColumn("_etl_file_date", F.current_date())
        .withColumn("_etl_pipeline", F.lit(pipeline))
    )


def deduplicate(df: DataFrame, key_columns: list, order_column: str = "ModifiedDate") -> DataFrame:
    """Deduplicate DataFrame keeping the latest record per key.
    Replaces implicit SSIS last-row-wins behavior.
    """
    window = Window.partitionBy(*key_columns).orderBy(F.desc(order_column))
    return (df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def compute_record_hash(df: DataFrame, columns: list) -> DataFrame:
    """Compute SHA-256 hash over specified columns for SCD Type 2 change detection.
    Replaces HASHBYTES('SHA2_256', CONCAT(...)) from SQL Server.
    """
    hash_expr = F.sha2(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]),
        256
    )
    return df.withColumn("record_hash", hash_expr)


def read_jdbc_incremental(spark, jdbc_url: str, table: str, watermark_col: str,
                          watermark_value: str, secret_scope: str = "kv-scope") -> DataFrame:
    """Read from SQL Server via JDBC with incremental watermark.
    Replaces SSIS OLE DB Source with parameterized query.
    """
    query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{watermark_value}') AS q"

    return (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", dbutils.secrets.get(secret_scope, "sql-user"))
        .option("password", dbutils.secrets.get(secret_scope, "sql-password"))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "10000")
        .load()
    )


def get_watermark(spark, catalog: str, table: str, column: str = "_etl_load_timestamp") -> str:
    """Get high watermark from target table for incremental loads.
    Replaces SSIS 'Get Last Modified Date' Execute SQL Task.
    """
    try:
        result = spark.sql(f"SELECT COALESCE(MAX({column}), '1900-01-01') FROM {catalog}.{table}")
        return str(result.collect()[0][0])
    except Exception:
        return "1900-01-01"


def log_audit(spark, catalog: str, layer: str, pipeline: str, table: str,
              rows: int, status: str = "Success", notes: str = "") -> None:
    """Write audit log entry.
    Replaces audit.PackageExecution and audit.MergeLog INSERTs.
    """
    spark.sql(f"""
        INSERT INTO {catalog}.{layer}._audit_log
        VALUES ('{pipeline}', '{table}', {rows}, current_timestamp(), '{status}', '{notes}')
    """)
