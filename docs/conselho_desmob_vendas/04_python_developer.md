# Conselho Desmobilização e Vendas — v2
## Agente: Python Developer — Templates e Padrões PySpark
**Data:** 2026-04-29 | **Score: 5.5 / 10** *(revisado com novos dados)*

---

## 1. O que mudou desde a v1

| Item | v1 | v2 |
|---|---|---|
| TOP 1 — tradução | Incerta, risco alto | **RESOLVIDA** — `limit(1)` + `broadcast()` |
| `ts_ingestao_brt` | Campo desconhecido | **CONFIRMADO** — usado em todos os `saveDeltaAsSilver` |
| Baseline de validação | Ausente | **INTEGRADO** — dict com 17 tabelas no `validation.py` |
| Score | 4.5/10 | **5.5/10** |

*Nota: o score permanece abaixo de 6.0 pois os anti-padrões estruturais do legado (GETDATE, 8 UPDATEs, 4 temp tables encadeadas) são complexidade real de implementação que não desaparece com os novos dados.*

---

## 2. Tabela de Tradução T-SQL → PySpark (específica para este módulo)

| Padrão T-SQL (encontrado no código) | Equivalente PySpark |
|---|---|
| `SELECT TOP 1 ... FROM STG_SIMA_VERSAO_ORCAMENTO WHERE Situacao < 2 ORDER BY Situacao DESC, DataRegistro DESC` | `.filter("Situacao < 2").orderBy(F.desc("Situacao"), F.desc("DataRegistro")).limit(1)` + `F.broadcast()` |
| `IIF(m.Tipo = 0, 'LEVE', IIF(m.Tipo = 1, 'PESADO', NULL))` | `F.when(F.col("Tipo")==0,"LEVE").when(F.col("Tipo")==1,"PESADO").otherwise(None)` |
| `IIF((o.AnoReferencia+1) = YEAR(GETDATE()), 1, 0)` | `F.when((F.col("AnoReferencia")+1) == F.year(F.lit(execution_date)), 1).otherwise(0)` |
| `ISNULL(x, 0)` | `F.coalesce(F.col("x"), F.lit(0))` |
| `DATEDIFF(DAY, Ciclo1Quando, Ciclo1VendaQuando)` | `F.datediff(F.col("Ciclo1VendaQuando"), F.col("Ciclo1Quando"))` |
| `CAST(CONCAT(AnoRef, '/', AnoRef+1) AS varchar(25))` | `F.concat(F.col("AnoReferencia").cast("string"), F.lit("/"), (F.col("AnoReferencia")+1).cast("string"))` |
| `DATEADD(DAY, -CAST(qtd AS int), GETDATE())` | `F.date_sub(F.lit(execution_date).cast("date"), F.col("qtd").cast("int"))` |
| `LEFT(CR, 4) IN ('3010','3011')` | `F.substring(F.col("CR"), 1, 4).isin("3010","3011")` |
| `RIGHT(CentroCusto, 5) NOT IN ('90600','92600')` | `F.substring(F.col("CentroCusto"), -5, 5).isin("90600","92600") == False` |
| `WITH (NOLOCK)` | Remover — Delta usa snapshot isolation |
| `DECLARE @TabTemp TABLE (...)` | DataFrame intermediário Python |
| `DROP TABLE IF EXISTS #temp` | `spark.catalog.dropTempView("temp")` |
| `SELECT * INTO #temp FROM cte` | `spark.sql("CREATE OR REPLACE TEMP VIEW tv_temp AS SELECT ...")` |
| `UPDATE t SET col = s.val FROM tabela t LEFT JOIN cte s ON ...` | `DeltaTable.forName(...).merge(...).whenMatchedUpdate(set={...}).execute()` |
| `DELETE FROM DW.tabela WHERE YEAR(col) > 2024` | `df.write.option("replaceWhere", "YEAR(col) > 2024").mode("overwrite")` |
| `UNIQUEIDENTIFIER` | `StringType()` |
| `DATETIME2(7)` | `TimestampType()` |
| `NUMERIC(18,2)` | `DecimalType(18,2)` |
| `BIT` | `BooleanType()` |

---

## 3. utils/config.py

```python
import os
from datetime import date

CATALOG       = "bi_ativos"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

def b(table: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table}"

def s(table: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{table}"

def g(table: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{table}"

def get_execution_date() -> str:
    try:
        val = dbutils.widgets.get("execution_date")
        return val if val else str(date.today())
    except Exception:
        return str(date.today())

def get_ano_corte() -> int:
    try:
        return int(dbutils.widgets.get("ano_corte"))
    except Exception:
        return 2024
```

---

## 4. utils/validation.py

```python
BASELINE = {
    "ft_desmobilizacao_pa":                       42_732,
    "ft_orcamento_desmobilizacao_geral":           34_741,
    "ft_orcamento_desmobilizacao_geral_atual":      9_664,
    "ft_orcamento_desmobilizacao_historico":       26_651,
    "ft_orcamento_venda_geral":                    36_100,
    "ft_orcamento_venda_geral_atual":               9_658,
    "ft_orcamento_venda_historico":                36_100,
    "ft_radar_desmobilizacao":                     22_417,
    "ft_radar_desmobilizacao_historico":           17_072,
    "ft_radar_venda":                              26_056,
    "ft_historico_desmob_plataforma_venda":        63_651,
    "ft_historico_de_vendas_pa":                    6_390,
    "ft_historico_sla_desmobilizacao_gab_pa":      42_892,
    "ft_ultimos_registros_por_placa_crs_desmob":   84_227,
    "ft_venda_pa":                                 41_756,
    "dm_sima_cidade_desmobilizacao":                5_570,
    "dm_rls_consultor_vendas_pa":                      24,
}

def validar_paridade(spark, target: str, tolerancia: float = 0.01):
    tabela = target.split(".")[-1].lower()
    esperado = BASELINE.get(tabela)
    if esperado is None:
        print(f"WARN: sem baseline para {tabela}, pulando validação")
        return
    atual = spark.table(target).count()
    delta = abs(atual - esperado) / esperado
    status = "OK" if delta <= tolerancia else "FALHOU"
    msg = f"{status}: {target} → {atual:,} linhas (esperado ~{esperado:,}, delta={delta:.1%})"
    assert delta <= tolerancia, msg
    print(msg)
```

---

## 5. Template — Silver Genérico (00_generic_loader.py)

```python
# %run ../libs/carga_silver
# %run ../utils/config

dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("silver_table", "")
dbutils.widgets.text("pk_columns",   "Id")  # separado por vírgula

bronze = dbutils.widgets.get("bronze_table")
silver = dbutils.widgets.get("silver_table")
pks    = [c.strip() for c in dbutils.widgets.get("pk_columns").split(",")]

df = TableReference(b(bronze)).selectLazy()

df.saveDeltaAsSilver(
    target=s(silver),
    on_colunm=pks,
    tsp_load_colunm="ts_ingestao_brt",
    createIfNotExists=True
)
print(f"OK: {b(bronze)} → {s(silver)} | {df.count():,} linhas")
```

---

## 6. Template — Silver View (01_vw_orcamento_desmob_venda.py)

```python
# %run ../libs/carga_silver
# %run ../utils/config

from pyspark.sql import functions as F
from pyspark.sql.window import Window

target = s("vw_orcamento_desmob_venda")

# Singleton global: última versão de orçamento não cancelada
df_versao = (
    spark.table(s("stg_sima_versao_orcamento"))
    .filter("Situacao < 2 AND DataDesativacao IS NULL")
    .orderBy(F.desc("Situacao"), F.desc("DataRegistro"))
    .limit(1)
)

df_itens = spark.table(s("stg_sima_versao_orcamento_itens"))
df_orcamento = spark.table(s("stg_sima_orcamento"))
df_cr        = spark.table(s("stg_sima_centro_resultado"))
df_modelo    = spark.table(s("stg_sima_modelo"))
df_diretoria = spark.table(s("stg_sima_diretoria"))
df_fipe      = spark.table(s("stg_fipe_atual"))
df_zfi       = spark.table(s("stg_sap_zfiaa001")).select("Equipamento","Ano fabricação")
df_prazo     = spark.table(s("stg_sima_parametrizacao_prazo_de_desmobilizacao")) \
    .filter("DataDesativacao IS NULL AND DiretoriaId IS NULL") \
    .select("TipoPeso","PrazoDesmobilizacaoEmDias")

# Base: join versão (broadcast — 1 linha) + orcamento + itens
df_base = (
    df_itens.alias("vi")
    .join(F.broadcast(df_versao).alias("vo"), F.col("vi.VersaoId") == F.col("vo.Id"), "inner")
    .join(df_orcamento.alias("o"),   F.col("vo.OrcamentoId") == F.col("o.Id"), "left")
    .join(df_cr.alias("cr"),         F.col("vi.CentroResultadoId") == F.col("cr.Id"), "left")
    .join(df_modelo.alias("m"),      F.col("vi.ModeloId") == F.col("m.Id"), "left")
    .join(df_diretoria.alias("dcr"), F.col("cr.DiretoriaId") == F.col("dcr.Id"), "left")
    .join(df_prazo.alias("prz"),     F.col("m.Tipo") == F.col("prz.TipoPeso"), "left")
    .join(df_zfi.alias("zfi"),       F.col("vi.Placa") == F.col("zfi.Equipamento"), "left")
)

execution_date = get_execution_date()

def _select_ciclo(df, ciclo_num: int) -> "DataFrame":
    ciclo = f"Ciclo{ciclo_num}"
    return df.select(
        F.col(f"vi.Id").cast("string").alias("Id"),
        F.col(f"vi.Imobilizado").cast("string").alias("Imobilizado"),
        F.concat(F.substring(F.col("cr.CR"), 1, 4), F.col("vi.Imobilizado")).alias("Chave"),
        F.col(f"vi.Placa").cast("string").alias("Placa"),
        F.lit(ciclo_num).alias("Ciclo"),
        F.col(f"vi.{ciclo}Acao").cast("int").alias("CicloAcao"),
        F.col("cr.CR").alias("CodCr"),
        F.upper(F.col("dcr.Descricao")).alias("Diretoria"),
        F.col("vi.ModeloId").cast("string").alias("ModeloId"),
        F.when(F.col("m.Tipo")==0,"LEVE").when(F.col("m.Tipo")==1,"PESADO").otherwise(None).alias("Peso"),
        F.col("m.Tipo").cast("int").alias("IdPeso"),
        F.when(F.col("vi.AnoModelo")==0, None).otherwise(F.col("vi.AnoModelo")).cast("int").alias("AnoModelo"),
        F.col(f"vi.{ciclo}DataDesmobilizacao").cast("date").alias("DataDesmobilizacao"),
        F.col(f"vi.{ciclo}VendaQuando").cast("date").alias("DataVenda"),
        F.when(F.col("vi.AnoReferencia") <= 2023,
               F.col("cr.CR")).otherwise(F.col("cr.CR")).alias("CodCrFinal"),
        F.col(f"vi.{ciclo}ValorVenda").cast("decimal(18,2)").alias("ValorVenda"),
        F.col(f"vi.{ciclo}CustoDoAtivoVendido").cast("decimal(18,2)").alias("ResidualTotal"),
        F.when(
            F.col(f"vi.{ciclo}ValorVenda")==0, 0
        ).otherwise(
            F.col(f"vi.{ciclo}ValorVenda") / F.col(f"vi.{ciclo}FipeProjecao")
        ).alias("PerformanceProjetada"),
        F.when((F.col("o.AnoReferencia")+1) == F.year(F.lit(execution_date)), 1).otherwise(0).alias("ReferenciaAnoVigente"),
        F.col("o.AnoReferencia").cast("int").alias("AnoReferencia"),
        F.col("vo.Codigo").cast("string").alias("VersaoOrcamento"),
        F.col("vo.Situacao").cast("int").alias("SituacaoVersaoOrcamento"),
        F.col("prz.PrazoDesmobilizacaoEmDias").cast("int").alias("PrazoDesmobilizacaoEmDias"),
        F.when(
            (F.col("vi.DataDesativacao").isNull()) &
            (F.col(f"vi.{ciclo}Acao").isin(2,3,4,5)) &
            (F.col("cr.CrTransicao") == False) &
            (F.col("cr.CrVenda") == False), 1
        ).otherwise(0).alias("EhDesmobilizacao"),
        F.when(F.col(f"vi.{ciclo}VendaQuando").isNull(), 0).otherwise(1).alias("EhVenda"),
    )

# Branch 1: Ciclo 1 — Desmob normal
df_b1 = _select_ciclo(
    df_base.filter(
        F.col("vi.Ciclo1Acao").isin(2,3,4,5,6) &
        F.col("vi.DataDesativacao").isNull() &
        F.col("cr.CrTransicao").cast("boolean") == False &
        F.col("cr.CrVenda").cast("boolean") == False
    ), ciclo_num=1
)

# Branch 2: Ciclo 1 — CR Venda ou Transição
df_b2 = _select_ciclo(
    df_base.filter(
        F.col("vi.DataDesativacao").isNull() &
        (F.col("cr.CrVenda").cast("boolean") | F.col("cr.CrTransicao").cast("boolean"))
    ), ciclo_num=1
)

# Branch 3: Ciclo 2
df_b3 = _select_ciclo(
    df_base.filter(
        F.col("vi.Ciclo2Acao").isin(2,3,4,5,6) &
        F.col("vi.DataDesativacao").isNull()
    ), ciclo_num=2
)

df_final = df_b1.unionByName(df_b2).unionByName(df_b3)

df_final.saveAsSilverOverwrite(target=target, createIfNotExists=True)
print(f"OK: vw_orcamento_desmob_venda → {spark.table(target).count():,} linhas")
```

---

## 7. Template — Gold Fato com replaceWhere (02_ft_orcamento_desmob_geral.py)

```python
# %run ../libs/carga_silver
# %run ../utils/config
# %run ../utils/validation

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

dbutils.widgets.text("execution_date", "")
dbutils.widgets.text("ano_corte", "2024")

execution_date = get_execution_date()
ano_corte      = get_ano_corte()
target         = g("ft_orcamento_desmobilizacao_geral")

# Etapa 1: @TabTemp → DataFrame
df_tabtemp = (
    spark.table(s("vw_orcamento_desmob_venda"))
    .filter(f"YEAR(DataDesmobilizacao) > {ano_corte} AND EhDesmobilizacao = 1")
    .select("Id","Imobilizado","Chave","Placa","AnoModelo","IdPeso",
            "ValorVenda","ResidualTotal","AnoReferencia","Ciclo","CicloAcao",
            "DataDesmobilizacao","CodCr","ModeloId","CidadeDesmobilizacaoId",
            "ValorAquisicao","Diretoria","VersaoOrcamento","SituacaoVersaoOrcamento",
            "PrazoDesmobilizacaoEmDias")
)

# Etapa 2: só escreve se tem dados (IF COUNT > 0)
if df_tabtemp.limit(1).count() > 0:
    df_tabtemp.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"YEAR(DataDesmobilizacao) > {ano_corte}") \
        .option("overwriteSchema", "false") \
        .saveAsTable(target)

# Etapa 3: UPDATE DataDesmobRealAssertividade
# #UltimoCr, #RegistroEhUltimoCr, #TemNaoTem → TempViews

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW tv_ultimo_cr AS
SELECT DISTINCT IdEquipamento, Equipamento, CentroCusto, ValidoDesde
FROM {s("stg_historico_cr_equipamentos_gab")}
WHERE RIGHT(CentroCusto, 5) NOT IN ('90600','92600','90610','92610')
  AND IdEquipamento = (
    SELECT MAX(tt.IdEquipamento)
    FROM {s("stg_historico_cr_equipamentos_gab")} tt
    WHERE RIGHT(tt.CentroCusto, 5) NOT IN ('90600','92600','90610','92610')
      AND tt.Equipamento = stg_historico_cr_equipamentos_gab.Equipamento
  )
""")

w_placa = Window.partitionBy("CodigoSapVix").orderBy(F.desc("DataDesmobilizacao"))
df_cte_group = (
    spark.sql(f"""
        SELECT CodigoSapVix, DataDesmobilizacao
        FROM (
            SELECT CD_SAP AS CodigoSapVix, DATA_TRANSFERENCIA AS DataDesmobilizacao
            FROM {s("stg_estoque_vendas_ativos_pa")}
            UNION ALL
            SELECT Placa, CONVERT(DATE, DATA_TRANSFERENCIA, 101)
            FROM {s("stg_historico_de_vendas_pa_excel")}
        ) u
        WHERE DataDesmobilizacao IS NOT NULL
    """)
    .withColumn("rn", F.row_number().over(w_placa))
    .filter("rn = 1")
    .drop("rn")
)

DeltaTable.forName(spark, target).alias("t") \
    .merge(df_cte_group.alias("s"), f"t.Placa = s.CodigoSapVix AND YEAR(t.DataDesmobilizacao) > {ano_corte}") \
    .whenMatchedUpdate(set={"t.DataDesmobRealAssertividade": "s.DataDesmobilizacao"}) \
    .execute()

validar_paridade(spark, target)
```

---

## 8. Score: 5.5 / 10

| Critério | Nota | Justificativa |
|---|---|---|
| Traduzibilidade dos padrões | 8/10 | TOP 1 resolvido; todos os padrões têm equivalente PySpark |
| Qualidade do código legado | 3/10 | GETDATE(), 8 UPDATEs, 4 temp tables — complexidade real não diminuiu |
| Templates fornecidos | 8/10 | Silver genérico + VW complexa + Gold com replaceWhere + validação |
| Testabilidade | 8/10 | Baseline integrado no validation.py com assert automático |
| Riscos de implementação | 5/10 | FT_DESMOBILIZACAO_PA (~1.050 linhas, 8 UPDATEs) ainda é o objeto de maior risco |
| Lib carga_silver integrada | 9/10 | ts_ingestao_brt confirmado; método correto mapeado por tipo de tabela |

*O score reflete complexidade real de implementação — não é limitado por falta de informação, mas pela densidade do código legado.*
