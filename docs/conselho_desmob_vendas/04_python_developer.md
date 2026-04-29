# Python Developer — Análise de Migração: Desmobilização e Vendas

**Agente:** Python Developer / PySpark + Código  
**Escopo:** `VWOrcamentoDesmobilizacaoVenda` (Silver) + `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` (Gold) + `PROC_FT_DESMOBILIZACAO_PA` (Gold)  
**Data:** 2026-04-29

---

## 1. Tabela de Tradução Específica

Cada padrão mapeado a partir do código real encontrado nos três arquivos SQL analisados.

### 1.1 Padrões de Expressão

| Anti-padrão T-SQL (código real) | Equivalente PySpark exato |
|---|---|
| `IIF(m.[Tipo] = 0, 'LEVE', IIF(m.[Tipo] = 1, 'PESADO', NULL))` | `F.when(F.col("m_Tipo") == 0, "LEVE").when(F.col("m_Tipo") == 1, "PESADO").otherwise(F.lit(None))` |
| `IIF(vi.[AnoModelo] = 0, NULL, vi.[AnoModelo])` | `F.when(F.col("vi_AnoModelo") == 0, F.lit(None)).otherwise(F.col("vi_AnoModelo"))` |
| `IIF(vi.[Ciclo1VendaQuando] IS NULL, 0, 1)` | `F.when(F.col("Ciclo1VendaQuando").isNull(), 0).otherwise(1)` |
| `IIF(LEFT(cr.[CR],4) in ('3010','3011'), 0, [Ciclo1ValorVenda]*0.2*0.12)` | `F.when(F.col("cr_CR").substr(1,4).isin("3010","3011"), 0).otherwise(F.col("Ciclo1ValorVenda") * 0.2 * 0.12)` |
| `IIF(m.[CodigoFipe] like '%sf%', NULL, m.[CodigoFipe])` | `F.when(F.lower(F.col("CodigoFipe")).contains("sf"), F.lit(None)).otherwise(F.col("CodigoFipe"))` |
| `IIF(CTE.[CodigoSapVix] IS NULL, NULL, CTE.[DataDesmobilizacao])` | `F.when(F.col("cte_CodigoSapVix").isNull(), F.lit(None)).otherwise(F.col("cte_DataDesmobilizacao"))` |
| `ISNULL(vi.[Ciclo1CustoPreparacao], 0)` | `F.coalesce(F.col("Ciclo1CustoPreparacao"), F.lit(0))` |
| `COALESCE(vi.[DataAtualizacao], vi.[DataRegistro])` | `F.coalesce(F.col("DataAtualizacao"), F.col("DataRegistro"))` |
| `COALESCE(acr1.[CR], cro.[CR], cr.[CR])` | `F.coalesce(F.col("acr1_CR"), F.col("cro_CR"), F.col("cr_CR"))` |

### 1.2 Padrões de Função de Data

| Anti-padrão T-SQL (código real) | Equivalente PySpark exato | Observação crítica |
|---|---|---|
| `IIF((o.[AnoReferencia]+1) = YEAR(GETDATE()), 1, 0)` | `F.when(F.col("AnoReferencia") + 1 == F.year(F.lit(execution_date)), 1).otherwise(0)` | `GETDATE()` vira parâmetro `execution_date` |
| `DATEADD(DAY, -CAST([Qtd dias no Centro de Custo atual] AS int), CAST(CONCAT(YEAR(GETDATE()),...) AS date))` | `F.date_sub(F.lit(execution_date).cast("date"), F.col("Qtd_dias_no_Centro_de_Custo_atual").cast("int"))` | Crítico: calcula DataDesmobilizacao; sem parâmetro = impossível backfill |
| `DATEDIFF(DAY, CAST(vi.[Ciclo1Quando] AS date), CAST(vi.[Ciclo1VendaQuando] AS date))` | `F.datediff(F.col("Ciclo1VendaQuando").cast("date"), F.col("Ciclo1Quando").cast("date"))` | Ordem dos argumentos é invertida em relação ao T-SQL |
| `YEAR(v.[DataDesmobilizacao]) > 2024` | `F.year(F.col("DataDesmobilizacao")) > 2024` | Filtro de partição para preservação histórica |
| `CAST(CONCAT(YEAR(GETDATE()),'-',MONTH(GETDATE()),'-',DAY(GETDATE())) AS date)` | `F.lit(execution_date).cast("date")` | Simplificação direta via parâmetro |

### 1.3 Padrões de Subquery Correlacionada → Window Function

| Anti-padrão T-SQL (código real) | Equivalente PySpark exato |
|---|---|
| `WHERE t1.[QuandoAlteracaoCentroResultado] = (SELECT MAX(t2.[QuandoAlteracaoCentroResultado]) FROM cte_alteracao_cr_ciclo1_geral AS t2 WHERE t1.[VersaoId] = t2.[VersaoId])` | `Window.partitionBy("VersaoId").orderBy(F.desc("QuandoAlteracaoCentroResultado"))` + `row_number() == 1` |
| `WHERE tabA.[MesAnoReferencia] = (SELECT MAX(tabB.[MesAnoReferencia]) FROM ... WHERE tabB.[Placa] = tabA.[Placa])` | `Window.partitionBy("Placa").orderBy(F.desc("MesAnoReferencia"))` + `row_number() == 1` |
| `WHERE t1.DATA_APROVACAO = (SELECT MAX(DATA_APROVACAO) FROM ... WHERE T2.[PLACA] = T1.[PLACA])` | `Window.partitionBy("PLACA").orderBy(F.desc("DATA_APROVACAO"))` + `row_number() == 1` |
| `WHERE [IdEquipamento] = (SELECT MAX([IdEquipamento]) FROM ... WHERE tt.[Equipamento] = t.[Equipamento])` | `Window.partitionBy("Equipamento").orderBy(F.desc("IdEquipamento"))` + `row_number() == 1` |
| `WHERE [IdEquipamento] = (SELECT MIN([IdEquipamento]) FROM ... WHERE t1.[Equipamento] = t2.[Equipamento])` | `Window.partitionBy("Equipamento").orderBy(F.asc("IdEquipamento"))` + `row_number() == 1` |
| `TOP 1 FROM ... ORDER BY Situacao DESC, DataRegistro DESC` (cte_versao_orcamento) | `Window.orderBy(F.desc("Situacao"), F.desc("DataRegistro"))` + `row_number() == 1` — **SEM PARTITION BY**: retorna uma única linha global; usar `F.broadcast()` do resultado |

### 1.4 Padrões de Tipo

| T-SQL (código real) | PySpark exato |
|---|---|
| `CAST(vi.[Id] AS uniqueidentifier)` | `.cast("string")` — UUID como string |
| `CAST(vi.[ModeloId] AS uniqueidentifier)` | `.cast("string")` |
| `CAST(vi.[Ciclo1DataDesmobilizacao] AS date)` | `.cast("date")` |
| `CAST(vo.[DataRegistro] AS datetime2(7))` | `.cast("timestamp")` |
| `CAST([Ciclo1ValorVenda] AS numeric(18,2))` | `.cast("decimal(18,2)")` |
| `CAST(1 AS int)` (literal Ciclo=1) | `F.lit(1).cast("integer")` |
| `CAST(cr.[CrTransicao] AS bit)` comparado com `CAST(0 AS bit)` | `.cast("boolean")` comparado com `F.lit(False)` |
| `CONCAT([CodigoFIPE],'-',[AnoModelo])` | `F.concat(F.col("CodigoFIPE"), F.lit("-"), F.col("AnoModelo"))` |
| `LEFT(cr.[CR], 4)` | `F.col("CR").substr(1, 4)` |
| `RIGHT([CentroCusto], 5)` | `F.col("CentroCusto").substr(-5, 5)` |
| `CAST(RIGHT([CentroCusto],5) AS int)` | `F.col("CentroCusto").substr(-5, 5).cast("integer")` |

### 1.5 Padrões de DML → Delta Lake

| Anti-padrão T-SQL (código real) | Estratégia Delta Lake |
|---|---|
| `DECLARE @TabTemp TABLE (...)` | DataFrame intermediário em memória — `df_tabtemp = df_silver.filter(...)` |
| `WITH (NOLOCK)` em todas as tabelas | Remover — Delta usa snapshot isolation por default |
| `DELETE FROM [DW].[FT_ORCAMENTO_DESMOBILIZACAO_GERAL] WHERE YEAR([DataDesmobilizacao]) > 2024` | `spark.sql("DELETE FROM bi_ativos.gold.ft_orcamento_desmobilizacao_geral WHERE year(DataDesmobilizacao) > 2024")` — suportado em Delta |
| `UPDATE OD SET OD.[DataDesmobRealAssertividade] = ... FROM ... LEFT JOIN cte_group` | `MERGE INTO ... USING ... ON ... WHEN MATCHED THEN UPDATE SET` |
| `SELECT * INTO #temp FROM cte` | `df_temp = spark.sql("SELECT ...").cache()` + `df_temp.createOrReplaceTempView("temp_nome")` |
| `DROP TABLE IF EXISTS #temp` | `spark.catalog.dropTempView("temp_nome")` (no final do notebook) |
| `INSERT INTO gold_table SELECT * FROM @TabTemp` | `df_tabtemp.write.format("delta").mode("append").saveAsTable(...)` |

---

## 2. Template Notebook Silver — VWOrcamentoDesmobilizacaoVenda

**Arquivo:** `notebooks/silver/vw_orcamento_desmobilizacao_venda.py`  
**Destino:** `bi_ativos.silver.vw_orcamento_desmobilizacao_venda`  
**Dependência SCD2:** lib fornecida pelo usuário — interface projetada na seção 4.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: VWOrcamentoDesmobilizacaoVenda
# MAGIC **Migrado de:** STG.VWOrcamentoDesmobilizacaoVenda (SQL Server VIEW)
# MAGIC
# MAGIC Implementação em PySpark puro (Workflows + Notebooks, sem DLT).
# MAGIC SCD2 gerenciado pela lib fornecida pelo usuário.
# MAGIC
# MAGIC | Origem | bi_ativos.bronze.stg_sima_versao_orcamento_itens (+ 9 tabelas) |
# MAGIC | Destino | bi_ativos.silver.vw_orcamento_desmobilizacao_venda |
# MAGIC | Padrão | Full overwrite — a view original não tem chave única de negócio estável |
# MAGIC | Uso SCD2 | NÃO — esta tabela é derivada/virtual; SCD2 aplica-se às tabelas-fonte Bronze→Silver |

# COMMAND ----------

dbutils.widgets.text("catalog", "bi_ativos", "Catalog")
dbutils.widgets.text("execution_date", "", "Data de execução (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog")
execution_date = dbutils.widgets.get("execution_date") or str(spark.sql("SELECT current_date()").collect()[0][0])

print(f"catalog={catalog} | execution_date={execution_date}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 1 — Leitura das tabelas Bronze

# COMMAND ----------

vi  = spark.table(f"{catalog}.bronze.stg_sima_versao_orcamento_itens")
vo  = spark.table(f"{catalog}.bronze.stg_sima_versao_orcamento")
o   = spark.table(f"{catalog}.bronze.stg_sima_orcamento")
cr  = spark.table(f"{catalog}.bronze.stg_sima_centro_resultado")
dcr = spark.table(f"{catalog}.bronze.stg_sima_diretoria")
m   = spark.table(f"{catalog}.bronze.stg_sima_modelo")
acr = spark.table(f"{catalog}.bronze.stg_sima_alteracao_centro_resultado_versao_orcamento_item")
prz = spark.table(f"{catalog}.bronze.stg_sima_parametrizacao_prazo_de_desmobilizacao")
fipe_raw = spark.table(f"{catalog}.bronze.stg_fipe_atual")
zfi_raw  = spark.table(f"{catalog}.bronze.stg_sap_zfiaa001")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 2 — cte_versao_orcamento
# MAGIC Substitui: TOP 1 ORDER BY Situacao DESC, DataRegistro DESC WHERE Situacao < 2
# MAGIC ATENÇÃO: o TOP 1 original era GLOBAL (sem PARTITION BY) — retorna UMA linha para toda a tabela.
# MAGIC Isso é um singleton usado como lookup. Aqui usamos row_number() global e broadcast.

# COMMAND ----------

# TOP 1 global sem PARTITION BY = ROW_NUMBER sem partição, manter rn == 1
# Resultado: 1 única linha que é joined com todas as versao_itens
w_versao = Window.orderBy(F.desc("Situacao"), F.desc("DataRegistro"))

df_versao_orcamento = (
    vo
    .filter(F.col("Situacao") < 2)
    .filter(F.col("DataDesativacao").isNull())
    .withColumn("_rn", F.row_number().over(w_versao))
    .filter(F.col("_rn") == 1)
    .select("Id", "OrcamentoId", "DataRegistro", "DataAtualizacao", "DataDesativacao", "Codigo", "Situacao")
    .drop("_rn")
)

# Broadcast: resultado é 1 linha — extremamente pequeno
df_versao_orcamento_bc = F.broadcast(df_versao_orcamento)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 3 — cte_alteracao_cr_ciclo1_ultimo e ciclo2_ultimo
# MAGIC Substitui: subquery correlacionada MAX(QuandoAlteracaoCentroResultado) por VersaoId

# COMMAND ----------

def build_ultimo_cr(acr_df, vi_df, ciclo_quando_col: str) -> "DataFrame":
    """
    Traduz as CTEs cte_alteracao_cr_ciclo{N}_geral + cte_alteracao_cr_ciclo{N}_ultimo.
    Substitui: subquery correlacionada WHERE QuandoAlteracao = (SELECT MAX(...) WHERE VersaoId = outer.VersaoId)
    """
    # Filtro geral (substitui cte_alteracao_cr_ciclo_geral)
    df_geral = (
        acr_df.alias("acr")
        .join(vi_df.alias("oi"), F.col("acr.VersaoId") == F.col("oi.Id"), "left")
        .filter(F.col("acr.DataDesativacao").isNull())
        .filter(F.col("oi.DataDesativacao").isNull())
        .filter(F.col("acr.QuandoAlteracaoCentroResultado") <= F.col(f"oi.{ciclo_quando_col}"))
        .select(
            F.col("acr.VersaoId"),
            F.col("acr.CentroResultadoId"),
            F.col("acr.QuandoAlteracaoCentroResultado"),
        )
        .distinct()
    )

    # Substitui subquery correlacionada: pegar o MAX por VersaoId via window
    w_cr = Window.partitionBy("VersaoId").orderBy(F.desc("QuandoAlteracaoCentroResultado"))

    df_ultimo = (
        df_geral
        .withColumn("_rn", F.row_number().over(w_cr))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .join(
            cr.select(F.col("Id").alias("cr_Id"), F.col("CR")),
            F.col("CentroResultadoId") == F.col("cr_Id"),
            "left",
        )
        .select("VersaoId", "CR")
        .distinct()
    )
    return df_ultimo


acr1 = build_ultimo_cr(acr, vi, "Ciclo1Quando")
acr2 = build_ultimo_cr(acr, vi, "Ciclo2Quando")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 4 — cte_prazo_desmobilizacao

# COMMAND ----------

df_prazo = (
    prz
    .filter(F.col("DataDesativacao").isNull())
    .filter(F.col("DiretoriaId").isNull())
    .select("TipoPeso", "PrazoDesmobilizacaoEmDias")
    .distinct()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 5 — Subqueries inline: FIPE e ZFI

# COMMAND ----------

df_fipe = (
    fipe_raw
    .withColumn("fipe_Id", F.concat(F.col("CodigoFIPE"), F.lit("-"), F.col("AnoModelo")))
    .withColumn("ValorFipeAtual", F.col("Price").cast("decimal(18,2)"))
    .select("fipe_Id", "ValorFipeAtual")
)

df_zfi = (
    zfi_raw
    .withColumn(
        "AnoFabricacao",
        F.when(
            (F.col("`Ano fabricação`") == "") | F.col("`Ano fabricação`").isNull(),
            F.lit(None)
        ).otherwise(F.col("`Ano fabricação`"))
    )
    .select(
        F.col("Equipamento"),
        F.col("AnoFabricacao"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 6 — Expressões de coluna compartilhadas (Ciclo 1 e Ciclo 2)
# MAGIC Substitui IIF() e CASE WHEN duplicados nos três branches do UNION ALL original.

# COMMAND ----------

execution_date_lit = F.lit(execution_date).cast("date")


def build_coluna_codcr(ano_referencia_col: str, cr_col: str, acr_col: str, cro_col: str):
    return F.when(
        F.col(ano_referencia_col) <= 2023,
        F.col(cr_col).cast("string")
    ).otherwise(
        F.coalesce(F.col(acr_col), F.col(cro_col), F.col(cr_col)).cast("string")
    )


def build_coluna_peso(tipo_col: str):
    return (
        F.when(F.col(tipo_col) == 0, "LEVE")
        .when(F.col(tipo_col) == 1, "PESADO")
        .otherwise(F.lit(None).cast("string"))
    )


def build_coluna_icms(cr_col: str, valor_venda_col: str):
    return F.when(
        F.col(cr_col).substr(1, 4).isin("3010", "3011"),
        F.lit(0).cast("decimal(18,2)")
    ).otherwise(
        (F.col(valor_venda_col) * 0.2 * 0.12).cast("decimal(18,2)")
    )


def build_coluna_custo_total(custo_prep_col: str, valor_venda_col: str, custo_ativo_col: str, cr_col: str):
    icms = F.when(
        F.col(cr_col).substr(1, 4).isin("3010", "3011"),
        F.lit(0)
    ).otherwise(F.coalesce(F.col(valor_venda_col), F.lit(0)) * 0.2 * 0.12)

    return (
        F.coalesce(F.col(custo_prep_col), F.lit(0))
        + icms
        + F.coalesce(F.col(custo_ativo_col), F.lit(0))
    ).cast("decimal(18,2)")


def build_coluna_resultado_venda(
    ano_referencia_col: str,
    valor_venda_col: str,
    custo_prep_col: str,
    custo_ativo_col: str,
    resultado_sima_col: str,
    cr_col: str,
):
    icms = F.when(
        F.col(cr_col).substr(1, 4).isin("3010", "3011"),
        F.lit(0)
    ).otherwise(F.coalesce(F.col(valor_venda_col), F.lit(0)) * 0.2 * 0.12)

    resultado_calculado = (
        F.coalesce(F.col(valor_venda_col), F.lit(0))
        - (F.coalesce(F.col(custo_prep_col), F.lit(0)) + icms + F.coalesce(F.col(custo_ativo_col), F.lit(0)))
    ).cast("decimal(18,2)")

    return F.when(
        F.col(ano_referencia_col) <= 2023,
        resultado_calculado
    ).otherwise(F.col(resultado_sima_col).cast("string"))


def build_coluna_referencia_ano_vigente(ano_referencia_col: str):
    return F.when(
        (F.col(ano_referencia_col) + 1) == F.year(execution_date_lit),
        F.lit(1)
    ).otherwise(F.lit(0)).cast("integer")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 7 — Joins base (compartilhados pelos 3 branches)
# MAGIC Evita repetir o bloco de joins 3 vezes como no SQL original.

# COMMAND ----------

# Join base: vi → versao_orcamento (substituindo INNER JOIN cte_versao_orcamento)
df_base = (
    vi
    .join(df_versao_orcamento_bc.alias("vo"), F.col("vi.VersaoId") == F.col("vo.Id"), "inner")
    .join(
        o.alias("o_tab"),
        F.col("vo.OrcamentoId") == F.col("o_tab.Id"),
        "left",
    )
    .join(
        cr.alias("cr_tab"),
        F.col("vi.CentroResultadoId") == F.col("cr_tab.Id"),
        "left",
    )
    .join(
        cr.select(
            F.col("Id").alias("cro_Id"),
            F.col("CR").alias("cro_CR"),
        ).alias("cro_tab"),
        F.col("vi.UltimoCentroResultadoOperacionalId") == F.col("cro_Id"),
        "left",
    )
    .join(
        dcr.select(
            F.col("Id").alias("dcr_Id"),
            F.col("Descricao").alias("dcr_Descricao"),
        ).alias("dcr_tab"),
        F.col("cr_tab.DiretoriaId") == F.col("dcr_Id"),
        "left",
    )
    .join(
        m.alias("m_tab"),
        F.col("vi.ModeloId") == F.col("m_tab.Id"),
        "left",
    )
    .join(
        df_fipe.alias("f_tab"),
        F.concat(F.col("m_tab.CodigoFipe"), F.lit("-"), F.col("vi.AnoModelo")) == F.col("f_tab.fipe_Id"),
        "left",
    )
    .join(
        df_zfi.alias("zfi_tab"),
        F.col("zfi_tab.Equipamento") == F.col("vi.Placa"),
        "left",
    )
    .join(
        df_prazo.alias("prz_tab"),
        F.col("prz_tab.TipoPeso") == F.col("m_tab.Tipo"),
        "left",
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 8 — Branch 1: Ciclo 1, CrTransicao=0 e CrVenda=0, Ciclo1Acao IN (2,3,4,5,6)
# MAGIC Regra de negócio: desmobilizações do ciclo 1 com CR não sendo de transição nem de venda.

# COMMAND ----------

def select_ciclo1_columns(df, acr_alias: str = "acr1_CR"):
    return df.select(
        F.col("vi.Id").cast("string").alias("Id"),
        F.col("vi.Id").cast("string").alias("OrcamentoImobilizadoId"),
        F.col("vi.Imobilizado").cast("string").alias("Imobilizado"),
        F.concat(F.col("cr_tab.CR").substr(1, 4), F.col("vi.Imobilizado")).cast("string").alias("Chave"),
        F.col("vi.Placa").cast("string").alias("Placa"),
        F.lit(1).cast("integer").alias("Ciclo"),
        F.col("vi.Ciclo1Acao").cast("integer").alias("CicloAcao"),
        build_coluna_codcr("o_tab.AnoReferencia", "cr_tab.CR", acr_alias, "cro_CR").alias("CodCr"),
        F.upper(F.col("dcr_Descricao")).cast("string").alias("Diretoria"),
        F.col("vi.ModeloId").cast("string").alias("ModeloId"),
        build_coluna_peso("m_tab.Tipo").alias("Peso"),
        F.col("m_tab.Tipo").cast("integer").alias("IdPeso"),
        F.when(F.col("vi.AnoModelo") == 0, F.lit(None)).otherwise(F.col("vi.AnoModelo")).cast("integer").alias("AnoModelo"),
        F.col("zfi_tab.AnoFabricacao").cast("integer").alias("AnoFabricacao"),
        F.col("vi.DataAquisicao").cast("date").alias("DataAquisicao"),
        F.col("vi.Ciclo1DataDesmobilizacao").cast("date").alias("DataDesmobilizacao"),
        F.col("vi.Ciclo1VendaQuando").cast("date").alias("DataVenda"),
        F.col("vi.Ciclo1CidadeDesmobilizacaoId").cast("string").alias("CidadeDesmobilizacaoId"),
        F.concat(F.col("o_tab.AnoReferencia").cast("string"), F.lit("/"), (F.col("o_tab.AnoReferencia") + 1).cast("string")).alias("Referencia"),
        build_coluna_referencia_ano_vigente("o_tab.AnoReferencia").alias("ReferenciaAnoVigente"),
        build_coluna_icms("cr_tab.CR", "vi.Ciclo1ValorVenda").alias("IcmsSobreVenda"),
        F.col("vi.Ciclo1CustoDoAtivoVendido").cast("decimal(18,2)").alias("CustoDoAtivoVendidoValorContabil"),
        F.col("vi.Ciclo1FipeProjecao").cast("decimal(18,2)").alias("FipeProjecao"),
        F.when(F.lower(F.col("m_tab.CodigoFipe")).contains("sf"), F.lit(None)).otherwise(F.col("m_tab.CodigoFipe")).cast("string").alias("CodigoFipeReal"),
        F.col("vi.Chave").cast("string").alias("ChaveSIMA"),
        F.col("vi.ValorAquisicao").cast("decimal(18,2)").alias("ValorAquisicao"),
        F.col("f_tab.ValorFipeAtual").cast("decimal(18,2)").alias("ValorFipeAtual"),
        F.col("vi.Ciclo1CustoDoAtivoVendido").cast("decimal(18,2)").alias("ResidualTotal"),
        build_coluna_custo_total("vi.Ciclo1CustoPreparacao", "vi.Ciclo1ValorVenda", "vi.Ciclo1CustoDoAtivoVendido", "cr_tab.CR").alias("CustoTotalVenda"),
        F.when(F.col("vi.Ciclo1ValorVenda") == 0, F.lit(0)).otherwise(F.col("vi.Ciclo1ValorVenda")).cast("decimal(18,2)").alias("ValorVenda"),
        F.col("vi.Ciclo1PercentualVenda").cast("decimal(18,2)").alias("PercentualVenda"),
        F.col("vi.Ciclo1CustoPreparacao").cast("decimal(18,2)").alias("CustoPreparacao"),
        build_coluna_resultado_venda("o_tab.AnoReferencia", "vi.Ciclo1ValorVenda", "vi.Ciclo1CustoPreparacao", "vi.Ciclo1CustoDoAtivoVendido", "vi.Ciclo1ResultadoDaVenda", "cr_tab.CR").alias("ValorResultadoVenda"),
        F.when(
            (F.coalesce(F.col("vi.Ciclo1ValorVenda"), F.lit(0)) == 0) | (F.coalesce(F.col("vi.Ciclo1FipeProjecao"), F.lit(0)) == 0),
            F.lit(0)
        ).otherwise(F.col("vi.Ciclo1ValorVenda") / F.col("vi.Ciclo1FipeProjecao")).alias("PerformanceProjetada"),
        F.col("o_tab.AnoReferencia").cast("integer").alias("AnoReferencia"),
        F.col("vo.Codigo").cast("string").alias("VersaoOrcamento"),
        F.col("vo.Situacao").cast("integer").alias("SituacaoVersaoOrcamento"),
        F.col("vo.DataRegistro").cast("timestamp").alias("UltimaDataRegistro"),
        F.when(
            F.col("vi.DataDesativacao").isNull()
            & F.col("vi.Ciclo1Acao").isin(2, 3, 4, 5)
            & (F.col("cr_tab.CrTransicao").cast("boolean") == F.lit(False))
            & (F.col("cr_tab.CrVenda").cast("boolean") == F.lit(False)),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("EhDesmobilizacao"),
        F.when(F.col("vi.Ciclo1VendaQuando").isNull(), F.lit(0)).otherwise(F.lit(1)).cast("integer").alias("EhVenda"),
        F.datediff(F.col("vi.Ciclo1VendaQuando").cast("date"), F.col("vi.Ciclo1Quando").cast("date")).alias("DiasEstoqueProjetado"),
        F.coalesce(F.col("vi.DataAtualizacao"), F.col("vi.DataRegistro")).cast("timestamp").alias("DataAlteracaoVersaoOrcItem"),
        F.coalesce(F.col("vo.DataAtualizacao"), F.col("vo.DataRegistro")).cast("timestamp").alias("DataAlteracaoVersaoOrc"),
        F.coalesce(F.col("o_tab.DataAtualizacao"), F.col("o_tab.DataRegistro")).cast("timestamp").alias("DataAlteracaoOrc"),
        F.col("vi.EhNovaCompra"),
        F.col("prz_tab.PrazoDesmobilizacaoEmDias").cast("integer").alias("PrazoDesmobilizacaoEmDias"),
    )


df_branch1 = (
    df_base
    .join(acr1.select(F.col("VersaoId").alias("acr1_VersaoId"), F.col("CR").alias("acr1_CR")), F.col("vi.Id") == F.col("acr1_VersaoId"), "left")
    .filter(F.col("vi.Ciclo1Acao").isin(2, 3, 4, 5, 6))
    .filter(F.col("vi.DataDesativacao").isNull())
    .filter(F.col("vo.DataDesativacao").isNull())
    .filter(F.col("o_tab.DataDesativacao").isNull())
    .filter(F.col("cr_tab.CrTransicao").cast("boolean") == F.lit(False))
    .filter(F.col("cr_tab.CrVenda").cast("boolean") == F.lit(False))
)

df_branch1 = select_ciclo1_columns(df_branch1, acr_alias="acr1_CR")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 9 — Branch 2: Ciclo 1, CrVenda=1 OU CrTransicao=1

# COMMAND ----------

df_branch2 = (
    df_base
    .join(acr1.select(F.col("VersaoId").alias("acr1_VersaoId"), F.col("CR").alias("acr1_CR")), F.col("vi.Id") == F.col("acr1_VersaoId"), "left")
    .filter(F.col("vi.DataDesativacao").isNull())
    .filter(F.col("vo.DataDesativacao").isNull())
    .filter(F.col("o_tab.DataDesativacao").isNull())
    .filter(
        (F.col("cr_tab.CrVenda").cast("boolean") == F.lit(True))
        | (F.col("cr_tab.CrTransicao").cast("boolean") == F.lit(True))
    )
)

df_branch2 = select_ciclo1_columns(df_branch2, acr_alias="acr1_CR")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 10 — Branch 3: Ciclo 2, Ciclo2Acao IN (2,3,4,5,6)

# COMMAND ----------

def select_ciclo2_columns(df):
    return df.select(
        F.col("vi.Id").cast("string").alias("Id"),
        F.col("vi.Id").cast("string").alias("OrcamentoImobilizadoId"),
        F.col("vi.Imobilizado").cast("string").alias("Imobilizado"),
        F.concat(F.col("cr_tab.CR").substr(1, 4), F.col("vi.Imobilizado")).cast("string").alias("Chave"),
        F.col("vi.Placa").cast("string").alias("Placa"),
        F.lit(2).cast("integer").alias("Ciclo"),
        F.col("vi.Ciclo2Acao").cast("integer").alias("CicloAcao"),
        build_coluna_codcr("o_tab.AnoReferencia", "cr_tab.CR", "acr2_CR", "cro_CR").alias("CodCr"),
        F.upper(F.col("dcr_Descricao")).cast("string").alias("Diretoria"),
        F.col("vi.ModeloId").cast("string").alias("ModeloId"),
        build_coluna_peso("m_tab.Tipo").alias("Peso"),
        F.col("m_tab.Tipo").cast("integer").alias("IdPeso"),
        F.when(F.col("vi.AnoModelo") == 0, F.lit(None)).otherwise(F.col("vi.AnoModelo")).cast("integer").alias("AnoModelo"),
        F.col("zfi_tab.AnoFabricacao").cast("integer").alias("AnoFabricacao"),
        F.col("vi.DataAquisicao").cast("date").alias("DataAquisicao"),
        F.col("vi.Ciclo2DataDesmobilizacao").cast("date").alias("DataDesmobilizacao"),
        F.col("vi.Ciclo2VendaQuando").cast("date").alias("DataVenda"),
        F.col("vi.Ciclo2CidadeDesmobilizacaoId").cast("string").alias("CidadeDesmobilizacaoId"),
        F.concat(F.col("o_tab.AnoReferencia").cast("string"), F.lit("/"), (F.col("o_tab.AnoReferencia") + 1).cast("string")).alias("Referencia"),
        build_coluna_referencia_ano_vigente("o_tab.AnoReferencia").alias("ReferenciaAnoVigente"),
        build_coluna_icms("cr_tab.CR", "vi.Ciclo2ValorVenda").alias("IcmsSobreVenda"),
        F.col("vi.Ciclo2CustoDoAtivoVendido").cast("decimal(18,2)").alias("CustoDoAtivoVendidoValorContabil"),
        F.col("vi.Ciclo2FipeProjecao").cast("decimal(18,2)").alias("FipeProjecao"),
        F.when(F.lower(F.col("m_tab.CodigoFipe")).contains("sf"), F.lit(None)).otherwise(F.col("m_tab.CodigoFipe")).cast("string").alias("CodigoFipeReal"),
        F.col("vi.Chave").cast("string").alias("ChaveSIMA"),
        F.col("vi.ValorAquisicao").cast("decimal(18,2)").alias("ValorAquisicao"),
        F.col("f_tab.ValorFipeAtual").cast("decimal(18,2)").alias("ValorFipeAtual"),
        F.col("vi.Ciclo2CustoDoAtivoVendido").cast("decimal(18,2)").alias("ResidualTotal"),
        build_coluna_custo_total("vi.Ciclo2CustoPreparacao", "vi.Ciclo2ValorVenda", "vi.Ciclo2CustoDoAtivoVendido", "cr_tab.CR").alias("CustoTotalVenda"),
        F.when(F.col("vi.Ciclo2ValorVenda") == 0, F.lit(0)).otherwise(F.col("vi.Ciclo2ValorVenda")).cast("decimal(18,2)").alias("ValorVenda"),
        F.col("vi.Ciclo2PercentualVenda").cast("decimal(18,2)").alias("PercentualVenda"),
        F.col("vi.Ciclo2CustoPreparacao").cast("decimal(18,2)").alias("CustoPreparacao"),
        build_coluna_resultado_venda("o_tab.AnoReferencia", "vi.Ciclo2ValorVenda", "vi.Ciclo2CustoPreparacao", "vi.Ciclo2CustoDoAtivoVendido", "vi.Ciclo2ResultadoDaVenda", "cr_tab.CR").alias("ValorResultadoVenda"),
        F.when(
            (F.coalesce(F.col("vi.Ciclo2ValorVenda"), F.lit(0)) == 0) | (F.coalesce(F.col("vi.Ciclo2FipeProjecao"), F.lit(0)) == 0),
            F.lit(0)
        ).otherwise(F.col("vi.Ciclo2ValorVenda") / F.col("vi.Ciclo2FipeProjecao")).alias("PerformanceProjetada"),
        F.col("o_tab.AnoReferencia").cast("integer").alias("AnoReferencia"),
        F.col("vo.Codigo").cast("string").alias("VersaoOrcamento"),
        F.col("vo.Situacao").cast("integer").alias("SituacaoVersaoOrcamento"),
        F.col("vo.DataRegistro").cast("timestamp").alias("UltimaDataRegistro"),
        F.when(
            F.col("vi.DataDesativacao").isNull()
            & F.col("vi.Ciclo2Acao").isin(2, 3, 4, 5)
            & (F.col("cr_tab.CrTransicao").cast("boolean") == F.lit(False))
            & (F.col("cr_tab.CrVenda").cast("boolean") == F.lit(False)),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("EhDesmobilizacao"),
        F.when(F.col("vi.Ciclo2VendaQuando").isNull(), F.lit(0)).otherwise(F.lit(1)).cast("integer").alias("EhVenda"),
        F.datediff(F.col("vi.Ciclo2VendaQuando").cast("date"), F.col("vi.Ciclo2Quando").cast("date")).alias("DiasEstoqueProjetado"),
        F.coalesce(F.col("vi.DataAtualizacao"), F.col("vi.DataRegistro")).cast("timestamp").alias("DataAlteracaoVersaoOrcItem"),
        F.coalesce(F.col("vo.DataAtualizacao"), F.col("vo.DataRegistro")).cast("timestamp").alias("DataAlteracaoVersaoOrc"),
        F.coalesce(F.col("o_tab.DataAtualizacao"), F.col("o_tab.DataRegistro")).cast("timestamp").alias("DataAlteracaoOrc"),
        F.col("vi.EhNovaCompra"),
        F.col("prz_tab.PrazoDesmobilizacaoEmDias").cast("integer").alias("PrazoDesmobilizacaoEmDias"),
    )


df_branch3 = (
    df_base
    .join(acr2.select(F.col("VersaoId").alias("acr2_VersaoId"), F.col("CR").alias("acr2_CR")), F.col("vi.Id") == F.col("acr2_VersaoId"), "left")
    .filter(F.col("vi.Ciclo2Acao").isin(2, 3, 4, 5, 6))
    .filter(F.col("vi.DataDesativacao").isNull())
    .filter(F.col("vo.DataDesativacao").isNull())
    .filter(F.col("o_tab.DataDesativacao").isNull())
)

df_branch3 = select_ciclo2_columns(df_branch3)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 11 — UNION ALL dos 3 branches
# MAGIC Substitui os três SELECT ... UNION ALL do SQL original.

# COMMAND ----------

df_final = df_branch1.unionByName(df_branch2).unionByName(df_branch3)

print(f"Total de linhas: {df_final.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 12 — Escrita full overwrite
# MAGIC Esta tabela Silver é derivada (equivalente a uma VIEW materializada).
# MAGIC SCD2 não se aplica aqui — aplica-se nas tabelas-fonte Bronze → Silver.
# MAGIC O job de Gold lê desta tabela Silver, portanto ela deve estar sempre atualizada.

# COMMAND ----------

(df_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.vw_orcamento_desmobilizacao_venda")
)

print("Silver VWOrcamentoDesmobilizacaoVenda: escrita concluida.")
```

---

## 3. Template Notebook Gold — FT_ORCAMENTO_DESMOBILIZACAO_GERAL

**Arquivo:** `notebooks/gold/ft_orcamento_desmobilizacao_geral.py`  
**Destino:** `bi_ativos.gold.ft_orcamento_desmobilizacao_geral`  
**Pré-requisito:** `bi_ativos.gold.ft_ultimos_registros_por_placa_crs_desmob` já executado no workflow.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: FT_ORCAMENTO_DESMOBILIZACAO_GERAL
# MAGIC **Migrado de:** DW.PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL
# MAGIC
# MAGIC Estratégia de carga:
# MAGIC 1. Lê Silver vw_orcamento_desmobilizacao_venda filtrando YEAR > 2024 e EhDesmobilizacao = 1
# MAGIC 2. Se há dados: DELETE WHERE YEAR > 2024, depois INSERT — preserva histórico <= 2024
# MAGIC 3. UPDATE DataDesmobRealAssertividade via MERGE usando 3 temp views encadeadas

# COMMAND ----------

dbutils.widgets.text("catalog", "bi_ativos", "Catalog")
dbutils.widgets.text("execution_date", "", "Data de execução (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog")
execution_date = dbutils.widgets.get("execution_date") or str(spark.sql("SELECT current_date()").collect()[0][0])

print(f"catalog={catalog} | execution_date={execution_date}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 1 — @TabTemp → DataFrame intermediário
# MAGIC Substitui: DECLARE @TabTemp TABLE (...) + INSERT INTO @TabTemp SELECT FROM VW WHERE YEAR > 2024 AND EhDesmobilizacao = 1

# COMMAND ----------

df_silver_vw = spark.table(f"{catalog}.silver.vw_orcamento_desmobilizacao_venda")

df_tabtemp = (
    df_silver_vw
    .filter(F.year(F.col("DataDesmobilizacao")) > 2024)
    .filter(F.col("EhDesmobilizacao") == 1)
    .select(
        "Id", "Imobilizado", "Chave",
        F.col("Placa").cast("string").alias("Placa"),
        "AnoModelo",
        F.col("IdPeso").cast("string").alias("IdPeso"),
        "ValorVenda", "ResidualTotal", "AnoReferencia",
        "Ciclo", "CicloAcao", "DataDesmobilizacao",
        F.col("CodCr").cast("string").alias("CodCr"),
        "ModeloId", "CidadeDesmobilizacaoId",
        "ValorAquisicao", "Diretoria",
        "VersaoOrcamento", "SituacaoVersaoOrcamento", "PrazoDesmobilizacaoEmDias",
    )
    # Placa e CodCr: cast equivalente ao SQL (nvarchar(11) e nvarchar(40))
    .withColumn("Placa", F.col("Placa").substr(1, 11))
    .withColumn("CodCr", F.col("CodCr").substr(1, 40))
    .cache()
)

tabtemp_count = df_tabtemp.count()
print(f"Registros em df_tabtemp (equivalente @TabTemp): {tabtemp_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 2 — Lógica IF @TabTemp > 0: DELETE + INSERT
# MAGIC
# MAGIC Estratégia:
# MAGIC - NÃO usar overwrite total — preservar registros com YEAR(DataDesmobilizacao) <= 2024
# MAGIC - DELETE via Delta SQL WHERE YEAR > 2024 (suportado desde Delta 1.0)
# MAGIC - INSERT via append

# COMMAND ----------

if tabtemp_count > 0:
    spark.sql(f"""
        DELETE FROM {catalog}.gold.ft_orcamento_desmobilizacao_geral
        WHERE year(DataDesmobilizacao) > 2024
    """)

    (df_tabtemp
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.gold.ft_orcamento_desmobilizacao_geral")
    )

    print(f"Inseridos {tabtemp_count} registros apos DELETE de YEAR > 2024.")
else:
    print("df_tabtemp vazio — nenhuma alteracao aplicada. Verifique Silver.")

df_tabtemp.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 3 — UPDATE DataDesmobRealAssertividade
# MAGIC Substitui: 3 temp tables (#UltimoCr, #RegistroEhUltimoCr, #TemNaoTem) + cte_group + UPDATE FROM JOIN
# MAGIC
# MAGIC Estratégia: substituir as temp tables por DataFrames cacheados e temp views,
# MAGIC depois usar MERGE para realizar o UPDATE em Delta.

# COMMAND ----------

df_hist_cr = spark.table(f"{catalog}.bronze.stg_historico_cr_equipamentos_gab")
df_hist_vendas = spark.table(f"{catalog}.bronze.stg_historico_de_vendas_pa_excel")
df_hist_ebec = spark.table(f"{catalog}.bronze.stg_excel_historico_desmobilizacao_ebec")
df_estoque_pa = spark.table(f"{catalog}.bronze.stg_estoque_vendas_ativos_pa")
df_zfi = spark.table(f"{catalog}.bronze.stg_sap_zfiaa001")
df_ultimos_crs_desmob = spark.table(f"{catalog}.gold.ft_ultimos_registros_por_placa_crs_desmob")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3a — #UltimoCrOperPorPeriodo
# MAGIC Substitui: cte_ultimo_cr_operacional → SELECT * INTO #UltimoCrOper
# MAGIC Anti-padrão: subquery correlacionada MAX(IdEquipamento) por Equipamento

# COMMAND ----------

FILTRO_CR_EXCLUIDOS = ("90600", "92600", "90610", "92610")

w_ultimo_cr = Window.partitionBy("Equipamento").orderBy(F.desc("IdEquipamento"))

df_ultimo_cr_oper = (
    df_hist_cr
    .filter(~F.col("CentroCusto").substr(-5, 5).isin(*FILTRO_CR_EXCLUIDOS))
    .withColumn("_rn", F.row_number().over(w_ultimo_cr))
    .filter(F.col("_rn") == 1)
    .select("IdEquipamento", "Equipamento", "CentroCusto", "ValidoDesde")
    .drop("_rn")
    .distinct()
)

df_ultimo_cr_oper.createOrReplaceTempView("tmp_ultimo_cr_oper")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3b — #RegistroEhUltimoCrOper
# MAGIC Substitui: cte_marcacao_ultimo_cr_operacional com IIF(tt.Equipamento IS NULL, 0, 1) → SELECT * INTO #Registro

# COMMAND ----------

df_registro_eh_ultimo = (
    df_hist_cr
    .filter(~F.col("CentroCusto").substr(-5, 5).isin(*FILTRO_CR_EXCLUIDOS))
    .alias("t")
    .join(
        df_ultimo_cr_oper.select("IdEquipamento", F.col("Equipamento").alias("uc_Equipamento")).alias("tt"),
        F.col("t.IdEquipamento") == F.col("tt.uc_IdEquipamento"),
        "left",
    )
    .withColumn(
        "EhUltimoCrOper",
        F.when(F.col("uc_Equipamento").isNull(), F.lit(0)).otherwise(F.lit(1))
    )
    .select(
        F.col("t.IdEquipamento"),
        F.col("t.Equipamento"),
        F.col("t.CentroCusto"),
        F.col("t.ValidoDesde"),
        F.col("EhUltimoCrOper"),
    )
    .distinct()
)

df_registro_eh_ultimo.createOrReplaceTempView("tmp_registro_eh_ultimo_cr")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3c — #TemNaoTem
# MAGIC Substitui: cte_tem_cr_operacional UNION ALL cte_nao_tem_cr_operacional → SELECT * INTO #TemNaoTem
# MAGIC Anti-padrão: subquery correlacionada WHERE IdEquipamento > (SELECT IdEquipamento FROM ... WHERE EhUltimoCrOper=1)

# COMMAND ----------

df_reg = spark.table("tmp_registro_eh_ultimo_cr")

# tem_cr: registros que possuem pelo menos um irmao com EhUltimoCrOper = 1
df_tem = (
    df_reg.alias("t")
    .join(
        df_reg.filter(F.col("EhUltimoCrOper") == 1).select("Equipamento").alias("tt"),
        F.col("t.Equipamento") == F.col("tt.Equipamento"),
        "inner",
    )
    .select("t.*")
)

# nao_tem_cr: registros cujo Equipamento nao aparece em df_tem
df_nao_tem = (
    df_reg.alias("t")
    .join(
        df_tem.select("Equipamento").distinct().alias("tt"),
        F.col("t.Equipamento") == F.col("tt.Equipamento"),
        "left_anti",
    )
)

# Para df_tem: manter apenas registros APOS o ultimo CR operacional
# Substitui: WHERE t.IdEquipamento > (SELECT IdEquipamento FROM cte_tem WHERE EhUltimoCrOper=1 AND Equipamento = t.Equipamento)
w_ultimo_op = Window.partitionBy("Equipamento")

df_tem_filtrado = (
    df_tem
    .withColumn(
        "id_ultimo_cr_oper",
        F.max(F.when(F.col("EhUltimoCrOper") == 1, F.col("IdEquipamento"))).over(w_ultimo_op)
    )
    .filter(F.col("IdEquipamento") > F.col("id_ultimo_cr_oper"))
    .drop("id_ultimo_cr_oper")
)

df_tem_naotem = (
    df_tem_filtrado
    .withColumn("NumeroContratoAbrev", F.col("CentroCusto").substr(-5, 5).cast("integer"))
    .unionByName(
        df_nao_tem.withColumn("NumeroContratoAbrev", F.col("CentroCusto").substr(-5, 5).cast("integer"))
    )
)

df_tem_naotem.createOrReplaceTempView("tmp_tem_naotem_cr")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3d — cte_ultima_data_transferencia
# MAGIC Substitui: TOP 1 ORDER BY IdEquipamento ASC WHERE NumeroContratoAbrev IN (90600,...) AND EhUltimoCrOper <> 1

# COMMAND ----------

df_tntn = spark.table("tmp_tem_naotem_cr")

FILTRO_CRS_DESMOB = ("90600", "92600", "90610", "92610")

w_ultima_data = Window.partitionBy("Equipamento").orderBy(F.asc("IdEquipamento"))

df_ultima_data_transf = (
    df_tntn
    .filter(F.col("NumeroContratoAbrev").isin(*[int(x) for x in FILTRO_CRS_DESMOB]))
    .filter(F.col("EhUltimoCrOper") != 1)
    .withColumn("_rn", F.row_number().over(w_ultima_data))
    .filter(F.col("_rn") == 1)
    .select(
        F.col("Equipamento"),
        F.col("ValidoDesde").cast("date").alias("DataTransferencia"),
    )
    .drop("_rn")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3e — cte_ultimos_registros_por_placa_cr_desmob_final
# MAGIC Substitui: subquery correlacionada MIN(IdEquipamento) por Equipamento em FT_ULTIMOS_REGISTROS

# COMMAND ----------

w_min_id = Window.partitionBy("Equipamento").orderBy(F.asc("IdEquipamento"))

df_ultimos_crs_final = (
    df_ultimos_crs_desmob
    .withColumn("_rn", F.row_number().over(w_min_id))
    .filter(F.col("_rn") == 1)
    .select("Equipamento", "ValidoDesde")
    .drop("_rn")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3f — CTEs historico, historico_ebec, plataforma_ativos, zfi
# MAGIC GETDATE() → execution_date (passo crítico para backfill)

# COMMAND ----------

execution_date_lit = F.lit(execution_date).cast("date")

df_historico = (
    df_hist_vendas
    .select(
        F.col("Placa").alias("CodigoSapVix"),
        F.to_date(F.col("DATA_TRANSFERENCIA"), "MM/dd/yyyy").alias("DataDesmobilizacao"),
    )
    .distinct()
)

df_historico_ebec = (
    df_hist_ebec
    .select(
        F.when(F.col("Equipamento") == "0", F.lit(None)).otherwise(F.col("Equipamento").cast("string")).alias("CodigoSapVix"),
        F.when(F.col("DataDesmobilizacao") == "0", F.lit(None)).otherwise(F.col("DataDesmobilizacao").cast("date")).alias("DataDesmobilizacao"),
    )
    .distinct()
)

# Plataforma ativos: GETDATE() → execution_date
df_plataforma = (
    df_estoque_pa.alias("EVAPA")
    .join(df_ultimos_crs_final.alias("URCRD"), F.col("URCRD.Equipamento") == F.col("EVAPA.CD_SAP"), "left")
    .join(df_ultima_data_transf.alias("T"), F.col("T.Equipamento") == F.col("EVAPA.CD_SAP"), "left")
    .join(df_historico.alias("ch"), F.col("EVAPA.PLACA") == F.col("ch.CodigoSapVix"), "left")
    .join(df_historico_ebec.alias("che"), F.col("EVAPA.PLACA") == F.col("che.CodigoSapVix"), "left")
    .filter(F.col("EVAPA.PLACA") != "MPU7761")
    .filter(F.col("EVAPA.CD_SAP") != "MPU7761")
    .filter(F.col("ch.CodigoSapVix").isNull())
    .filter(F.col("che.CodigoSapVix").isNull())
    .filter(F.col("EVAPA.CR_ORIGEM").substr(1, 1) != "2")
    .select(
        F.col("EVAPA.CD_SAP").alias("CodigoSapVix"),
        F.coalesce(
            F.col("URCRD.ValidoDesde"),
            F.col("T.DataTransferencia"),
            F.col("EVAPA.DATA_TRANSFERENCIA"),
        ).cast("date").alias("DataDesmobilizacao"),
    )
    .distinct()
)

# ZFI: DATEADD(DAY, -Qtd_dias, GETDATE()) → date_sub(execution_date, Qtd_dias)
df_zfi_crs_desmob = (
    df_zfi.alias("zfi")
    .withColumn(
        "CodigoSapVix",
        F.when(
            F.col("Equipamento") != F.col("`Local de instalação`"),
            F.col("`Local de instalação`")
        ).otherwise(F.col("Equipamento"))
    )
    # GETDATE() → execution_date (ponto crítico de migração)
    .withColumn(
        "DataDesmobilizacao",
        F.date_sub(execution_date_lit, F.col("`Qtd dias no Centro de Custo atual`").cast("integer"))
    )
    .join(df_historico.alias("ch"), F.col("CodigoSapVix") == F.col("ch.CodigoSapVix"), "left")
    .join(df_historico_ebec.alias("che"), F.col("CodigoSapVix") == F.col("che.CodigoSapVix"), "left")
    .join(df_plataforma.alias("pa"), F.col("CodigoSapVix") == F.col("pa.CodigoSapVix"), "left")
    .filter(F.col("zfi.`Centro custo`").substr(-5, 5).isin("90660", "92660"))
    .filter(F.col("ch.CodigoSapVix").isNull())
    .filter(F.col("che.CodigoSapVix").isNull())
    .filter(F.col("pa.CodigoSapVix").isNull())
    .filter(F.col("`Status Sistema`").isin("LIDI", "MONT"))
    .select("CodigoSapVix", "DataDesmobilizacao")
    .distinct()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3g — cte_union + cte_group (ROW_NUMBER já presente no SQL original)
# MAGIC O SQL original já havia migrado MAX/GROUP BY para ROW_NUMBER — mantemos a mesma lógica.

# COMMAND ----------

df_union = (
    df_historico.filter(F.col("DataDesmobilizacao").isNotNull())
    .unionAll(df_historico_ebec.filter(F.col("DataDesmobilizacao").isNotNull()))
    .unionAll(df_plataforma.filter(F.col("DataDesmobilizacao").isNotNull()))
    .unionAll(df_zfi_crs_desmob.filter(F.col("DataDesmobilizacao").isNotNull()))
)

w_group = Window.partitionBy("CodigoSapVix").orderBy(F.desc("DataDesmobilizacao"))

df_cte_group = (
    df_union
    .withColumn("_rn", F.row_number().over(w_group))
    .filter(F.col("_rn") == 1)
    .select("CodigoSapVix", "DataDesmobilizacao")
    .drop("_rn")
)

df_cte_group.createOrReplaceTempView("tmp_cte_group")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Passo 3h — UPDATE DataDesmobRealAssertividade via MERGE
# MAGIC Substitui: UPDATE OD SET DataDesmobRealAssertividade = IIF(CTE IS NULL, NULL, CTE.DataDesmobilizacao)
# MAGIC FROM FT_ORCAMENTO_DESMOBILIZACAO_GERAL LEFT JOIN cte_group

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {catalog}.gold.ft_orcamento_desmobilizacao_geral AS target
    USING (
        SELECT
            od.Placa,
            CASE
                WHEN cte.CodigoSapVix IS NULL THEN NULL
                ELSE cte.DataDesmobilizacao
            END AS DataDesmobRealAssertividade_nova
        FROM {catalog}.gold.ft_orcamento_desmobilizacao_geral od
        LEFT JOIN tmp_cte_group cte ON cte.CodigoSapVix = od.Placa
        WHERE year(od.DataDesmobilizacao) > 2024
    ) AS source
    ON target.Placa = source.Placa
       AND year(target.DataDesmobilizacao) > 2024
    WHEN MATCHED THEN UPDATE SET
        target.DataDesmobRealAssertividade = source.DataDesmobRealAssertividade_nova
""")

print("UPDATE DataDesmobRealAssertividade concluido via MERGE.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 4 — Limpeza de temp views

# COMMAND ----------

for view in ["tmp_ultimo_cr_oper", "tmp_registro_eh_ultimo_cr", "tmp_tem_naotem_cr", "tmp_cte_group"]:
    spark.catalog.dropTempView(view)

print("Temp views removidas.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Passo 5 — Auditoria

# COMMAND ----------

from common_utils import log_audit

final_count = spark.sql(f"SELECT count(*) FROM {catalog}.gold.ft_orcamento_desmobilizacao_geral").collect()[0][0]
log_audit(spark, catalog, "gold", "ft_orcamento_desmobilizacao_geral", "ft_orcamento_desmobilizacao_geral", final_count)
print(f"Auditoria registrada. Total de linhas na tabela Gold: {final_count}")
```

---

## 4. Estrutura comum utils.py

Funções que todos os notebooks deste job utilizarão. Amplia o `common_utils.py` existente com as necessidades específicas identificadas no código legado.

**Arquivo:** `notebooks/common_utils_desmob.py`

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utils — Desmobilização e Vendas
# MAGIC Importar via %run ./common_utils_desmob no topo de cada notebook do job.

# COMMAND ----------

from __future__ import annotations

from typing import Sequence
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Substituição de GETDATE()
# ---------------------------------------------------------------------------

def get_execution_date(dbutils_ref, widget_name: str = "execution_date") -> str:
    """
    Retorna a data de execução do widget ou a data atual como fallback.
    Resolve o anti-padrão GETDATE() — permite backfill determinístico.

    Uso em notebooks:
        execution_date = get_execution_date(dbutils)
        execution_date_lit = F.lit(execution_date).cast("date")
    """
    raw = dbutils_ref.widgets.get(widget_name)
    if raw:
        return raw
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    return str(spark.sql("SELECT current_date()").collect()[0][0])


# ---------------------------------------------------------------------------
# TOP 1 sem PARTITION BY (cte_versao_orcamento)
# ---------------------------------------------------------------------------

def top1_global(
    df: DataFrame,
    order_cols: list[tuple[str, str]],
    select_cols: Sequence[str] | None = None,
) -> DataFrame:
    """
    Substitui TOP 1 ORDER BY sem PARTITION BY — retorna UMA linha global.
    Equivalente SQL: SELECT TOP 1 ... FROM table ORDER BY col1 DESC, col2 DESC

    Args:
        df: DataFrame de entrada.
        order_cols: lista de tuplas (nome_coluna, "asc"|"desc").
        select_cols: colunas a retornar; None retorna todas.

    Retorna DataFrame com 1 linha, pronto para broadcast join.

    Exemplo (cte_versao_orcamento):
        df_versao = top1_global(
            df=spark.table("bi_ativos.bronze.stg_sima_versao_orcamento").filter(F.col("Situacao") < 2),
            order_cols=[("Situacao", "desc"), ("DataRegistro", "desc")],
        )
        df_versao_bc = F.broadcast(df_versao)
    """
    order_exprs = [
        F.desc(c) if d == "desc" else F.asc(c)
        for c, d in order_cols
    ]
    w = Window.orderBy(*order_exprs)
    result = (
        df
        .withColumn("_rn_global", F.row_number().over(w))
        .filter(F.col("_rn_global") == 1)
        .drop("_rn_global")
    )
    if select_cols:
        result = result.select(*select_cols)
    return result


# ---------------------------------------------------------------------------
# TOP 1 com PARTITION BY (subqueries correlacionadas)
# ---------------------------------------------------------------------------

def top1_by_partition(
    df: DataFrame,
    partition_cols: Sequence[str],
    order_cols: list[tuple[str, str]],
    select_cols: Sequence[str] | None = None,
) -> DataFrame:
    """
    Substitui subquery correlacionada WHERE col = (SELECT MAX/MIN col FROM ... WHERE outer.key = inner.key).
    Usa ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1.

    Exemplos de uso deste código:
        # cte_alteracao_cr_ciclo1_ultimo (MAX QuandoAlteracaoCentroResultado por VersaoId)
        top1_by_partition(df_geral, ["VersaoId"], [("QuandoAlteracaoCentroResultado", "desc")])

        # cte_sla_vistoria_aprovada (MAX DATA_APROVACAO por PLACA)
        top1_by_partition(df_vistorias, ["PLACA"], [("DATA_APROVACAO", "desc")])

        # cte_valor_aquisicao (MIN IdEquipamento por Equipamento)
        top1_by_partition(df_hist_cr, ["Equipamento"], [("IdEquipamento", "asc")])
    """
    order_exprs = [
        F.desc(c) if d == "desc" else F.asc(c)
        for c, d in order_cols
    ]
    w = Window.partitionBy(*partition_cols).orderBy(*order_exprs)
    result = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    if select_cols:
        result = result.select(*select_cols)
    return result


# ---------------------------------------------------------------------------
# Tradução de IIF() aninhado de StatusFaturamento
# ---------------------------------------------------------------------------

def mapa_status_faturamento(status_col: str) -> F.Column:
    """
    Traduz o IIF() aninhado de StatusFaturamento para CASE WHEN.
    Extraído de PROC_FT_DESMOBILIZACAO_PA — UPDATE STATUS FATURAMENTO.

    Substitui:
        IIF(Status IN ('Aguardando Contato','Em Negociação'), 'Em Negociação',
            IIF(Status IN ('Ag. Liberação de Crédito','Aguardando Liberação de Crédito'), 'Em Faturamento',
                IIF(Status IN ('Vendido','Vendido Fora da Plataforma','Liberado para Retirada'), 'Veículo Faturado',
                    IIF(Status = 'Documentação Pendente', 'Documentação Pendente', NULL))))
    """
    return (
        F.when(F.col(status_col).isin("Aguardando Contato", "Em Negociação"), "Em Negociação")
        .when(F.col(status_col).isin("Ag. Liberação de Crédito", "Aguardando Liberação de Crédito"), "Em Faturamento")
        .when(F.col(status_col).isin("Vendido", "Vendido Fora da Plataforma", "Liberado para Retirada"), "Veículo Faturado")
        .when(F.col(status_col) == "Documentação Pendente", "Documentação Pendente")
        .otherwise(F.lit(None).cast("string"))
    )


# ---------------------------------------------------------------------------
# Tradução de StatusDesmobilizacao
# ---------------------------------------------------------------------------

def mapa_status_desmobilizacao(mes_ano_desmob_col: str, mes_ano_radar_col: str) -> F.Column:
    """
    Traduz o CASE WHEN de StatusDesmobilizacao.
    Extraído de PROC_FT_DESMOBILIZACAO_PA — UPDATE COLUNA StatusDesmobilizacao.

    0 = Fora do Radar
    1 = Atrasada
    2 = Em Dia
    3 = Antecipado
    """
    return (
        F.when(F.col(mes_ano_radar_col).isNull(), F.lit(0))
        .when(F.col(mes_ano_desmob_col) > F.col(mes_ano_radar_col), F.lit(1))
        .when(F.col(mes_ano_desmob_col) == F.col(mes_ano_radar_col), F.lit(2))
        .when(F.col(mes_ano_desmob_col) < F.col(mes_ano_radar_col), F.lit(3))
        .otherwise(F.lit(0))
    )


# ---------------------------------------------------------------------------
# VendidoAnoVigente — IIF aninhado com GETDATE()
# ---------------------------------------------------------------------------

def mapa_vendido_ano_vigente(
    status_faturamento_col: str,
    data_venda_col: str,
    execution_date: str,
) -> F.Column:
    """
    Substitui:
        IIF(YEAR(DataVenda) < YEAR(GETDATE()) AND Status = 'Veículo Faturado', 'NaoVendidoAnoVigente',
            IIF(YEAR(DataVenda) = YEAR(GETDATE()) AND Status = 'Veículo Faturado', 'VendidoAnoVigente',
                'NaoVendido'))

    execution_date substitui GETDATE() para determinismo.
    """
    import datetime
    ano_exec = datetime.date.fromisoformat(execution_date).year

    return (
        F.when(
            (F.year(F.col(data_venda_col)) < ano_exec)
            & (F.col(status_faturamento_col) == "Veículo Faturado"),
            "NaoVendidoAnoVigente",
        )
        .when(
            (F.year(F.col(data_venda_col)) == ano_exec)
            & (F.col(status_faturamento_col) == "Veículo Faturado"),
            "VendidoAnoVigente",
        )
        .otherwise("NaoVendido")
    )


# ---------------------------------------------------------------------------
# Escrita Gold particionada (usado em todos os notebooks Gold deste job)
# ---------------------------------------------------------------------------

def write_gold_append_after_delete(
    spark,
    df: DataFrame,
    catalog: str,
    table: str,
    delete_condition: str,
) -> int:
    """
    Padrão DELETE + INSERT preservando histórico.
    Substitui o padrão: DELETE WHERE filtro; INSERT FROM @TabTemp.

    Args:
        df: DataFrame com os dados novos.
        catalog: nome do catalog Unity.
        table: nome da tabela no formato schema.table (ex: gold.ft_orcamento_desmobilizacao_geral).
        delete_condition: expressão SQL para o WHERE do DELETE (ex: "year(DataDesmobilizacao) > 2024").

    Retorna: número de linhas inseridas.
    """
    count = df.count()
    if count == 0:
        return 0

    spark.sql(f"DELETE FROM {catalog}.{table} WHERE {delete_condition}")

    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}.{table}")
    )
    return count


# ---------------------------------------------------------------------------
# Interface SCD2 — contrato de uso da lib fornecida pelo usuário
# ---------------------------------------------------------------------------

class SCD2Interface:
    """
    Interface contratual para a lib SCD2 que será fornecida pelo usuário.
    Todos os notebooks Silver que precisam de SCD2 instanciam esta classe.

    A lib real deve implementar os métodos abaixo com a mesma assinatura.
    Use esta classe como duck-type contract — não importe a lib diretamente
    nos notebooks de negócio.

    Tabelas Silver que usarão SCD2 neste job:
        - stg_sima_versao_orcamento_itens (chave: Id)
        - stg_sima_versao_orcamento       (chave: Id)
        - stg_sima_orcamento              (chave: Id)
        - stg_sima_centro_resultado       (chave: Id)
        - stg_historico_cr_equipamentos_gab (chave: IdEquipamento)

    Observação: a view materializada vw_orcamento_desmobilizacao_venda
    é full-overwrite — não usa SCD2 diretamente.
    """

    def __init__(self, spark, catalog: str, schema: str = "silver"):
        self._spark = spark
        self._catalog = catalog
        self._schema = schema

    def apply(
        self,
        df_source: DataFrame,
        target_table: str,
        business_key_cols: list[str],
        tracked_cols: list[str],
        effective_date_col: str = "_etl_load_timestamp",
    ) -> dict[str, int]:
        """
        Aplica lógica SCD Type 2 na tabela Silver alvo.

        Args:
            df_source: DataFrame com os dados Bronze limpos.
            target_table: nome da tabela destino sem catalog/schema (ex: "stg_sima_versao_orcamento").
            business_key_cols: colunas que identificam a chave de negócio (ex: ["Id"]).
            tracked_cols: colunas cujas mudancas geram nova versao SCD2.
            effective_date_col: coluna de data para ordenar versoes.

        Retorna:
            {"inserted": int, "expired": int, "unchanged": int}

        Implementacao esperada da lib:
            1. Calcula hash das tracked_cols no df_source
            2. Compara com registro current=True na target
            3. Expira registros alterados (is_current=False, effective_end_date=now)
            4. Insere novas versoes
            5. Insere novos registros (nao existiam na target)
        """
        raise NotImplementedError(
            "Substitua SCD2Interface pela lib real fornecida pelo usuario. "
            f"Chamada: table={target_table}, keys={business_key_cols}"
        )

    def full_overwrite(
        self,
        df_source: DataFrame,
        target_table: str,
    ) -> int:
        """
        Escrita full overwrite para tabelas Silver que nao requerem historico SCD2.
        Equivale a: TRUNCATE + INSERT no padrao SSIS.

        Retorna: número de linhas escritas.
        """
        full_name = f"{self._catalog}.{self._schema}.{target_table}"
        count = df_source.count()
        (df_source.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_name)
        )
        return count
```

---

## 5. Score de Migração: 4.5/10

### Justificativa Técnica Detalhada

**O que foi analisado:** três artefatos com ~1.400 linhas de T-SQL e densidade de anti-padrões muito acima da média do projeto.

#### Pontos que reduzem o score

**Anti-padrão 1: TOP 1 global sem PARTITION BY na cte_versao_orcamento (–2.0 pontos)**  
Esta é a decisão de design mais perigosa do código. A `cte_versao_orcamento` retorna **uma única linha para toda a tabela** `STG_SIMA_VERSAO_ORCAMENTO`, e essa linha é então usada como INNER JOIN de todas as 70+ colunas dos três branches do UNION ALL. Em Spark, `Window.orderBy()` sem `partitionBy()` força a coletar todos os dados em **um único executor** — em produção, isso é um full shuffle + OOM garantido se a tabela crescer. A solução correta é separar o join em `F.broadcast()` após materializar a linha única, conforme mostrado no template. O risco de regressão silenciosa é alto: se a lógica de negócio mudar e mais de uma linha passar pelo filtro `Situacao < 2`, o comportamento será diferente entre T-SQL (deterministicamente o TOP 1) e Spark (indeterministico sem seed de ordering).

**Anti-padrão 2: GETDATE() em cálculos de DataDesmobilizacao (–1.5 pontos)**  
Encontrado em dois lugares críticos: `cte_zfi` na PROC_FT_ORCAMENTO e no branch `FT_ESTOQUE_CRTRANSICAO_PA` da PROC_FT_DESMOBILIZACAO_PA. O cálculo `DATEADD(DAY, -[Qtd_dias], GETDATE())` significa que **reprocessar o mesmo dia com dados históricos produzirá resultados diferentes**. Em Databricks Workflows com schedule diário isso passa despercebido, mas no primeiro backfill ou reprocessamento de incidente, os números de DataDesmobilizacao estarão errados para todos os registros de transição. O parâmetro `execution_date` resolve isso, mas exige que todos os jobs do Workflow propaguem o parâmetro de forma consistente.

**Anti-padrão 3: Três temp tables encadeadas com dependência cruzada (#UltimoCr, #Registro, #TemNaoTem) (–1.0 ponto)**  
O padrão de temp tables em cascata é um sinal claro de lógica iterativa sendo expressa em SQL declarativo de forma forçada. A tradução para PySpark é tecnicamente correta (DataFrames cacheados + TempViews), mas a lógica é difícil de testar unitariamente porque o estado de `#TemNaoTem` depende de `#RegistroEhUltimoCr` que depende de `#UltimoCr`. O risco operacional é que qualquer falha no meio do notebook deixa o estado da tabela Gold inconsistente (half-written após o DELETE). Recomenda-se materializar df_tabtemp como tabela temporária Delta (`saveAsTable` em `_temp_schema`) antes do DELETE para garantir idempotência.

**Anti-padrão 4: Subqueries correlacionadas em pelo menos 8 locais (–0.5 ponto)**  
Todas traduzíveis para `ROW_NUMBER()` conforme mapeado, mas a densidade delas indica que o código nunca foi otimizado para execução em conjunto. Em Spark, cada subquery correlacionada que não é reescrita como join/window resulta em nested loop equivalente, que em DataFrames com cartesian join pode gerar planos de execução extremamente ineficientes.

**Anti-padrão 5: PROC_FT_DESMOBILIZACAO_PA tem múltiplos UPDATEs sequenciais pós-INSERT (–0.5 ponto)**  
Oito UPDATEs separados em `StatusDesmobilizacao`, `ClassificacaoRadar`, `PlacaNoOrcamentoAtual`, `PlacaReprogramadaOrcamentoAtual`, `ModeloTcoId` (duas passagens), `ValorAquisicaoPM` e `StatusFaturamentoVenda`. Cada um deles é um MERGE separado em Delta, com scan da tabela Gold em disco a cada chamada. O custo pode ser alto se a tabela for grande. A forma correta de migrar é materializar todos os lookups antes do INSERT principal e computar todas as colunas de uma vez no DataFrame antes de escrever — eliminando 8 MERGE por 1 INSERT.

#### Pontos que sustentam o score

**O que já está bem no legado (+pontos de viabilidade):**  
O código já demonstra consciência do problema do TOP 1 — o comentário `SOS#6263815` nas procedures registra explicitamente a substituição por ROW_NUMBER. Isso indica que a equipe tem disposição para refatoração. O filtro `YEAR > 2024` para preservar histórico é um design consciente e correto para o modelo de carga incremental. O schema das tabelas é razoavelmente limpo (sem cursores, sem dynamic SQL).

**Conclusão do score:**  
O código é **migrável**, mas não é **diretamente portável**. Requer reescrita ativa das subqueries correlacionadas, eliminação do TOP 1 global e parametrização de todos os GETDATE(). A complexidade real está na PROC_FT_DESMOBILIZACAO_PA, que com ~1.050 linhas, 8 UPDATEs sequenciais, dependência em 5+ tabelas Gold e joins em 15+ tabelas Silver, é o artefato de maior risco técnico do módulo Desmobilização. Recomenda-se implementar a PROC_FT_DESMOBILIZACAO_PA como o último notebook do job (após todos os outros Gold estarem estáveis) e cobri-la com testes de paridade antes do cutover.
