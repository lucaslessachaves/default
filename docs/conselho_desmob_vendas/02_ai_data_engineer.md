# Conselho de Migração — AI Data Engineer
## Pipeline: Desmobilização e Vendas (SSIS → Databricks)

**Data:** 2026-04-29
**Agente:** AI Data Engineer — Medallion Architecture & Databricks Specialist
**Score:** 6.0/10

---

## 1. Mapeamento de Camadas

### Convenção de nomenclatura
`bi_ativos.<camada>.<nome_tabela_em_snake_case>`

### Bronze (já existe — espelho fiel do SQL Server)

| Origem SQL Server (ODS) | Destino Unity Catalog | Observação |
|---|---|---|
| `ODS.SIMA_VERSAO_ORCAMENTO_ITENS` | `bi_ativos.bronze.sima_versao_orcamento_itens` | Chave `Id` como STRING (era UNIQUEIDENTIFIER) |
| `ODS.SIMA_VERSAO_ORCAMENTO` | `bi_ativos.bronze.sima_versao_orcamento` | `Situacao` INT: 0=Gerada, 1=Aprovada, 2=Cancelada |
| `ODS.SIMA_ORCAMENTO` | `bi_ativos.bronze.sima_orcamento` | |
| `ODS.SIMA_CENTRO_RESULTADO` | `bi_ativos.bronze.sima_centro_resultado` | Flags `CrTransicao`, `CrVenda` como BOOLEAN |
| `ODS.SIMA_DIRETORIA` | `bi_ativos.bronze.sima_diretoria` | |
| `ODS.SIMA_MODELO` | `bi_ativos.bronze.sima_modelo` | `Tipo` INT: 0=Leve, 1=Pesado |
| `ODS.FIPE_ATUAL` | `bi_ativos.bronze.fipe_atual` | `Price` → DECIMAL(18,2) |
| `ODS.SAP_ZFIAA001` | `bi_ativos.bronze.sap_zfiaa001` | `Ano fabricação` STRING (pode ser vazio — tratar na Silver) |
| `ODS.SIMA_ALTERACAO_CENTRO_RESULTADO_VERSAO_ORCAMENTO_ITEM` | `bi_ativos.bronze.sima_alteracao_cr_versao_orcamento_item` | |
| `ODS.SIMA_PARAMETRIZACAO_PRAZO_DE_DESMOBILIZACAO` | `bi_ativos.bronze.sima_parametrizacao_prazo_desmobilizacao` | |
| `ODS.HISTORICO_CR_EQUIPAMENTOS_GAB` | `bi_ativos.bronze.historico_cr_equipamentos_gab` | Usado no UPDATE de `DataDesmobRealAssertividade` |
| `ODS.ESTOQUE_VENDAS_ATIVOS_PA` | `bi_ativos.bronze.estoque_vendas_ativos_pa` | Fonte da cte_plataforma_ativos |
| `ODS.HISTORICO_DE_VENDAS_PA_EXCEL` | `bi_ativos.bronze.historico_de_vendas_pa_excel` | Excel ingerido via Bronze existente |
| `ODS.EXCEL_HISTORICO_DESMOBILIZACAO_EBEC` | `bi_ativos.bronze.excel_historico_desmobilizacao_ebec` | Excel ingerido |
| `ODS.NEGOCIACAO_PA` | `bi_ativos.bronze.negociacao_pa` | Usada em FT_DESMOBILIZACAO_PA |
| `ODS.LANCE_PA` | `bi_ativos.bronze.lance_pa` | |
| `ODS.OFERTA_PA` | `bi_ativos.bronze.oferta_pa` | |
| `ODS.RADAR_DESMOBILIZACAO_SIMA` | `bi_ativos.bronze.radar_desmobilizacao_sima` | |
| `ODS.VEICULOS_PA` | `bi_ativos.bronze.veiculos_pa` | |
| `ODS.VEICULO_VISTORIA_PA` | `bi_ativos.bronze.veiculo_vistoria_pa` | |
| `ODS.HISTORICO_PLACA_PLATAFORMA_VENDA` | `bi_ativos.bronze.historico_placa_plataforma_venda` | |
| `ODS.AVALIA_GAB_VEICULO` | `bi_ativos.bronze.avalia_gab_veiculo` | |
| `ODS.AVALIA_GAB_PRECIFICACAO` | `bi_ativos.bronze.avalia_gab_precificacao` | |

### Silver (a criar — Materialized Tables via SCD2 lib)

| Origem STG (SQL Server View/Table) | Destino Unity Catalog | Tipo de carga |
|---|---|---|
| `STG.STG_SIMA_VERSAO_ORCAMENTO_ITENS` | `bi_ativos.silver.sima_versao_orcamento_itens` | SCD2 lib (chave: `Id`) |
| `STG.STG_SIMA_VERSAO_ORCAMENTO` | `bi_ativos.silver.sima_versao_orcamento` | SCD2 lib (chave: `Id`) |
| `STG.STG_SIMA_ORCAMENTO` | `bi_ativos.silver.sima_orcamento` | SCD2 lib (chave: `Id`) |
| `STG.STG_SIMA_CENTRO_RESULTADO` | `bi_ativos.silver.sima_centro_resultado` | SCD2 lib (chave: `Id`) |
| `STG.STG_SIMA_DIRETORIA` | `bi_ativos.silver.sima_diretoria` | SCD2 lib (chave: `Id`) |
| `STG.STG_SIMA_MODELO` | `bi_ativos.silver.sima_modelo` | SCD2 lib (chave: `Id`) |
| `STG.STG_FIPE_ATUAL` | `bi_ativos.silver.fipe_atual` | Overwrite (snapshot diário) |
| `STG.STG_SAP_ZFIAA001` | `bi_ativos.silver.sap_zfiaa001` | Overwrite (snapshot diário) |
| `STG.STG_SIMA_ALTERACAO_CR_VERSAO_ORCAMENTO_ITEM` | `bi_ativos.silver.sima_alteracao_cr_versao_orcamento_item` | SCD2 lib (chave: `VersaoId + CentroResultadoId`) |
| `STG.STG_SIMA_PARAMETRIZACAO_PRAZO_DE_DESMOBILIZACAO` | `bi_ativos.silver.sima_parametrizacao_prazo_desmobilizacao` | Overwrite (tabela pequena) |
| `STG.STG_HISTORICO_CR_EQUIPAMENTOS_GAB` | `bi_ativos.silver.historico_cr_equipamentos_gab` | Overwrite (snapshot) |
| `STG.STG_ESTOQUE_VENDAS_ATIVOS_PA` | `bi_ativos.silver.estoque_vendas_ativos_pa` | Overwrite |
| `STG.STG_HISTORICO_DE_VENDAS_PA_EXCEL` | `bi_ativos.silver.historico_de_vendas_pa_excel` | Overwrite |
| `STG.STG_EXCEL_HISTORICO_DESMOBILIZACAO_EBEC` | `bi_ativos.silver.excel_historico_desmobilizacao_ebec` | Overwrite |
| **`STG.VWOrcamentoDesmobilizacaoVenda`** | `bi_ativos.silver.vw_orcamento_desmobilizacao_venda` | **Materialized Table (notebook separado)** |

> A view `VWOrcamentoDesmobilizacaoVenda` é materializada como tabela Delta pura — não é tratada como SCD2 porque ela já encapsula a lógica de "última versão do orçamento". O notebook a recria por overwrite completo a cada ciclo, após todas as Silver dependentes estarem atualizadas.

### Gold (a criar)

| Procedure SQL Server | Destino Unity Catalog | Estratégia |
|---|---|---|
| `DW.PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | `bi_ativos.gold.ft_orcamento_desmobilizacao_geral` | DELETE parcial por ano + INSERT + UPDATE (ver seção 4) |
| `DW.PROC_FT_DESMOBILIZACAO_PA` | `bi_ativos.gold.ft_desmobilizacao_pa` | DELETE/INSERT por EhHistorico + UPDATE em colunas |
| `DW.PROC_FT_VENDA_PA` (e demais FT_*) | `bi_ativos.gold.ft_venda_pa` etc. | Padrão overwrite ou DELETE+INSERT por critério |
| `DW.FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` | `bi_ativos.gold.ft_ultimos_registros_por_placa_crs_desmob` | **Deve rodar PRIMEIRO** (dependencia Gold→Gold) |

---

## 2. Design da Silver Layer

### 2.1 Tabelas simples — interface esperada da SCD2 lib

O usuário fornecerá a lib. O design abaixo projeta a interface que o notebook deve usar:

```python
# notebooks/silver/sima_versao_orcamento_itens.py
# Origem: bi_ativos.bronze.sima_versao_orcamento_itens
# Destino: bi_ativos.silver.sima_versao_orcamento_itens

from scd2_lib import SCD2Manager  # lib fornecida pelo usuário

scd2 = SCD2Manager(
    spark=spark,
    source_table="bi_ativos.bronze.sima_versao_orcamento_itens",
    target_table="bi_ativos.silver.sima_versao_orcamento_itens",
    business_key=["Id"],                          # chave natural da entidade
    tracking_columns=[                             # colunas que disparam nova versão
        "VersaoId", "CentroResultadoId", "Ciclo1Acao", "Ciclo2Acao",
        "Ciclo1Quando", "Ciclo2Quando", "DataDesativacao",
        "Ciclo1ValorVenda", "Ciclo2ValorVenda", "Ciclo1ResultadoDaVenda",
        "Ciclo2ResultadoDaVenda", "Ciclo1DataDesmobilizacao", "Ciclo2DataDesmobilizacao",
    ],
    effective_from_col="DataRegistro",             # coluna que drive a timeline
    effective_to_col="DataDesativacao",
    active_flag_col="is_current",                  # adicionado pela lib
    watermark_col="DataAtualizacao",               # para leitura incremental da bronze
)

scd2.run()
```

**Contrato mínimo esperado da SCD2 lib:**
- Recebe `source_table`, `target_table`, `business_key`, `tracking_columns`
- Calcula hash SHA-256 das `tracking_columns` para detectar mudanças (nunca compara coluna a coluna)
- Expira linhas antigas (`is_current = False`, seta `_scd2_end_date`)
- Insere nova versão com `is_current = True`, `_scd2_start_date = current_timestamp()`
- Expoe método `.run()` e opcionalmente `.validate()` para contagem de linhas

### 2.2 Materialized Table: VWOrcamentoDesmobilizacaoVenda

Esta view é o caso mais crítico. Contém 3 UNION ALL, 4 CTEs e 10+ joins. Ela **não usa SCD2** — é recriada por overwrite completo a cada ciclo.

**Notebook:** `notebooks/silver/vw_orcamento_desmobilizacao_venda.py`

#### Anti-padrões identificados e suas substituições

| Anti-padrão T-SQL | Localização na view | Substituição PySpark |
|---|---|---|
| `TOP 1 ORDER BY Situacao DESC, DataRegistro DESC` | `cte_versao_orcamento` | `ROW_NUMBER() OVER (PARTITION BY OrcamentoId ORDER BY Situacao DESC, DataRegistro DESC) = 1` |
| `ISNULL(x, y)` | múltiplos lugares | `F.coalesce(F.col("x"), F.lit(y))` |
| `IIF(cond, a, b)` | ~12 ocorrências | `F.when(cond, a).otherwise(b)` |
| `WITH (NOLOCK)` | todas as tabelas | remover (Delta usa snapshot isolation) |
| `CAST(... AS uniqueidentifier)` | Id, ModeloId, CidadeDesmobilizacaoId | `.cast("string")` — esses campos já são STRING no bronze |
| `YEAR(GETDATE())` em `ReferenciaAnoVigente` | linha 151 | `F.year(F.lit(execution_date))` (parametrizado) |
| Correlated subquery em `cte_alteracao_cr_ciclo1_ultimo` | linhas 54-59 | Window function: `MAX() OVER (PARTITION BY VersaoId)` |
| `DATEDIFF(DAY, ...)` | `DiasEstoqueProjetado` | `F.datediff(F.col("Ciclo1VendaQuando"), F.col("Ciclo1Quando"))` |

#### Estrutura do notebook (pseudocódigo PySpark)

```python
# Cell 1: Parâmetros
dbutils.widgets.text("execution_date", "")
execution_year = int(execution_date[:4])  # para ReferenciaAnoVigente

# Cell 2: Leitura das Silver dependentes
versao_itens = spark.table("bi_ativos.silver.sima_versao_orcamento_itens")
versao_orc   = spark.table("bi_ativos.silver.sima_versao_orcamento")
orcamento    = spark.table("bi_ativos.silver.sima_orcamento")
centro_res   = spark.table("bi_ativos.silver.sima_centro_resultado")
diretoria    = spark.table("bi_ativos.silver.sima_diretoria")
modelo       = spark.table("bi_ativos.silver.sima_modelo")
fipe         = spark.table("bi_ativos.silver.fipe_atual")
sap_zfi      = spark.table("bi_ativos.silver.sap_zfiaa001")
alteracao_cr = spark.table("bi_ativos.silver.sima_alteracao_cr_versao_orcamento_item")
prazo_desmob = spark.table("bi_ativos.silver.sima_parametrizacao_prazo_desmobilizacao")

# Cell 3: cte_versao_orcamento — TOP 1 substituído por ROW_NUMBER
from pyspark.sql import Window
import pyspark.sql.functions as F

w_versao = Window.partitionBy("OrcamentoId").orderBy(
    F.col("Situacao").desc(), F.col("DataRegistro").desc()
)
cte_versao_orcamento = (
    versao_orc
    .filter(F.col("Situacao") < 2)
    .withColumn("_rn", F.row_number().over(w_versao))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
# IMPORTANTE: o TOP 1 original na view NÃO tem PARTITION BY — ele retorna
# exatamente 1 linha para TODA a tabela. Isso é um bug de design no legado:
# o INNER JOIN posterior em cte_versao_orcamento filtra todas as linhas de
# STG_SIMA_VERSAO_ORCAMENTO_ITENS para apenas as que pertencem a ESSA versão
# única. A regra de negócio (SOS#6263815) exige que o BI mostre sempre a
# última versão; se aprovada, a aprovada mais atual.
# Portanto, o PARTITION BY correto é OrcamentoId — cada orçamento tem sua
# melhor versão. Validar com o negócio antes de ir para produção.

# Cell 4: CTEs de alteração de CR (ciclos 1 e 2)
# Correlated subquery → MAX() OVER
w_cr = Window.partitionBy("VersaoId")
cte_acr_ciclo1_geral = (
    alteracao_cr
    .filter(F.col("DataDesativacao").isNull())
    .join(
        versao_itens.filter(F.col("DataDesativacao").isNull())
                    .select("Id", "Ciclo1Quando"),
        alteracao_cr["VersaoId"] == versao_itens["Id"], "left"
    )
    .filter(F.col("QuandoAlteracaoCentroResultado") <= F.col("Ciclo1Quando"))
    .withColumn("_max_quando", F.max("QuandoAlteracaoCentroResultado").over(w_cr))
    .filter(F.col("QuandoAlteracaoCentroResultado") == F.col("_max_quando"))
    .drop("_max_quando")
    .dropDuplicates(["VersaoId", "CentroResultadoId", "QuandoAlteracaoCentroResultado"])
)
# ... (idem para ciclo 2)

# Cell 5: cte_prazo_desmobilizacao
cte_prazo = (
    prazo_desmob
    .filter(F.col("DataDesativacao").isNull() & F.col("DiretoriaId").isNull())
    .select("TipoPeso", "PrazoDesmobilizacaoEmDias")
    .dropDuplicates()
)

# Cell 6: Subquery de FIPE e SAP normalizadas
fipe_norm = fipe.select(
    F.concat(F.col("CodigoFIPE"), F.lit("-"), F.col("AnoModelo")).alias("_fipe_key"),
    F.col("Price").cast("decimal(18,2)").alias("ValorFipeAtual")
)
sap_norm = sap_zfi.select(
    F.col("Equipamento"),
    F.when(
        F.col("Ano fabricação").isNull() | (F.col("Ano fabricação") == ""),
        F.lit(None).cast("integer")
    ).otherwise(F.col("Ano fabricação").cast("integer")).alias("AnoFabricacao")
)

# Cell 7: UNION ALL dos 3 blocos (Ciclo1-Desmob, Ciclo1-VendaTransicao, Ciclo2)
# Cada bloco aplica os mesmos joins e as mesmas transformações de coluna.
# Extrair função auxiliar para evitar duplicação:

def build_ciclo_block(
    vi, vo, o, cr, cro, acr, dcr, m, fipe_norm, sap_norm, prazo,
    ciclo_num: int, execution_year: int, where_filter
):
    ciclo_prefix = f"Ciclo{ciclo_num}"
    return (
        vi
        .join(vo, vi["VersaoId"] == vo["Id"], "inner")
        .join(o, vo["OrcamentoId"] == o["Id"], "left")
        .join(cr, vi["CentroResultadoId"] == cr["Id"], "left")
        .join(cro, vi["UltimoCentroResultadoOperacionalId"] == cro["Id"], "left")
        .join(acr, acr["VersaoId"] == vi["Id"], "left")
        .join(dcr, dcr["Id"] == cr["DiretoriaId"], "left")
        .join(m, m["Id"] == vi["ModeloId"], "left")
        .join(fipe_norm,
              F.concat(m["CodigoFipe"], F.lit("-"), vi["AnoModelo"]) == fipe_norm["_fipe_key"],
              "left")
        .join(sap_norm, sap_norm["Equipamento"] == vi["Placa"], "left")
        .join(prazo, prazo["TipoPeso"] == m["Tipo"], "left")
        .filter(where_filter)
        .select(
            vi["Id"].cast("string").alias("Id"),
            vi["Id"].cast("string").alias("OrcamentoImobilizadoId"),
            vi["Imobilizado"].cast("string").alias("Imobilizado"),
            F.concat(F.substring(cr["CR"], 1, 4), vi["Imobilizado"]).alias("Chave"),
            vi["Placa"].cast("string").alias("Placa"),
            F.lit(ciclo_num).cast("integer").alias("Ciclo"),
            vi[f"{ciclo_prefix}Acao"].cast("integer").alias("CicloAcao"),
            F.when(o["AnoReferencia"] <= 2023, cr["CR"].cast("string"))
             .otherwise(F.coalesce(acr["CR"], cro["CR"], cr["CR"]).cast("string"))
             .alias("CodCr"),
            F.upper(dcr["Descricao"]).cast("varchar(255)").alias("Diretoria"),
            vi["ModeloId"].cast("string").alias("ModeloId"),
            F.when(m["Tipo"] == 0, "LEVE")
             .when(m["Tipo"] == 1, "PESADO")
             .otherwise(F.lit(None)).alias("Peso"),
            m["Tipo"].cast("integer").alias("IdPeso"),
            F.when(vi["AnoModelo"] == 0, F.lit(None).cast("integer"))
             .otherwise(vi["AnoModelo"].cast("integer")).alias("AnoModelo"),
            sap_norm["AnoFabricacao"],
            vi["DataAquisicao"].cast("date").alias("DataAquisicao"),
            vi[f"{ciclo_prefix}DataDesmobilizacao"].cast("date").alias("DataDesmobilizacao"),
            vi[f"{ciclo_prefix}VendaQuando"].cast("date").alias("DataVenda"),
            vi[f"{ciclo_prefix}CidadeDesmobilizacaoId"].cast("string").alias("CidadeDesmobilizacaoId"),
            F.concat(o["AnoReferencia"].cast("string"), F.lit("/"),
                     (o["AnoReferencia"] + 1).cast("string")).alias("Referencia"),
            F.when((o["AnoReferencia"] + 1) == execution_year, 1).otherwise(0)
             .cast("integer").alias("ReferenciaAnoVigente"),
            # ... demais colunas financeiras com F.when/F.coalesce
            F.coalesce(vi[f"{ciclo_prefix}CustoPreparacao"], F.lit(0)).cast("decimal(18,2)")
             .alias("CustoPreparacao"),
            fipe_norm["ValorFipeAtual"],
            prazo["PrazoDesmobilizacaoEmDias"].cast("integer"),
        )
    )

df_ciclo1_desmob = build_ciclo_block(
    vi=versao_itens.filter(
        F.col("Ciclo1Acao").isin(2,3,4,5,6) &
        F.col("DataDesativacao").isNull()
    ),
    # ... passar demais args
    ciclo_num=1,
    execution_year=execution_year,
    where_filter=(
        F.col("vo_DataDesativacao").isNull() &
        F.col("o_DataDesativacao").isNull() &
        (F.col("cr_CrTransicao") == False) &
        (F.col("cr_CrVenda") == False)
    )
)

df_ciclo1_venda = build_ciclo_block(
    # filtro: CrVenda=1 OR CrTransicao=1 (bloco 2 do UNION ALL)
    ...
)

df_ciclo2 = build_ciclo_block(
    # Ciclo2Acao IN (2,3,4,5,6) (bloco 3 do UNION ALL)
    ciclo_num=2,
    ...
)

df_final = df_ciclo1_desmob.unionByName(df_ciclo1_venda).unionByName(df_ciclo2)

# Cell 8: Persistência — overwrite completo (a view não tem estado)
(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bi_ativos.silver.vw_orcamento_desmobilizacao_venda")
)
```

#### Risco critico documentado: TOP 1 sem PARTITION BY

O `cte_versao_orcamento` original usa `TOP 1 ORDER BY Situacao DESC, DataRegistro DESC` **sem WHERE OrcamentoId** — isso retorna uma única linha de toda a `STG_SIMA_VERSAO_ORCAMENTO`. Em Spark, `LIMIT 1 ORDER BY` é não-determinístico em cluster distribuído.

A correção proposta (`PARTITION BY OrcamentoId`) altera o comportamento: em vez de uma versão global, cada orçamento recebe sua melhor versão. **Esta mudança deve ser validada com o negócio** antes de go-live — pode ser que o design original seja um bug que existe há anos.

---

## 3. Design da Gold Layer

### 3.1 FT_ORCAMENTO_DESMOBILIZACAO_GERAL

**Notebook:** `notebooks/gold/ft_orcamento_desmobilizacao_geral.py`

O procedimento original tem dois estágios distintos que devem ser notebooks separados ou seções claramente demarcadas:

**Estágio A — Carga principal (DELETE parcial + INSERT)**

```python
# Cell 1: Parâmetros
dbutils.widgets.text("execution_date", "")  # substitui GETDATE()
execution_date = dbutils.widgets.get("execution_date")
ano_corte = 2024  # parametrizável no futuro

# Cell 2: Ler silver materializada
df_source = (
    spark.table("bi_ativos.silver.vw_orcamento_desmobilizacao_venda")
    .filter(
        (F.year(F.col("DataDesmobilizacao")) > ano_corte) &
        (F.col("EhDesmobilizacao") == 1)
    )
    .select(
        F.col("Id").alias("Id"),
        F.col("Imobilizado"),
        F.col("Chave"),
        F.col("Placa").cast("string").alias("Placa"),    # nvarchar(11) → STRING
        F.col("AnoModelo"),
        F.col("IdPeso"),
        F.col("ValorVenda"),
        F.col("ResidualTotal"),
        F.col("AnoReferencia"),
        F.col("Ciclo"),
        F.col("CicloAcao"),
        F.col("DataDesmobilizacao"),
        F.col("CodCr").cast("string").alias("CodCr"),   # nvarchar(40) → STRING
        F.col("ModeloId"),
        F.col("CidadeDesmobilizacaoId"),
        F.col("ValorAquisicao"),
        F.col("Diretoria"),
        F.col("VersaoOrcamento"),
        F.col("SituacaoVersaoOrcamento"),
        F.col("PrazoDesmobilizacaoEmDias"),
    )
)

row_count = df_source.count()

# Cell 3: Equivalente ao IF (SELECT COUNT(*) FROM @TabTemp) > 0
if row_count > 0:
    # DELETE parcial: remover apenas registros com ano > corte
    spark.sql(f"""
        DELETE FROM bi_ativos.gold.ft_orcamento_desmobilizacao_geral
        WHERE YEAR(DataDesmobilizacao) > {ano_corte}
    """)

    # INSERT equivalente
    (
        df_source.write
        .format("delta")
        .mode("append")
        .saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
    )
else:
    print(f"[SKIP] Nenhum registro com YEAR(DataDesmobilizacao) > {ano_corte} na Silver. Carga abortada.")
```

**Estágio B — UPDATE de DataDesmobRealAssertividade**

Este estágio usa 3 tabelas temporárias encadeadas + 6 CTEs lendo de:
- `bi_ativos.bronze.historico_cr_equipamentos_gab` (Silver candidate)
- `bi_ativos.gold.ft_ultimos_registros_por_placa_crs_desmob` (Gold→Gold dep)
- `bi_ativos.bronze.estoque_vendas_ativos_pa`
- `bi_ativos.bronze.historico_de_vendas_pa_excel`
- `bi_ativos.bronze.excel_historico_desmobilizacao_ebec`
- `bi_ativos.bronze.sap_zfiaa001`

```python
# Cell 4: Reconstrução das 3 temp tables como TempViews

# --- #UltimoCrOperPorPeriodo ---
historico_cr = spark.table("bi_ativos.bronze.historico_cr_equipamentos_gab")
w_ult = Window.partitionBy("Equipamento")

ultimo_cr_oper = (
    historico_cr
    .filter(~F.right(F.col("CentroCusto"), 5).isin("90600","92600","90610","92610"))
    .withColumn("_max_id", F.max("IdEquipamento").over(w_ult))
    .filter(F.col("IdEquipamento") == F.col("_max_id"))
    .drop("_max_id")
    .dropDuplicates(["IdEquipamento","Equipamento","CentroCusto","ValidoDesde"])
)
ultimo_cr_oper.createOrReplaceTempView("tmp_ultimo_cr_oper")

# --- #RegistroEhUltimoCrOper ---
marcacao = (
    historico_cr
    .join(
        ultimo_cr_oper.select("IdEquipamento").alias("ult"),
        historico_cr["IdEquipamento"] == F.col("ult.IdEquipamento"),
        "left"
    )
    .withColumn("EhUltimoCrOper", F.when(F.col("ult.IdEquipamento").isNull(), 0).otherwise(1))
    .dropDuplicates(["IdEquipamento","Equipamento","CentroCusto","ValidoDesde"])
)
marcacao.createOrReplaceTempView("tmp_marcacao")

# --- #TemNaoTem (UNION ALL das cte_tem + cte_nao_tem filtradas) ---
# Replicar a lógica de filtragem por IdEquipamento > ultimo_cr_oper
# ... (construção análoga às CTEs da procedure)

# Cell 5: Demais CTEs (historico, ebec, plataforma_ativos, zfi)
# Substituir GETDATE() por execution_date
from datetime import datetime
exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")

cte_zfi = (
    spark.table("bi_ativos.bronze.sap_zfiaa001")
    .filter(F.right(F.col("Centro custo"), 5).isin("90660","92660"))
    .filter(F.col("Status Sistema").isin("LIDI","MONT"))
    .withColumn(
        "DataDesmobilizacao",
        F.date_sub(F.lit(execution_date).cast("date"),
                   F.col("Qtd dias no Centro de Custo atual").cast("integer"))
    )
    # LEFT JOIN anti-joins com historico e plataforma_ativos
    # ... (filtros WHERE ch.CodigoSapVix IS NULL AND che.CodigoSapVix IS NULL)
)

# Cell 6: UNION ALL das 4 fontes → cte_group com ROW_NUMBER
# A ROW_NUMBER já existe na procedure (SOS#6263815) — manter exatamente
w_group = Window.partitionBy("CodigoSapVix").orderBy(F.col("DataDesmobilizacao").desc())
cte_group = (
    spark.sql("SELECT * FROM cte_union")  # view temporária da union anterior
    .filter(F.col("DataDesmobilizacao").isNotNull())
    .withColumn("_rn", F.row_number().over(w_group))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
cte_group.createOrReplaceTempView("tmp_cte_group")

# Cell 7: UPDATE via MERGE (Delta não tem UPDATE...FROM nativo com join)
spark.sql("""
    MERGE INTO bi_ativos.gold.ft_orcamento_desmobilizacao_geral AS od
    USING (
        SELECT
            g.CodigoSapVix,
            g.DataDesmobilizacao AS nova_data
        FROM tmp_cte_group g
    ) AS src
    ON od.Placa = src.CodigoSapVix
       AND YEAR(od.DataDesmobilizacao) > 2024
    WHEN MATCHED THEN
        UPDATE SET od.DataDesmobRealAssertividade = src.nova_data
    WHEN NOT MATCHED BY SOURCE AND YEAR(od.DataDesmobilizacao) > 2024 THEN
        UPDATE SET od.DataDesmobRealAssertividade = NULL
""")
```

> **Nota:** O `UPDATE ... FROM` do T-SQL (com LEFT JOIN que seta NULL quando não encontra) mapeia para um `MERGE INTO` Delta com duas cláusulas: `WHEN MATCHED THEN UPDATE` e `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET col = NULL`. Isso é o padrão correto no Databricks.

### 3.2 FT_DESMOBILIZACAO_PA

**Notebook:** `notebooks/gold/ft_desmobilizacao_pa.py`

A procedure tem dois estágios independentes que podem ser seções no mesmo notebook:

```python
# Estágio 1: EhHistorico = 1 (registros históricos)
spark.sql("DELETE FROM bi_ativos.gold.ft_desmobilizacao_pa WHERE EhHistorico = 1")

df_historico = spark.sql("""
    SELECT
        ep.CD_SAP       AS Equipamento,
        ep.PLACA        AS Placa,
        -- ... demais colunas lendo de ~5 Silver tables
        1               AS EhHistorico
    FROM bi_ativos.silver.estoque_vendas_ativos_pa ep
    LEFT JOIN bi_ativos.silver.historico_de_vendas_pa_excel h ON ...
    LEFT JOIN bi_ativos.silver.excel_historico_desmobilizacao_ebec ebec ON ...
    -- ...
""")
df_historico.write.format("delta").mode("append").saveAsTable("bi_ativos.gold.ft_desmobilizacao_pa")

# Estágio 2: EhHistorico = 0 (registros atuais — 15+ STG tables)
spark.sql("DELETE FROM bi_ativos.gold.ft_desmobilizacao_pa WHERE EhHistorico = 0")

df_atual = spark.sql("""
    SELECT
        -- ... ~40 colunas com múltiplos LEFT JOINs
        0 AS EhHistorico
    FROM bi_ativos.silver.estoque_vendas_ativos_pa ep
    LEFT JOIN bi_ativos.silver.sima_versao_orcamento_itens vi ON ...
    LEFT JOIN bi_ativos.silver.negociacao_pa neg ON ...
    LEFT JOIN bi_ativos.silver.lance_pa lp ON ...
    -- ...
""")
df_atual.write.format("delta").mode("append").saveAsTable("bi_ativos.gold.ft_desmobilizacao_pa")

# Estágio 3: UPDATE das 8 colunas (StatusDesmobilizacao, ClassificacaoRadar, ...)
# Cada UPDATE vira um MERGE INTO separado
spark.sql("""
    MERGE INTO bi_ativos.gold.ft_desmobilizacao_pa AS ft
    USING (
        SELECT Placa, <lógica de StatusDesmobilizacao> AS novo_status
        FROM ...
    ) AS src ON ft.Placa = src.Placa AND ft.EhHistorico = 0
    WHEN MATCHED THEN UPDATE SET ft.StatusDesmobilizacao = src.novo_status
""")
# ... repetir para ClassificacaoRadar, PlacaNoOrcamentoAtual, etc.
```

### 3.3 Mapeamento de tipos Gold DDL

```sql
-- bi_ativos.gold.ft_orcamento_desmobilizacao_geral
CREATE TABLE IF NOT EXISTS bi_ativos.gold.ft_orcamento_desmobilizacao_geral (
    Id                          STRING,           -- era UNIQUEIDENTIFIER
    Imobilizado                 STRING,
    Chave                       STRING,
    Placa                       STRING,           -- nvarchar(11)
    AnoModelo                   INT,
    IdCr                        INT,
    IdModelo                    INT,
    IdPeso                      STRING,
    IdDataDesmobilizacao        INT,
    ValorVenda                  DECIMAL(18,2),
    ResidualTotal               DECIMAL(18,2),
    Ciap                        DECIMAL(38,2),
    AnoReferencia               INT,
    Ciclo                       INT,
    QuantidadeComSemData        INT,
    CicloAcao                   INT,
    DataDesmobilizacao          DATE,
    CodCr                       STRING,           -- nvarchar(40)
    ModeloId                    STRING,           -- era UNIQUEIDENTIFIER
    CidadeDesmobilizacaoId      STRING,           -- era UNIQUEIDENTIFIER
    ValorAquisicao              DECIMAL(18,2),
    Diretoria                   STRING,
    DataDesmobRealAssertividade DATE,
    VersaoOrcamento             STRING,
    SituacaoVersaoOrcamento     INT,
    DataCarga                   TIMESTAMP,        -- DEFAULT substitui-se por _etl_load_ts
    PrazoDesmobilizacaoEmDias   INT,
    _etl_load_ts                TIMESTAMP         -- coluna de auditoria
)
USING DELTA
LOCATION 'dbfs:/mnt/bi_ativos/gold/ft_orcamento_desmobilizacao_geral';

-- bi_ativos.gold.ft_desmobilizacao_pa
CREATE TABLE IF NOT EXISTS bi_ativos.gold.ft_desmobilizacao_pa (
    Equipamento                     STRING,
    Placa                           STRING,
    CodCr                           INT,
    IdModelo                        DECIMAL(5,0),
    IdFipe                          STRING,
    CodigoFipe                      STRING,
    AnoModelo                       INT,
    Peso                            STRING,
    ValorAquisicao                  DECIMAL(18,2),
    DataAquisicao                   DATE,
    DataTransferencia               DATE,
    DataDesmobilizacao              DATE,
    ResidualContabil                DECIMAL(38,2),
    Ciap                            DECIMAL(38,2),
    TipoDeCr                        STRING,
    EhHistorico                     INT,
    DataVenda                       DATE,
    IdVersaoModelo                  DECIMAL(10,0),
    ModeloTcoId                     STRING,       -- era UNIQUEIDENTIFIER
    Modelo                          STRING,
    Implemento                      STRING,
    StatusDesmobilizacao            INT,
    ClassificacaoRadar              INT,
    Diretoria                       STRING,
    CodigoNomeCr                    STRING,
    Status                          STRING,
    PlacaNoOrcamentoAtual           INT,
    PlacaReprogramadaOrcamentoAtual INT,
    ValorVenda                      DECIMAL(18,2),
    ResidualTotalVenda              DECIMAL(18,2),
    ValorAquisicaoPM                DECIMAL(18,2),
    CargaEBEC                       INT,
    StatusFaturamentoVenda          STRING,
    VendidoAnoVigente               STRING,
    ResultadoTotalVenda             DECIMAL(18,2),
    CodCrOrcDesmobAnterior          INT,
    CodCrOrcDesmobAtual             INT,
    ValorVendaOrcadoAtual           DECIMAL(18,2),
    ValorPrecificacaoAG             DECIMAL(18,2),
    IdStatusVeiculoVistoriaAprovAG  INT,
    SlaVitoriaAprovadaAvaliaGab     INT,
    _etl_load_ts                    TIMESTAMP
)
USING DELTA;
```

---

## 4. Padrao de Carga — DELETE Parcial por Ano em Delta Lake

### Problema
A procedure usa `DELETE FROM DW.tabela WHERE YEAR(col) > 2024` seguido de `INSERT` — padrão "delete parcial + reinsert". Isso NÃO é truncate total. Em Delta Lake, este padrão tem implicações específicas.

### Solucao recomendada: MERGE com predicado de partição

```python
# Abordagem 1: DELETE + INSERT (fiel ao legado — mais simples, mais segura para validação)
# Exige que a tabela Gold seja particionada por AnoReferencia para eficiência
spark.sql(f"""
    DELETE FROM bi_ativos.gold.ft_orcamento_desmobilizacao_geral
    WHERE YEAR(DataDesmobilizacao) > {ano_corte}
""")
df_source.write.format("delta").mode("append").saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
```

```python
# Abordagem 2: MERGE INTO com replaceWhere (preferida para tabelas particionadas grandes)
(
    df_source.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"YEAR(DataDesmobilizacao) > {ano_corte}")
    .saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
)
```

### Decisao recomendada

Usar **Abordagem 1 (DELETE + INSERT)** na fase inicial de migração para facilitar validação de paridade linha a linha com o SQL Server. Após validação, migrar para `replaceWhere` para melhor performance.

### Particionamento recomendado para a tabela Gold

```sql
-- Adicionar ao CREATE TABLE
PARTITIONED BY (AnoReferencia)
```

O particionamento por `AnoReferencia` garante que o `DELETE WHERE YEAR(DataDesmobilizacao) > 2024` toque apenas as partições relevantes (file pruning), em vez de fazer full scan da tabela.

### Consideracoes sobre idempotencia

O padrão `DELETE + INSERT` é idempotente se a Silver estiver estável:
- Se o job falhar após DELETE e antes do INSERT: rerun recria os dados corretamente
- Se a Silver mudar entre falha e rerun: os dados reinseridos refletem o estado mais atual (comportamento desejado)
- O UPDATE de `DataDesmobRealAssertividade` é idempotente por natureza (MERGE)

---

## 5. Estrutura de Pastas dos Notebooks

A empresa nao tem token de workspace — notebooks ficam em pasta local gitignored.

```
/root/default/
├── .gitignore                          # deve conter: notebooks/
├── notebooks/                          # GITIGNORED — nunca commitado
│   ├── silver/
│   │   ├── sima_versao_orcamento_itens.py
│   │   ├── sima_versao_orcamento.py
│   │   ├── sima_orcamento.py
│   │   ├── sima_centro_resultado.py
│   │   ├── sima_diretoria.py
│   │   ├── sima_modelo.py
│   │   ├── fipe_atual.py
│   │   ├── sap_zfiaa001.py
│   │   ├── sima_alteracao_cr_versao_orcamento_item.py
│   │   ├── sima_parametrizacao_prazo_desmobilizacao.py
│   │   ├── historico_cr_equipamentos_gab.py
│   │   ├── estoque_vendas_ativos_pa.py
│   │   ├── historico_de_vendas_pa_excel.py
│   │   ├── excel_historico_desmobilizacao_ebec.py
│   │   └── vw_orcamento_desmobilizacao_venda.py   # Materialized Table
│   │
│   └── gold/
│       ├── ft_ultimos_registros_por_placa_crs_desmob.py  # PRIMEIRO (dep Gold→Gold)
│       ├── ft_orcamento_desmobilizacao_geral.py
│       ├── ft_desmobilizacao_pa.py
│       ├── ft_venda_pa.py
│       ├── ft_orcamento_desmobilizacao_geral_atual.py
│       ├── ft_orcamento_desmobilizacao_historico.py
│       ├── ft_orcamento_venda_geral.py
│       ├── ft_orcamento_venda_geral_atual.py
│       ├── ft_orcamento_venda_historico.py
│       ├── ft_radar_desmobilizacao.py
│       ├── ft_radar_desmobilizacao_historico.py
│       ├── ft_radar_venda.py
│       ├── ft_historico_desmob_plataforma_venda.py
│       ├── ft_historico_de_vendas_pa.py
│       ├── ft_historico_sla_desmobilizacao_gab_pa.py
│       ├── ft_preparacao_realizado_desmobilizacao_avalia_gab.py
│       └── ft_vistoria_realizado_desmobilizacao_avalia_gab.py
│
└── docs/
    └── conselho_desmob_vendas/
        └── 02_ai_data_engineer.md      # este arquivo
```

**Entrada no .gitignore:**
```
notebooks/
```

**Convencao de upload para o workspace Databricks:**
Os notebooks `.py` sao carregados manualmente via Databricks UI (Import > File) ou via CLI sem token persistente:
```bash
databricks workspace import notebooks/silver/vw_orcamento_desmobilizacao_venda.py \
  /Users/<user>/desmob_vendas/silver/vw_orcamento_desmobilizacao_venda \
  --language PYTHON --overwrite
```

---

## 6. Workflow Job JSON Skeleton

O job respeita a dependencia critica `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` antes dos demais Gold.

```json
{
  "name": "bi_ativos_desmobilizacao_vendas",
  "parameters": [
    {
      "name": "execution_date",
      "default": "{{start_date}}"
    },
    {
      "name": "ano_corte",
      "default": "2024"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "shared_cluster",
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 6
        },
        "spark_conf": {
          "spark.sql.shuffle.partitions": "32"
        }
      }
    }
  ],
  "tasks": [
    {
      "task_key": "silver_sima_versao_orcamento_itens",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/sima_versao_orcamento_itens",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_sima_versao_orcamento",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/sima_versao_orcamento",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_sima_orcamento",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/sima_orcamento",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_sima_centro_resultado",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/sima_centro_resultado",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_lookup_tables",
      "depends_on": [],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/lookup_tables_batch",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "description": "Tabelas pequenas: sima_diretoria, sima_modelo, fipe_atual, sap_zfiaa001, sima_parametrizacao_prazo_desmobilizacao",
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_alteracao_cr",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/sima_alteracao_cr_versao_orcamento_item",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_fontes_historico",
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/fontes_historico_batch",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "description": "historico_cr_equipamentos_gab, estoque_vendas_ativos_pa, historico_de_vendas_pa_excel, excel_historico_desmobilizacao_ebec",
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "silver_vw_orcamento_desmobilizacao_venda",
      "depends_on": [
        {"task_key": "silver_sima_versao_orcamento_itens"},
        {"task_key": "silver_sima_versao_orcamento"},
        {"task_key": "silver_sima_orcamento"},
        {"task_key": "silver_sima_centro_resultado"},
        {"task_key": "silver_lookup_tables"},
        {"task_key": "silver_alteracao_cr"}
      ],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/silver/vw_orcamento_desmobilizacao_venda",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "gold_ft_ultimos_registros_crs_desmob",
      "depends_on": [
        {"task_key": "silver_fontes_historico"}
      ],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/gold/ft_ultimos_registros_por_placa_crs_desmob",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS",
      "description": "CRITICO: deve rodar antes de ft_orcamento_desmobilizacao_geral e ft_desmobilizacao_pa"
    },
    {
      "task_key": "gold_ft_orcamento_desmobilizacao_geral",
      "depends_on": [
        {"task_key": "silver_vw_orcamento_desmobilizacao_venda"},
        {"task_key": "gold_ft_ultimos_registros_crs_desmob"}
      ],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/gold/ft_orcamento_desmobilizacao_geral",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}",
          "ano_corte": "{{job.parameters.ano_corte}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "gold_ft_desmobilizacao_pa",
      "depends_on": [
        {"task_key": "silver_vw_orcamento_desmobilizacao_venda"},
        {"task_key": "gold_ft_ultimos_registros_crs_desmob"}
      ],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/gold/ft_desmobilizacao_pa",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    },
    {
      "task_key": "gold_ft_venda_pa_e_demais",
      "depends_on": [
        {"task_key": "silver_vw_orcamento_desmobilizacao_venda"}
      ],
      "job_cluster_key": "shared_cluster",
      "notebook_task": {
        "notebook_path": "/Users/<user>/desmob_vendas/gold/ft_venda_pa",
        "base_parameters": {
          "execution_date": "{{job.parameters.execution_date}}"
        }
      },
      "run_if": "ALL_SUCCESS"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 5 * * ?",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "PAUSED"
  },
  "email_notifications": {
    "on_failure": ["<engenharia@empresa.com>"],
    "on_start": [],
    "on_success": []
  },
  "max_concurrent_runs": 1
}
```

**Grafo de dependencias do job:**

```
silver_sima_versao_orcamento_itens ─┐
silver_sima_versao_orcamento        ├─► silver_vw_orcamento_desmobilizacao_venda ─┬─► gold_ft_orcamento_desmobilizacao_geral
silver_sima_orcamento               │                                              │
silver_sima_centro_resultado        │                                              └─► gold_ft_desmobilizacao_pa
silver_lookup_tables                │                                              │
silver_alteracao_cr                 ┘                                              └─► gold_ft_venda_pa_e_demais

silver_fontes_historico ─────────────────────────────────────────────────────────────► gold_ft_ultimos_registros_crs_desmob ─┤
                                                                                                                              (dep. Gold→Gold)
```

---

## 7. Score de Migracao: 6.0/10

### Justificativa

| Dimensao | Score | Motivo |
|---|---|---|
| Complexidade do legado | 2.5/5 | View com 3 UNION ALL + 4 CTEs + 10 joins; procedure com 3 temp tables encadeadas e UPDATE via LEFT JOIN |
| Risco de semantica | -1.0 | TOP 1 sem PARTITION BY e GETDATE() em logica de negocio sao riscos de paridade dados, nao apenas tecnicos |
| Mapeamento de padroes | +1.5 | Todos os anti-padroes T-SQL tem equivalente PySpark documentado e testavel |
| Dependencias Gold→Gold | -1.0 | FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB cria acoplamento que pode crescer; requer verificacao de grafo completo |
| Estrategia de carga | +1.0 | DELETE parcial + INSERT eh idiomatico em Delta Lake com replaceWhere; padrao bem definido |
| Observabilidade | -0.5 | Zero testes no legado; sem baseline de contagem de linhas para validar paridade |
| Restricoes respeitadas | +1.5 | Workflows Jobs + Notebooks only; SCD2 lib; notebooks gitignored; sem DLT; sem AutoLoader |

### Onde perder menos nota

1. **Validar o TOP 1 sem PARTITION BY com o negocio antes de codificar** — se a interpretacao estiver errada, o PowerBI exibira dados incorretos para todos os orcamentos exceto o mais recente.
2. **Construir script de paridade antes de ir ao ar** — `SELECT COUNT(*), SUM(ValorVenda), MIN(DataDesmobilizacao), MAX(DataDesmobilizacao) FROM DW.FT_ORCAMENTO_DESMOBILIZACAO_GERAL` no SQL Server vs. a tabela Gold correspondente.
3. **Mapear o grafo completo Gold→Gold** com o scanner de dependencias do Sprint Zero antes de definir a ordem final do job.
4. **Parametrizar `ano_corte`** no job (ja feito no skeleton) — quando o negocio quiser processar 2026, basta alterar o parametro sem reescrever o notebook.

### O que eleva o score para 8+/10

- Testes pytest de paridade para cada Gold table (delta de linhas < 1%)
- `ano_corte` externalizado via Databricks Secrets ou job parameter (ja parcialmente feito)
- Monitoramento de qualidade: `SELECT COUNT(*) WHERE DataDesmobRealAssertividade IS NULL AND YEAR(DataDesmobilizacao) > 2024` como metrica de saude pos-carga
- Documentacao de linhagem no Unity Catalog System Tables apos primeiro ciclo completo bem-sucedido
