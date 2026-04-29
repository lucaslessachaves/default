# Conselho Desmobilização e Vendas — v2
## Agente: AI Data Engineer — Medallion Architecture
**Data:** 2026-04-29 | **Score: 7.0 / 10** *(revisado com novos dados)*

---

## 1. O que mudou desde a v1

| Item | v1 | v2 |
|---|---|---|
| TOP 1 VWOrcamentoDesmobilizacaoVenda | Risco alto | **RESOLVIDO** — `limit(1)` + `broadcast()` |
| `ts_ingestao_brt` | Incerto | **CONFIRMADO** — padrão para todas as Bronze |
| Baseline row counts | Ausente | **17 tabelas mapeadas** com valores exatos |
| Score | 6.0/10 | **7.0/10** |

---

## 2. Mapeamento de Camadas — Completo

### Bronze → Silver (lib carga_silver.py)

| Tabela Bronze | Método da Lib | Chave (`on_colunm`) |
|---|---|---|
| `ods_sima_versao_orcamento_itens` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_versao_orcamento` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_orcamento` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_centro_resultado` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_diretoria` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_modelo` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_parametrizacao_prazo_de_desmobilizacao` | `saveDeltaAsSilver` | `['Id']` |
| `ods_sima_alteracao_cr_versao_orcamento_item` | `saveDeltaAsSilver` | `['Id']` |
| `ods_fipe_atual` | `saveDeltaAsSilver` | `['CodigoFIPE', 'AnoModelo']` |
| `ods_sap_zfiaa001` | `saveAsSilverOverwrite` | — (sem PK) |
| `ods_estoque_vendas_ativos_pa` | `saveDeltaAsSilver` | `['Id']` |
| `ods_historico_de_vendas_pa_excel` | `saveLastLoadAsSilver` | tsp=`ts_ingestao_brt` |
| `ods_excel_historico_desmobilizacao_ebec` | `saveLastLoadAsSilver` | tsp=`ts_ingestao_brt` |
| `ods_historico_cr_equipamentos_gab` | `saveDeltaAsSilver` | `['IdEquipamento']` |
| *demais ~10 tabelas STG* | `saveDeltaAsSilver` | `['Id']` |

### Silver Views → Silver Materialized Tables

| View SSIS | Silver Destino | Método | Complexidade |
|---|---|---|---|
| `VWOrcamentoDesmobilizacaoVenda` | `silver.vw_orcamento_desmob_venda` | `saveAsSilverOverwrite` | Alta — 3 UNION ALL |
| `VWRadarDesmobilizacaoVendaDinamico` | `silver.vw_radar_desmob_venda` | `saveAsSilverOverwrite` | Alta |
| `VWOrcamentoAnteriorDesmobilizacaoVenda` | `silver.vw_orcamento_anterior_desmob` | `saveAsSilverOverwrite` | Média |
| `VWRadarDesmobilizacaoVendaVersao` | `silver.vw_radar_desmob_versao` | `saveAsSilverOverwrite` | Média |

### Silver → Gold

| Procedure SSIS | Gold Destino | Baseline | Padrão de Carga |
|---|---|---|---|
| `PROC_FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` | `gold.ft_ultimos_registros_por_placa_crs_desmob` | 84.227 | overwrite total |
| `PROC_FT_DESMOBILIZACAO_PA` | `gold.ft_desmobilizacao_pa` | 42.732 | overwrite total |
| `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | `gold.ft_orcamento_desmobilizacao_geral` | 34.741 | `replaceWhere YEAR > 2024` |
| `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL_ATUAL` | `gold.ft_orcamento_desmobilizacao_geral_atual` | 9.664 | overwrite total |
| `PROC_FT_ORCAMENTO_DESMOBILIZACAO_HISTORICO` | `gold.ft_orcamento_desmobilizacao_historico` | 26.651 | overwrite total |
| `PROC_FT_VENDA_PA` | `gold.ft_venda_pa` | 41.756 | overwrite total |
| `PROC_FT_ORCAMENTO_VENDA_GERAL` | `gold.ft_orcamento_venda_geral` | 36.100 | overwrite total |
| `PROC_FT_ORCAMENTO_VENDA_GERAL_ATUAL` | `gold.ft_orcamento_venda_geral_atual` | 9.658 | overwrite total |
| `PROC_FT_ORCAMENTO_VENDA_HISTORICO` | `gold.ft_orcamento_venda_historico` | 36.100 | overwrite total |
| `PROC_FT_RADAR_DESMOBILIZACAO_E_HISTORICO` | `gold.ft_radar_desmobilizacao` + `gold.ft_radar_desmobilizacao_historico` | 22.417 + 17.072 | overwrite total |
| `PROC_FT_RADAR_VENDA` | `gold.ft_radar_venda` | 26.056 | overwrite total |
| `PROC_FT_HISTORICO_DESMOB_PLATAFORMA_VENDA` | `gold.ft_historico_desmob_plataforma_venda` | 63.651 | overwrite total |
| `PROC_FT_HISTORICO_DE_VENDAS_PA` | `gold.ft_historico_de_vendas_pa` | 6.390 | overwrite total |
| `PROC_FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA` | `gold.ft_historico_sla_desmobilizacao_gab_pa` | 42.892 | overwrite total |
| `PROC_DM_SIMA_CIDADE_DESMOBILIZACAO` | `gold.dm_sima_cidade_desmobilizacao` | 5.570 | overwrite total |
| `PROC_DM_RLS_CONSULTOR_VENDAS_PA` | `gold.dm_rls_consultor_vendas_pa` | 24 | overwrite total |

---

## 3. Design da Silver Layer — VWOrcamentoDesmobilizacaoVenda

### Tradução do TOP 1 (regra confirmada com negócio)

```python
# cte_versao_orcamento → singleton global
# Regra: última versão não cancelada (Situacao < 2), priorizando Aprovada (1) > Gerada (0)
from pyspark.sql import functions as F

df_versao = (
    spark.table("bi_ativos.silver.stg_sima_versao_orcamento")
    .filter("Situacao < 2 AND DataDesativacao IS NULL")
    .orderBy(F.desc("Situacao"), F.desc("DataRegistro"))
    .limit(1)
    .select("Id", "OrcamentoId", "DataRegistro", "DataAtualizacao",
            "DataDesativacao", "Codigo", "Situacao")
)

# Broadcast: linha única → sem shuffle
df_itens = spark.table("bi_ativos.silver.stg_sima_versao_orcamento_itens")
df_base = df_itens.join(F.broadcast(df_versao), df_itens.VersaoId == df_versao.Id, "inner")
```

### Tradução dos 3 UNION ALL branches

```python
# Branch 1: Ciclo 1 — Desmobilização (EhDesmobilizacao = 1)
df_ciclo1_desmob = _build_branch(df_base, ciclo=1, filtro_cr="normal")

# Branch 2: Ciclo 1 — CR Venda/Transição (CrVenda=1 ou CrTransicao=1)
df_ciclo1_venda = _build_branch(df_base, ciclo=1, filtro_cr="venda_transicao")

# Branch 3: Ciclo 2 — Ação 2,3,4,5,6
df_ciclo2 = _build_branch(df_base, ciclo=2, filtro_cr="normal")

df_final = df_ciclo1_desmob.unionByName(df_ciclo1_venda).unionByName(df_ciclo2)

df_final.saveAsSilverOverwrite(
    target="bi_ativos.silver.vw_orcamento_desmob_venda",
    createIfNotExists=True
)
```

---

## 4. Design da Gold Layer — FT_ORCAMENTO_DESMOBILIZACAO_GERAL

### Tradução das 3 temp tables encadeadas

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# @TabTemp → DataFrame intermediário
df_versao_orc = spark.table("bi_ativos.silver.vw_orcamento_desmob_venda") \
    .filter(f"YEAR(DataDesmobilizacao) > {ano_corte} AND EhDesmobilizacao = 1") \
    .select("Id","Imobilizado","Chave","Placa","AnoModelo","IdPeso",
            "ValorVenda","ResidualTotal","AnoReferencia","Ciclo","CicloAcao",
            "DataDesmobilizacao","CodCr","ModeloId","CidadeDesmobilizacaoId",
            "ValorAquisicao","Diretoria","VersaoOrcamento","SituacaoVersaoOrcamento",
            "PrazoDesmobilizacaoEmDias")

# Só escreve se tem dados (IF COUNT > 0)
if df_versao_orc.limit(1).count() > 0:
    df_versao_orc.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"YEAR(DataDesmobilizacao) > {ano_corte}") \
        .option("overwriteSchema", "false") \
        .saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")

# #UltimoCr, #RegistroEhUltimoCr, #TemNaoTem → TempViews
spark.sql("CREATE OR REPLACE TEMP VIEW tv_ultimo_cr_oper AS ...")
spark.sql("CREATE OR REPLACE TEMP VIEW tv_registro_eh_ultimo AS ...")
spark.sql("CREATE OR REPLACE TEMP VIEW tv_tem_nao_tem AS ...")

# UPDATE DataDesmobRealAssertividade → MERGE Delta
w_group = Window.partitionBy("CodigoSapVix").orderBy(F.desc("DataDesmobilizacao"))
df_cte_group = spark.sql("SELECT * FROM tv_tem_nao_tem") \
    .withColumn("rn", F.row_number().over(w_group)) \
    .filter("rn = 1").drop("rn")

from delta.tables import DeltaTable
dt_gold = DeltaTable.forName(spark, "bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
dt_gold.alias("t") \
    .merge(df_cte_group.alias("s"), "t.Placa = s.CodigoSapVix AND YEAR(t.DataDesmobilizacao) > {ano_corte}") \
    .whenMatchedUpdate(set={"t.DataDesmobRealAssertividade": "s.DataDesmobilizacao"}) \
    .execute()
```

---

## 5. Workflow JSON — Skeleton

```json
{
  "name": "desmob_vendas_daily",
  "tasks": [
    {
      "task_key": "silver_tables",
      "notebook_task": {"notebook_path": "notebooks/silver/00_generic_loader"},
      "depends_on": []
    },
    {
      "task_key": "silver_views",
      "notebook_task": {"notebook_path": "notebooks/silver/01_vw_orcamento_desmob_venda"},
      "depends_on": [{"task_key": "silver_tables"}]
    },
    {
      "task_key": "gold_dimensoes",
      "notebook_task": {"notebook_path": "notebooks/gold/06_dm_sima_cidade_desmob"},
      "depends_on": [{"task_key": "silver_views"}]
    },
    {
      "task_key": "gold_ft_ultimos_registros",
      "notebook_task": {"notebook_path": "notebooks/gold/00_ft_ultimos_registros_crs"},
      "depends_on": [{"task_key": "silver_views"}]
    },
    {
      "task_key": "gold_fatos_principais",
      "notebook_task": {"notebook_path": "notebooks/gold/01_ft_desmobilizacao_pa"},
      "depends_on": [{"task_key": "gold_ft_ultimos_registros"}]
    }
  ],
  "parameters": [
    {"name": "execution_date", "default": ""},
    {"name": "ano_corte", "default": "2024"}
  ]
}
```

---

## 6. Score: 7.0 / 10

| Critério | Nota | Justificativa |
|---|---|---|
| Mapeamento de camadas | 9/10 | 17 tabelas Gold + 4 views Silver + 25 tabelas STG mapeadas |
| Padrão de carga Silver | 8/10 | lib carga_silver com método correto por tabela, ts_ingestao_brt confirmado |
| Padrão de carga Gold | 7/10 | replaceWhere atômico; MERGE para UPDATE pós-INSERT |
| Baseline de validação | 9/10 | 17 tabelas com row counts exatos do SQL Server |
| Riscos residuais | 6/10 | GETDATE() e 8 UPDATEs ainda exigem reescrita ativa |
| Workflow design | 8/10 | DAG explícito com task bloqueadora mapeada |
