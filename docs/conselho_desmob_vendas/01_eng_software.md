# Conselho Desmobilização e Vendas
## Agente: Conselheiro de Engenharia de Software
**Data:** 2026-04-29
**Score: 5.5 / 10**

---

## 1. Diagnóstico Arquitetural

O pipeline "Desmobilização e Vendas" é o módulo de maior densidade técnica do projeto SSIS. Não é apenas um conjunto de procedures — é um grafo de dependências onde **uma view Silver (`VWOrcamentoDesmobilizacaoVenda`) alimenta 6+ tabelas Gold**, e uma dessas tabelas Gold (`FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB`) é lida por outras duas Gold (`FT_DESMOBILIZACAO_PA` e `FT_ORCAMENTO_DESMOBILIZACAO_GERAL`), criando dependência Gold→Gold explícita.

O SSIS gerencia esse grafo via sequência de execução de pacotes — mecanismo completamente opaco para o Databricks. A migração precisa tornar esse grafo explícito como DAG no Workflows.

### Inventário de Objetos Envolvidos

| Camada | Objeto | Tipo | Complexidade |
|---|---|---|---|
| Silver (view) | `VWOrcamentoDesmobilizacaoVenda` | 3 UNION ALL + 4 CTEs + 10 joins | **Alta** |
| Silver (view) | `VWRadarDesmobilizacaoVendaDinamico` | CTEs + múltiplos joins | **Alta** |
| Silver (tabelas) | ~25 tabelas STG_* | Pass-through do Bronze | Baixa |
| Gold (auxiliar) | `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` | Lida por outras Gold | **Crítica** |
| Gold (fato) | `FT_DESMOBILIZACAO_PA` | ~1.050 linhas, 8 UPDATEs pós-INSERT | **Alta** |
| Gold (orçamento) | `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | 4 temp tables encadeadas + UPDATE | **Alta** |
| Gold (vendas) | `FT_VENDA_PA` | INSERT histórico + INSERT atual | **Média** |
| Gold (orç. venda) | `FT_ORCAMENTO_VENDA_GERAL` | Lê VWOrcamentoDesmobilizacaoVenda | **Média** |
| Gold (radar) | `FT_RADAR_DESMOBILIZACAO_E_HISTORICO` | CTEs + temp tables | **Média** |
| Dimensões | `DM_SIMA_CIDADE_DESMOBILIZACAO`, `DM_RLS_CONSULTOR_VENDAS_PA` | DELETE seletivo + INSERT | Baixa |

---

## 2. Riscos Críticos (específicos deste relatório)

### R1 — TOP 1 ORDER BY global sem PARTITION BY na `cte_versao_orcamento`
**Onde:** `STG.VWOrcamentoDesmobilizacaoVenda`, usada como INNER JOIN pelos 3 branches do UNION ALL.
```sql
-- Legado (problemático em Spark):
SELECT TOP 1 Id, OrcamentoId, ... FROM STG_SIMA_VERSAO_ORCAMENTO
WHERE Situacao < 2
ORDER BY Situacao DESC, DataRegistro DESC
```
Em Spark, `Window.orderBy()` sem `partitionBy()` força shuffle global — OOM garantido com tabela grande. A semântica "último orçamento aprovado ou gerado" precisa ser validada com o negócio antes de reescrever.

**Impacto:** Bug silencioso — números aparecem no PowerBI, mas podem ser do orçamento errado.

### R2 — `GETDATE()` em lógica de DataDesmobilizacao
```sql
-- PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL, cte_zfi:
DATEADD(DAY, -CAST([Qtd dias no Centro de Custo atual] AS int), CAST(CONCAT(YEAR(GETDATE()),...) AS date))
```
Qualquer reprocessamento histórico calculará datas diferentes do original. O Workflow **deve** receber `execution_date` como parâmetro e todos os notebooks devem consumi-lo via `dbutils.widgets.get("execution_date")`.

**Impacto:** Backfills e reprocessamentos de incidente produzem dados incorretos silenciosamente.

### R3 — DELETE parcial por ano sem atomicidade
```sql
DELETE FROM DW.FT_ORCAMENTO_DESMOBILIZACAO_GERAL WHERE YEAR(DataDesmobilizacao) > 2024;
INSERT INTO ... -- se falhar aqui, histórico de 2025+ está perdido
```
**Mitigação:** Substituir por `df.write.option("replaceWhere", "YEAR(DataDesmobilizacao) > 2024").mode("overwrite")` — operação atômica em Delta Lake.

### R4 — Dependência Gold→Gold não explícita
`FT_ORCAMENTO_DESMOBILIZACAO_GERAL` e `FT_DESMOBILIZACAO_PA` leem de `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB`. No Workflows, essa task precisa de `depends_on` explícito. Se configurado com tasks paralelas sem esse vínculo, as duas tabelas lerão dados desatualizados.

### R5 — 8 UPDATEs sequenciais pós-INSERT em `PROC_FT_DESMOBILIZACAO_PA`
O legado faz 1 INSERT + 8 UPDATEs de colunas distintas. Em Delta, cada UPDATE é um MERGE com full scan. Para tabela de fatos com histórico, 8 scans pós-INSERT são caros e deixam janela de inconsistência entre o INSERT e o último UPDATE.

**Mitigação:** Consolidar todos os lookups no DataFrame antes do único `write.saveAsTable`, eliminando os 8 MERGEs.

---

## 3. Proposta de Solução

### Estrutura de Pastas (gitignored)
```
notebooks/
  silver/
    00_generic_silver_loader.py       # genérico para ~25 tabelas STG simples
    01_vw_orcamento_desmob_venda.py   # saveAsSilverOverwrite
    02_vw_radar_desmob_venda.py       # saveAsSilverOverwrite
  gold/
    00_ft_ultimos_registros_crs.py    # DEVE rodar primeiro (dependência)
    01_ft_desmobilizacao_pa.py
    02_ft_orcamento_desmob_geral.py
    03_ft_venda_pa.py
    04_ft_orcamento_venda_geral.py
    05_ft_radar_desmobilizacao.py
    06_dm_sima_cidade_desmob.py
    07_dm_rls_consultor_vendas.py
  utils/
    config.py          # catálogo, schemas, execution_date
    validation.py      # row count checks
  libs/
    carga_silver.py    # lib copiada de legacy_data/
```

### Padrão Silver com carga_silver.py
```python
# STG tables com PK (Id UNIQUEIDENTIFIER) e ts_ingestao_bronze
tr = TableReference('bi_ativos.bronze.ods_sima_modelo')
df = tr.selectLazy()
df.saveDeltaAsSilver(
    target='bi_ativos.silver.stg_sima_modelo',
    on_colunm=['Id'],
    tsp_load_colunm='ts_ingestao_brt',
    createIfNotExists=True
)

# VWOrcamentoDesmobilizacaoVenda (view calculada, sem PK de origem)
df_view = spark.sql("SELECT ... FROM bi_ativos.silver.stg_sima_versao_orcamento_itens ...")
df_view.saveAsSilverOverwrite(
    target='bi_ativos.silver.vw_orcamento_desmob_venda',
    createIfNotExists=True
)
```

### Padrão Gold — DELETE parcial → `replaceWhere` atômico
```python
df_result.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"YEAR(DataDesmobilizacao) > {ano_corte}") \
    .option("overwriteSchema", "false") \
    .saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
```

### Padrão Gold — 8 UPDATEs consolidados em 1 write
```python
# Consolidar todos os lookups ANTES de escrever
df_final = (
    df_base
    .join(df_status_lookup,    on='Placa', how='left')   # StatusDesmobilizacao
    .join(df_radar_lookup,     on='Placa', how='left')   # ClassificacaoRadar
    .join(df_placa_orc_lookup, on='Placa', how='left')   # PlacaNoOrcamentoAtual
    # ... demais 5 lookups
    .select(colunas_finais)
)
df_final.write.format("delta").mode("overwrite").saveAsTable(target)
```

---

## 4. DAG de Dependências — Ordem de Execução no Workflow

```
TASK 1: [Silver Tables ~25]          ──┐
TASK 2: [Silver VW Orcamento Desmob] ──┤
TASK 3: [Silver VW Radar Desmob]     ──┤
                                       ▼
TASK 4: [DM_SIMA_CIDADE_DESMOB]      ──┐  (paralelas, pós-Silver)
TASK 5: [DM_RLS_CONSULTOR_VENDAS]    ──┤
                                       ▼
TASK 6: [FT_ULTIMOS_REGISTROS_CRS]   ──┐  ← BLOQUEADOR: deve terminar antes dos seguintes
                                       ▼
TASK 7: [FT_DESMOBILIZACAO_PA]       ──┐  (depends_on TASK 6)
TASK 8: [FT_ORCAMENTO_DESMOB_GERAL]  ──┤  (depends_on TASK 6)
TASK 9: [FT_ORCAMENTO_DESMOB_HIST]   ──┤
TASK 10:[FT_ORCAMENTO_DESMOB_ATUAL]  ──┤
                                       ▼
TASK 11:[FT_VENDA_PA]                ──┐  (paralelas, pós-TASK 6)
TASK 12:[FT_ORCAMENTO_VENDA_GERAL]   ──┤
TASK 13:[FT_RADAR_DESMOBILIZACAO]    ──┤
TASK 14:[FT_HISTORICO_DESMOB]        ──┘
```

---

## 5. Pré-condições Bloqueadoras

1. **Validar semântica do `TOP 1 ORDER BY Situacao DESC, DataRegistro DESC`** com o negócio antes de codificar. Pergunta: "O PowerBI sempre usa o orçamento mais recente, ou há casos onde versões antigas devem ser exibidas?"
2. **Confirmar existência de `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` no Databricks** — se não existir no Gold, toda a cadeia de `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` falha.
3. **Definir `ano_corte` como job parameter** (atualmente hardcoded como 2024).
4. **Verificar se `ts_ingestao_bronze` existe** em todas as tabelas Bronze — a lib `saveDeltaAsSilver` depende dessa coluna.

---

## 6. Score de Migração: 5.5 / 10

| Critério | Nota | Justificativa |
|---|---|---|
| Clareza do escopo | 8/10 | DDLs + procedures completas disponíveis |
| Qualidade do código legado | 3/10 | TOP 1 global, GETDATE(), 8 UPDATEs sequenciais, 4 temp tables encadeadas |
| Dependências mapeáveis | 6/10 | Gold→Gold identificada, mas pode haver outras não mapeadas |
| Padrão de carga | 5/10 | DELETE parcial + INSERT correto em conceito, mas perigoso sem atomicidade |
| Testabilidade | 2/10 | Zero testes no legado, zero baseline de row counts |
| Restrições respeitadas | 9/10 | Workflows + Notebooks + lib carga_silver — decisões arquiteturais claras |

**O que faz a diferença entre 5.5 e 7.0:** resolver o TOP 1 sem PARTITION BY com validação de negócio, eliminar os 8 UPDATEs sequenciais em `PROC_FT_DESMOBILIZACAO_PA`, e criar baseline de row counts antes do cutover. Sem essas três ações, o risco de divergência silenciosa de dados no PowerBI é alto.
