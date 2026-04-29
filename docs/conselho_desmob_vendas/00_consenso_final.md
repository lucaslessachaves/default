# Conselho Desmobilização e Vendas
## Consenso Final — Síntese dos 4 Agentes
**Data:** 2026-04-29

---

## Scores Individuais

| Agente | Especialidade | Score |
|---|---|---|
| Conselheiro Eng. Software | Arquitetura, DAG, riscos operacionais | **5.5 / 10** |
| AI Data Engineer | Medallion design, Workflows, padrões Delta | **6.0 / 10** |
| The Planner | Roadmap, sprints, inventário, pré-condições | **6.0 / 10** |
| Python Developer | PySpark, tradução T-SQL, templates de código | **4.5 / 10** |

### Score Final: **5.5 / 10**

> O módulo "Desmobilização e Vendas" é **migrável**, porém não é **diretamente portável**. A Bronze pronta e as restrições arquiteturais já decididas (Workflows + Notebooks + lib `carga_silver`) são forças positivas. O que segura o score são três anti-padrões em pontos centrais do código — não periféricos — que exigem reescrita ativa, não find-and-replace. Com o plano correto, o risco de divergência silenciosa de dados no PowerBI é controlável.

---

## Consenso Técnico

### O que o conselho concordou unanimemente

**1. A view `VWOrcamentoDesmobilizacaoVenda` é o objeto mais crítico do módulo**
Todos os 4 agentes identificaram essa view como o bloqueador central. Ela alimenta 6+ tabelas Gold, tem 3 UNION ALL com 10+ joins cada, e usa `TOP 1 ORDER BY` global sem `PARTITION BY`. Se a tradução para Spark estiver errada, **todos os orçamentos no PowerBI ficam incorretos** — e o erro é silencioso (números aparecem, mas são do orçamento errado).

**Solução acordada:**
```python
# Substituir: SELECT TOP 1 FROM STG_SIMA_VERSAO_ORCAMENTO WHERE Situacao < 2 ORDER BY Situacao DESC, DataRegistro DESC
# Por:
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.desc("Situacao"), F.desc("DataRegistro"))
df_versao = (
    spark.table("bi_ativos.silver.stg_sima_versao_orcamento")
    .filter("Situacao < 2")
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
# Usar F.broadcast(df_versao) no join — é uma única linha
df_final = df_itens.join(F.broadcast(df_versao), df_itens.VersaoId == df_versao.Id, "inner")
```
> **Validar com o negócio antes de implementar:** a pergunta é "o PowerBI sempre exibe o orçamento mais recente, ou pode exibir versões históricas por placa/ano?"

**2. `GETDATE()` deve virar parâmetro `execution_date` em todos os notebooks Gold**
```python
# Em todos os notebooks Gold:
dbutils.widgets.text("execution_date", "")
execution_date = dbutils.widgets.get("execution_date") or str(date.today())

# Substituir GETDATE():
from datetime import date, timedelta
data_desmob = date.fromisoformat(execution_date) - timedelta(days=qtd_dias)
```

**3. DELETE parcial + INSERT → `replaceWhere` atômico em Delta**
```python
# Substitui: DELETE WHERE YEAR > 2024; INSERT ...
# Por operação atômica:
df_result.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"YEAR(DataDesmobilizacao) > {ano_corte}") \
    .saveAsTable("bi_ativos.gold.ft_orcamento_desmobilizacao_geral")
```

**4. DAG explícito no Workflow — `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` é task bloqueadora**
A dependência Gold→Gold precisa de `depends_on` explícito no JSON do job. Se configurado com tasks paralelas sem esse vínculo, `FT_DESMOBILIZACAO_PA` e `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` lerão dados desatualizados.

**5. 8 UPDATEs sequenciais de `PROC_FT_DESMOBILIZACAO_PA` → 1 write consolidado**
```python
# Em vez de 8 MERGE pós-INSERT, consolidar todos os lookups antes:
df_final = (
    df_base
    .join(df_status,     on='Placa', how='left')  # StatusDesmobilizacao
    .join(df_radar,      on='Placa', how='left')  # ClassificacaoRadar
    .join(df_placa_orc,  on='Placa', how='left')  # PlacaNoOrcamentoAtual
    .join(df_modelo_tco, on='Placa', how='left')  # ModeloTcoId
    .join(df_valor_aq,   on='Placa', how='left')  # ValorAquisicaoPM
    .join(df_fat_venda,  on='Placa', how='left')  # StatusFaturamentoVenda
    .join(df_ag_prec,    on='Placa', how='left')  # ValorPrecificacaoAG
    .join(df_sla,        on='Placa', how='left')  # SlaVitoriaAprovadaAvaliaGab
    .select(schema_final_columns)
)
df_final.write.format("delta").mode("overwrite").saveAsTable(target)
```

---

## Mapa de Uso da lib `carga_silver.py`

| Objeto | Método | Motivo |
|---|---|---|
| STG tables com `Id` + `ts_ingestao_bronze` | `saveDeltaAsSilver(on_colunm=['Id'])` | UNIQUEIDENTIFIER como PK, suporta CDC |
| STG tables com PK composta | `saveDeltaAsSilver(on_colunm=['col1','col2'])` | Unicidade composta |
| STG tables sem PK | `saveLastLoadAsSilver` | Sem identificador único |
| `VWOrcamentoDesmobilizacaoVenda` | `saveAsSilverOverwrite` | View calculada, sem PK de origem |
| `VWRadarDesmobilizacaoVendaDinamico` | `saveAsSilverOverwrite` | Idem |
| `STG_SAP_ZFIAA001` | `saveAsSilverOverwrite` | View SAP sem timestamp |
| STG Excel sources | `saveLastLoadAsSilver` | Sem PK, controle por data de carga |

---

## Plano de Execução — Sprints Recomendados

### Pré-Sprint (3 dias) — NÃO pule esta etapa
- [ ] **Dia 1:** Validar com negócio a semântica do `TOP 1 ORDER BY Situacao DESC` da `VWOrcamentoDesmobilizacaoVenda`
- [ ] **Dia 1:** Confirmar se `ts_ingestao_bronze` existe em todas as tabelas Bronze relevantes
- [ ] **Dia 2:** Verificar se `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` já existe no Gold Databricks
- [ ] **Dia 2:** Rodar row counts das tabelas Gold no SQL Server para criar baseline de paridade
- [ ] **Dia 3:** PoC: notebook Silver genérico com `saveDeltaAsSilver` para 3 tabelas STG simples
- [ ] **Dia 3:** Definir `ano_corte` como job parameter, confirmar valor inicial (2024)

### Sprint 1 (1 semana) — Silver
- 25 tabelas STG simples via notebook genérico parametrizado
- `VWOrcamentoDesmobilizacaoVenda` em PySpark com `saveAsSilverOverwrite`
- `VWRadarDesmobilizacaoVendaDinamico` em PySpark com `saveAsSilverOverwrite`
- Testes de paridade: row count Silver == row count STG no SQL Server

### Sprint 2 (1 semana) — Gold Dimensões + Auxiliar
- `DM_SIMA_CIDADE_DESMOBILIZACAO`, `DM_RLS_CONSULTOR_VENDAS_PA`
- `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` (task bloqueadora)
- Configurar Workflow com DAG explícito

### Sprint 3 (2 semanas) — Gold Fatos Principais
- `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` (mais complexo: 4 temp tables + UPDATE)
- `FT_DESMOBILIZACAO_PA` (mais longo: ~1.050 linhas, 8 UPDATEs)
- Testes de paridade por tabela Gold

### Sprint 4 (1 semana) — Gold Fatos Secundários
- `FT_VENDA_PA`, `FT_ORCAMENTO_VENDA_GERAL`, `FT_RADAR_DESMOBILIZACAO_E_HISTORICO`
- Históricos: `FT_HISTORICO_DESMOB_PLATAFORMA_VENDA`, `FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA`

### Sprint 5 (1 semana) — Cutover
- Operação paralela SQL Server + Databricks por 5 dias úteis
- Diff diário automatizado
- Handoff e monitoramento

---

## Estrutura Final de Pastas (gitignored)

```
notebooks/          # → adicionar ao .gitignore
  libs/
    carga_silver.py   # copiar de legacy_data/
  utils/
    config.py         # CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, execution_date
    validation.py     # row_count_check(), sum_check()
  silver/
    00_generic_loader.py              # parametrizado: bronze_table → silver_table
    01_vw_orcamento_desmob_venda.py
    02_vw_radar_desmob_venda.py
  gold/
    00_ft_ultimos_registros_crs.py   # PRIMEIRO
    01_ft_desmobilizacao_pa.py
    02_ft_orcamento_desmob_geral.py
    03_ft_venda_pa.py
    04_ft_orcamento_venda_geral.py
    05_ft_orcamento_desmob_historico.py
    06_ft_radar_desmobilizacao.py
    07_ft_historico_desmob_plataforma.py
    08_ft_historico_sla_desmob.py
    09_ft_preparacao_realizado_desmob.py
    10_ft_vistoria_realizado_desmob.py
    11_dm_sima_cidade_desmob.py
    12_dm_rls_consultor_vendas.py
  workflows/
    desmob_vendas_workflow.json       # job definition com DAG
```

---

## Veredito Final

O módulo "Desmobilização e Vendas" pode ser migrado com segurança se três condições forem atendidas antes de escrever código:

1. **Validar com negócio** a lógica do `TOP 1 ORDER BY` em `VWOrcamentoDesmobilizacaoVenda`
2. **Confirmar Bronze** tem `ts_ingestao_bronze` nas tabelas relevantes
3. **Construir baseline de row counts** das tabelas Gold no SQL Server para validação pós-migração

Sem essas três pré-condições, o risco de divergência silenciosa de dados no PowerBI é alto. Com elas, a probabilidade de sucesso é **alta** — o código legado tem documentação, os padrões são repetitivos, e a lib `carga_silver.py` resolve 80% da Silver layer com uma linha por tabela.

**Próxima ação imediata:** compartilhe os row counts das principais tabelas Gold no SQL Server para montar o baseline. Com esse baseline, podemos começar a geração dos notebooks imediatamente.

---

*Análises individuais em: `01_eng_software.md`, `02_ai_data_engineer.md`, `03_the_planner.md`, `04_python_developer.md`*
