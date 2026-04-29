# 03 — The Planner: Plano Estratégico de Migração

> **Relatório:** PowerBI "Desmobilização e Vendas"
> **Origem:** SSIS + SQL Server (Projeto_ativos)
> **Destino:** Databricks (`bi_ativos.{bronze,silver,gold}`)
> **Data:** 2026-04-29
> **Threshold do agente:** 0.90

---

## Sumário Executivo

Migração de 17 tabelas Gold (FT_*), 2 dimensões (DM_*) e 9+ views Silver (VW*) que alimentam um único relatório PowerBI. O escopo concentra alta complexidade analítica em poucas procedures (a `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` sozinha tem 4 temp tables encadeadas + CTEs + UPDATE pos-INSERT). A dependência Gold→Gold sobre `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` é o ponto crítico de orquestração. A regra de negócio "última versão aprovada do orçamento" da view `VWOrcamentoDesmobilizacaoVenda` é não-determinística no legado (TOP 1 ORDER BY sem PARTITION BY) e exige reconstrução com `ROW_NUMBER()` antes de ser portada.

**Score de Migração: 6.0/10** — viável e bem delimitado, mas com débito técnico crítico em 2 pontos (Gold→Gold + não-determinismo) que precisam ser resolvidos antes do código.

---

## 1. Inventário Completo

### 1.1 Camada Gold (`bi_ativos.gold.*`)

#### Fatos principais (alimentam diretamente o PowerBI)

| # | Tabela Gold | Procedure de origem | Tipo de carga | Dependências Gold→Gold |
|---|---|---|---|---|
| 1 | `ft_desmobilizacao_pa` | `PROC_FT_DESMOBILIZACAO_PA` | DELETE/INSERT por `EhHistorico` (0/1) + UPDATEs posteriores | lê `FT_HISTORICO_DE_VENDAS_PA`, `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` |
| 2 | `ft_orcamento_desmobilizacao_geral` | `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | DELETE WHERE YEAR>2024 + INSERT + UPDATE assertividade | lê `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` |
| 3 | `ft_orcamento_desmobilizacao_geral_atual` | (proc derivada) | Snapshot da versão atual | lê `ft_orcamento_desmobilizacao_geral` |
| 4 | `ft_orcamento_desmobilizacao_historico` | (proc derivada) | INSERT incremental (snapshot histórico) | lê `ft_orcamento_desmobilizacao_geral` |
| 5 | `ft_venda_pa` | `PROC_FT_VENDA_PA` | DELETE/INSERT | Silver puro |
| 6 | `ft_orcamento_venda_geral` | (proc) | DELETE/INSERT | Silver |
| 7 | `ft_orcamento_venda_geral_atual` | (proc derivada) | Snapshot atual | lê `ft_orcamento_venda_geral` |
| 8 | `ft_orcamento_venda_historico` | (proc derivada) | INSERT incremental | lê `ft_orcamento_venda_geral` |
| 9 | `ft_radar_desmobilizacao` | (proc) | DELETE/INSERT | lê view `VWRadarDesmobilizacaoVendaDinamico` |
| 10 | `ft_radar_desmobilizacao_historico` | (proc derivada) | INSERT incremental | lê `ft_radar_desmobilizacao` |
| 11 | `ft_radar_venda` | (proc) | DELETE/INSERT | view radar |
| 12 | `ft_historico_desmob_plataforma_venda` | (proc) | INSERT acumulativo | Silver |
| 13 | `ft_historico_de_vendas_pa` | (proc) | INSERT acumulativo / append | Silver Excel |
| 14 | `ft_historico_sla_desmobilizacao_gab_pa` | (proc) | DELETE/INSERT | Silver AvaliaGab |
| 15 | `ft_preparacao_realizado_desmobilizacao_avalia_gab` | (proc) | DELETE/INSERT | Silver AvaliaGab |
| 16 | `ft_vistoria_realizado_desmobilizacao_avalia_gab` | (proc) | DELETE/INSERT | Silver AvaliaGab |

#### Fato auxiliar (dependência Gold→Gold crítica)

| # | Tabela Gold | Observação |
|---|---|---|
| 17 | `ft_ultimos_registros_por_placa_crs_desmob` | **Pré-requisito de #1 e #2.** Precisa rodar ANTES das fatos principais. |

#### Dimensões

| # | Dimensão Gold | Origem | Tipo |
|---|---|---|---|
| D1 | `dm_sima_cidade_desmobilizacao` | `PROC_DM_SIMA_CIDADE_DESMOBILIZACAO` | Overwrite + linha N/A com GUID hardcoded |
| D2 | `dm_rls_consultor_vendas_pa` | `PROC_DM_RLS_CONSULTOR_VENDAS_PA` | Overwrite (RLS — Row Level Security) |

**Total Gold: 17 fatos + 2 dimensões = 19 objetos.**

### 1.2 Camada Silver (`bi_ativos.silver.*`)

#### Views (lógica de negócio — porta para notebooks PySpark individuais)

| # | View Silver | Complexidade | Anti-padrões críticos |
|---|---|---|---|
| V1 | `vw_orcamento_desmobilizacao_venda` | **ALTA** | 3 UNION ALL, 10+ joins, TOP 1 ORDER BY não-determinístico, CTEs encadeadas (cte_alteracao_cr_ciclo1_*, cte_alteracao_cr_ciclo2_*) |
| V2 | `vw_radar_desmobilizacao_venda_dinamico` | **ALTA** | TOP 1 dinâmico, CASE WHEN amplo, joins múltiplos |

#### Tabelas Silver simples (notebook genérico parametrizado, espelho fiel da Bronze + SCD2)

Total: **~25 tabelas STG** envolvidas. Categorizadas por origem:

**SIMA (sistema de gestão — 10 tabelas)**
- `stg_sima_versao_orcamento_itens`
- `stg_sima_versao_orcamento`
- `stg_sima_orcamento`
- `stg_sima_centro_resultado`
- `stg_sima_diretoria`
- `stg_sima_modelo`
- `stg_sima_alteracao_centro_resultado_versao_orcamento_item`
- `stg_sima_parametrizacao_prazo_de_desmobilizacao`
- `stg_radar_desmobilizacao_sima`
- `stg_estoque_vendas_ativos_pa`

**FIPE / SAP (3 tabelas)**
- `stg_fipe_atual`
- `stg_sap_zfiaa001`
- `stg_historico_cr_equipamentos_gab`

**Excel manual (2 tabelas — fonte frágil)**
- `stg_historico_de_vendas_pa_excel`
- `stg_excel_historico_desmobilizacao_ebec`

**AvaliaGab (vistoria/precificação — 2 tabelas)**
- `stg_avalia_gab_veiculo`
- `stg_avalia_gab_precificacao`

**Plataforma de Ativos / Negociação (8 tabelas)**
- `stg_plataforma_ativos_*` (placeholder — várias)
- `stg_negociacao_pa`
- `stg_lance_pa`
- `stg_oferta_pa`
- `stg_veiculos_pa`
- `stg_veiculo_vistoria_pa`
- `stg_historico_placa_plataforma_venda`

### 1.3 Camada Bronze (`bi_ativos.bronze.*`)

**Status:** Já existe, espelho fiel SQL Server (assumido pelo contexto). Não há trabalho de Bronze neste escopo. Validar somente que todas as ~25 tabelas STG estão refletidas em Bronze antes da Silver começar.

---

## 2. Complexidade por Objeto

### Critério de classificação

| Nível | Critério |
|---|---|
| **Simples** | Espelho 1:1, sem lógica, sem TOP 1/CTE/UPDATE pos-INSERT. SCD2 padrão. |
| **Média** | 1-3 joins, alguns IIF/COALESCE, sem temp tables, determinístico. |
| **Alta** | TOP 1 sem PARTITION BY, multiple temp tables, UPDATE pos-INSERT, CTEs encadeadas, dependência Gold→Gold, GETDATE() em pipeline de fato. |

### 2.1 Procedures Gold

| Objeto | Complexidade | Justificativa |
|---|---|---|
| `PROC_FT_DESMOBILIZACAO_PA` | **ALTA** | 2 blocos (histórico/atual), ~15 STG tables com LEFT JOINs, ~8 UPDATEs sequenciais em colunas separadas, lê 2 fatos Gold |
| `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | **ALTA** | DECLARE @TabTemp + 3 #temp tables encadeadas (#UltimoCr, #RegistroEhUltimoCr, #TemNaoTem) + cte_group com ROW_NUMBER + UPDATE pos-INSERT + GETDATE() + DELETE parcial por YEAR |
| `PROC_FT_VENDA_PA` | **Média** | Padrão DELETE/INSERT, joins moderados |
| `PROC_FT_ORCAMENTO_VENDA_GERAL` | **Média** | Espelho de orçamento desmob mas sem GETDATE() pesado |
| `PROC_FT_RADAR_DESMOBILIZACAO` | **Média** | Lê view radar dinâmico (lógica está na view) |
| `PROC_FT_RADAR_VENDA` | **Média** | Idem |
| `PROC_FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` | **Média** | Padrão "último registro por placa" — ROW_NUMBER direto |
| Procedures `_ATUAL` e `_HISTORICO` (snapshots) | **Simples** | INSERT/SELECT direto da fato master |
| `PROC_FT_HISTORICO_DESMOB_PLATAFORMA_VENDA` | **Média** | Append incremental |
| `PROC_FT_HISTORICO_DE_VENDAS_PA` | **Média** | Lê fonte Excel + dedup |
| `PROC_FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA` | **Média** | Cálculo de SLA com DATEDIFF |
| `PROC_FT_PREPARACAO_REALIZADO_DESMOBILIZACAO_AVALIA_GAB` | **Média** | Joins AvaliaGab |
| `PROC_FT_VISTORIA_REALIZADO_DESMOBILIZACAO_AVALIA_GAB` | **Média** | Joins AvaliaGab |
| `PROC_DM_SIMA_CIDADE_DESMOBILIZACAO` | **Simples** | Overwrite + linha N/A hardcoded |
| `PROC_DM_RLS_CONSULTOR_VENDAS_PA` | **Simples** | Overwrite tabela RLS |

**Total Gold: 2 ALTA, 9 Média, 4 Simples.**

### 2.2 Views Silver

| View | Complexidade | Justificativa |
|---|---|---|
| `VWOrcamentoDesmobilizacaoVenda` | **ALTA** | 3 UNION ALL + 4 CTEs encadeadas (ciclo1/ciclo2 × geral/ultimo) + TOP 1 ORDER BY Situacao DESC não-determinístico + 10+ joins |
| `VWRadarDesmobilizacaoVendaDinamico` | **ALTA** | Lógica de classificação dinâmica de radar com múltiplos CASE WHEN aninhados |

### 2.3 Tabelas Silver simples

**~23 tabelas Simples** (notebook genérico parametrizado + SCD2 lib do usuário).
Únicas com atenção:
- `stg_historico_de_vendas_pa_excel` e `stg_excel_historico_desmobilizacao_ebec` — fontes Excel, validar tipos antes do SCD2.
- `stg_sima_alteracao_centro_resultado_versao_orcamento_item` — alta cardinalidade, candidata a Z-ORDER por `VersaoId`.

---

## 3. Plano de Sprints

> **Premissa:** 1 dev em tempo integral. SCD2 lib entregue até o final da Pré-Sprint. Notebooks gerados em pasta gitignored e exportados manualmente para o workspace Databricks.

### Pré-Sprint (Sprint Zero específica do relatório) — 5 dias úteis

**Objetivo:** Levantar dependências reais e ter a SCD2 lib em mãos.

**Entregáveis:**
- D1: Scanner de dependências Gold→Gold rodado nas 17 procs. Saída em `docs/conselho_desmob_vendas/dependencias.md` com grafo de execução.
- D2: Scanner de anti-padrões SQL nas 17 procs + 2 views. Saída em `antipatterns.md` (esperado: GETDATE, TOP 1, IIF, NOLOCK, #temp).
- D3: SCD2 lib do usuário recebida, lida e documentada (`scd2_lib_contract.md` — assinatura das funções, parâmetros obrigatórios, comportamento esperado).
- D4: Validação de cobertura Bronze — query `SHOW TABLES IN bi_ativos.bronze` cruzada com lista de 25 STG; gap report.
- D5: Setup do repo local: pasta `notebooks/` no `.gitignore`, script de export para `.py` Databricks-compatível.

**Critério de saída:** grafo de dependências aprovado + SCD2 lib testada com 1 tabela piloto.

### Sprint 1 — Silver Foundation (5 dias úteis)

**Objetivo:** Notebook genérico de Silver simples + 23 tabelas STG portadas.

**Entregáveis:**
- Notebook `silver_generic_scd2.py` parametrizado (input: nome tabela bronze; output: silver com SCD2).
- 23 tabelas STG portadas em batch (estimativa: 5/dia após o notebook genérico funcionar).
- Validação de paridade: `count(*)` Bronze vs Silver vs SQL Server (amostral, 5 tabelas).
- Workflow Job 01: "silver_simples_desmob_vendas" com 23 tasks paralelas.

**Critério de saída:** todas as 23 tabelas Silver carregadas com SCD2 + paridade ≥ 99.5% nas 5 amostradas.

### Sprint 2 — Silver Views Críticas (5 dias úteis)

**Objetivo:** Portar `VWOrcamentoDesmobilizacaoVenda` e `VWRadarDesmobilizacaoVendaDinamico`.

**Entregáveis:**
- D1-D2: Reescrita de `vw_orcamento_desmobilizacao_venda`:
  - Substituir `TOP 1 ORDER BY Situacao DESC` por `ROW_NUMBER() OVER (PARTITION BY OrcamentoId ORDER BY Situacao DESC, DataAprovacao DESC, Id) = 1` — definir tie-breaker com o usuário.
  - Migrar 4 CTEs encadeadas para DataFrames PySpark separados com `cache()`.
  - Notebook: `silver_vw_orcamento_desmobilizacao_venda.py`.
- D3: Reescrita de `vw_radar_desmobilizacao_venda_dinamico` com mesmo padrão.
- D4: Suite pytest para as 2 views — comparar 100 placas amostradas com resultado SQL Server.
- D5: Workflow Job 02: "silver_views_desmob_vendas" (depende do Job 01).

**Critério de saída:** paridade ≥ 99% por placa nas duas views vs legado.

### Sprint 3 — Gold Dimensões + Auxiliar (5 dias úteis)

**Objetivo:** 2 dimensões + tabela auxiliar `ft_ultimos_registros_por_placa_crs_desmob` (pré-requisito Gold→Gold).

**Entregáveis:**
- D1: Notebook `gold_dm_sima_cidade_desmobilizacao.py` (overwrite + injeção de linha N/A com GUID `CD9B0202-E70B-449B-ABD1-134D7D686275` ou GUID atual).
- D2: Notebook `gold_dm_rls_consultor_vendas_pa.py`.
- D3-D4: Notebook `gold_ft_ultimos_registros_por_placa_crs_desmob.py` — usar `ROW_NUMBER() OVER (PARTITION BY Placa ORDER BY DataRegistro DESC)`.
- D5: Workflow Job 03: "gold_dim_aux_desmob_vendas" (depende do Job 02).

**Critério de saída:** as 3 tabelas existem em `bi_ativos.gold` e são pré-requisito documentado para Sprint 4.

### Sprint 4 — Gold Fatos Vendas (5 dias úteis)

**Objetivo:** Portar família VENDAS (caminho mais simples primeiro).

**Entregáveis:**
- D1-D2: `gold_ft_venda_pa.py` + `gold_ft_orcamento_venda_geral.py`.
- D3: `gold_ft_orcamento_venda_geral_atual.py` + `gold_ft_orcamento_venda_historico.py`.
- D4: `gold_ft_radar_venda.py` + `gold_ft_historico_de_vendas_pa.py`.
- D5: Workflow Job 04: "gold_fatos_venda" (depende do Job 03).

**Critério de saída:** paridade ≥ 99% nas 6 fatos venda.

### Sprint 5 — Gold Fatos Desmobilização (5 dias úteis) — CRÍTICO

**Objetivo:** Portar família DESMOBILIZAÇÃO (mais complexa).

**Entregáveis:**
- D1-D2: `gold_ft_desmobilizacao_pa.py` — separar histórico/atual em 2 tasks Spark; converter os ~8 UPDATEs em uma única projeção com `withColumn`.
- D3-D4: `gold_ft_orcamento_desmobilizacao_geral.py` — substituir GETDATE() por parâmetro `execution_date`; substituir `@TabTemp + #UltimoCr + #RegistroEhUltimoCr + #TemNaoTem` por DataFrames PySpark com `cache()`; substituir DELETE WHERE YEAR>2024 por `replaceWhere`.
- D5: Notebooks restantes — `_atual`, `_historico`, `radar_desmobilizacao`, `radar_desmobilizacao_historico`.

**Critério de saída:** paridade ≥ 99% em `ft_desmobilizacao_pa` e `ft_orcamento_desmobilizacao_geral` vs legado, executados com `execution_date = '2026-04-29'`.

### Sprint 6 — Gold Fatos Auxiliares + Cutover (5 dias úteis)

**Objetivo:** Fechar os 4 fatos restantes + cutover do PowerBI.

**Entregáveis:**
- D1: `gold_ft_historico_desmob_plataforma_venda.py`.
- D2: `gold_ft_historico_sla_desmobilizacao_gab_pa.py` + `gold_ft_preparacao_realizado_desmobilizacao_avalia_gab.py` + `gold_ft_vistoria_realizado_desmobilizacao_avalia_gab.py`.
- D3: Workflow Job 05: "gold_fatos_desmob" — orquestração end-to-end (Bronze → Silver → Gold) com SLA documentado.
- D4: Repointar dataset PowerBI para `bi_ativos.gold.*` em ambiente DEV. Validação visual com analista de negócio em 5 visuais críticos.
- D5: Cutover PROD + run paralelo de 1 semana (SSIS continua rodando como fallback) + descomissionamento agendado.

**Critério de saída:** PowerBI em produção apontando para Databricks; SSIS desligado após 7 dias de paralelo OK.

### Resumo do cronograma

```text
Pré-Sprint    Sprint 1     Sprint 2     Sprint 3     Sprint 4     Sprint 5     Sprint 6
 (5d)          (5d)         (5d)         (5d)         (5d)         (5d)         (5d)
|------|----------|------------|------------|------------|------------|------------|
W0       W1           W2           W3           W4           W5            W6

Total: 7 semanas (~35 dias úteis) com 1 dev em tempo integral.
Caminho crítico: Pré-Sprint → Silver views (S2) → Gold auxiliar (S3) → ft_orcamento_desmob (S5)
```

---

## 4. Matriz de Riscos (específica deste relatório)

| # | Risco | Impacto | Probabilidade | Mitigação |
|---|---|---|---|---|
| R1 | **TOP 1 ORDER BY Situacao DESC** em `VWOrcamentoDesmobilizacaoVenda` retorna ordens diferentes em Spark vs SQL Server, mudando qual versão de orçamento aparece no PowerBI | **ALTO** | **ALTO** | Definir tie-breaker explícito com usuário ANTES de codar Sprint 2: `ROW_NUMBER() OVER (PARTITION BY OrcamentoId ORDER BY Situacao DESC, DataAprovacao DESC, Id) = 1`. Validar com 100 placas amostradas. |
| R2 | **GETDATE() em PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL** calcula `DATEADD(DAY,-X,GETDATE())` — re-executar carga em data diferente muda o resultado (impede backfill) | **ALTO** | **ALTO** | Parametrizar `execution_date` no notebook Sprint 5. Documentar que reruns só com data congelada. |
| R3 | **Dependência Gold→Gold em `ft_ultimos_registros_por_placa_crs_desmob`** quebra se executada fora de ordem | **ALTO** | **MÉDIO** | Workflow Job 03 declara dependência explícita. Sprint 5 só inicia após Sprint 3 OK. |
| R4 | **SCD2 lib do usuário não chega a tempo** ou tem contrato incompatível com PySpark+Delta | **ALTO** | **MÉDIO** | Pré-Sprint D3 é portão. Se não chegar, escalar imediatamente. PoC com 1 tabela piloto valida o contrato. |
| R5 | **Linhas N/A com GUID hardcoded** nas 2 dimensões: se GUID mudar, RLS PowerBI quebra | **MÉDIO** | **BAIXO** | Catalogar GUIDs em arquivo de configuração `dim_na_guids.yml` versionado; reusar nos notebooks. |
| R6 | **Fontes Excel** (`stg_historico_de_vendas_pa_excel`, `stg_excel_historico_desmobilizacao_ebec`) com tipos inconsistentes quebram SCD2 | **MÉDIO** | **MÉDIO** | Schema enforcement com `expect_or_fail` no notebook Silver simples; teste pytest validando tipos em Pré-Sprint. |
| R7 | **DELETE FROM tabela WHERE YEAR(col) > 2024** em padrão de carga de orçamento — se backfill rodar em ano errado, apaga dados em produção | **CRÍTICO** | **BAIXO** | Substituir por `replaceWhere` Delta com guard rail: `assert execution_date.year >= 2025`. |
| R8 | **142 procedures Gold no projeto inteiro** mas só 17 neste relatório — risco de portar dependência transitiva não mapeada | **MÉDIO** | **MÉDIO** | Scanner de dependências da Pré-Sprint detecta. Se procedure fora do escopo for lida, escalar imediatamente. |
| R9 | **VWRadarDesmobilizacaoVendaDinamico** — lógica de classificação dinâmica não documentada; risco de perder regra de negócio | **MÉDIO** | **MÉDIO** | Sessão de 2h com analista de negócio na Sprint 2 D3 para mapear cada CASE WHEN. |
| R10 | **Sem token de workspace Databricks** — notebooks têm que ser exportados manualmente, propenso a versionar errado | **BAIXO** | **ALTO** | Script de export `.py` na Pré-Sprint D5; pasta gitignored mas com hash SHA-256 versionado em arquivo de manifesto. |
| R11 | **Cluster pequeno** durante backfill de 23 tabelas Silver no Sprint 1 pode causar OOM em `stg_sima_alteracao_centro_resultado_versao_orcamento_item` (alta cardinalidade) | **MÉDIO** | **BAIXO** | Z-ORDER por `VersaoId` + cluster com autoscale 2-8. |
| R12 | **Run paralelo SSIS+Databricks** durante Sprint 6 D5 — se ambos escreverem na mesma tabela DW (caso fallback ativo), inconsistência | **ALTO** | **BAIXO** | Databricks escreve em `bi_ativos.gold` (catalog separado). PowerBI em DEV aponta para Databricks; PROD continua SSIS até cutover formal. |

### Matriz visual

```text
                  │ Baixa Prob.  │ Média Prob.  │ Alta Prob.    │
──────────────────┼──────────────┼──────────────┼───────────────┤
Impacto CRÍTICO   │ R7           │              │               │
──────────────────┼──────────────┼──────────────┼───────────────┤
Impacto ALTO      │ R12          │ R3, R4       │ R1, R2        │
──────────────────┼──────────────┼──────────────┼───────────────┤
Impacto MÉDIO     │ R5, R11      │ R6, R8, R9   │ R10           │
──────────────────┴──────────────┴──────────────┴───────────────┘
```

**Riscos críticos a mitigar antes de qualquer código:** R1, R2, R4.

---

## 5. Pré-condições (antes de começar)

> Nada do plano de sprints inicia até que TODOS os 8 itens abaixo estejam OK.

### 5.1 Do usuário

- [ ] **SCD2 lib Python entregue** com:
  - Código fonte (módulo Python instalável: `pip install -e .`)
  - README com assinatura das funções principais
  - 1 exemplo de uso completo (input Bronze DataFrame → output Silver DataFrame com SCD2)
  - Definição clara de chaves de negócio, hash de mudança, colunas de validade (`valid_from`, `valid_to`, `is_current`)
- [ ] **Tie-breaker da regra "última versão aprovada"** confirmado por escrito (resolve R1)
- [ ] **GUIDs das linhas N/A** das 2 dimensões confirmados (ou autorização para gerar novos)
- [ ] **Janela de cutover PowerBI** acordada com analistas de negócio (Sprint 6 D5)

### 5.2 Do ambiente Databricks

- [ ] **Catálogo `bi_ativos.silver` e `bi_ativos.gold`** criados com permissões corretas
- [ ] **Cluster de desenvolvimento** disponível (DBR ≥ 14.3 LTS, autoscale 2-8 nós, Photon habilitado)
- [ ] **Bronze validada** — script `SHOW TABLES IN bi_ativos.bronze` retorna as 25 tabelas STG do escopo
- [ ] **Workflows habilitado** no workspace + permissão de criar Jobs

### 5.3 Do repositório local

- [ ] Pasta `notebooks/` no `.gitignore`
- [ ] Script `export_notebook.py` que converte `.py` local em formato Databricks (`# COMMAND ----------` separadores)
- [ ] `requirements.txt` com `pyspark`, `delta-spark`, `pytest`, e referência à SCD2 lib

---

## 6. Primeiros 3 Dias de Ação

### Dia 1 (segunda — 2026-05-04)

**Manhã (4h)**
- [ ] **09:00** Reunião com usuário (30min): confirmar entrega da SCD2 lib + tie-breaker R1 + GUIDs
- [ ] **09:30** Criar diretório `docs/conselho_desmob_vendas/sprint_zero/`
- [ ] **10:00** Implementar e rodar **Scanner de dependências Gold→Gold** (script da Sprint Zero do CLAUDE.md, adaptado para 17 procs do escopo)
- [ ] **11:30** Output: `dependencias.md` com grafo de execução visual

**Tarde (4h)**
- [ ] **14:00** Implementar e rodar **Scanner de anti-padrões SQL** nas 17 procs + 2 views
- [ ] **15:30** Output: `antipatterns.md` com contagem por padrão (GETDATE, TOP 1, IIF, NOLOCK, #temp, @TabTemp, UNIQUEIDENTIFIER)
- [ ] **16:30** Cross-check: cada anti-padrão tem mapeamento na tabela "T-SQL → Delta Lake"
- [ ] **17:00** Commit: `git commit -m "Sprint Zero: scanners de dependência e anti-padrões"`

### Dia 2 (terça — 2026-05-05)

**Manhã (4h)**
- [ ] **09:00** Receber SCD2 lib do usuário (se ainda não recebida, escalar)
- [ ] **09:30** Ler código + README da lib; documentar contrato em `scd2_lib_contract.md`
- [ ] **11:00** Identificar 1 tabela Silver piloto (sugestão: `stg_sima_diretoria` — pequena, simples)
- [ ] **11:30** Setup do projeto local: `notebooks/` gitignored, `requirements.txt`, instalar SCD2 lib

**Tarde (4h)**
- [ ] **14:00** PoC: notebook `silver_generic_scd2.py` chamando a SCD2 lib na tabela piloto
- [ ] **16:00** Validação: rodar PoC contra Bronze; comparar count + checksum vs SQL Server
- [ ] **17:00** Output: PoC funcionando OU bug report enviado ao autor da lib

### Dia 3 (quarta — 2026-05-06)

**Manhã (4h)**
- [ ] **09:00** Validar cobertura Bronze: `SHOW TABLES IN bi_ativos.bronze` cruzado com lista de 25 STG
- [ ] **10:00** Output: `bronze_coverage.md` com gap report (tabelas faltantes, se houver)
- [ ] **11:00** Se gap detectado: escalar e definir plano (criar ingestão Bronze adicional)

**Tarde (4h)**
- [ ] **14:00** Setup do Workflow Job genérico: `Job 01 - silver_simples_desmob_vendas` (vazio, só estrutura)
- [ ] **15:00** Documentar template de notebook Silver: cabeçalho padrão, parâmetros, validações
- [ ] **16:00** **Demo interna** ao usuário: PoC SCD2 funcionando + grafo de dependências + anti-padrões
- [ ] **17:00** **Decisão de Go/No-Go** para iniciar Sprint 1 na quinta

**Critério de saída do Dia 3:** Pré-Sprint completa OU lista clara de blockers para o usuário.

---

## 7. Score de Migração: 6.0/10

### Justificativa

#### Pontos positivos (+)

- **Escopo bem delimitado**: 17 fatos + 2 dimensões + 2 views críticas é gerenciável em 7 semanas com 1 dev.
- **Bronze já existe**: elimina toda a camada de ingestão (que costuma ser 30% do esforço).
- **Restrições claras**: Workflows + Notebooks + SCD2 lib obrigatória — decisões arquiteturais já tomadas, sem espaço para divagação.
- **Documentação legada disponível**: 100% das procedures e DDLs estão em `/tmp/ssis_project/`.
- **Dependência Gold→Gold conhecida**: `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` já mapeada (não é surpresa).

#### Pontos negativos (-)

- **2 anti-padrões críticos** em pontos centrais: `TOP 1 ORDER BY Situacao DESC` em `VWOrcamentoDesmobilizacaoVenda` (afeta a regra de negócio principal do relatório) e `GETDATE()` em `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` (impede backfill). Não são bugs do legado — são comportamentos que o legado tolera mas o Spark expõe.
- **`PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` é uma única procedure-monstro**: 4 temp tables encadeadas + UPDATE pos-INSERT + DELETE parcial por YEAR. Reescrita exige tradução semântica, não literal.
- **Dependência externa do usuário**: SCD2 lib é blocker absoluto. Se contrato for incompatível com Delta Lake, perde 1 sprint reescrevendo.
- **Zero testes no legado**: paridade só pode ser validada por amostragem.
- **Sem token de workspace Databricks**: deploy manual de notebooks é fonte de erro humano.
- **2 fontes Excel**: SCD2 + Excel é receita para inconsistência de schema.

#### Por que 6.0 e não 5.5 ou 6.5

- **Acima de 5.5** (consenso geral do projeto SSIS→Databricks): este escopo é mais focado, com Bronze pronta e decisões arquiteturais já tomadas. Não tem a complexidade de 142 procs ou a indefinição do consenso geral.
- **Abaixo de 7**: os 2 anti-padrões críticos (R1 e R2) são em pontos centrais do relatório, não periféricos. A `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` sozinha consome 1/3 do Sprint 5. A SCD2 lib é dependência externa fora do controle do executor.

### Recomendação

**GO** com as 8 pré-condições atendidas. Se a SCD2 lib não chegar até o Dia 2 da Pré-Sprint, **HOLD** até resolver.

---

## Próxima ação imediata

Após o usuário ler este documento, executar:

1. Confirmar entrega da SCD2 lib + data
2. Confirmar tie-breaker da regra "última versão aprovada" (R1)
3. Iniciar Pré-Sprint Dia 1 nos próximos 7 dias

**Confidence:** 0.88 (logo abaixo do threshold 0.90).
**Razão:** plano sólido baseado em contexto rico, mas a complexidade real de `PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL` só será conhecida após a leitura completa da procedure (lida só parcialmente), e o contrato exato da SCD2 lib é desconhecido. Recomenda-se revisão pelo `python_developer` antes de Sprint 5.
