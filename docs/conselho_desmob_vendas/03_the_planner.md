# Conselho Desmobilização e Vendas — v2
## Agente: The Planner — Roadmap Estratégico
**Data:** 2026-04-29 | **Score: 7.0 / 10** *(revisado com novos dados)*

---

## 1. O que mudou desde a v1

| Item | v1 | v2 |
|---|---|---|
| TOP 1 VWOrcamentoDesmobilizacaoVenda | Blocker — aguardar validação com negócio | **RESOLVIDO** — regra confirmada, sprint desbloqueado |
| `ts_ingestao_brt` | Pré-condição pendente | **ELIMINADA** — confirmado em todas as Bronze |
| Baseline de row counts | Pré-condição pendente | **ENTREGUE** — 17 tabelas com valores exatos |
| Pré-condições | 8 itens (3 bloqueadores) | **2 itens restantes** |
| Score | 6.0/10 | **7.0/10** |

---

## 2. Inventário Completo de Objetos

### Gold — 17 Fatos + 2 Dimensões

| Tabela | Baseline (linhas) | Complexidade | Procedure |
|---|---|---|---|
| `FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB` | 84.227 | **Bloqueadora** | PROC_FT_ULTIMOS_REGISTROS |
| `FT_DESMOBILIZACAO_PA` | 42.732 | **Alta** | PROC_FT_DESMOBILIZACAO_PA (~1.050 linhas) |
| `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | 34.741 | **Alta** | PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL |
| `FT_ORCAMENTO_DESMOBILIZACAO_GERAL_ATUAL` | 9.664 | Média | PROC_FT_ORCAMENTO_DESMOBILIZACAO_GERAL_ATUAL |
| `FT_ORCAMENTO_DESMOBILIZACAO_HISTORICO` | 26.651 | Média | PROC_FT_ORCAMENTO_DESMOBILIZACAO_HISTORICO |
| `FT_VENDA_PA` | 41.756 | **Alta** | PROC_FT_VENDA_PA |
| `FT_ORCAMENTO_VENDA_GERAL` | 36.100 | Média | PROC_FT_ORCAMENTO_VENDA_GERAL |
| `FT_ORCAMENTO_VENDA_GERAL_ATUAL` | 9.658 | Baixa | PROC_FT_ORCAMENTO_VENDA_GERAL_ATUAL |
| `FT_ORCAMENTO_VENDA_HISTORICO` | 36.100 | Baixa | PROC_FT_ORCAMENTO_VENDA_HISTORICO |
| `FT_RADAR_DESMOBILIZACAO` | 22.417 | Média | PROC_FT_RADAR_DESMOBILIZACAO_E_HISTORICO |
| `FT_RADAR_DESMOBILIZACAO_HISTORICO` | 17.072 | Média | PROC_FT_RADAR_DESMOBILIZACAO_E_HISTORICO |
| `FT_RADAR_VENDA` | 26.056 | Média | PROC_FT_RADAR_VENDA |
| `FT_HISTORICO_DESMOB_PLATAFORMA_VENDA` | 63.651 | Média | PROC_FT_HISTORICO_DESMOB_PLATAFORMA_VENDA |
| `FT_HISTORICO_DE_VENDAS_PA` | 6.390 | Baixa | PROC_FT_HISTORICO_DE_VENDAS_PA |
| `FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA` | 42.892 | Média | PROC_FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA |
| `FT_PREPARACAO_REALIZADO_DESMOB_AVALIA_GAB` | — | Baixa | PROC_FT_PREPARACAO_REALIZADO_DESMOB |
| `FT_VISTORIA_REALIZADO_DESMOB_AVALIA_GAB` | — | Baixa | PROC_FT_VISTORIA_REALIZADO_DESMOB |
| `DM_SIMA_CIDADE_DESMOBILIZACAO` | 5.570 | Baixa | PROC_DM_SIMA_CIDADE_DESMOBILIZACAO |
| `DM_RLS_CONSULTOR_VENDAS_PA` | 24 | Baixa | PROC_DM_RLS_CONSULTOR_VENDAS_PA |

### Silver — Views Críticas

| View | Silver Destino | Complexidade |
|---|---|---|
| `VWOrcamentoDesmobilizacaoVenda` | `vw_orcamento_desmob_venda` | **Alta** — 3 UNION ALL, 10+ joins, TOP 1 resolvido |
| `VWRadarDesmobilizacaoVendaDinamico` | `vw_radar_desmob_venda` | **Alta** |
| `VWOrcamentoAnteriorDesmobilizacaoVenda` | `vw_orcamento_anterior_desmob` | Média |
| `VWRadarDesmobilizacaoVendaVersao` | `vw_radar_desmob_versao` | Média |

### Silver — Tabelas Simples (~25 tabelas STG)

Todas via notebook genérico + `saveDeltaAsSilver(on_colunm=['Id'], tsp_load_colunm='ts_ingestao_brt')`.

---

## 3. Pré-condições Restantes (apenas 2)

| # | Pré-condição | Responsável | Urgência |
|---|---|---|---|
| 1 | Definir `ano_corte` como job parameter (valor inicial: 2024) | Usuário / Arquiteto | Alta |
| 2 | Confirmar owner das tabelas Gold no Unity Catalog (`ALTER TABLE ... OWNER TO`) | Usuário / Admin | Média |

---

## 4. Plano de Sprints — Atualizado

### Pré-Sprint (1 dia — já 90% concluído)
- [x] Baseline de row counts disponível (`ativos.csv`)
- [x] Regra de negócio TOP 1 confirmada
- [x] `ts_ingestao_brt` confirmado em todas as Bronze
- [ ] Definir `ano_corte` como parâmetro do Workflow

### Sprint 1 — Silver (5 dias úteis)

| Dia | Entregável |
|---|---|
| 1 | `notebooks/utils/config.py` + `notebooks/utils/validation.py` com baseline |
| 1 | `notebooks/libs/carga_silver.py` (copiar de `legacy_data/`) |
| 2 | `notebooks/silver/00_generic_loader.py` + testar com 5 tabelas STG |
| 3 | Rodar genérico para todas as ~25 tabelas STG simples |
| 4 | `notebooks/silver/01_vw_orcamento_desmob_venda.py` (view crítica) |
| 5 | `notebooks/silver/02_vw_radar_desmob_venda.py` + testes de paridade Silver |

**Critério de saída:** Silver completo com row count igual ao SQL Server

### Sprint 2 — Gold Dimensões + Auxiliar (3 dias úteis)

| Dia | Entregável |
|---|---|
| 1 | `gold/11_dm_sima_cidade_desmob.py` + `gold/12_dm_rls_consultor_vendas.py` |
| 2 | `gold/00_ft_ultimos_registros_crs.py` (task bloqueadora — 84.227 linhas) |
| 3 | Configurar Workflow JSON com DAG + testar tasks 1-4 end-to-end |

### Sprint 3 — Gold Fatos Alta Complexidade (7 dias úteis)

| Dia | Entregável |
|---|---|
| 1–2 | `gold/02_ft_orcamento_desmob_geral.py` (4 temp tables + replaceWhere + MERGE) |
| 3 | `gold/03_ft_orcamento_desmob_geral_atual.py` + `04_ft_orcamento_desmob_historico.py` |
| 4–5 | `gold/01_ft_desmobilizacao_pa.py` (8 UPDATEs consolidados — maior risco) |
| 6 | `gold/03_ft_venda_pa.py` (histórico + atual) |
| 7 | Paridade: assert automático para todas as 6 tabelas Gold deste sprint |

### Sprint 4 — Gold Fatos Médios (5 dias úteis)

| Dia | Entregável |
|---|---|
| 1 | `gold/04_ft_orcamento_venda_*.py` (3 notebooks) |
| 2 | `gold/05_ft_radar_desmobilizacao.py` + `06_ft_radar_venda.py` |
| 3 | `gold/07_ft_historico_desmob_plataforma_venda.py` |
| 4 | `gold/08_ft_historico_de_vendas_pa.py` + `09_ft_historico_sla_desmob.py` |
| 5 | Testes de paridade para todas as tabelas deste sprint |

### Sprint 5 — Cutover (5 dias úteis)

| Dia | Entregável |
|---|---|
| 1–5 | SQL Server + Databricks rodando em paralelo — diff diário automatizado |
| 5 | Go/No-Go decision + descomissionar SSIS |

**Total: ~21 dias úteis (~1 mês) com 1 engenheiro**

---

## 5. Matriz de Riscos Atualizada

| Risco | Prob. | Impacto | Status | Mitigação |
|---|---|---|---|---|
| TOP 1 não-determinístico | ~~Alta~~ | ~~Crítico~~ | **ELIMINADO** | Regra confirmada com negócio |
| `ts_ingestao_brt` ausente | ~~Média~~ | ~~Alto~~ | **ELIMINADO** | Confirmado em todas as Bronze |
| GETDATE() em DataDesmobilizacao | Alta | Alto | **ATIVO** | Parametrizar `execution_date` |
| DELETE + INSERT não atômico | Alta | Alto | **ATIVO** | Usar `replaceWhere` |
| Gold→Gold sem DAG explícito | Alta | Alto | **ATIVO** | `depends_on` no Workflow JSON |
| 8 UPDATEs sequenciais FT_DESMOB_PA | Média | Médio | **ATIVO** | Consolidar em 1 write |
| Schema drift Bronze→Silver | Baixa | Médio | Latente | `createIfNotExists=True` na lib |
| Divergência histórico (EhHistorico=1) | Baixa | Alto | Latente | Testar separadamente: `WHERE EhHistorico = 1` |

---

## 6. Score: 7.0 / 10

| Critério | Nota | Justificativa |
|---|---|---|
| Escopo delimitado | 9/10 | 19 objetos Gold + 4 views Silver + 25 STG com baseline exato |
| Bronze pronta | 10/10 | Não há trabalho de ingestão |
| Pré-condições eliminadas | 9/10 | De 8 para 2 pré-condições restantes |
| Plano de sprints | 8/10 | 21 dias úteis, critério de saída por sprint |
| Riscos residuais | 6/10 | 4 riscos ativos com mitigação definida |
| Restrições respeitadas | 9/10 | Workflows + Notebooks + lib — tudo mapeado |
