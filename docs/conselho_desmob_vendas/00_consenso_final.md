# Conselho Desmobilização e Vendas — v2
## Consenso Final — Síntese dos 4 Agentes
**Data:** 2026-04-29

---

## Scores Individuais

| Agente | Especialidade | Score v1 | Score v2 |
|---|---|---|---|
| Conselheiro Eng. Software | Arquitetura, DAG, riscos operacionais | 5.5/10 | **6.5/10** |
| AI Data Engineer | Medallion design, Workflows, Delta Lake | 6.0/10 | **7.0/10** |
| The Planner | Roadmap, sprints, inventário | 6.0/10 | **7.0/10** |
| Python Developer | PySpark, tradução T-SQL, templates | 4.5/10 | **5.5/10** |

### Score de Migração: **6.5 / 10** *(era 5.5 na v1)*

---

## Score de Qualidade da Entrega: **8.0 / 10**

*O que a solução vai entregar quando estiver pronta:*

| Critério | Nota | Justificativa |
|---|---|---|
| Atomicidade dos dados | 9/10 | `replaceWhere` elimina janela de inconsistência DELETE+INSERT |
| Paridade com SQL Server | 9/10 | Baseline de 17 tabelas + assert automático em cada notebook |
| Corretude do TOP 1 | 9/10 | Regra validada com negócio, `broadcast()` garante eficiência |
| Rastreabilidade temporal | 8/10 | `execution_date` parametrizado, `ts_ingestao_brt` em todas as Silver |
| Dependências explícitas | 8/10 | DAG com `depends_on` no Workflow JSON — nada implícito |
| Performance | 7/10 | 8 UPDATEs consolidados em 1 write; broadcast no TOP 1 |
| Testabilidade | 8/10 | assert de paridade + tolerância 1% por tabela |
| Manutenibilidade | 7/10 | `ano_corte` e `execution_date` como parâmetros; sem hardcode |
| **Média** | **8.0/10** | |

> Uma entrega com essas características é **sólida para produção**. O que impede nota 9+ é a ausência de testes unitários das transformações individuais (apenas paridade de row count) e o fato de que `FT_DESMOBILIZACAO_PA` ainda é tradução manual de ~1.050 linhas.

---

## Score de Dificuldade de Implementação: **7.5 / 10**

*Quão difícil é fazer essa migração:*

| Componente | Dificuldade | Motivo |
|---|---|---|
| Silver genérica (~25 tabelas) | 2/10 | Notebook genérico parametrizado; 1 linha por tabela |
| `VWOrcamentoDesmobilizacaoVenda` | 8/10 | 3 UNION ALL, 10+ joins, lógica de negócio Ciclo1/Ciclo2 |
| `FT_ORCAMENTO_DESMOBILIZACAO_GERAL` | 8/10 | 4 temp tables encadeadas + replaceWhere + MERGE pós-escrita |
| `FT_DESMOBILIZACAO_PA` | 9/10 | ~1.050 linhas, 8 UPDATEs sequenciais, 15+ joins, histórico+atual |
| `FT_VENDA_PA` | 7/10 | Histórico + atual, múltiplos LEFT JOINs |
| Gold fatos simples (8 tabelas) | 4/10 | Overwrite total, CTEs diretas |
| Gold dimensões (2 tabelas) | 3/10 | DELETE seletivo + INSERT simples |
| Configuração do Workflow DAG | 5/10 | JSON com `depends_on`, parametrização e alertas |
| **Média ponderada** | **7.5/10** | Puxado para cima por FT_DESMOBILIZACAO_PA e VW Orcamento |

> A dificuldade é **alta, mas bem-definida**. Não há partes desconhecidas — todos os padrões têm solução mapeada. O que consome tempo é a densidade do legado, não a incerteza.

---

## Riscos Eliminados vs. Ativos

### Eliminados (não bloqueiam mais)
| Risco | Como foi resolvido |
|---|---|
| TOP 1 não-determinístico | Regra de negócio confirmada → `limit(1)` + `broadcast()` |
| `ts_ingestao_brt` ausente | Confirmado em todas as tabelas Bronze |
| Baseline inexistente | `ativos.csv` com 17 tabelas mapeadas → `validation.py` pronto |

### Ativos (precisam de atenção durante implementação)
| Risco | Mitigação |
|---|---|
| `GETDATE()` em DataDesmobilizacao | Parametrizar `execution_date` em todos os notebooks Gold |
| DELETE + INSERT não atômico | Usar `replaceWhere` — nunca DELETE separado + INSERT |
| Gold→Gold sem DAG explícito | `depends_on: gold_ft_ultimos_registros` no Workflow JSON |
| 8 UPDATEs em FT_DESMOBILIZACAO_PA | Consolidar todos os lookups em 1 DataFrame antes do write |

---

## Mapeamento Final da lib `carga_silver.py`

| Tipo de tabela | Método | Exemplo |
|---|---|---|
| Bronze com `Id` + `ts_ingestao_brt` | `saveDeltaAsSilver(on_colunm=['Id'], tsp_load_colunm='ts_ingestao_brt')` | ~20 tabelas STG_SIMA_* |
| Bronze com PK composta + `ts_ingestao_brt` | `saveDeltaAsSilver(on_colunm=['col1','col2'], tsp_load_colunm='ts_ingestao_brt')` | STG_FIPE_ATUAL |
| Bronze sem PK + `ts_ingestao_brt` | `saveLastLoadAsSilver(tsp_load_colunm='ts_ingestao_brt')` | Excel históricos |
| Views SAP / calculadas sem timestamp | `saveAsSilverOverwrite` | STG_SAP_ZFIAA001, VW* |

---

## Próximos Passos — Em Ordem

1. **Agora (30 min):** Definir `ano_corte=2024` como parâmetro padrão do Workflow
2. **Agora (30 min):** Criar pasta `notebooks/` e copiar `carga_silver.py` de `legacy_data/`
3. **Dia 1:** Criar `utils/config.py` + `utils/validation.py` com baseline do `ativos.csv`
4. **Dia 1:** Notebook genérico Silver + testar com 3 tabelas STG simples
5. **Dias 2-5:** Silver completo — 25 tabelas + 4 views
6. **Semana 2:** Gold dimensões + `FT_ULTIMOS_REGISTROS` (task bloqueadora)
7. **Semanas 3-4:** Gold fatos em ordem de DAG
8. **Semana 5:** Cutover paralelo

**Total estimado: 21 dias úteis com 1 engenheiro.**

---

## Veredito Final

O projeto saiu de **"migrável com riscos altos"** (v1, score 5.5) para **"pronto para iniciar"** (v2, score 6.5). Os três bloqueadores mais relevantes foram eliminados. O que resta são desafios de implementação — com solução conhecida, não incertezas.

**Qualidade da entrega esperada: 8.0/10 — sólida para produção.**
**Dificuldade de implementação: 7.5/10 — alta, mas bem-definida e sem surpresas.**

---

*Análises individuais em: `01_eng_software.md`, `02_ai_data_engineer.md`, `03_the_planner.md`, `04_python_developer.md`*
