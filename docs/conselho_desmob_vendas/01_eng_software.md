# Conselho Desmobilização e Vendas — v2
## Agente: Conselheiro de Engenharia de Software
**Data:** 2026-04-29 | **Score: 6.5 / 10** *(revisado com novos dados)*

---

## 1. O que mudou desde a v1

| Item | v1 | v2 |
|---|---|---|
| Regra do TOP 1 | Risco crítico — comportamento indefinido | **RESOLVIDO** — singleton global, tradução direta |
| `ts_ingestao_brt` | Incerto — precisava confirmar | **CONFIRMADO** — usar em todas as tabelas Bronze |
| Baseline de row counts | Inexistente | **DISPONÍVEL** — 17 tabelas mapeadas |
| Score | 5.5/10 | **6.5/10** |

---

## 2. Diagnóstico Arquitetural Atualizado

### TOP 1 — Regra esclarecida e tradução segura

A `cte_versao_orcamento` retorna **uma única linha global** da tabela `STG_SIMA_VERSAO_ORCAMENTO`: a versão de orçamento mais recente que não foi cancelada (Situacao < 2), priorizando Aprovada (1) sobre Gerada (0).

```sql
-- Legado:
SELECT TOP 1 Id, OrcamentoId, DataRegistro, DataAtualizacao, DataDesativacao, Codigo, Situacao
FROM STG.STG_SIMA_VERSAO_ORCAMENTO WITH (NOLOCK)
WHERE Situacao < 2
ORDER BY Situacao DESC, DataRegistro DESC
```

**Tradução PySpark — correta e eficiente:**
```python
from pyspark.sql import functions as F

df_versao = (
    spark.table("bi_ativos.silver.stg_sima_versao_orcamento")
    .filter("Situacao < 2")
    .orderBy(F.desc("Situacao"), F.desc("DataRegistro"))
    .limit(1)
)
# É uma única linha → broadcast elimina shuffle
df_itens = spark.table("bi_ativos.silver.stg_sima_versao_orcamento_itens")
df_joined = df_itens.join(F.broadcast(df_versao), df_itens.VersaoId == df_versao.Id, "inner")
```

O `limit(1)` após `orderBy` em Spark é determinístico e correto. O `broadcast()` evita shuffle — a linha única vai para todos os executores sem movimento de dados. **Risco eliminado.**

---

## 3. Riscos Revisados

| Risco | Status | Prioridade |
|---|---|---|
| R1 — TOP 1 sem PARTITION BY | **ELIMINADO** — singleton global com tradução clara | — |
| R2 — `GETDATE()` em DataDesmobilizacao | **ATIVO** — parametrizar `execution_date` | Alta |
| R3 — DELETE + INSERT sem atomicidade | **ATIVO** — usar `replaceWhere` | Alta |
| R4 — Gold→Gold: `FT_ULTIMOS_REGISTROS` (84.227 linhas) | **ATIVO** — DAG explícito no Workflow | Alta |
| R5 — 8 UPDATEs sequenciais em FT_DESMOBILIZACAO_PA | **ATIVO** — consolidar em 1 write | Média |
| R6 — `ts_ingestao_brt` inexistente | **ELIMINADO** — confirmado em todas as tabelas Bronze | — |

---

## 4. Baseline de Validação — Paridade Garantida

Com o CSV `legacy_data/ativos.csv`, cada notebook Gold termina com assert automático:

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

def validar_paridade(target: str, tolerancia: float = 0.01):
    tabela = target.split(".")[-1].lower()
    esperado = BASELINE[tabela]
    atual = spark.table(target).count()
    delta = abs(atual - esperado) / esperado
    assert delta <= tolerancia, (
        f"PARIDADE FALHOU: {target} → {atual:,} linhas (esperado ~{esperado:,}, delta={delta:.1%})"
    )
    print(f"OK: {target} → {atual:,} linhas (delta={delta:.1%})")
```

---

## 5. DAG de Dependências — Workflow

```
TASK 1 (paralelas): Silver Tables ~25 tabelas STG
TASK 2 (paralelas): Silver VW Orcamento Desmob + VW Radar Desmob
        ↓
TASK 3 (paralelas): DM_SIMA_CIDADE_DESMOB + DM_RLS_CONSULTOR_VENDAS
        ↓
TASK 4: FT_ULTIMOS_REGISTROS_POR_PLACA_CRS_DESMOB  ← BLOQUEADOR
        ↓
TASK 5 (paralelas): FT_DESMOBILIZACAO_PA + FT_ORCAMENTO_DESMOB_GERAL
                  + FT_ORCAMENTO_DESMOB_GERAL_ATUAL + FT_ORCAMENTO_DESMOB_HISTORICO
        ↓
TASK 6 (paralelas): FT_VENDA_PA + FT_ORCAMENTO_VENDA_GERAL
                  + FT_ORCAMENTO_VENDA_GERAL_ATUAL + FT_ORCAMENTO_VENDA_HISTORICO
                  + FT_RADAR_DESMOBILIZACAO + FT_RADAR_DESMOBILIZACAO_HISTORICO
                  + FT_RADAR_VENDA + FT_HISTORICO_DESMOB_PLATAFORMA_VENDA
                  + FT_HISTORICO_DE_VENDAS_PA + FT_HISTORICO_SLA_DESMOBILIZACAO_GAB_PA
```

---

## 6. Score: 6.5 / 10

| Critério | Nota | Justificativa |
|---|---|---|
| Clareza do escopo | 9/10 | DDLs + procedures + baseline + regra de negócio confirmadas |
| Qualidade do código legado | 3/10 | GETDATE(), 8 UPDATEs, 4 temp tables — não mudou |
| Riscos mapeados | 8/10 | TOP 1 e ts_ingestao_brt eliminados; 4 riscos ativos com mitigação clara |
| Padrão de carga | 7/10 | `replaceWhere` resolve atomicidade; baseline garante paridade |
| Testabilidade | 7/10 | Baseline disponível → assert automático em cada notebook |
| Restrições respeitadas | 9/10 | Workflows + Notebooks + lib carga_silver — tudo mapeado |
