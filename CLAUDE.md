# Projeto: Migração SSIS → Databricks (bi_ativos)

## Ação obrigatória ao abrir a sessão

Ao iniciar qualquer conversa neste projeto, execute imediatamente:

1. Leia todos os arquivos em `docs/conselho_migracao/` (00 a 04)
2. Exiba o painel de status abaixo atualizado
3. Pergunte ao usuário onde quer continuar

---

## Contexto do Projeto

**Objetivo:** Migrar pipeline de dados legado (SSIS + SQL Server) para Databricks usando arquitetura Medallion (Bronze → Silver → Gold).

**Repositório do legado:** `legacy_data/Projeto_ativos.zip` (1.087 arquivos SQL)

**Conteúdo do ZIP extraído em `/tmp/ssis_project/`:**
- `ODS/` — 87 DDLs de tabelas (= contrato Bronze)
- `STG/Tables/` — 210 DDLs de tabelas Silver (espelhos das ODS)
- `STG/Views/` — 9 views com lógica de negócio Silver
- `DW/Procedures/` — 142 stored procedures Gold (dimensões + fatos)

**Unity Catalog:** `bi_ativos.bronze` (já existe) · `bi_ativos.silver` (a criar) · `bi_ativos.gold` (a criar)

---

## Estado Atual do Projeto

### Conselho de Agentes — Concluído
Documentos em `docs/conselho_migracao/`:

| Arquivo | Agente | Score |
|---|---|---|
| `01_conselheiro_eng_software.md` | Eng. Software / Arquitetura Distribuída | 5.0/10 |
| `02_ai_data_engineer.md` | AI Data Engineer / Medallion + DLT | 6.5/10 |
| `03_the_planner.md` | The Planner / Roadmap Estratégico | 5.5/10 |
| `04_python_developer.md` | Python Developer / PySpark + Código | 5.0/10 |
| `00_consenso_final.md` | **Consenso Geral** | **5.5/10** |

### Fases do Projeto

| Fase | Status | Semanas |
|---|---|---|
| Sprint Zero — Inventário de dependências | **PENDENTE** | 1 |
| Silver — 210 tabelas simples + 9 views | **PENDENTE** | 3 |
| Gold Dimensões — ~20 procedures DM_* | **PENDENTE** | 3 |
| Gold Fatos — ~122 procedures FT_* | **PENDENTE** | 5 |
| Cutover e descomissionamento SSIS | **PENDENTE** | 2 |

---

## Próxima Ação Recomendada (Sprint Zero)

Implementar dois scripts Python antes de qualquer notebook:

**1. Scanner de dependências Gold→Gold**
```python
import re, pathlib
deps = {}
for f in pathlib.Path("/tmp/ssis_project/DW/Procedures").glob("*.sql"):
    sql = f.read_text()
    reads  = re.findall(r'\[STG\]\.\[(\w+)\]', sql)
    reads += re.findall(r'\[DW\]\.\[(\w+)\]', sql)  # Gold lendo Gold!
    writes = re.findall(r'INTO \[DW\]\.\[(\w+)\]', sql)
    deps[f.stem] = {"reads": reads, "writes": writes}
```

**2. Scanner de anti-padrões SQL**
```python
ANTIPATTERNS = ["IIF(", "GETDATE()", "TOP 1", "WITH (NOLOCK)", "UNIQUEIDENTIFIER"]
for f in pathlib.Path("/tmp/ssis_project").rglob("*.sql"):
    sql = f.read_text()
    hits = [p for p in ANTIPATTERNS if p.upper() in sql.upper()]
    if hits:
        print(f"{f.name}: {hits}")
```

---

## Decisões Técnicas Já Tomadas

| Decisão | Escolha |
|---|---|
| Ingestion Bronze | Auto Loader / COPY INTO (já operacional) |
| Silver simples | Notebook PySpark parametrizado genérico |
| Silver views | Notebook PySpark individual por view |
| Gold dimensões | Notebook overwrite + preservar linha NA |
| Gold fatos | Notebook overwrite com parâmetro `execution_date` |
| Orquestração | Databricks Workflows (substitui SSIS) |
| Qualidade | DLT `@expect_or_fail` + pytest |
| Nomenclatura | `bi_ativos.{bronze,silver,gold}.nome_tabela` |

## Riscos Críticos Identificados

1. **Gold→Gold dependencies ocultas** — procedures FT_* que lêem tabelas DW de outras procedures
2. **`TOP 1 ORDER BY` sem PARTITION BY** — não-determinístico em Spark (identificado em `VWOrcamentoDesmobilizacaoVenda`)
3. **`GETDATE()` em procedures de fato** — impede backfill e re-execução
4. **GUIDs hardcoded** — linha "Não Aplicável" de cada dimensão (ex: `CD9B0202-E70B-449B-ABD1-134D7D686275`)
5. **Zero testes no legado** — nenhuma validação de paridade

## Mapeamento de Tipos T-SQL → Delta Lake

| T-SQL | Delta/Spark |
|---|---|
| `UNIQUEIDENTIFIER` | `STRING` |
| `DATETIME2(7)` | `TIMESTAMP` |
| `NVARCHAR(MAX)` | `STRING` |
| `NUMERIC(18,2)` | `DECIMAL(18,2)` |
| `BIT` | `BOOLEAN` |
| `IIF(c,a,b)` | `CASE WHEN c THEN a ELSE b END` |
| `GETDATE()` | parâmetro `execution_date` |
| `TOP 1 ORDER BY x` | `ROW_NUMBER() OVER (ORDER BY x) = 1` |
| `WITH (NOLOCK)` | remover (Delta usa snapshot isolation) |
| `#temp_table` | `createOrReplaceTempView()` |
| `ISNULL(x,y)` | `COALESCE(x,y)` |
