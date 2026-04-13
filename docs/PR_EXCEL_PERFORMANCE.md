# PR: Performance do motor Excel (pós-formatação openpyxl)

## Objetivo

Reduzir o tempo de geração do workbook final **sem alterar** regras de negócio, fórmulas financeiras, estrutura crítica das abas nem a usabilidade esperada do arquivo.

## Escopo

| Área | Alteração |
|------|-----------|
| `services/processador_uau.py` | Heartbeat temporal (`_notify_timed`); Consolidado com `iter_rows` + borda de fechamento no mesmo passe; turbo Consolidado; turbo DADOS GERAL; formatação por coluna em DADOS RECEBER/RECEBIDOS; modo leve PENDÊNCIAS; `_autoajustar_colunas_e_linhas(..., modo_rapido=True)` |
| `scripts/audit_scpgo_paridade_financeira.py` | Verificação objetiva de paridade financeira + merges/autofilter/freeze na aba **Consolidado SCPGO** |
| `scripts/run_benchmark_scpgo_once.py` | Utilitário reproduzível de benchmark (baseline via worktree ou otimizado na raiz) |

### Thresholds centralizados (rollback operacional)

Em `services/processador_uau.py`, após o marcador `# ESTILO`:

- `LIMIAR_LINHAS_TURBO_CONSOLIDADO` = **25000**
- `LIMIAR_LINHAS_TURBO_RELATORIO_ANALITICO` = **30000**
- `LIMIAR_LINHAS_TURBO_PENDENCIAS` = **12000**

**Plano de rollback:** em incidente visual ou regressão percebida, **aumentar** esses valores (ex.: `9_999_999`) desativa os modos turbo sem reverter o restante do PR.

## Evidências (dataset SCPGO — mesmos arquivos em `uploads/`)

### Tabela before / after (segundos)

| Métrica | Baseline (HEAD) | Otimizado | Ganho |
|---------|-----------------|-----------|-------|
| **tempo total** | 6472.14 | 469.20 | **92,75%** |
| `montar_consolidado_total` | 213.87 | 149.43 | 30,13% |
| `_validar_pre_exportacao` | 158.31 | 117.01 | 26,09% |
| `excel_escrever_openpyxl` | 77.14 | 54.76 | 28,93% |
| **`excel_pos_formatacao_openpyxl`** | **5896.73** | **66.95** | **98,86%** |

**Requisito ≥ 40% em `excel_pos_formatacao_openpyxl`:** **atendido** (~98,86%).

### Paridade financeira (aba `SCPGO – Consolidado`)

| Campo | Delta baseline → otimizado |
|-------|------------------------------|
| Vl.Carteira | 0,00 |
| Vl.Pago | 0,00 |
| Vl.Vencer | 0,00 |
| Vl.Principal (Encargos) | 0,00 |
| Qtd.Parc.Atrasada | 0,00 |

Comando:

```text
venv\Scripts\python.exe scripts\audit_scpgo_paridade_financeira.py ^
  outputs\_audit_perf\run_baseline_wt\audit_baseline_wt_scpgo.xlsx ^
  outputs\_audit_perf\run_optimized_main\audit_optimized_main_scpgo.xlsx
```

### Estrutura crítica (Consolidado)

- Merges linha 7: `A7:G7` … `Y7:AA7` — **preservado**
- `auto_filter`: início **A8** (`A8:AA1348`) — **preservado**
- `freeze_panes`: **A9** — **preservado**

## Checklist de aceitação

- [x] Sem regressão financeira (paridade consolidado)
- [x] Sem regressão estrutural crítica (merges / autofilter / freeze)
- [x] Ganho de performance comprovado (`excel_pos` ≥ 40%)
- [x] `python -m py_compile services/processador_uau.py` OK

## Nota sobre `auditar_carteiras_geral_final.py`

O script atual assume o perfil **CARTEIRAS GERAL** (cores e layout de todas as abas como o consolidado). Workbooks **single-empreendimento (SCPGO)** geram falsos negativos. **Fora deste PR:** parametrizar por perfil (`carteiras_geral` | `empreendimento_unico`).

## Como reproduzir o benchmark

1. Baseline (código em commit HEAD, sem tocar no `processador_uau.py` local):

   ```text
   git worktree add _wt_baseline HEAD
   venv\Scripts\python.exe -u scripts\run_benchmark_scpgo_once.py _wt_baseline baseline_wt
   ```

2. Otimizado (raiz do repo com branch deste PR):

   ```text
   venv\Scripts\python.exe -u scripts\run_benchmark_scpgo_once.py . optimized_main
   ```

Logs `[TEMPO]` aparecem no stdout; `summary.txt` e o `.xlsx` ficam em `outputs/_audit_perf/run_<label>/`.
