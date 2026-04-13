#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Benchmark reproduzível: uma execução completa `processar_e_gerar_excel` no dataset SCPGO.

Uso:
  python -u scripts/run_benchmark_scpgo_once.py <sys_path_root> <label>

- sys_path_root: raiz do repo com `services/` (código a medir), ou worktree `_wt_baseline`
  para baseline em commit HEAD sem alterar o working tree principal.
- label: sufixo da pasta em outputs/_audit_perf/run_<label>/

Requer em <repo>/uploads/:
  rec_00_SCPGO_-LOT.SCP_RESIDENCIAL_GOIANIA_-_RECEBER.txt
  reb_00_SCPGO_-LOT.SCP_RESIDENCIAL_GOIANIA_-_RECEBIDOS.txt
  est_00_ESTOQUE_ATUALIZADO.txt

Saída: XLSX em outputs/_audit_perf/run_<label>/, summary.txt e linhas [TEMPO] no stdout.
"""
from __future__ import annotations

import os
import sys
import time


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Uso: python -u scripts/run_benchmark_scpgo_once.py <sys_path_root> <label>",
            file=sys.stderr,
        )
        return 2

    root = os.path.abspath(sys.argv[1])
    label = sys.argv[2]
    sys.path.insert(0, root)
    os.chdir(root)

    base = os.path.basename(root.rstrip(os.sep))
    repo = os.path.dirname(root) if base == "_wt_baseline" else root

    rec = os.path.join(
        repo,
        "uploads",
        "rec_00_SCPGO_-LOT.SCP_RESIDENCIAL_GOIANIA_-_RECEBER.txt",
    )
    reb = os.path.join(
        repo,
        "uploads",
        "reb_00_SCPGO_-LOT.SCP_RESIDENCIAL_GOIANIA_-_RECEBIDOS.txt",
    )
    est = os.path.join(repo, "uploads", "est_00_ESTOQUE_ATUALIZADO.txt")
    aud = os.path.join(repo, "outputs", "_audit_perf")
    out_dir = os.path.join(aud, f"run_{label}")
    os.makedirs(out_dir, exist_ok=True)
    placeholder = os.path.join(out_dir, "_ph.xlsx")
    override = f"audit_{label}_scpgo.xlsx"

    from services.processador_uau import processar_e_gerar_excel

    t0 = time.perf_counter()
    path, tt = processar_e_gerar_excel(
        rec,
        reb,
        placeholder,
        caminho_estoque=est,
        gerar_aba_consolidado_estoque=True,
        nome_arquivo_xlsx_override=override,
    )
    wall = time.perf_counter() - t0
    log = os.path.join(out_dir, "summary.txt")
    with open(log, "w", encoding="utf-8") as f:
        f.write(f"path={path}\n")
        f.write(f"tempo_total={tt}\n")
        f.write(f"wall={wall}\n")
    print("__PATH_FINAL__", path, flush=True)
    print("__TEMPO_TOTAL__", tt, flush=True)
    print("__WALL__", wall, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
