# -*- coding: utf-8 -*-
"""Valida equivalência entre modo por empreendimento e modo arquivos gerais."""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from services.orquestrador_lote_uau import processar_entrada_simples_ou_lote  # noqa: E402


def _listar_por_prefixo(pasta: Path, prefixo: str) -> list[str]:
    return sorted(str(p) for p in pasta.glob(f"{prefixo}_*.txt") if p.is_file())


def _somas_consolidado(path_xlsx: Path) -> dict:
    if not path_xlsx.is_file():
        return {}
    xl = pd.ExcelFile(path_xlsx, engine="openpyxl")
    abas = [x for x in xl.sheet_names if x != "RESUMO GERAL"]
    tot = {
        "Vl.Carteira": 0.0,
        "Vl.Pago": 0.0,
        "Vl.Vencer": 0.0,
        "Vl.Principal (Encargos)": 0.0,
        "Qtd.Parc.Atrasada": 0.0,
    }
    for aba in abas:
        df = pd.read_excel(path_xlsx, sheet_name=aba, header=7, engine="openpyxl")
        for col in list(tot.keys()):
            if col in df.columns:
                tot[col] += float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    return {k: round(v, 2) for k, v in tot.items()}


def main() -> int:
    uploads = BASE / "uploads"
    rec = _listar_por_prefixo(uploads, "rec")
    reb = _listar_por_prefixo(uploads, "reb")
    if not rec or not reb:
        print(
            json.dumps(
                {
                    "ok": False,
                    "motivo": "Arquivos rec_*.txt / reb_*.txt não encontrados em uploads/ para validação comparativa."
                },
                ensure_ascii=False,
            )
        )
        return 2

    with tempfile.TemporaryDirectory(prefix="uau_valid_gerais_") as tmp:
        tmp_path = Path(tmp)
        geral_rec = tmp_path / "GERAL_RECEBER.txt"
        geral_reb = tmp_path / "GERAL_RECEBIDOS.txt"

        geral_rec.write_text("\n".join(Path(p).read_text(encoding="utf-8", errors="ignore") for p in rec), encoding="utf-8")
        geral_reb.write_text("\n".join(Path(p).read_text(encoding="utf-8", errors="ignore") for p in reb), encoding="utf-8")

        out_sep = tmp_path / "out_sep"
        out_ger = tmp_path / "out_ger"
        out_sep.mkdir(parents=True, exist_ok=True)
        out_ger.mkdir(parents=True, exist_ok=True)

        saida_sep, tempo_sep = processar_entrada_simples_ou_lote(
            rec, reb, str(out_sep), "POR_EMPREENDIMENTO", caminhos_estoque=None
        )
        saida_ger, tempo_ger = processar_entrada_simples_ou_lote(
            [str(geral_rec)], [str(geral_reb)], str(out_ger), "ARQUIVOS_GERAIS", caminhos_estoque=None
        )

        x_sep = Path(saida_sep[0] if isinstance(saida_sep, tuple) else saida_sep)
        x_ger = Path(saida_ger[0] if isinstance(saida_ger, tuple) else saida_ger)
        s_sep = _somas_consolidado(x_sep)
        s_ger = _somas_consolidado(x_ger)
        diff = {k: round(float(s_ger.get(k, 0)) - float(s_sep.get(k, 0)), 2) for k in sorted(set(s_sep) | set(s_ger))}

        print(
            json.dumps(
                {
                    "ok": True,
                    "tempo_sep_s": round(float(tempo_sep), 2),
                    "tempo_gerais_s": round(float(tempo_ger), 2),
                    "somas_por_empreendimento": s_sep,
                    "somas_arquivos_gerais": s_ger,
                    "diff_gerais_menos_sep": diff,
                    "arquivo_sep": str(x_sep),
                    "arquivo_gerais": str(x_ger),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
