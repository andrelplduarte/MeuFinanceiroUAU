# -*- coding: utf-8 -*-
"""Regenera outputs/CARTEIRAS GERAL.xlsx com todos os rec_*.txt / reb_*.txt em uploads/."""
from __future__ import annotations

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from services.orquestrador_lote_uau import processar_entrada_simples_ou_lote  # noqa: E402


def main() -> int:
    uploads = RAIZ / "uploads"
    rec = sorted(str(p) for p in uploads.glob("rec_*.txt"))
    reb = sorted(str(p) for p in uploads.glob("reb_*.txt"))
    if not rec or not reb:
        print("ERRO: faltam rec_*.txt ou reb_*.txt")
        return 2
    # Mesmo contrato do app: arquivo placeholder dentro de outputs/ (dirname → pasta de saída).
    base = str(RAIZ / "outputs" / "consolidacao_uau.xlsx")
    out, t = processar_entrada_simples_ou_lote(
        rec, reb, base, "POR_EMPREENDIMENTO", caminhos_estoque=None
    )
    print("OK", out, "tempo", round(t, 2), "s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
