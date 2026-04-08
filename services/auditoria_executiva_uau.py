# -*- coding: utf-8 -*-
"""
Resumo executivo de auditoria (somente diagnóstico / log).

Não altera DataFrames de saída nem Excel — uso interno e DEBUG.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

import pandas as pd


def gerar_resumo_auditoria_consolidado(
    df_consolidado: pd.DataFrame,
    df_alertas: Optional[pd.DataFrame] = None,
    scores_por_venda: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """
    Retorna métricas agregadas para diretoria / Power BI prep (sem efeitos colaterais).

    - total_vendas
    - vendas_com_alerta (distinct Venda em df_alertas)
    - pct_saudavel
    - ranking_piores_vendas (por contagem de alertas; desempate score mínimo se fornecido)
    - principais_tipos_erro (Tipo_Alerta mais frequentes)
    """
    out: Dict[str, Any] = {
        "total_vendas": 0,
        "vendas_com_inconsistencia": 0,
        "pct_saudavel": 100.0,
        "ranking_piores_vendas": [],
        "principais_tipos_erro": [],
    }
    if df_consolidado is None or df_consolidado.empty:
        return out
    if "Venda" not in df_consolidado.columns:
        return out

    vendas_u = df_consolidado["Venda"].fillna("").astype(str).str.strip()
    vendas_u = vendas_u[vendas_u != ""]
    total = int(vendas_u.nunique())
    out["total_vendas"] = total
    if total == 0:
        return out

    contagem_por_venda: Counter[str] = Counter()
    tipos: Counter[str] = Counter()

    if df_alertas is not None and not df_alertas.empty and "Venda" in df_alertas.columns:
        da = df_alertas.copy()
        da["Venda"] = da["Venda"].fillna("").astype(str).str.strip()
        da = da[da["Venda"] != ""]
        if "Tipo_Alerta" in da.columns:
            for _, row in da.iterrows():
                v = str(row["Venda"]).strip()
                contagem_por_venda[v] += 1
                ta = str(row.get("Tipo_Alerta", "") or "").strip()
                if ta:
                    tipos[ta] += 1
    com_alerta_n = len(contagem_por_venda)
    out["vendas_com_inconsistencia"] = com_alerta_n
    out["pct_saudavel"] = round(100.0 * (total - com_alerta_n) / float(total), 2) if total else 100.0
    out["principais_tipos_erro"] = [{"tipo": t, "qtd": n} for t, n in tipos.most_common(12)]

    ranking: List[Dict[str, Any]] = []
    for v, n in contagem_por_venda.most_common(25):
        sc = None
        if scores_por_venda and v in scores_por_venda:
            sc = float(scores_por_venda[v])
        ranking.append({"Venda": v, "qtd_alertas": int(n), "score_qualidade": sc})
    if scores_por_venda and ranking:
        ranking.sort(
            key=lambda x: (-x["qtd_alertas"], x["score_qualidade"] if x["score_qualidade"] is not None else 100.0),
        )
    out["ranking_piores_vendas"] = ranking[:15]
    return out


def formatar_resumo_auditoria_para_log(resumo: Dict[str, Any]) -> str:
    linhas = [
        "[AUDITORIA_EXECUTIVA] "
        f"total_vendas={resumo.get('total_vendas', 0)} "
        f"vendas_com_inconsistencia={resumo.get('vendas_com_inconsistencia', 0)} "
        f"pct_saudavel={resumo.get('pct_saudavel', 0)}%",
    ]
    tipos = resumo.get("principais_tipos_erro") or []
    if tipos:
        amostra = ", ".join(f"{t['tipo']}:{t['qtd']}" for t in tipos[:8])
        linhas.append(f"[AUDITORIA_EXECUTIVA] principais_tipos_erro: {amostra}")
    piores = resumo.get("ranking_piores_vendas") or []
    if piores:
        amostra_v = ", ".join(f"{p['Venda']}({p['qtd_alertas']})" for p in piores[:8])
        linhas.append(f"[AUDITORIA_EXECUTIVA] piores_vendas(amostra): {amostra_v}")
    return "\n".join(linhas)
