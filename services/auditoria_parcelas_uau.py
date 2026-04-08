# -*- coding: utf-8 -*-
"""
Diagnósticos estruturais de parcelas por venda (não bloqueantes).

Alimenta alertas do consolidado para auditoria; não altera valores financeiros.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List

import pandas as pd


def _norm_parcela_local(parcela: str) -> str:
    """Espelho mínimo de normalizar_parcela (evita import circular no load do módulo)."""
    import re

    s = str(parcela or "").strip().upper()
    if not s:
        return ""
    s = s.replace("\\", "/").replace("-", "/")
    s = re.sub(r"\b(PARCELA|PARCELAS|PARC|PCL|P)\b", "", s)
    s = re.sub(r"\s+", "", s)
    m = re.search(r"(\d{1,4})/(\d{1,4})", s)
    if not m:
        return ""
    atual, total = int(m.group(1)), int(m.group(2))
    if atual <= 0 or total <= 0:
        return ""
    return f"{atual}/{total}"


def _alerta(
    venda: str,
    tipo: str,
    mensagem: str,
    valor_esperado: str = "",
    valor_encontrado: str = "",
    regra: str = "",
) -> Dict[str, Any]:
    return {
        "Venda": str(venda).strip(),
        "Cliente_Base": "",
        "Tipo_Alerta": tipo,
        "Mensagem": mensagem,
        "Valor_Esperado": str(valor_esperado),
        "Valor_Encontrado": str(valor_encontrado),
        "Regra": regra or "Auditoria estrutural por Venda",
        "Observacao": "Nao bloqueante",
    }


def auditoria_sequencia_parcelas_receber(df_receber: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Por venda: ordem, gaps, duplicidade de Parc_Num e sobreposição de status na mesma parcela.
    """
    alertas: List[Dict[str, Any]] = []
    alertas.extend(_auditoria_ordem_e_gaps(df_receber))
    alertas.extend(_auditoria_duplicadas_e_sobrepostas(df_receber))
    return alertas


def _auditoria_ordem_e_gaps(df_receber: pd.DataFrame) -> List[Dict[str, Any]]:
    if df_receber is None or df_receber.empty:
        return []
    if "Venda" not in df_receber.columns or "Parc_Num" not in df_receber.columns:
        return []

    d = df_receber.copy()
    d["Venda"] = d["Venda"].fillna("").astype(str).str.strip()
    d["_pn"] = pd.to_numeric(d["Parc_Num"], errors="coerce")
    alertas: List[Dict[str, Any]] = []

    for venda, g in d.groupby("Venda", sort=False):
        vs = str(venda).strip()
        if not vs:
            continue
        nums = g["_pn"].dropna().astype(int).unique().tolist()
        nums_pos = sorted([int(x) for x in nums if int(x) > 0])
        if len(nums_pos) < 2:
            continue

        ordem_linha = g["_pn"].tolist()
        ordem_ok = all(
            pd.isna(a) or pd.isna(b) or float(a) <= float(b)
            for a, b in zip(ordem_linha, ordem_linha[1:])
            if not (pd.isna(a) and pd.isna(b))
        )
        if not ordem_ok:
            alertas.append(
                _alerta(
                    vs,
                    "PARCELAS_FORA_ORDEM",
                    "Possivel desordenacao de Parc_Num na base Receber (ordem de linhas).",
                    regra="Sequencia Parc_Num por ordem de linhas",
                )
            )

        lo, hi = min(nums_pos), max(nums_pos)
        esperado = set(range(lo, hi + 1))
        faltando = sorted(esperado - set(nums_pos))
        if faltando and len(faltando) <= 30:
            alertas.append(
                _alerta(
                    vs,
                    "PARCELAS_GAP",
                    f"Lacunas na sequencia de numeros de parcela (amostra): {faltando[:15]}",
                    str(len(esperado)),
                    str(len(nums_pos)),
                    "Gaps entre min(Parc_Num) e max(Parc_Num)",
                )
            )

    return alertas


def _auditoria_duplicadas_e_sobrepostas(df_receber: pd.DataFrame) -> List[Dict[str, Any]]:
    if df_receber is None or df_receber.empty or "Venda" not in df_receber.columns:
        return []
    d = df_receber.copy()
    d["Venda"] = d["Venda"].fillna("").astype(str).str.strip()
    if "Parc_Num" in d.columns:
        d["_pn"] = pd.to_numeric(d["Parc_Num"], errors="coerce")
    else:
        d["_pn"] = float("nan")
    if "Parcela" in d.columns:
        d["_pk"] = d["Parcela"].map(_norm_parcela_local)
    else:
        d["_pk"] = ""
    if "Status_Vencimento" not in d.columns:
        d["Status_Vencimento"] = ""
    d["_st"] = d["Status_Vencimento"].fillna("").astype(str).str.strip().str.upper()

    alertas: List[Dict[str, Any]] = []

    for venda, g in d.groupby("Venda", sort=False):
        vs = str(venda).strip()
        if not vs:
            continue
        # Duplicadas: mesmo Parc_Num > 0 repetido
        sub = g.dropna(subset=["_pn"])
        sub = sub[sub["_pn"] > 0]
        if not sub.empty:
            vc = sub["_pn"].astype(int).value_counts()
            dup = vc[vc > 1]
            if not dup.empty:
                amostra = dup.head(5).index.tolist()
                alertas.append(
                    _alerta(
                        vs,
                        "PARCELAS_DUPLICADAS",
                        f"Parc_Num repetido na venda (amostra): {amostra}",
                        regra="Contagem Parc_Num por Venda",
                    )
                )
        # Sobreposição: mesma parcela normalizada com status mutuamente exclusivos típicos
        pk_st: Dict[str, set] = {}
        for _, r in g.iterrows():
            pk = str(r.get("_pk") or "").strip()
            if not pk:
                continue
            st = str(r.get("_st") or "").strip()
            if not st:
                continue
            pk_st.setdefault(pk, set()).add(st)
        for pk, sts in pk_st.items():
            tem_venc = "VENCIDO" in sts
            tem_av = "A VENCER" in sts
            if tem_venc and tem_av:
                alertas.append(
                    _alerta(
                        vs,
                        "PARCELAS_SOBREPOSTAS",
                        f"Parcela {pk} com linhas VENCIDO e A VENCER simultaneas.",
                        regra="Status_Vencimento por parcela canonica",
                    )
                )
                break

    return alertas


def auditoria_alertas_qtd_parcelas_consolidado(
    consolidado: pd.DataFrame,
    mapa_universo_distinto: Dict[str, int],
    mapa_moda_receber: Dict[str, int],
    mapa_moda_recebidos: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    Alertas sobre Qtd.Parc.Total vs moda dominante (Parc_Total) e vs universo distinto.
    Executar após lift de Qtd.Parc.Total e mapa_universo_distinto.
    """
    alertas: List[Dict[str, Any]] = []
    if consolidado is None or consolidado.empty or "Venda" not in consolidado.columns:
        return alertas

    def _moda_ref(v: str) -> int:
        a = int(mapa_moda_receber.get(v, 0) or 0)
        b = int(mapa_moda_recebidos.get(v, 0) or 0)
        if a > 0:
            return a
        return b

    for _, row in consolidado.iterrows():
        v = str(row.get("Venda", "") or "").strip()
        if not v:
            continue
        qt = int(float(row.get("Qtd.Parc.Total", 0) or 0))
        moda = _moda_ref(v)
        u = int(mapa_universo_distinto.get(v, 0) or 0)

        if u > 0 and qt < u:
            alertas.append(
                _alerta(
                    v,
                    "PARCELAS_INCONSISTENTES_TOTAL",
                    f"Qtd.Parc.Total ({qt}) menor que universo distinto de parcelas ({u}).",
                    str(u),
                    str(qt),
                    "Blindagem universo distinto",
                )
            )

        if moda > 0 and qt > 2 * moda:
            alertas.append(
                _alerta(
                    v,
                    "PARCELAS_INCONSISTENTES_TOTAL",
                    f"Qtd.Parc.Total ({qt}) excede 2x moda dominante Parc_Total ({moda}).",
                    str(2 * moda),
                    str(qt),
                    "Ratio total vs denominador dominante",
                )
            )

        if moda > 0:
            lim = max(5, int(moda * 0.5))
            if abs(qt - moda) > lim:
                alertas.append(
                    _alerta(
                        v,
                        "PARCELAS_INCONSISTENTES_TOTAL",
                        f"Divergencia forte: Qtd.Parc.Total={qt} vs moda dominante={moda} (limiar {lim}).",
                        str(moda),
                        str(qt),
                        "Total vs mapa estrutural Parc_Total",
                    )
                )

    return alertas


def ajustar_scores_com_alertas_tot(
    scores: Dict[str, float],
    alertas: List[Dict[str, Any]],
) -> Dict[str, float]:
    """Penaliza score quando há alertas PARCELAS_INCONSISTENTES_TOTAL na mesma venda."""
    out = dict(scores)
    for a in alertas or []:
        if str(a.get("Tipo_Alerta", "") or "").strip() != "PARCELAS_INCONSISTENTES_TOTAL":
            continue
        v = str(a.get("Venda", "") or "").strip()
        if not v:
            continue
        out[v] = max(0.0, float(out.get(v, 100.0)) - 15.0)
    return out


def calcular_score_qualidade_parcelas_por_venda(df_receber: pd.DataFrame) -> Dict[str, float]:
    """
    Score 0–100 por venda (100 = sem penalidades nas heurísticas locais).
    """
    scores: Dict[str, float] = {}
    if df_receber is None or df_receber.empty or "Venda" not in df_receber.columns:
        return scores

    alertas_por_venda: Dict[str, List[str]] = {}
    for a in auditoria_sequencia_parcelas_receber(df_receber):
        v = str(a.get("Venda", "") or "").strip()
        if not v:
            continue
        alertas_por_venda.setdefault(v, []).append(str(a.get("Tipo_Alerta", "") or ""))

    vendas = df_receber["Venda"].fillna("").astype(str).str.strip().unique()
    for v in vendas:
        if not v:
            continue
        tipos = alertas_por_venda.get(v, [])
        c = Counter(tipos)
        pen = 0.0
        ordem_hit = 1 if (c.get("PARCELAS_FORA_ORDEM", 0) + c.get("PARCELAS_ORDEM", 0)) > 0 else 0
        gap_hit = 1 if c.get("PARCELAS_GAP", 0) > 0 else 0
        pen += 12 * (gap_hit + ordem_hit)
        pen += 18 * min(1, c.get("PARCELAS_DUPLICADAS", 0))
        pen += 22 * min(1, c.get("PARCELAS_SOBREPOSTAS", 0))
        pen += 15 * c.get("PARCELAS_INCONSISTENTES_TOTAL", 0)
        scores[v] = max(0.0, 100.0 - pen)

    return scores
