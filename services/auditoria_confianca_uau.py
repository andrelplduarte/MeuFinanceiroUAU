# -*- coding: utf-8 -*-
"""
Camadas de auditoria de confiança e rastreabilidade (produção).

Não altera valores financeiros nem o consolidado — apenas gera alertas e metadados.
Integridade Carteira = Pago + Vl.Principal (Encargos) + Vencer (definição do motor UAU).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

TOL_FIN_CARTEIRA = 0.02
TOL_PR_HETEROGENEIDADE = 0.02


def _alerta_base(
    venda: str,
    cliente_base: str,
    tipo: str,
    mensagem: str,
    valor_esperado: str = "",
    valor_encontrado: str = "",
    regra: str = "",
    observacao: str = "Nao bloqueante",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    row = {
        "Venda": str(venda or "").strip(),
        "Cliente_Base": str(cliente_base or "").strip(),
        "Tipo_Alerta": tipo,
        "Mensagem": mensagem,
        "Valor_Esperado": str(valor_esperado),
        "Valor_Encontrado": str(valor_encontrado),
        "Regra": regra or "Auditoria de confianca",
        "Observacao": observacao,
    }
    if extra:
        for k, v in extra.items():
            row[k] = v
    return row


def coletar_alertas_grupos_deduplicacao(
    base: pd.DataFrame,
    chave_g: List[str],
    origem: str,
) -> List[Dict[str, Any]]:
    """
    Por grupo real de deduplicação (chave_g): baixa confiança de ID e heterogeneidade de valores.
    """
    out: List[Dict[str, Any]] = []
    if base is None or base.empty or not chave_g:
        return out
    miss = [c for c in chave_g if c not in base.columns]
    if miss:
        return out

    b = base.copy()
    if "_Vlr_R" not in b.columns and "Vlr_Parcela" in b.columns:
        b["_Vlr_R"] = pd.to_numeric(b["Vlr_Parcela"], errors="coerce").fillna(0).round(2)
    if "_Pr_R" not in b.columns and "Principal" in b.columns:
        b["_Pr_R"] = pd.to_numeric(b["Principal"], errors="coerce").fillna(0).round(2)

    for _, g in b.groupby(chave_g, sort=False):
        n = len(g)
        if n < 2:
            continue
        venda = str(g["Venda"].iloc[0]).strip() if "Venda" in g.columns else ""
        cb = str(g["Cliente_Base"].iloc[0]).strip() if "Cliente_Base" in g.columns else ""
        id_col = "_Id_Key_Dedup" if "_Id_Key_Dedup" in g.columns else None
        vazios = 0
        if id_col:
            vazios = int((g[id_col].astype(str).str.strip() == "").sum())
        ratio_vazio = vazios / float(n)
        if ratio_vazio >= 0.5 or vazios == n:
            out.append(
                _alerta_base(
                    venda,
                    cb,
                    "DEDUP_BAIXA_CONFIANCA",
                    f"Dedup {origem}: {n} linhas; identificador forte vazio em {vazios} ({ratio_vazio:.0%}).",
                    str(n),
                    f"id_vazio={vazios};disp_id={ratio_vazio:.2f}",
                    f"Agrupamento dedup {origem} sem evidencia forte de Unidades/Identificador",
                    extra={"Origem_Dedup": origem, "Linhas_Grupo": n, "Id_Vazio_Count": vazios},
                )
            )

        pr = pd.to_numeric(g["_Pr_R"], errors="coerce").fillna(0).round(2) if "_Pr_R" in g.columns else pd.Series([0.0] * n)
        disp_pr = float(pr.max() - pr.min()) if len(pr) else 0.0
        vlr = pd.to_numeric(g["_Vlr_R"], errors="coerce").fillna(0).round(2) if "_Vlr_R" in g.columns else pd.Series([0.0] * n)
        nu_vlr = int(vlr.nunique()) if len(vlr) else 0
        if disp_pr > TOL_PR_HETEROGENEIDADE or nu_vlr > 1:
            out.append(
                _alerta_base(
                    venda,
                    cb,
                    "GRUPO_HETEROGENEO",
                    f"Dedup {origem}: grupo de {n} linhas com variacao Principal max-min={disp_pr:.2f} ou Vlr_Parcela distintos={nu_vlr}.",
                    f"principal_constante_tol={TOL_PR_HETEROGENEIDADE}",
                    f"disp_principal={disp_pr:.2f};nunique_vlr={nu_vlr}",
                    f"Agrupamento dedup {origem}: dispersao interna",
                    extra={"Origem_Dedup": origem, "Linhas_Grupo": n, "Dispersao_Principal": round(disp_pr, 4), "Nunique_Vlr_Parcela": nu_vlr},
                )
            )
    return out


def coletar_alertas_conflito_duplicidade_flag(df: pd.DataFrame, origem: str) -> List[Dict[str, Any]]:
    """Uma linha de alerta por venda quando há linhas com POSSIVEL_CONFLITO_DUPLICIDADE=True."""
    out: List[Dict[str, Any]] = []
    if df is None or df.empty or "POSSIVEL_CONFLITO_DUPLICIDADE" not in df.columns:
        return out
    m = df["POSSIVEL_CONFLITO_DUPLICIDADE"].fillna(False).astype(bool)
    if not m.any():
        return out
    sub = df.loc[m].copy()
    sub["Venda"] = sub["Venda"].fillna("").astype(str).str.strip()
    for venda, g in sub.groupby("Venda", sort=False):
        vs = str(venda).strip()
        if not vs:
            continue
        cb = str(g["Cliente_Base"].iloc[0]).strip() if "Cliente_Base" in g.columns else ""
        out.append(
            _alerta_base(
                vs,
                cb,
                "POSSIVEL_CONFLITO_DUPLICIDADE",
                f"Flag de possivel conflito de deduplicacao ({origem}): {len(g)} linha(s) na base tratada.",
                "",
                str(len(g)),
                f"Split por identificador forte distinto | {origem}",
                extra={"Origem": origem, "Linhas_Flag": len(g)},
            )
        )
    return out


def coletar_alertas_cliente_base(
    df_receber: pd.DataFrame,
    df_recebidos: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Colisão de Cliente_Base por Venda e dispersão de nomes de Cliente."""
    out: List[Dict[str, Any]] = []
    frames = []
    if df_receber is not None and not df_receber.empty and "Venda" in df_receber.columns:
        d = df_receber.copy()
        d["_origem"] = "Receber"
        frames.append(d)
    if df_recebidos is not None and not df_recebidos.empty and "Venda" in df_recebidos.columns:
        d = df_recebidos.copy()
        d["_origem"] = "Recebidos"
        frames.append(d)
    if not frames:
        return out
    u = pd.concat(frames, ignore_index=True)
    u["Venda"] = u["Venda"].fillna("").astype(str).str.strip()
    if "Cliente_Base" not in u.columns or "Cliente" not in u.columns:
        return out
    u["Cliente_Base"] = u["Cliente_Base"].fillna("").astype(str).str.strip()
    u["Cliente"] = u["Cliente"].fillna("").astype(str).str.strip()

    for venda, g in u.groupby("Venda", sort=False):
        vs = str(venda).strip()
        if not vs:
            continue
        bases = sorted({b for b in g["Cliente_Base"].tolist() if b})
        if len(bases) > 1:
            score_disp = float(len(bases))
            out.append(
                _alerta_base(
                    vs,
                    "",
                    "CLIENTE_INCONSISTENTE",
                    f"Multiplos Cliente_Base na mesma Venda: {bases[:8]}{'...' if len(bases) > 8 else ''}",
                    "1",
                    str(len(bases)),
                    "Unicidade Cliente_Base por Venda",
                    extra={"Score_Dispersao_Base": score_disp, "Bases": "|".join(bases[:20])},
                )
            )
        nomes = g.loc[g["Cliente"] != "", "Cliente"]
        nu = int(nomes.nunique()) if len(nomes) else 0
        nb = max(len(bases), 1)
        ratio_nomes = float(nu) / float(nb)
        if nu >= 4 and ratio_nomes >= 2.0:
            out.append(
                _alerta_base(
                    vs,
                    bases[0] if bases else "",
                    "CLIENTE_INCONSISTENTE",
                    f"Muitos nomes de Cliente distintos ({nu}) para a venda (ratio={ratio_nomes:.2f}).",
                    str(nb),
                    str(nu),
                    "Dispersao de Cliente por Venda",
                    extra={"Score_Dispersao_Nomes": round(ratio_nomes, 3), "Nomes_Unicos": nu},
                )
            )
    return out


def auditoria_integridade_financeira_obrigatoria(consolidado: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Sempre ativa: Vl.Carteira = saldo em aberto = Vl.Principal (Encargos) + Vl.Vencer
    (não inclui Vl.Pago na carteira).
    """
    out: List[Dict[str, Any]] = []
    if consolidado is None or consolidado.empty:
        return out
    req = ("Vl.Carteira", "Vl.Pago", "Vl.Principal (Encargos)", "Vl.Vencer", "Venda")
    if not all(c in consolidado.columns for c in req):
        return out
    c = consolidado
    enc = pd.to_numeric(c["Vl.Principal (Encargos)"], errors="coerce").fillna(0).round(2)
    vl_v = pd.to_numeric(c["Vl.Vencer"], errors="coerce").fillna(0).round(2)
    cart = pd.to_numeric(c["Vl.Carteira"], errors="coerce").fillna(0).round(2)
    esperado_aberto = (enc + vl_v).round(2)
    diff = (cart - esperado_aberto).abs()
    mask = diff > TOL_FIN_CARTEIRA
    if not mask.any():
        return out
    sub = c.loc[mask]
    for _, row in sub.iterrows():
        v = str(row.get("Venda", "") or "").strip()
        esp = float(esperado_aberto.loc[row.name]) if row.name in esperado_aberto.index else float("nan")
        enco = float(cart.loc[row.name]) if row.name in cart.index else float("nan")
        d = float(diff.loc[row.name]) if row.name in diff.index else 0.0
        denom = max(abs(esp), 1.0)
        pct = 100.0 * d / denom
        out.append(
            _alerta_base(
                v,
                str(row.get("Cliente_Base", "") or "").strip() if "Cliente_Base" in row.index else "",
                "FINANCEIRO_INCONSISTENTE",
                f"Vl.Carteira diverge de Principal(Encargos)+Vencer além de {TOL_FIN_CARTEIRA} | diff={d:.2f} | erro_pct={pct:.4f}%",
                f"{esp:.2f}",
                f"{enco:.2f}",
                "Identidade: Vl.Carteira = Vl.Principal (Encargos) + Vl.Vencer (saldo em aberto)",
                extra={"Diferenca_Abs": round(d, 4), "Erro_Pct": round(pct, 6)},
            )
        )
    return out


def montar_alertas_etl_de_metricas(metricas: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converte estatísticas acumuladas do ETL em alertas informativos (rastreabilidade)."""
    out: List[Dict[str, Any]] = []
    if not metricas:
        return out
    tot_d = int(metricas.get("descarte_ruido", 0) or 0) + int(metricas.get("descarte_fragmento", 0) or 0)
    tot_d += int(metricas.get("descarte_cabecalho_repetido", 0) or 0)
    if tot_d <= 0:
        return out
    amostras = metricas.get("amostras") or {}
    msg = (
        f"ETL: entrada={metricas.get('linhas_entrada', 0)} saida={metricas.get('linhas_saida', 0)} | "
        f"ruido={metricas.get('descarte_ruido', 0)} fragmento={metricas.get('descarte_fragmento', 0)} "
        f"cab_rep={metricas.get('descarte_cabecalho_repetido', 0)}"
    )
    out.append(
        _alerta_base(
            "",
            "",
            "ETL_DESCARTE_RASTREADO",
            msg,
            str(metricas.get("linhas_entrada", "")),
            str(tot_d),
            "Preprocessamento TXT UAU",
            extra={
                "Amostra_Ruido": "; ".join((amostras.get("ruido") or [])[:3]),
                "Amostra_Fragmento": "; ".join((amostras.get("fragmento") or [])[:3]),
                "Amostra_Cabecalho": "; ".join((amostras.get("cabecalho_repetido") or [])[:3]),
            },
        )
    )
    return out


def classificar_alertas_confiabilidade(alertas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Marca cada alerta: ALERTA_VALIDO | PROVAVEL_RUIDO | GAP_EVIDENCIA_FRACA
    (heurística conservadora; não remove alertas).
    """
    if not alertas:
        return []
    por_venda_tipo = Counter(
        (str(a.get("Venda", "")).strip(), str(a.get("Tipo_Alerta", "")).strip())
        for a in alertas
    )
    fortes = {
        "FINANCEIRO_INCONSISTENTE",
        "CLIENTE_INCONSISTENTE",
        "DEDUP_BAIXA_CONFIANCA",
        "GRUPO_HETEROGENEO",
        "POSSIVEL_CONFLITO_DUPLICIDADE",
    }
    out: List[Dict[str, Any]] = []
    for a in alertas:
        ac = dict(a)
        tipo = str(ac.get("Tipo_Alerta", "")).strip()
        v = str(ac.get("Venda", "")).strip()
        if tipo in fortes:
            ac["Classificacao_Alerta"] = "ALERTA_VALIDO"
        elif tipo == "PARCELAS_INCONSISTENTES_TOTAL" and por_venda_tipo.get((v, tipo), 0) >= 4:
            ac["Classificacao_Alerta"] = "PROVAVEL_RUIDO"
        elif tipo == "PARCELAS_GAP" and len(str(ac.get("Mensagem", ""))) > 140:
            ac["Classificacao_Alerta"] = "PROVAVEL_RUIDO"
        else:
            ac["Classificacao_Alerta"] = "NEUTRO_AUDITORIA"
        out.append(ac)
    return out


def calcular_confianca_final_por_venda(
    alertas: List[Dict[str, Any]],
    scores_parcelas: Optional[Dict[str, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Score 0–100 e classe ALTA / MEDIA / BAIXA por Venda.
    """
    scores_parcelas = scores_parcelas or {}
    pen: Dict[str, float] = defaultdict(float)
    for a in alertas:
        v = str(a.get("Venda", "")).strip()
        if not v:
            continue
        tipo = str(a.get("Tipo_Alerta", "")).strip()
        cls = str(a.get("Classificacao_Alerta", "")).strip()
        if cls == "PROVAVEL_RUIDO":
            pen[v] += 4
            continue
        if tipo == "FINANCEIRO_INCONSISTENTE":
            pen[v] += 35
        elif tipo == "CLIENTE_INCONSISTENTE":
            pen[v] += 22
        elif tipo == "DEDUP_BAIXA_CONFIANCA":
            pen[v] += 18
        elif tipo == "GRUPO_HETEROGENEO":
            pen[v] += 14
        elif tipo == "PARCELAS_INCONSISTENTES_TOTAL":
            pen[v] += 12
        elif tipo in ("PARCELAS_GAP", "PARCELAS_FORA_ORDEM", "PARCELAS_DUPLICADAS", "PARCELAS_SOBREPOSTAS"):
            pen[v] += 8
        elif tipo == "POSSIVEL_CONFLITO_DUPLICIDADE":
            pen[v] += 10
        elif tipo == "ETL_DESCARTE_RASTREADO":
            pen[v] += 3
        else:
            pen[v] += 2

    todas_vendas = set(scores_parcelas.keys()) | set(pen.keys())
    out: Dict[str, Dict[str, Any]] = {}
    for v in todas_vendas:
        base_p = float(scores_parcelas.get(v, 100.0))
        pts = max(0.0, min(100.0, base_p - pen.get(v, 0.0)))
        if pts >= 80:
            nivel = "ALTA CONFIANCA"
        elif pts >= 55:
            nivel = "MEDIA CONFIANCA"
        else:
            nivel = "BAIXA CONFIANCA"
        out[v] = {"Pontos": round(pts, 2), "Nivel": nivel, "Penalidade_Auditoria": round(pen.get(v, 0.0), 2)}
    return out


def resumo_confianca_executivo(mapa: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if not mapa:
        return {"total": 0, "alta": 0, "media": 0, "baixa": 0}
    alta = sum(1 for x in mapa.values() if x.get("Nivel") == "ALTA CONFIANCA")
    med = sum(1 for x in mapa.values() if x.get("Nivel") == "MEDIA CONFIANCA")
    baixa = sum(1 for x in mapa.values() if x.get("Nivel") == "BAIXA CONFIANCA")
    return {"total": len(mapa), "alta": alta, "media": med, "baixa": baixa}
