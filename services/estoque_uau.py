# -*- coding: utf-8 -*-
"""
Camada complementar: relatório de estoque UAU × consolidado financeiro (somente leitura do motor).
Não altera Vl.Pago, Vl.Vencer, carteira, identificador nas linhas do consolidado — apenas consome o DataFrame exportado.
"""
from __future__ import annotations

import os
import re
import unicodedata

import pandas as pd

from services.processador_uau import (
    escolher_moda_texto,
    ler_texto_robusto,
    limpar_texto_nome,
    normalizar_emp_obra,
    normalizar_identificador,
)

NOME_ABA_CONSOLIDADO_ESTOQUE = "CONSOLIDADO ESTOQUE"

# Painel gerencial + grade: pandas startrow 0-based → cabeçalho tabular na linha 19 (1-based), dados a partir da 20.
CONSOLIDADO_ESTOQUE_PANDAS_STARTROW = 18

COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE = [
    "EMP/OBRA",
    "EMPREENDIMENTO",
    "IDENTIFICADOR",
    "VENDA",
    "CLIENTE",
    "SITUAÇÃO",
    "QTD.PAGO",
    "VL.PAGO",
    "QTD.VENCIDA",
    "VL.VENCIDO",
    "QTD.A VENCER",
    "VL.A VENCER",
    "VL.CARTEIRA",
    "% PAGO",
    "% VENCIDO",
    "% A VENCER",
    "STATUS CONSTRUÇÃO",
    "OBS",
]


def calcular_indicadores_painel_consolidado_estoque(df: pd.DataFrame) -> dict:
    """
    Indicadores do painel superior da aba CONSOLIDADO ESTOQUE (somente leitura do DataFrame).
    """
    out = {
        "qtd_total": 0,
        "qtd_vendidas": 0,
        "qtd_livres": 0,
        "pct_vendidas": 0.0,
        "pct_livres": 0.0,
        "qtd_quitadas": 0,
        "qtd_adimplentes": 0,
        "qtd_inadimplentes": 0,
        "pct_quitadas_sobre_vend": 0.0,
        "pct_adimplentes_sobre_vend": 0.0,
        "pct_inadimplentes_sobre_vend": 0.0,
    }
    if df is None or getattr(df, "empty", True):
        return out
    try:
        d = df.reindex(columns=COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE, fill_value="")
    except Exception:
        d = df
    n = len(d)
    if n == 0:
        return out
    sit_col = "SITUAÇÃO" if "SITUAÇÃO" in d.columns else None
    if not sit_col:
        return out
    sit = d[sit_col].fillna("").astype(str).map(_fold_upper)
    tag_livre = _fold_upper("DISPONIVEL")
    qtd_livres = int((sit == tag_livre).sum())
    qtd_total = int(n)
    qtd_vendidas = qtd_total - qtd_livres
    out["qtd_total"] = qtd_total
    out["qtd_livres"] = qtd_livres
    out["qtd_vendidas"] = qtd_vendidas
    if qtd_total:
        out["pct_vendidas"] = 100.0 * qtd_vendidas / qtd_total
        out["pct_livres"] = 100.0 * qtd_livres / qtd_total
    q_quit = int((sit == _fold_upper("QUITADO")).sum())
    q_adimpl = int((sit == _fold_upper("ADIMPLENTE")).sum())
    q_inad = int((sit == _fold_upper("INADIMPLENTE")).sum())
    out["qtd_quitadas"] = q_quit
    out["qtd_adimplentes"] = q_adimpl
    out["qtd_inadimplentes"] = q_inad
    if qtd_vendidas:
        out["pct_quitadas_sobre_vend"] = 100.0 * q_quit / qtd_vendidas
        out["pct_adimplentes_sobre_vend"] = 100.0 * q_adimpl / qtd_vendidas
        out["pct_inadimplentes_sobre_vend"] = 100.0 * q_inad / qtd_vendidas
    return out


def _fold_upper(s: str) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return t.upper().strip()


def _norm_header_cell(x: str) -> str:
    return _fold_upper(re.sub(r"\s+", " ", str(x or "").strip()))


def _indice_coluna(headers_norm: list[str], aliases: tuple[str, ...], contem: tuple[str, ...] = ()) -> int:
    for j, h in enumerate(headers_norm):
        for a in aliases:
            if h == a or h.replace(" ", "") == a.replace(" ", ""):
                return j
        for frag in contem:
            if frag in h:
                return j
    return -1


def _indice_status_estoque_coluna(headers_norm: list[str]) -> int:
    """Evita confundir STATUS DO ESTOQUE com STATUS DA VENDA / CONSTRUÇÃO."""
    prefer = (
        "STATUS ESTOQUE",
        "STATUS DA UNIDADE",
        "SITUACAO ESTOQUE",
        "SITUAÇÃO ESTOQUE",
    )
    for pref in prefer:
        j = _indice_coluna(headers_norm, (pref,), ())
        if j >= 0:
            return j
    for j, h in enumerate(headers_norm):
        if "STATUS" not in h:
            continue
        if "VENDA" in h or "CONSTR" in h:
            continue
        if h == "STATUS" or "ESTOQUE" in h or "UNIDADE" in h:
            return j
    return -1


def _split_tsv(linha: str) -> list[str]:
    return str(linha or "").rstrip("\r\n").split("\t")


def _normalizar_linha_estoque(linha: str) -> str:
    s = str(linha or "").replace("\xa0", " ").strip()
    if not s:
        return ""
    for token in ("LOTE", "QUADRA", "EMPREENDIMENTO", "UNIDADE", "BLOCO", "TORRE"):
        pad = r"".join([re.escape(ch) + r"\s*" for ch in token])
        s = re.sub(rf"\b{pad}\b", token, s, flags=re.IGNORECASE)
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s*-\s*", " - ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def _pretratar_texto_estoque(texto: str) -> str:
    lixo = (
        "UAU! SOFTWARE",
        "PÁGINA",
        "PAGINA",
        "PAGE",
    )
    out = []
    last = ""
    for ln in str(texto or "").splitlines():
        s = _normalizar_linha_estoque(ln)
        if not s:
            continue
        su = _fold_upper(s)
        if any(x in su for x in lixo):
            continue
        if su == last:
            continue
        out.append(s)
        last = su
    return "\n".join(out)


def carregar_estoque_bruto(caminho_arquivo: str | None) -> pd.DataFrame:
    """
    Lê TXT tabular de estoque UAU. Colunas opcionais: VENDA, STATUS_DA_CONSTRUCAO (ou sinônimos).
    Se o arquivo estiver ausente, vazio ou ilegível, retorna DataFrame vazio (não quebra o fluxo).
    """
    vazio = pd.DataFrame(
        columns=[
            "Obra",
            "Identificador",
            "Status_Estoque",
            "Motivo_Estoque",
            "Venda",
            "Status_Construcao",
            "Valores_Ref",
        ]
    )
    if not caminho_arquivo or not os.path.isfile(caminho_arquivo):
        return vazio
    try:
        texto = ler_texto_robusto(caminho_arquivo)
    except Exception:
        return vazio
    if not str(texto or "").strip():
        return vazio

    texto = _pretratar_texto_estoque(texto)
    linhas = [x.rstrip("\r\n") for x in texto.splitlines()]
    header_i = -1
    headers_raw: list[str] = []
    for i, ln in enumerate(linhas[:300]):
        partes = _split_tsv(ln)
        if len(partes) < 3:
            continue
        hn = [_norm_header_cell(p) for p in partes]
        tem_obra = any(
            x in hn
            for x in ("OBRA", "EMP/OBRA", "EMPRESA/OBRA")
        ) or any("OBRA" in x for x in hn)
        tem_id = any(
            "IDENTIFICADOR" in x or x in ("ID", "UNIDADE", "CODIGO UNIDADE") for x in hn
        )
        if tem_obra and tem_id:
            header_i = i
            headers_raw = partes
            break

    if header_i < 0:
        return vazio

    hn = [_norm_header_cell(h) for h in headers_raw]
    j_obra = _indice_coluna(
        hn,
        ("OBRA", "EMP/OBRA", "COD OBRA", "CODIGO OBRA"),
        ("OBRA",),
    )
    j_id = _indice_coluna(
        hn,
        ("IDENTIFICADOR", "ID UNIDADE", "CODIGO UNIDADE"),
        ("IDENTIFICADOR", "IDENTIFICADOR PRODUTO"),
    )
    j_st = _indice_status_estoque_coluna(hn)
    j_mot = _indice_coluna(hn, ("MOTIVO", "MOTIVO ESTOQUE", "JUSTIFICATIVA"), ())
    j_venda = _indice_coluna(
        hn,
        ("VENDA", "NUMERO DA VENDA", "Nº VENDA", "NR VENDA", "NUM VENDA"),
        ("VENDA",),
    )
    j_cons = _indice_coluna(
        hn,
        (
            "STATUS DA CONSTRUCAO",
            "STATUS CONSTRUCAO",
            "STATUS DA CONSTRUÇÃO",
            "STATUS CONSTRUÇÃO",
        ),
        ("CONSTRUC", "CONSTRUÇ"),
    )
    j_val = _indice_coluna(
        hn,
        ("VALOR", "VALORES", "VL REF", "VL. REF", "PRECO", "PREÇO"),
        ("VALOR",),
    )

    if j_obra < 0 or j_id < 0:
        return vazio

    registros = []
    for ln in linhas[header_i + 1 :]:
        if not str(ln).strip():
            continue
        partes = _split_tsv(ln)
        if len(partes) <= max(j_obra, j_id):
            continue
        obra = partes[j_obra].strip() if j_obra < len(partes) else ""
        ident = partes[j_id].strip() if j_id < len(partes) else ""
        if not ident:
            continue
        st = partes[j_st].strip() if j_st >= 0 and j_st < len(partes) else ""
        mot = partes[j_mot].strip() if j_mot >= 0 and j_mot < len(partes) else ""
        vnd = partes[j_venda].strip() if j_venda >= 0 and j_venda < len(partes) else ""
        cons = partes[j_cons].strip() if j_cons >= 0 and j_cons < len(partes) else ""
        vref = ""
        if j_val >= 0 and j_val < len(partes):
            vref = partes[j_val].strip()
        registros.append(
            {
                "Obra": obra,
                "Identificador": ident,
                "Status_Estoque": st,
                "Motivo_Estoque": mot,
                "Venda": vnd,
                "Status_Construcao": cons,
                "Valores_Ref": vref,
            }
        )

    if not registros:
        return vazio
    return pd.DataFrame(registros)


def _join_vendas(serie: pd.Series) -> str:
    u = sorted({str(x).strip() for x in serie.tolist() if str(x).strip()})
    return "; ".join(u[:40])


def _agg_financeiro_por_chave(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por (Emp/Obra normalizada, Identificador normalizado)."""
    rows = []
    for (eo, iid), g in df.groupby(["_eo", "_id"], dropna=False):
        g = g.copy()
        for c in ("Qtd.Parc.Atrasada", "Qtd.Parc.Paga", "Qtd.Parc.A Vencer", "Vl.Pago", "Vl.Principal (Encargos)", "Vl.Vencer", "Vl.Carteira"):
            if c not in g.columns:
                g[c] = 0
        g["_qa"] = pd.to_numeric(g["Qtd.Parc.Atrasada"], errors="coerce").fillna(0).astype(int)
        g["_qp"] = pd.to_numeric(g["Qtd.Parc.Paga"], errors="coerce").fillna(0).astype(int)
        g["_qv"] = pd.to_numeric(g["Qtd.Parc.A Vencer"], errors="coerce").fillna(0).astype(int)
        idx = g["_qa"].idxmax()
        row_pick = g.loc[idx]
        pago = pd.to_numeric(g["Vl.Pago"], errors="coerce").fillna(0).sum()
        enc = pd.to_numeric(g["Vl.Principal (Encargos)"], errors="coerce").fillna(0).sum()
        vv = pd.to_numeric(g["Vl.Vencer"], errors="coerce").fillna(0).sum()
        vc = pd.to_numeric(g["Vl.Carteira"], errors="coerce").fillna(0).sum()
        den = max(float(pago + enc + vv), 0.0)
        pp = (pago / den) if den > 1e-9 else 0.0
        pi = (enc / den) if den > 1e-9 else 0.0
        pa = (vv / den) if den > 1e-9 else 0.0
        rows.append(
            {
                "_eo": eo,
                "_id": iid,
                "EMP/OBRA": str(row_pick.get("Emp/Obra", "") or "").strip(),
                "EMPREENDIMENTO": escolher_moda_texto(
                    [str(x).strip() for x in g["Empreendimento"].tolist() if str(x).strip()]
                )
                if "Empreendimento" in g.columns
                else "",
                "IDENTIFICADOR": str(row_pick.get("Identificador", "") or "").strip(),
                "VENDA": _join_vendas(g["Venda"]) if "Venda" in g.columns else "",
                "CLIENTE": escolher_moda_texto(
                    [str(x).strip() for x in g["Cliente"].tolist() if str(x).strip()]
                )
                if "Cliente" in g.columns
                else "",
                "STATUS VENDA": str(row_pick.get("Status venda", "") or "").strip(),
                "QTD.PAGO": int(g["_qp"].sum()),
                "VL.PAGO": round(float(pago), 2),
                "QTD.VENCIDA": int(g["_qa"].sum()),
                "VL.VENCIDO": round(float(enc), 2),
                "QTD.A VENCER": int(g["_qv"].sum()),
                "VL.A VENCER": round(float(vv), 2),
                "VL.CARTEIRA": round(float(vc), 2),
                "% PAGO": pp,
                "% VENCIDO": pi,
                "% A VENCER": pa,
            }
        )
    return pd.DataFrame(rows)


def _mapa_venda_para_fin(df: pd.DataFrame) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if df is None or df.empty or "Venda" not in df.columns:
        return out
    for _, r in df.iterrows():
        v = str(r.get("Venda", "") or "").strip()
        if not v:
            continue
        if v in out:
            continue
        enc = float(pd.to_numeric(r.get("Vl.Principal (Encargos)", 0), errors="coerce") or 0)
        vv = float(pd.to_numeric(r.get("Vl.Vencer", 0), errors="coerce") or 0)
        vc = float(pd.to_numeric(r.get("Vl.Carteira", 0), errors="coerce") or 0)
        out[v] = {
            "EMP/OBRA": str(r.get("Emp/Obra", "") or "").strip(),
            "EMPREENDIMENTO": str(r.get("Empreendimento", "") or "").strip(),
            "IDENTIFICADOR": str(r.get("Identificador", "") or "").strip(),
            "VENDA": v,
            "CLIENTE": str(r.get("Cliente", "") or "").strip(),
            "STATUS VENDA": str(r.get("Status venda", "") or "").strip(),
            "QTD.PAGO": int(float(pd.to_numeric(r.get("Qtd.Parc.Paga", 0), errors="coerce") or 0)),
            "VL.PAGO": round(float(pd.to_numeric(r.get("Vl.Pago", 0), errors="coerce") or 0), 2),
            "QTD.VENCIDA": int(float(pd.to_numeric(r.get("Qtd.Parc.Atrasada", 0), errors="coerce") or 0)),
            "VL.VENCIDO": round(enc, 2),
            "QTD.A VENCER": int(float(pd.to_numeric(r.get("Qtd.Parc.A Vencer", 0), errors="coerce") or 0)),
            "VL.A VENCER": round(vv, 2),
            "VL.CARTEIRA": round(vc, 2),
            "% PAGO": float(pd.to_numeric(r.get("% Pago", 0), errors="coerce") or 0),
            "% VENCIDO": float(pd.to_numeric(r.get("% Inadimplência", 0), errors="coerce") or 0),
            "% A VENCER": float(pd.to_numeric(r.get("% A Vencer", 0), errors="coerce") or 0),
        }
    return out


def _aplicar_mapa_venda_em_merged(merged: pd.DataFrame, mapa_v: dict[str, dict]) -> set:
    """Preenche linhas só-estoque quando há número de venda no estoque e match no consolidado. Retorna índices alterados."""
    altered: set = set()
    if "_venda_est" not in merged.columns or not mapa_v:
        return altered
    for i in merged.index:
        v_est = str(merged.at[i, "_venda_est"] or "").strip()
        if not v_est:
            continue
        cart = float(pd.to_numeric(merged.at[i, "VL.CARTEIRA"], errors="coerce") or 0)
        v_fin = str(merged.at[i, "VENDA"] or "").strip()
        if cart > 1e-9 or v_fin:
            continue
        fin = mapa_v.get(v_est)
        if not fin:
            continue
        for k, val in fin.items():
            if k in merged.columns:
                try:
                    merged.at[i, k] = val
                except (TypeError, ValueError):
                    if pd.api.types.is_integer_dtype(merged[k]):
                        merged[k] = merged[k].astype("float64")
                    else:
                        merged[k] = merged[k].astype("object")
                    merged.at[i, k] = val
        altered.add(i)
    return altered


def _tem_dados_financeiros_row(r: pd.Series) -> bool:
    v = str(r.get("VENDA", "") or "").strip()
    if v:
        return True
    q = int(pd.to_numeric(r.get("QTD.VENCIDA", 0), errors="coerce") or 0)
    if q > 0:
        return True
    cart = float(pd.to_numeric(r.get("VL.CARTEIRA", 0), errors="coerce") or 0)
    if cart > 1e-9:
        return True
    inad = float(pd.to_numeric(r.get("VL.VENCIDO", 0), errors="coerce") or 0)
    if inad > 1e-9:
        return True
    vv = float(pd.to_numeric(r.get("VL.A VENCER", 0), errors="coerce") or 0)
    if vv > 1e-9:
        return True
    return False


def _tem_dados_estoque_row(st_e: str, mot: str, st_c: str) -> bool:
    return bool(str(st_e or "").strip() or str(mot or "").strip() or str(st_c or "").strip())


def _inferir_tipo_cruzamento(
    idx: int,
    idx_mapa_venda: set,
    r: pd.Series,
    st_e: str,
    mot: str,
    st_c: str,
) -> str:
    if idx in idx_mapa_venda:
        return "VENDA (ESTOQUE) → CONSOLIDADO"
    t_fin = _tem_dados_financeiros_row(r)
    t_est = _tem_dados_estoque_row(st_e, mot, st_c)
    if t_fin and t_est:
        return "EMP/OBRA + IDENTIFICADOR"
    if t_fin and not t_est:
        return "SÓ FINANCEIRO"
    if not t_fin and t_est:
        return "SÓ ESTOQUE"
    return "N/A"


def classificar_consolidado_estoque(
    status_estoque: str,
    status_venda: str,
    qtd_atraso: int,
    vl_carteira: float,
    vl_inad: float,
    tem_venda_fin: bool,
    tem_linha_estoque: bool,
) -> tuple[str, str]:
    """
    Regra gerencial simples. Retorna (CLASSIFICAÇÃO FINAL, observação).
    """
    se = limpar_texto_nome(status_estoque)
    sv = limpar_texto_nome(status_venda)
    q = int(qtd_atraso or 0)
    cart = float(vl_carteira or 0)
    inad = float(vl_inad or 0)

    def _div(msg: str) -> tuple[str, str]:
        return "DIVERGENTE", msg

    # --- Divergências explícitas ---
    if tem_linha_estoque and ("DISPON" in se or se in ("LIVRE", "DISPONIVEL")):
        if tem_venda_fin and cart > 1e-6:
            return _div("Estoque indica disponível, mas há venda/carteira ativa no financeiro.")
        if q > 0 or inad > 1e-6:
            return _div("Estoque indica disponível, mas há atraso ou inadimplência no financeiro.")

    if tem_linha_estoque and any(
        x in se for x in ("QUITADO", "ESCRITUR", "FORA DE VENDA", "FORA DE VENDAS")
    ):
        if q > 0 or inad > 1e-6:
            return _div("Estoque indica encerramento/fora de venda, mas há inadimplência ou atraso.")

    if tem_linha_estoque and ("VENDID" in se or se.endswith(" VENDIDO") or se == "VENDIDO"):
        if not tem_venda_fin:
            return _div("Estoque indica vendido, sem venda correspondente no consolidado.")

    if tem_linha_estoque and ("SUSPENS" in se or "INATIV" in se):
        if q > 0 or inad > 1e-6 or cart > 1e-6:
            return _div("Estoque indica suspenso/inativo, mas há posição financeira relevante.")

    # --- Quitado ---
    if "QUITADO" in sv or (tem_venda_fin and cart <= 1e-6 and q == 0 and inad <= 1e-6):
        return "QUITADO", ""

    # --- Só estoque / sem venda ---
    if tem_linha_estoque and ("DISPON" in se or se in ("LIVRE", "DISPONIVEL")) and not tem_venda_fin:
        return "DISPONÍVEL", ""

    if tem_linha_estoque and any(x in se for x in ("SUSPENS", "FORA DE VENDA", "FORA DE VENDAS", "INATIV")):
        return "SUSPENSO / FORA DE VENDA", ""

    if not tem_venda_fin and not tem_linha_estoque:
        return "N/A", "Sem linha de estoque e sem venda no consolidado para esta chave."

    if not tem_venda_fin:
        return "N/A", "Sem venda no consolidado; conferir cadastro."

    # --- Com venda ativa (carteira ou rótulo) ---
    if q >= 2:
        return "INADIMPLENTE", ""
    if q == 1:
        return "ATENÇÃO", ""
    if q == 0:
        return "ADIMPLENTE", ""

    return "N/A", ""


def montar_dataframe_consolidado_estoque(
    df_consolidado: pd.DataFrame,
    df_estoque: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cruzamento prioritário: Emp/Obra + Identificador normalizados; reforço por Venda quando existir no estoque.
    Não altera df_consolidado.
    """
    out_empty = pd.DataFrame(columns=COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE)

    mapa_v = _mapa_venda_para_fin(df_consolidado)

    fin = pd.DataFrame()
    if df_consolidado is not None and not df_consolidado.empty:
        need = ("Emp/Obra", "Identificador", "Venda")
        if all(c in df_consolidado.columns for c in need):
            cfin = df_consolidado.loc[
                df_consolidado["Identificador"].fillna("").astype(str).str.strip() != ""
            ].copy()
            if not cfin.empty:
                cfin["_eo"] = cfin["Emp/Obra"].map(normalizar_emp_obra)
                cfin["_id"] = cfin["Identificador"].map(normalizar_identificador)
                fin = _agg_financeiro_por_chave(cfin)

    est = pd.DataFrame()
    if df_estoque is not None and not df_estoque.empty:
        est = df_estoque.copy()
        est["_eo"] = est["Obra"].map(normalizar_emp_obra)
        est["_id"] = est["Identificador"].map(normalizar_identificador)
        est = est.loc[est["_id"].astype(str).str.strip() != ""]
        if not est.empty:
            est = est.sort_values(
                by=["Obra", "Identificador"], kind="mergesort"
            ).drop_duplicates(["_eo", "_id"], keep="last")

    if fin.empty and est.empty:
        return out_empty

    if fin.empty:
        merged = est.copy()
        merged["EMP/OBRA"] = merged["Obra"].fillna("").astype(str)
        merged["EMPREENDIMENTO"] = ""
        merged["IDENTIFICADOR"] = merged["Identificador"].fillna("").astype(str)
        merged["VENDA"] = ""
        merged["CLIENTE"] = ""
        merged["STATUS VENDA"] = ""
        merged["QTD PARC. EM ATRASO"] = 0
        merged["VALOR INADIMPLÊNCIA"] = 0.0
        merged["VALOR A VENCER"] = 0.0
        merged["VL.CARTEIRA"] = 0.0
        if "Venda" in merged.columns:
            merged["_venda_est"] = merged["Venda"].fillna("").astype(str)
        else:
            merged["_venda_est"] = ""
    elif est.empty:
        merged = fin.copy()
        merged["Status_Estoque"] = ""
        merged["Motivo_Estoque"] = ""
        merged["Status_Construcao"] = ""
        merged["Valores_Ref"] = ""
        merged["_venda_est"] = ""
    else:
        est_m = est.rename(
            columns={
                "Venda": "Venda_Estoque",
            }
        )
        merged = fin.merge(est_m, on=["_eo", "_id"], how="outer")
        merged["EMP/OBRA"] = merged["EMP/OBRA"].fillna("").astype(str)
        m_obra = merged["EMP/OBRA"].str.strip() == ""
        merged.loc[m_obra, "EMP/OBRA"] = merged.loc[m_obra, "Obra"].fillna("").astype(str)
        merged["IDENTIFICADOR"] = merged["IDENTIFICADOR"].fillna("").astype(str)
        m_id = merged["IDENTIFICADOR"].str.strip() == ""
        merged.loc[m_id, "IDENTIFICADOR"] = merged.loc[m_id, "Identificador"].fillna("").astype(str)
        merged["_venda_est"] = merged["Venda_Estoque"].fillna("").astype(str)

    if "Status_Estoque" not in merged.columns:
        merged["Status_Estoque"] = ""
    if "Motivo_Estoque" not in merged.columns:
        merged["Motivo_Estoque"] = ""
    if "Status_Construcao" not in merged.columns:
        merged["Status_Construcao"] = ""
    if "Valores_Ref" not in merged.columns:
        merged["Valores_Ref"] = ""

    for c in [
        "EMP/OBRA",
        "EMPREENDIMENTO",
        "IDENTIFICADOR",
        "VENDA",
        "CLIENTE",
        "STATUS VENDA",
        "QTD.PAGO",
        "VL.PAGO",
        "QTD.VENCIDA",
        "VL.VENCIDO",
        "QTD.A VENCER",
        "VL.A VENCER",
        "VL.CARTEIRA",
    ]:
        if c not in merged.columns:
            merged[c] = "" if c in ("VENDA", "CLIENTE", "STATUS VENDA", "EMPREENDIMENTO") else (
                0 if "QTD" in c or "VALOR" in c or "VL." in c or "%" in c else ""
            )

    merged["QTD.PAGO"] = pd.to_numeric(merged["QTD.PAGO"], errors="coerce").fillna(0).astype(int)
    merged["VL.PAGO"] = pd.to_numeric(merged["VL.PAGO"], errors="coerce").fillna(0.0)
    merged["QTD.VENCIDA"] = pd.to_numeric(merged["QTD.VENCIDA"], errors="coerce").fillna(0).astype(int)
    merged["VL.VENCIDO"] = pd.to_numeric(merged["VL.VENCIDO"], errors="coerce").fillna(0.0)
    merged["QTD.A VENCER"] = pd.to_numeric(merged["QTD.A VENCER"], errors="coerce").fillna(0).astype(int)
    merged["VL.A VENCER"] = pd.to_numeric(merged["VL.A VENCER"], errors="coerce").fillna(0.0)
    merged["VL.CARTEIRA"] = pd.to_numeric(merged["VL.CARTEIRA"], errors="coerce").fillna(0.0)
    if "% PAGO" not in merged.columns:
        merged["% PAGO"] = 0.0
    if "% VENCIDO" not in merged.columns:
        merged["% VENCIDO"] = 0.0
    if "% A VENCER" not in merged.columns:
        merged["% A VENCER"] = 0.0
    merged["% PAGO"] = pd.to_numeric(merged["% PAGO"], errors="coerce").fillna(0.0)
    merged["% VENCIDO"] = pd.to_numeric(merged["% VENCIDO"], errors="coerce").fillna(0.0)
    merged["% A VENCER"] = pd.to_numeric(merged["% A VENCER"], errors="coerce").fillna(0.0)
    merged["EMPREENDIMENTO"] = merged["EMPREENDIMENTO"].fillna("").astype(str)
    merged["VENDA"] = merged["VENDA"].fillna("").astype(str)
    merged["CLIENTE"] = merged["CLIENTE"].fillna("").astype(str)
    merged["STATUS VENDA"] = merged["STATUS VENDA"].fillna("").astype(str)

    if "_venda_est" not in merged.columns:
        merged["_venda_est"] = ""

    idx_mapa_venda = _aplicar_mapa_venda_em_merged(merged, mapa_v)

    # Montagem final + classificação
    linhas = []
    for i, r in merged.iterrows():
        st_e = str(r.get("Status_Estoque", "") or "")
        mot = str(r.get("Motivo_Estoque", "") or "")
        st_c = str(r.get("Status_Construcao", "") or "")
        v_fin = str(r.get("VENDA", "") or "").strip()
        tem_v = bool(v_fin)
        q = int(r.get("QTD.VENCIDA", 0) or 0)
        cart = float(r.get("VL.CARTEIRA", 0) or 0)
        inad = float(r.get("VL.VENCIDO", 0) or 0)
        tem_est = bool(str(st_e or mot or st_c).strip())

        tipo_cruz = _inferir_tipo_cruzamento(i, idx_mapa_venda, r, st_e, mot, st_c)

        clf, obs = classificar_consolidado_estoque(
            st_e,
            str(r.get("STATUS VENDA", "") or ""),
            q,
            cart,
            inad,
            tem_v,
            tem_est,
        )
        situ = "ADIMPLENTE"
        if "DISPON" in _fold_upper(clf):
            situ = "DISPONIVEL"
        elif "QUITADO" in _fold_upper(clf):
            situ = "QUITADO"
        elif "INAD" in _fold_upper(clf) or "ATEN" in _fold_upper(clf) or "DIVERG" in _fold_upper(clf):
            situ = "INADIMPLENTE"
        extra = str(r.get("Valores_Ref", "") or "").strip()
        if extra and obs:
            obs = f"{obs} | Ref.: {extra}"
        elif extra and not obs:
            obs = f"Ref. estoque: {extra}"

        linhas.append(
            {
                "EMP/OBRA": str(r.get("EMP/OBRA", "") or "").strip(),
                "EMPREENDIMENTO": str(r.get("EMPREENDIMENTO", "") or "").strip(),
                "IDENTIFICADOR": str(r.get("IDENTIFICADOR", "") or "").strip(),
                "VENDA": v_fin,
                "CLIENTE": str(r.get("CLIENTE", "") or "").strip(),
                "SITUAÇÃO": situ,
                "QTD.PAGO": int(r.get("QTD.PAGO", 0) or 0),
                "VL.PAGO": round(float(r.get("VL.PAGO", 0) or 0), 2),
                "QTD.VENCIDA": q,
                "VL.VENCIDO": round(inad, 2),
                "QTD.A VENCER": int(r.get("QTD.A VENCER", 0) or 0),
                "VL.A VENCER": round(float(r.get("VL.A VENCER", 0) or 0), 2),
                "VL.CARTEIRA": round(cart, 2),
                "% PAGO": float(r.get("% PAGO", 0) or 0),
                "% VENCIDO": float(r.get("% VENCIDO", 0) or 0),
                "% A VENCER": float(r.get("% A VENCER", 0) or 0),
                "STATUS CONSTRUÇÃO": st_c if st_c else "N/A",
                "OBS": obs if obs else (mot if mot else tipo_cruz),
            }
        )

    ex = pd.DataFrame(linhas)
    if ex.empty:
        return out_empty
    ex["EMP/OBRA"] = ex["EMP/OBRA"].fillna("").astype(str).str.strip()
    ex["EMPREENDIMENTO"] = ex["EMPREENDIMENTO"].fillna("").astype(str).str.strip()
    ex["IDENTIFICADOR"] = ex["IDENTIFICADOR"].fillna("").astype(str).str.strip()
    ex["VENDA"] = ex["VENDA"].fillna("").astype(str).str.strip()
    ex["CLIENTE"] = ex["CLIENTE"].fillna("").astype(str).str.strip()
    ex["STATUS CONSTRUÇÃO"] = ex["STATUS CONSTRUÇÃO"].fillna("").astype(str).str.strip().replace("", "N/A")

    # Ordenação estável para consistência entre execuções (sem alterar classificação financeira).
    ex = ex.sort_values(
        by=["% VENCIDO", "QTD.VENCIDA", "EMP/OBRA", "IDENTIFICADOR", "VENDA"],
        ascending=[False, False, True, True, True],
        kind="mergesort",
    )
    return ex[COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE].reset_index(drop=True)
