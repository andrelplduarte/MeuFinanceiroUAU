# -*- coding: utf-8 -*-
"""Auditoria objetiva do CARTEIRAS GERAL.xlsx (estrutura, cores, formatos, exibição, totais)."""
from __future__ import annotations

import argparse
import re
import unicodedata
from decimal import Decimal

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string, get_column_letter

AZUL = "1F4E78"
VERDE = "92D050"
VERMELHO_ESPERADO = {"F8696B", "FF0000", "C00000", "E74C3C"}  # modelo/código comuns
AZUL_CLARO = "00B0F0"
AMARELO = "FFFF00"
BRANCO = "FFFFFF"
PRETO = "000000"

NOME_RESUMO = "RESUMO GERAL"

# Siglas com mapa oficial em processador_uau (para checar NÃO INFORMADO)
SIGLAS_MAPA = frozenset(
    "NVLOT LTMAG SCPTO SCPTI CIDAN VROLT ALVLT LTMON RVERD LTVIL LTMIN SCPGO ARAHF BVGWH MANHA MONTB LIFE".split()
)


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s.strip().upper())


def _norm_cols_df(df):
    return {_fold(c): c for c in df.columns}


def _norm_rgb(v) -> str:
    if v is None:
        return ""
    s = str(v).upper().replace("#", "")
    if len(s) == 8 and s.startswith("FF"):
        s = s[2:]
    elif len(s) == 8 and s.startswith("00"):
        s = s[2:]  # ARGB openpyxl
    return s


def _cell_fill_rgb(cell) -> str:
    try:
        fg = cell.fill.fgColor
        if fg and fg.type == "rgb" and fg.rgb:
            return _norm_rgb(fg.rgb)
        if fg and fg.type == "theme":
            return ""  # ignorar theme sem resolver
    except Exception:
        pass
    return ""


def _merged_ranges(ws):
    return list(ws.merged_cells.ranges)


def _range_covers_row(mr, row: int) -> bool:
    return mr.min_row <= row <= mr.max_row


def _range_text(mr) -> str:
    return f"{get_column_letter(mr.min_col)}{mr.min_row}:{get_column_letter(mr.max_col)}{mr.max_row}"


def audit(path: str) -> dict:
    out: dict = {"path": path, "ok": True, "falhas": [], "avisos": []}

    wb = load_workbook(path, data_only=False)
    names = wb.sheetnames

    if NOME_RESUMO not in names:
        out["ok"] = False
        out["falhas"].append("Aba RESUMO GERAL ausente.")
        return out

    # --- 1) RESUMO GERAL estrutura linha 7 ---
    ws_r = wb[NOME_RESUMO]
    merges_r7 = [mr for mr in _merged_ranges(ws_r) if mr.min_row == mr.max_row == 7]
    out["resumo_merges_linha7"] = sorted(_range_text(m) for m in merges_r7)
    esperado_r7 = {"A7:C7", "D7:E7", "F7:G7", "H7:I7", "J7:M7", "N7:N7"}
    if set(out["resumo_merges_linha7"]) != esperado_r7:
        out["ok"] = False
        out["falhas"].append(
            f"RESUMO linha7 merges esperado {sorted(esperado_r7)} obteve {out['resumo_merges_linha7']}"
        )

    # Cores títulos bloco linha 7 RESUMO (célula canto sup. esquerdo de cada merge)
    blocos_rg = [
        ("A7", AZUL, BRANCO),
        ("D7", VERDE, BRANCO),
        ("F7", None, BRANCO),  # vermelho flexível
        ("H7", AZUL_CLARO, BRANCO),
        ("J7", AMARELO, PRETO),
        ("N7", AZUL, BRANCO),
    ]
    for ref, fg_e, font_e in blocos_rg:
        c = ws_r[ref]
        rgb = _cell_fill_rgb(c)
        if ref == "F7":
            if rgb not in VERMELHO_ESPERADO and rgb != "":
                out["avisos"].append(f"RESUMO F7 fill={rgb} (vermelho não padrão)")
        elif fg_e and rgb != fg_e:
            out["ok"] = False
            out["falhas"].append(f"RESUMO {ref} fill esperado {fg_e} obteve {rgb or '(vazio/theme)'}")

    # --- Abas empreendimento ---
    emp_sheets = [n for n in names if n != NOME_RESUMO]
    out["abas_empreendimento"] = emp_sheets
    out["emp_audit"] = {}

    for sn in emp_sheets:
        ws = wb[sn]
        ad = {"nome": sn}

        # Colunas usadas até AA
        ad["max_column"] = ws.max_column
        if ws.max_column < column_index_from_string("AA"):
            out["ok"] = False
            out["falhas"].append(f"[{sn}] max_column {ws.max_column} < 27 (AA)")

        merges_7 = [mr for mr in _merged_ranges(ws) if mr.min_row == mr.max_row == 7]
        ad["merges_linha7"] = sorted(_range_text(m) for m in merges_7)
        esp7 = {"A7:G7", "H7:K7", "L7:R7", "S7:T7", "U7:X7", "Y7:AA7"}
        if set(ad["merges_linha7"]) != esp7:
            out["ok"] = False
            out["falhas"].append(f"[{sn}] linha7 merges esperado {sorted(esp7)} obteve {ad['merges_linha7']}")

        # Autofiltro linha 8
        af = ws.auto_filter
        ad["auto_filter_ref"] = str(af.ref) if af and af.ref else ""
        if not ad["auto_filter_ref"]:
            out["ok"] = False
            out["falhas"].append(f"[{sn}] autofiltro ausente")
        else:
            # deve começar em A8 e incluir linha 8
            if not ad["auto_filter_ref"].upper().startswith("A8"):
                out["avisos"].append(f"[{sn}] autofiltro ref={ad['auto_filter_ref']} (esperado iniciar A8)")

        # Cores blocos linha 7 (A7,H7,L7,S7,U7,Y7)
        checks = [
            ("A7", AZUL, BRANCO),
            ("H7", VERDE, BRANCO),
            ("L7", None, BRANCO),
            ("S7", AZUL_CLARO, BRANCO),
            ("U7", AMARELO, PRETO),
            ("Y7", AZUL, BRANCO),
        ]
        for ref, fg_e, font_e in checks:
            c = ws[ref]
            rgb = _cell_fill_rgb(c)
            if ref == "L7":
                if rgb not in VERMELHO_ESPERADO and rgb != "":
                    out["avisos"].append(f"[{sn}] L7 fill={rgb}")
            elif fg_e and rgb != fg_e:
                out["ok"] = False
                out["falhas"].append(f"[{sn}] {ref} fill esperado {fg_e} obteve {rgb or '(vazio)'}")

        # Cabeçalho linha 8: amostra cores por coluna (A,G,H,L,S,U,Y)
        hdr_samples = {}
        for col_letter in ("A", "G", "H", "L", "S", "U", "Y"):
            cell = ws[f"{col_letter}8"]
            hdr_samples[col_letter] = _cell_fill_rgb(cell)
        ad["header8_fill_sample"] = hdr_samples

        out["emp_audit"][sn] = ad

    wb.close()

    # --- pandas: dados linha 9+ , header linha 8 (índice 7) ---
    # Colunas exportadas em CAIXA ALTA com pontos (ex.: VL.CARTEIRA).
    COL_DET = {
        "Vl.Carteira": "VL.CARTEIRA",
        "Vl.Pago": "VL.PAGO",
        "Vl.Vencer": "VL.VENCER",
        "Vl.Principal (Encargos)": "VL.PRINCIPAL (ENCARGOS)",
        "Qtd.Parc.Atrasada": "QTD.PARC.ATRASADA",
    }
    COL_RES = {
        "Vl.Pago": "VALOR PAGO",
        "Vl.Vencer": "VALOR A VENCER",
        "Vl.Principal (Encargos)": "VALOR INADIMPLÊNCIA",
        "Vl.Carteira": "VL.CARTEIRA",
        "Qtd.Parc.Atrasada": "QTD PARC. INADIMPLÊNCIA",
    }

    def sum_metric_all_emp(metric: str) -> float:
        col = _fold(COL_DET[metric])
        total = Decimal("0")
        for sn in emp_sheets:
            df = pd.read_excel(path, sheet_name=sn, header=7, engine="openpyxl")
            m = _norm_cols_df(df)
            if col not in m:
                continue
            s = pd.to_numeric(df[m[col]], errors="coerce").fillna(0)
            total += Decimal(str(float(s.sum())))
        return float(total)

    metrics = list(COL_DET.keys())
    out["somas_abas_empreendimento"] = {m: sum_metric_all_emp(m) for m in metrics}

    df_res = pd.read_excel(path, sheet_name=NOME_RESUMO, header=7, engine="openpyxl")
    mr = _norm_cols_df(df_res)
    out["somas_resumo_geral"] = {}
    for m in metrics:
        rc = _fold(COL_RES[m])
        if rc in mr:
            s = pd.to_numeric(df_res[mr[rc]], errors="coerce").fillna(0)
            out["somas_resumo_geral"][m] = float(s.sum())
        else:
            out["somas_resumo_geral"][m] = None

    # Cruzamento: soma linha-a-linha (abas empreendimento) vs soma agregados (RESUMO)
    out["delta_resumo_vs_soma_abas"] = {}
    tol = 0.02
    for m in metrics:
        a = out["somas_abas_empreendimento"].get(m, 0)
        b = out["somas_resumo_geral"].get(m)
        if b is None:
            out["delta_resumo_vs_soma_abas"][m] = None
            out["ok"] = False
            out["falhas"].append(f"Coluna resumo ausente para métrica [{m}]")
            continue
        out["delta_resumo_vs_soma_abas"][m] = round(a - b, 2)
        if abs(a - b) > tol:
            out["ok"] = False
            out["falhas"].append(f"Soma [{m}] detalhe={a:.2f} vs resumo={b:.2f} diff={a-b:.2f}")

    # Antes x Depois (mesmo arquivo, dupla leitura): detecta corrupção de leitura; diff esperado 0
    out["somas_abas_empreendimento_rep"] = {m: sum_metric_all_emp(m) for m in metrics}
    out["delta_leitura_dupla"] = {
        m: round(out["somas_abas_empreendimento"][m] - out["somas_abas_empreendimento_rep"][m], 6)
        for m in metrics
    }

    # --- Integridade exibição ---
    corrupt_re = re.compile(r"BVGWH{2,}", re.I)
    out["emp_obra_corrompidos"] = []
    out["nao_informado_com_sigla"] = []

    for sn in emp_sheets:
        df = pd.read_excel(path, sheet_name=sn, header=7, engine="openpyxl")
        mc = _norm_cols_df(df)
        c_eo = mc.get(_fold("Emp/Obra")) or mc.get(_fold("EMP/OBRA"))
        c_emp = mc.get(_fold("Empreendimento")) or mc.get(_fold("EMPREENDIMENTO"))
        if not c_eo or not c_emp:
            continue
        for idx, val in df[c_eo].items():
            s = str(val).strip().upper()
            if corrupt_re.search(s) or "BVGWHHHH" in s or "BVGWHHH" in s:
                out["emp_obra_corrompidos"].append({"aba": sn, "linha": idx + 9, "valor": val})
        emp_col = df[c_emp].fillna("").astype(str).str.strip()
        eo_col = df[c_eo].fillna("").astype(str).str.strip()
        for idx, (eo, emp) in enumerate(zip(eo_col, emp_col)):
            if "NÃO INFORMADO" not in emp.upper() and "NAO INFORMADO" not in emp.upper():
                continue
            # extrair sigla após /
            parts = eo.upper().split("/")
            sig = parts[-1].strip() if len(parts) >= 2 else ""
            sig = re.sub(r"[^A-Z0-9]", "", sig)
            if sig in SIGLAS_MAPA:
                out["nao_informado_com_sigla"].append({"aba": sn, "linha": idx + 9, "Emp/Obra": eo})

    if out["emp_obra_corrompidos"]:
        out["ok"] = False
        out["falhas"].append(f"EMP/OBRA corrompidos: {len(out['emp_obra_corrompidos'])} ocorrências")
    if out["nao_informado_com_sigla"]:
        out["ok"] = False
        out["falhas"].append(
            f"NÃO INFORMADO com sigla mapeável: {len(out['nao_informado_com_sigla'])} linhas"
        )

    # --- ####### : largura vs comprimento string numérica ---
    wb2 = load_workbook(path, data_only=True)
    hashes = []
    money_cols_letters = {"G", "J", "L", "M", "N", "O", "P", "Q", "S", "T"}  # consolidado padrão
    for sn in emp_sheets:
        ws = wb2[sn]
        max_r = min(ws.max_row or 0, 5000)
        for row in range(9, max_r + 1):
            for col in range(1, min(ws.max_column or 0, 27) + 1):
                cell = ws.cell(row=row, column=col)
                if cell.value is None:
                    continue
                if isinstance(cell.value, str) and cell.value.strip() == "#######":
                    hashes.append({"aba": sn, "cell": cell.coordinate})
                    continue
                letter = get_column_letter(col)
                if letter in money_cols_letters and isinstance(cell.value, (int, float)) and cell.value != 0:
                    wch = ws.column_dimensions[letter].width or 8.43
                    # heurística grosseira: valor > 1e6 com col estreita
                    if abs(float(cell.value)) >= 1_000_000 and wch < 10:
                        hashes.append({"aba": sn, "cell": cell.coordinate, "hint": "valor grande col estreita"})
    wb2.close()
    out["celulas_hash"] = hashes[:50]
    out["celulas_hash_total"] = len(hashes)
    if hashes:
        out["avisos"].append(f"Possível exibição truncada (#### ou col estreita): {len(hashes)} casos")

    # --- Formatos numéricos (amostra linha 10) ---
    wb3 = load_workbook(path, data_only=False)
    fmt_samples = {}
    for sn in emp_sheets[:1] or []:
        ws = wb3[sn]
        for letter, esperado_sub in [("G", "R$"), ("J", "R$"), ("U", "%"), ("C", "0")]:
            c = ws[f"{letter}10"]
            fmt_samples[f"{sn}!{letter}10"] = (c.number_format or "")
    wb3.close()
    out["formato_amostra"] = fmt_samples

    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("xlsx", nargs="?", default="outputs/CARTEIRAS GERAL.xlsx")
    args = p.parse_args()
    r = audit(args.xlsx)
    import json

    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
