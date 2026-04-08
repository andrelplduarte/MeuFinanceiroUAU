# -*- coding: utf-8 -*-
"""
Pipeline de pré-processamento bruto de TXT exportados pelo UAU.

Objetivo: robustez de entrada (ETL) antes do parsing em processador_uau.py.
Não altera lógica financeira nem estrutura do Excel — apenas normaliza o texto bruto.

Regras:
- Não remover marcadores necessários a identificar_tipo_relatorio_uau_por_texto
  (ex.: presença de "CONTAS A RECEBER" / "CONTAS RECEBIDAS" no texto global).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

# Acumulado por execução do pipeline (reset explícito em processar_e_gerar_excel).
ETL_STATS_ACUMULADO: Dict[str, Any] = {
    "linhas_entrada": 0,
    "linhas_saida": 0,
    "descarte_ruido": 0,
    "descarte_fragmento": 0,
    "descarte_cabecalho_repetido": 0,
    "amostras": {"ruido": [], "fragmento": [], "cabecalho_repetido": []},
}


def reset_etl_stats_acumulado() -> None:
    global ETL_STATS_ACUMULADO
    ETL_STATS_ACUMULADO = {
        "linhas_entrada": 0,
        "linhas_saida": 0,
        "descarte_ruido": 0,
        "descarte_fragmento": 0,
        "descarte_cabecalho_repetido": 0,
        "amostras": {"ruido": [], "fragmento": [], "cabecalho_repetido": []},
    }


def obter_etl_stats_acumulado() -> Dict[str, Any]:
    return dict(ETL_STATS_ACUMULADO)


def _push_amostra(bucket: List[str], linha: str, max_itens: int = 5, max_chars: int = 180) -> None:
    if len(bucket) >= max_itens:
        return
    s = str(linha or "").replace("\r", "").strip()
    if not s:
        return
    bucket.append(s[:max_chars])

# Alinhado ao motor principal (evita import circular com processador_uau).
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
EMP_RE = re.compile(r"^\d+/\S+")

# Continuação típica de identificação de unidade (linha seguinte sem novo Emp/Obra).
_CONT_UNIDADE_RE = re.compile(
    r"(?i)^\s*(QUADRA|LOTE|QD\.?|LT\.?|APTO|APT|APART|BLOCO|TORRE|UNIDADE|CASA|LOTEAMENTO)\b"
)


def _split_linha_tabular(line: str) -> List[str]:
    raw = str(line or "").replace("\ufeff", "").rstrip("\r\n")
    if not raw:
        return []
    if "\t" in raw:
        return [p.strip() for p in raw.split("\t")]
    return [p.strip() for p in re.split(r"\s{2,}", raw)]


def _linha_descartavel_ruido(linha: str) -> bool:
    s = str(linha or "").strip()
    if not s:
        return False
    sup = s.upper()

    if "UAU!" in sup and "SOFTWARE" in sup:
        return True
    if re.match(r"^\s*PÁGINA\s+\d+", s, re.IGNORECASE):
        return True
    if re.match(r"^\s*PAGINA\s+\d+", s, re.IGNORECASE):
        return True
    if re.search(r"PÁGINA\s+\d+\s*/\s*\d+", s, re.IGNORECASE):
        return True
    if re.search(r"PAGINA\s+\d+\s*/\s*\d+", s, re.IGNORECASE):
        return True
    if "TOTAL POR CLIENTE" in sup or "TOTAL CLIENTE" in sup:
        return True
    if re.match(r"^TOTAL\s+CLIENTE", sup):
        return True
    # Somatórios intermediários comuns em exportações UAU
    if re.match(r"^\s*SUBTOTAL\b", sup):
        return True
    if re.match(r"^\s*SOMA\s+DO\s+", sup):
        return True
    if "SOMATORIO" in sup or "SOMATÓRIO" in sup:
        return True
    if re.match(r"^\s*TOTAL\s+GERAL\b", sup):
        return True
    # Linhas só com valores monetários agregados (rodapé de página)
    if re.match(r"^[\d\.\,\sR\$\(\)]{8,}$", s) and len(s) < 120:
        return True

    return False


def _eh_cabecalho_tabela_principal(s: str) -> bool:
    t = str(s or "").strip().upper()
    return "EMP/OBRA" in t and "VENDA" in t and "CLIENTE" in t


def _linha_fragmento_contaminante(linha: str) -> bool:
    """
    Linha sem estrutura mínima de registro: fragmento numérico ou pedaço de unidade isolado.
    Não remove linhas que já casem como Emp/Obra principal.
    """
    raw = str(linha or "").replace("\ufeff", "").strip()
    if not raw:
        return False
    first_tok = raw.split()[0] if raw.split() else ""
    if EMP_RE.match(first_tok.strip()):
        return False
    parts = _split_linha_tabular(raw)
    if len(parts) >= 3:
        return False
    s = raw.strip()
    sup = s.upper()
    if re.match(r"^\d{1,4}$", s):
        return True
    if re.match(r"^\d{1,2}\s*[/\\]\s*\d{1,4}$", s):
        return True
    if re.match(r"^\d{1,2}\s*/\s*LOTE\b", sup):
        return True
    if _CONT_UNIDADE_RE.match(s) and len(s) < 80:
        return True
    # Só dígitos, barra, espaço e poucos caracteres (ex.: "10 / 21")
    if len(s) <= 24 and re.match(r"^[\d\s/\\.,-]+$", s):
        return True
    return False


def _remover_cabecalhos_repetidos_consecutivos(linhas: List[str]) -> List[str]:
    """Mantido para compatibilidade; preferir filtro global no preprocessamento."""
    out: List[str] = []
    prev_header = False
    for ln in linhas:
        is_h = _eh_cabecalho_tabela_principal(ln)
        if is_h and prev_header:
            continue
        out.append(ln)
        prev_header = is_h
    return out


def _filtrar_cabecalhos_repetidos_nao_consecutivos(linhas: List[str]) -> List[str]:
    """
    Mantém apenas a primeira ocorrência do cabeçalho EMP/OBRA…VENDA…CLIENTE;
    descarta todas as repetições (mesmo com linhas de dados entre elas).
    """
    out: List[str] = []
    header_relevante_visto = False
    for ln in linhas:
        if _eh_cabecalho_tabela_principal(ln):
            if header_relevante_visto:
                continue
            header_relevante_visto = True
        out.append(ln)
    return out


def _parece_linha_dados_incompleta(parts: List[str]) -> bool:
    if len(parts) < 2:
        return False
    if not EMP_RE.match(str(parts[0]).strip()):
        return False
    return len(parts) < 7


def _proxima_e_nova_linha_principal(linha_seguinte: str) -> bool:
    ps = _split_linha_tabular(linha_seguinte)
    if len(ps) < 2:
        return False
    if not EMP_RE.match(str(ps[0]).strip()):
        return False
    for idx in range(3, min(len(ps), 9)):
        if DATE_RE.match(str(ps[idx]).strip()):
            return True
    return len(ps) >= 7


def _eh_linha_continuacao_unidade_ou_endereco(linha: str) -> bool:
    """
    Próxima linha é continuação (texto de unidade / endereço) sem novo Emp/Obra tabular.
    """
    raw = str(linha or "").strip()
    if not raw:
        return False
    ps = _split_linha_tabular(raw)
    if ps and EMP_RE.match(str(ps[0]).strip()):
        return False
    if _eh_cabecalho_tabela_principal(raw):
        return False
    if _linha_descartavel_ruido(raw):
        return False
    if _CONT_UNIDADE_RE.match(raw):
        return True
    # Linha curta alfabética com tokens típicos de endereço
    sup = raw.upper()
    if len(raw) < 120 and any(
        w in sup for w in ("QUADRA", "LOTE", "APTO", "BLOCO", "TORRE", "CASA", "UNIDADE", "QD ", " LT ")
    ):
        if not DATE_RE.match(raw.strip()):
            return True
    return False


def _fundir_linhas_dados_quebradas(linhas: List[str]) -> List[str]:
    """
    Concatena linhas quando o registro parece partido (Emp/Obra válido porém poucas colunas).
    Usa espaço entre fragmentos; split_linha_tabular do motor tolera separadores.
    """
    if not linhas:
        return linhas
    out: List[str] = []
    i = 0
    max_fusoes = 8
    while i < len(linhas):
        cur = linhas[i]
        parts = _split_linha_tabular(cur)
        merges = 0
        while i + 1 < len(linhas) and merges < max_fusoes:
            nxt = linhas[i + 1]
            if _proxima_e_nova_linha_principal(nxt):
                break
            if _eh_cabecalho_tabela_principal(nxt):
                break
            if _linha_descartavel_ruido(nxt):
                i += 1
                merges += 1
                continue
            if _parece_linha_dados_incompleta(parts):
                cur = f"{cur.rstrip()} {str(nxt).strip()}"
                parts = _split_linha_tabular(cur)
                i += 1
                merges += 1
                continue
            if _eh_linha_continuacao_unidade_ou_endereco(nxt):
                cur = f"{cur.rstrip()} {str(nxt).strip()}"
                parts = _split_linha_tabular(cur)
                i += 1
                merges += 1
                continue
            break
        out.append(cur)
        i += 1
    return out


def preprocessar_texto_uau_bruto(texto: str) -> str:
    """
    Etapa 1 do pipeline ETL: limpeza e resiliência do TXT antes do parsing.

    - Remove linhas de ruído (UAU Software, Página, totais por cliente).
    - Remove cabeçalhos Emp/Obra repetidos (qualquer repetição após o primeiro).
    - Remove fragmentos contaminantes soltos.
    - Funde linhas de dados claramente quebradas e continuações de unidade (LOTE/QUADRA/...).

    Não remove linhas que contenham apenas os títulos de relatório usados na identificação
    do tipo de arquivo (mantém ocorrências de "Contas a Receber" / "Contas Recebidas" no texto).

    Atualiza ETL_STATS_ACUMULADO (use reset_etl_stats_acumulado no início do lote).
    """
    global ETL_STATS_ACUMULADO
    if texto is None:
        return ""
    t = str(texto).replace("\x00", "")
    raw_lines = t.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    n_in = len(raw_lines)
    loc_r, loc_f, loc_c = 0, 0, 0
    sam_r: List[str] = []
    sam_f: List[str] = []
    sam_c: List[str] = []

    filtradas: List[str] = []
    for ln in raw_lines:
        if _linha_descartavel_ruido(ln):
            loc_r += 1
            _push_amostra(sam_r, ln)
            continue
        if _linha_fragmento_contaminante(ln):
            loc_f += 1
            _push_amostra(sam_f, ln)
            continue
        filtradas.append(ln)

    out_cab: List[str] = []
    header_relevante_visto = False
    for ln in filtradas:
        if _eh_cabecalho_tabela_principal(ln):
            if header_relevante_visto:
                loc_c += 1
                _push_amostra(sam_c, ln)
                continue
            header_relevante_visto = True
        out_cab.append(ln)

    fundidas = _fundir_linhas_dados_quebradas(out_cab)
    n_out = len(fundidas)

    ETL_STATS_ACUMULADO["linhas_entrada"] = int(ETL_STATS_ACUMULADO.get("linhas_entrada", 0)) + n_in
    ETL_STATS_ACUMULADO["linhas_saida"] = int(ETL_STATS_ACUMULADO.get("linhas_saida", 0)) + n_out
    ETL_STATS_ACUMULADO["descarte_ruido"] = int(ETL_STATS_ACUMULADO.get("descarte_ruido", 0)) + loc_r
    ETL_STATS_ACUMULADO["descarte_fragmento"] = int(ETL_STATS_ACUMULADO.get("descarte_fragmento", 0)) + loc_f
    ETL_STATS_ACUMULADO["descarte_cabecalho_repetido"] = int(
        ETL_STATS_ACUMULADO.get("descarte_cabecalho_repetido", 0)
    ) + loc_c
    am = ETL_STATS_ACUMULADO.setdefault("amostras", {"ruido": [], "fragmento": [], "cabecalho_repetido": []})
    for x in sam_r:
        _push_amostra(am.setdefault("ruido", []), x)
    for x in sam_f:
        _push_amostra(am.setdefault("fragmento", []), x)
    for x in sam_c:
        _push_amostra(am.setdefault("cabecalho_repetido", []), x)

    return "\n".join(fundidas)
