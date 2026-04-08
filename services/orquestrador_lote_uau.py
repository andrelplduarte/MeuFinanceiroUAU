# -*- coding: utf-8 -*-
"""
Orquestração em lote acima do motor processar_e_gerar_excel.
Não altera regras financeiras — apenas funde TXT, agrupa por empreendimento e compõe workbooks.
"""
from __future__ import annotations

import os
import re
import shutil
import tempfile
import time
import uuid
from collections import defaultdict
from copy import copy
from datetime import datetime
from typing import Dict, List, Sequence, Tuple

import pandas as pd
from openpyxl import Workbook, load_workbook

from services.estoque_uau import (
    COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE,
    CONSOLIDADO_ESTOQUE_PANDAS_STARTROW,
    NOME_ABA_CONSOLIDADO_ESTOQUE,
    calcular_indicadores_painel_consolidado_estoque,
    carregar_estoque_bruto,
    montar_dataframe_consolidado_estoque,
)
from services.processador_uau import (
    NOME_ABA_RESUMO_GERAL,
    ProcessamentoUAUErro,
    aplicar_estilo_excel,
    aplicar_estilo_arquivo_so_aba_consolidado_estoque,
    aplicar_estilo_arquivo_so_aba_resumo_geral,
    extrair_nome_empreendimento_nome_arquivo,
    extrair_nome_empreendimento_txt,
    identificar_tipo_relatorio_uau_por_texto,
    ler_dataframe_consolidado_de_xlsx_motor,
    ler_texto_robusto,
    limpar_nome_empreendimento,
    montar_dataframe_resumo_geral,
    processar_e_gerar_excel,
    sanitizar_nome_arquivo,
    _estrutura_minima_uau_ok,
    _ler_texto_validacao_entrada,
)

MODO_POR_EMPREENDIMENTO = "POR_EMPREENDIMENTO"

# Prefixos genéricos no nome do arquivo que não são sigla de obra (ex.: REC_*, RECEBER_*).
# Se o primeiro segmento for só isso, o regex antigo colapsava todo o lote numa única chave "REC".
_PREFIXOS_NAO_SIGLA = frozenset(
    {
        "REC",
        "RECEBER",
        "RECEBIDOS",
        "RECEB",
        "PAG",
        "PAGOS",
        "CONTAS",
        "LOT",
        "LOTE",
        "DADOS",
        "UAC",
        "UAU",
        "EMP",
        "FILE",
        "TEMP",
        "TXT",
        "REL",
        "RELATORIO",
    }
)


def _sigla_curta_do_caminho(caminho: str) -> str:
    """Prefixo tipo SCPGO a partir do nome do arquivo (ex.: ALVLT, SCPGO)."""
    try:
        b = os.path.basename(str(caminho or "")).upper()
    except Exception:
        b = ""
    # Uploads Flask: 00_SCPGO_-LOT... → ignorar prefixo numérico
    b = re.sub(r"^\d+_", "", b)
    base_sem_ext = os.path.splitext(b)[0]
    # Preferir o primeiro token alfanumérico que não seja prefixo de ruído (evita REC, RECEBER, LOT…).
    partes = [p for p in re.split(r"[_\-\s]+", base_sem_ext) if p]
    for raw in partes:
        token = sanitizar_nome_arquivo(raw.split(".")[0])
        if not token or token.isdigit():
            continue
        if token in _PREFIXOS_NAO_SIGLA:
            continue
        if len(token) < 2:
            continue
        if not re.match(r"^[A-Z0-9]+$", token):
            continue
        return token[:20]
    m = re.match(r"^([A-Z0-9]+)[_-]", b)
    if m:
        return sanitizar_nome_arquivo(m.group(1))[:20]
    base = os.path.splitext(b)[0][:12]
    return sanitizar_nome_arquivo(base) if base else "EMP"


def _chave_pareamento_por_prefixo_arquivo(caminho: str) -> str:
    """
    Pareamento estável no modo por empreendimento: mesma sigla no nome do arquivo
    (Receber e Recebidos do mesmo código, ex. LTMIN / LTMIN).
    Se não houver sigla reconhecível, cai no agrupamento por nome canônico do TXT.
    """
    s = _sigla_curta_do_caminho(caminho)
    if s and s != "EMP":
        return s
    return _chave_grupo_empreendimento(caminho)


def _chave_grupo_empreendimento(caminho: str) -> str:
    k = extrair_nome_empreendimento_txt(caminho) or extrair_nome_empreendimento_nome_arquivo(caminho)
    k = limpar_nome_empreendimento(k)
    if not k:
        k = _sigla_curta_do_caminho(caminho)
    return sanitizar_nome_arquivo(k)


def _validar_tipo_arquivo(caminho: str, esperado: str, campo: str) -> None:
    texto = _ler_texto_validacao_entrada(caminho)
    tipo = identificar_tipo_relatorio_uau_por_texto(texto)
    if esperado == "RECEBER" and tipo != "RECEBER":
        raise ProcessamentoUAUErro(
            etapa="validação de entrada",
            funcao="orquestrador_lote_uau",
            validacao="tipo de relatório",
            mensagem=f"Arquivo em {campo} não foi reconhecido como Contas a Receber: {os.path.basename(caminho)}",
            campo_ou_aba=campo,
        )
    if esperado == "RECEBIDOS" and tipo != "RECEBIDOS":
        raise ProcessamentoUAUErro(
            etapa="validação de entrada",
            funcao="orquestrador_lote_uau",
            validacao="tipo de relatório",
            mensagem=f"Arquivo em {campo} não foi reconhecido como Contas Recebidas: {os.path.basename(caminho)}",
            campo_ou_aba=campo,
        )
    if not _estrutura_minima_uau_ok(texto, esperado):
        msg = (
            "Estrutura mínima incompatível (Contas a Receber)."
            if esperado == "RECEBER"
            else "Estrutura mínima incompatível (Contas Recebidas)."
        )
        raise ProcessamentoUAUErro(
            etapa="validação de entrada",
            funcao="orquestrador_lote_uau",
            validacao="estrutura mínima",
            mensagem=f"{msg} Arquivo: {os.path.basename(caminho)}",
            campo_ou_aba=campo,
        )


def _fundir_textos_em_temp(caminhos: Sequence[str], prefixo: str) -> str:
    partes = []
    for p in caminhos:
        partes.append(ler_texto_robusto(p))
    corpo = "\n".join(partes)
    fd, path = tempfile.mkstemp(prefix=prefixo + "_", suffix=".txt", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(corpo)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return path


def _remover_seguro(path: str) -> None:
    try:
        if path and os.path.isfile(path):
            os.unlink(path)
    except OSError:
        pass


def _resolver_estoque_unificado_lote(caminhos_est: Sequence[str], temporarios: List[str]) -> str | None:
    """Funde vários TXT de estoque num único ficheiro temporário (modo unificado / par único com N estoques)."""
    paths = [os.path.abspath(os.path.normpath(p)) for p in (caminhos_est or []) if p]
    if not paths:
        return None
    if len(paths) == 1:
        return paths[0]
    m = _fundir_textos_em_temp(paths, "uau_est_u_")
    temporarios.append(m)
    return m


def _resolver_estoque_por_chave_lote(
    chave: str, caminhos_est: Sequence[str], temporarios: List[str]
) -> str | None:
    """
    Um ficheiro de estoque → aplicado a todos os grupos.
    Vários ficheiros → agrupa pela mesma chave de prefixo do nome (paralelo ao Receber/Recebidos).
    """
    paths = [os.path.abspath(os.path.normpath(p)) for p in (caminhos_est or []) if p]
    if not paths:
        return None
    if len(paths) == 1:
        return paths[0]
    grupos: Dict[str, List[str]] = defaultdict(list)
    for p in paths:
        grupos[_chave_pareamento_por_prefixo_arquivo(p)].append(p)
    lst = sorted(grupos.get(chave, []))
    if not lst:
        return None
    if len(lst) == 1:
        return lst[0]
    m = _fundir_textos_em_temp(lst, f"est_{chave}_")
    temporarios.append(m)
    return m


def _data_base_de_primeiro_xlsx_motor(caminho: str):
    """Lê B2 do painel do Consolidado no workbook gerado pelo motor."""
    if not caminho or not os.path.isfile(caminho):
        return None
    wb = None
    try:
        wb = load_workbook(caminho, data_only=True)
        for nm in wb.sheetnames:
            su = str(nm).upper().replace("Í", "I")
            if "CONSOLIDADO" in su and "CRIT" not in su:
                v = wb[nm]["B2"].value
                if isinstance(v, datetime):
                    return v
                if isinstance(v, str) and str(v).strip():
                    try:
                        return datetime.strptime(str(v).strip(), "%d/%m/%Y")
                    except ValueError:
                        return None
                return None
    except Exception:
        return None
    finally:
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass
    return None


def _mapear_titulo_aba_por_empreendimento(nome_original: str, sigla: str) -> str:
    n = str(nome_original or "").strip()
    nu = n.upper()
    if nu == "CRITÉRIOS" or nu == "CRITERIOS" or nu == "CRITERIOS ANALISES":
        return "CRITERIOS ANALISES"
    if "CONSOLIDADO" in nu and "CRIT" not in nu:
        return f"{sigla} – CONSOLIDADO"
    if n == "Dados Receber":
        return f"DADOS RECEBER - {sigla}"
    if n == "Dados Recebidos":
        return f"DADOS RECEBIDOS - {sigla}"
    if n == "Pendencias_Parcelas" or n == "PENDENCIAS_PARCELAS" or n == "PEND.PARCELAS":
        return f"PEND.PARCELAS - {sigla}"
    if n == "RELATORIO ANALITICO" or "RELATORIO" in nu:
        return f"RELATORIO ANALITICO - {sigla}"
    return f"{sigla} – {n}"


def _titulo_aba_unico(_wb: Workbook, titulo: str, usados: set) -> str:
    base = (titulo or "ABA")[:31]
    nome = base
    i = 2
    while nome in usados:
        suf = f"_{i}"
        nome = (base[: 31 - len(suf)] + suf)[:31]
        i += 1
    usados.add(nome)
    return nome


def _copiar_planilha_estilizada(ws_src, wb_dst, titulo: str):
    ws = wb_dst.create_sheet(title=titulo)
    max_r = ws_src.max_row or 1
    max_c = ws_src.max_column or 1
    for r in range(1, max_r + 1):
        for c in range(1, max_c + 1):
            src = ws_src.cell(row=r, column=c)
            dst = ws.cell(row=r, column=c)
            dst.value = src.value
            if src.has_style:
                try:
                    dst.font = copy(src.font)
                    dst.fill = copy(src.fill)
                    dst.border = copy(src.border)
                    dst.alignment = copy(src.alignment)
                    dst.number_format = src.number_format
                except Exception:
                    pass
    for col_letter, dim in ws_src.column_dimensions.items():
        if dim.width is not None:
            ws.column_dimensions[col_letter].width = dim.width
        if dim.hidden:
            ws.column_dimensions[col_letter].hidden = True
    for row_idx, rd in ws_src.row_dimensions.items():
        if rd.height is not None:
            ws.row_dimensions[row_idx].height = rd.height
    try:
        merges = sorted(
            ws_src.merged_cells.ranges,
            key=lambda x: (x.min_row, x.min_col, x.max_row, x.max_col),
        )
        for mc in merges:
            ws.merge_cells(str(mc))
    except Exception:
        pass
    if ws_src.freeze_panes:
        ws.freeze_panes = ws_src.freeze_panes
    if ws_src.auto_filter and ws_src.auto_filter.ref:
        ws.auto_filter.ref = ws_src.auto_filter.ref


def _nome_aba_consolidado_motor(wb) -> str | None:
    """Aba gerencial por obra (exclui estoque e critérios)."""
    for s in wb.sheetnames:
        su = str(s).upper().replace("Í", "I")
        if "CONSOLIDADO" in su and "CRIT" not in su and "ESTOQUE" not in su:
            return s
    return None


def _anexar_somente_consolidado_por_sigla(
    wb_dest: Workbook, caminho_xlsx: str, sigla: str, titulos_usados: set
) -> None:
    """No lote por empreendimento: só a aba consolidada da obra entra separada no workbook final."""
    wb = load_workbook(caminho_xlsx, data_only=False)
    try:
        nome = _nome_aba_consolidado_motor(wb)
        if not nome:
            return
        titulo = _mapear_titulo_aba_por_empreendimento("Consolidado Venda", sigla)
        titulo_final = _titulo_aba_unico(wb_dest, titulo[:31], titulos_usados)
        _copiar_planilha_estilizada(wb[nome], wb_dest, titulo_final)
    finally:
        wb.close()


def _ler_df_aba_xlsx_motor(caminho: str, nome_aba: str, header_row_0based: int) -> pd.DataFrame:
    """Lê aba já formatada pelo motor (cabeçalho tabular na linha Excel = header_row_0based + 1)."""
    if not caminho or not os.path.isfile(caminho):
        return pd.DataFrame()
    try:
        return pd.read_excel(caminho, sheet_name=nome_aba, header=header_row_0based)
    except Exception:
        try:
            xl = pd.ExcelFile(caminho)
            alvo = str(nome_aba or "").strip().upper()
            aliases = {alvo}
            if alvo == "DADOS RECEBER":
                aliases.add("DADOS RECEBER")
                aliases.add("DADOS RECEBER".title())
            elif alvo == "DADOS RECEBIDOS":
                aliases.add("DADOS RECEBIDOS")
                aliases.add("DADOS RECEBIDOS".title())
            elif alvo == "DADOS GERAL":
                aliases.update({"RELATORIO ANALITICO", "DADOS GERAL"})
            elif alvo == "PENDENCIAS_PARCELAS" or alvo == "PEND.PARCELAS":
                aliases.update({"PEND.PARCELAS", "PENDENCIAS_PARCELAS", "PENDENCIAS_PARCELAS".title(), "Pendencias_Parcelas"})
            elif alvo == "CRITERIOS" or alvo == "CRITERIOS ANALISES":
                aliases.update({"CRITERIOS ANALISES", "CRITERIOS", "CRITÉRIOS", "CRITERIOS".title(), "Critérios"})
            for sn in xl.sheet_names:
                su = str(sn or "").strip().upper()
                if su in aliases:
                    return pd.read_excel(caminho, sheet_name=sn, header=header_row_0based)
        except Exception:
            pass
        return pd.DataFrame()


def _concat_dfs_vertical(partes: List[pd.DataFrame]) -> pd.DataFrame:
    ok = [x for x in partes if x is not None and not x.empty]
    if not ok:
        return pd.DataFrame()
    return pd.concat(ok, ignore_index=True, sort=False)


def _copiar_abas_ordenadas_para_destino(
    wb_fonte: Workbook, wb_dest: Workbook, nomes_origem: List[str], titulos_destino: List[str], titulos_usados: set
) -> None:
    for orig, tit in zip(nomes_origem, titulos_destino):
        if orig not in wb_fonte.sheetnames:
            continue
        titulo_final = _titulo_aba_unico(wb_dest, tit[:31], titulos_usados)
        _copiar_planilha_estilizada(wb_fonte[orig], wb_dest, titulo_final)


def processar_lote_uau(
    caminhos_receber: Sequence[str],
    caminhos_recebidos: Sequence[str],
    caminho_saida_base: str,
    modo_geracao: str,
    caminhos_estoque: Sequence[str] | None = None,
) -> Tuple[Tuple[str, str], float]:
    """
    Entrada: listas de caminhos absolutos já salvos em disco.
    Saída: ((caminho_xlsx_principal, caminho_xlsx_base_opcional), tempo_total_segundos).
    """
    cr = [os.path.abspath(os.path.normpath(p)) for p in caminhos_receber if p]
    cp = [os.path.abspath(os.path.normpath(p)) for p in caminhos_recebidos if p]
    if len(cr) < 1 or len(cp) < 1:
        raise ProcessamentoUAUErro(
            etapa="validação",
            funcao="processar_lote_uau",
            validacao="arquivos insuficientes",
            mensagem="Envie ao menos um TXT em Contas a Receber e outro em Contas Recebidas.",
            campo_ou_aba="upload",
        )

    modo = str(modo_geracao or "").strip().upper()
    if modo in ("POR_EMPREENDIMENTO", "POR EMPREENDIMENTO", "EMP", "2"):
        modo = MODO_POR_EMPREENDIMENTO
    else:
        raise ProcessamentoUAUErro(
            etapa="validação",
            funcao="processar_lote_uau",
            validacao="modo de geração",
            mensagem="Modo de geração inválido. Use POR_EMPREENDIMENTO.",
            campo_ou_aba="modo_geracao",
        )

    todos = set(cr) | set(cp)
    if len(todos) < len(cr) + len(cp):
        raise ProcessamentoUAUErro(
            etapa="validação",
            funcao="processar_lote_uau",
            validacao="arquivos duplicados",
            mensagem="Há caminhos duplicados entre os anexos. Remova duplicatas.",
            campo_ou_aba="upload",
        )

    t0 = time.perf_counter()
    pasta_saida = os.path.dirname(os.path.abspath(caminho_saida_base)) or "."
    os.makedirs(pasta_saida, exist_ok=True)
    pasta_temp_local = os.path.join(pasta_saida, "_tmp_lote_uau")
    os.makedirs(pasta_temp_local, exist_ok=True)

    for p in cr:
        _validar_tipo_arquivo(p, "RECEBER", "Contas a Receber")
    for p in cp:
        _validar_tipo_arquivo(p, "RECEBIDOS", "Contas Recebidas")

    grupos_r: Dict[str, List[str]] = {}
    for p in cr:
        k = _chave_pareamento_por_prefixo_arquivo(p)
        grupos_r.setdefault(k, []).append(p)
    grupos_p: Dict[str, List[str]] = {}
    for p in cp:
        k = _chave_pareamento_por_prefixo_arquivo(p)
        grupos_p.setdefault(k, []).append(p)

    chaves_ok = sorted(set(grupos_r) & set(grupos_p))
    if not chaves_ok:
        raise ProcessamentoUAUErro(
            etapa="validação",
            funcao="processar_lote_uau",
            validacao="pareamento por empreendimento",
            mensagem=(
                "Não foi possível parear Contas a Receber e Contas Recebidas pelo mesmo empreendimento. "
                "Verifique se cada empreendimento tem um par de arquivos (nomes/cabeçalhos coerentes)."
            ),
            campo_ou_aba="lote",
        )

    apenas_r = set(grupos_r) - set(grupos_p)
    apenas_p = set(grupos_p) - set(grupos_r)
    if apenas_r or apenas_p:
        msg_extra = []
        if apenas_r:
            msg_extra.append(f"Somente Receber: {', '.join(list(apenas_r)[:5])}")
        if apenas_p:
            msg_extra.append(f"Somente Recebidos: {', '.join(list(apenas_p)[:5])}")
        raise ProcessamentoUAUErro(
            etapa="validação",
            funcao="processar_lote_uau",
            validacao="pareamento por empreendimento",
            mensagem="Empreendimentos sem par completo. " + " | ".join(msg_extra),
            campo_ou_aba="lote",
        )

    wb_exec = Workbook()
    wb_exec.remove(wb_exec.active)
    titulos_exec: set = set()
    wb_base = Workbook()
    wb_base.remove(wb_base.active)
    titulos_base: set = set()
    temporarios: List[str] = []
    pastas_temp_workbook: List[str] = []
    caminhos_motor_para_resumo: List[str] = []
    consolidado_por_sigla: List[Tuple[str, str, float]] = []
    consolidado_cache_por_path: Dict[str, pd.DataFrame] = {}
    pares_motor_est: List[Tuple[str, str | None]] = []
    partes_dr: List[pd.DataFrame] = []
    partes_dp: List[pd.DataFrame] = []
    partes_ra: List[pd.DataFrame] = []
    partes_pend: List[pd.DataFrame] = []
    df_criterios_ref: pd.DataFrame | None = None

    try:
        for chave in chaves_ok:
            lista_r = sorted(grupos_r[chave])
            lista_p = sorted(grupos_p[chave])
            tmp_r = _fundir_textos_em_temp(lista_r, "rec_")
            tmp_p = _fundir_textos_em_temp(lista_p, "pag_")
            temporarios.extend([tmp_r, tmp_p])
            # Pasta exclusiva: processar_e_gerar_excel limpa *todos* os .xlsx em dirname(caminho_saida).
            # Se o placeholder estiver em %TEMP%, o mkstemp .xlsx era apagado antes do anexo.
            wdir = os.path.join(pasta_temp_local, f"uau_lote_wk_{uuid.uuid4().hex[:10]}")
            os.makedirs(wdir, exist_ok=True)
            pastas_temp_workbook.append(wdir)
            placeholder = os.path.join(wdir, "base.xlsx")
            sigla = _sigla_curta_do_caminho(lista_r[0])
            ce_chave = _resolver_estoque_por_chave_lote(chave, caminhos_estoque or [], temporarios)
            caminho_xlsx_motor, _ = processar_e_gerar_excel(
                tmp_r,
                tmp_p,
                placeholder,
                gerar_aba_resumo_geral=False,
                gerar_aba_consolidado_estoque=False,
                caminho_estoque=ce_chave,
            )
            caminhos_motor_para_resumo.append(caminho_xlsx_motor)
            pares_motor_est.append((caminho_xlsx_motor, ce_chave))
            dfc_sigla = ler_dataframe_consolidado_de_xlsx_motor(caminho_xlsx_motor)
            consolidado_cache_por_path[caminho_xlsx_motor] = (
                dfc_sigla if dfc_sigla is not None else pd.DataFrame()
            )
            soma_inad = 0.0
            if dfc_sigla is not None and not dfc_sigla.empty and "Vl.Principal (Encargos)" in dfc_sigla.columns:
                try:
                    serie_inad = pd.to_numeric(
                        dfc_sigla["Vl.Principal (Encargos)"],
                        errors="coerce",
                    ).fillna(0.0)
                except Exception:
                    serie_inad = pd.to_numeric(
                        dfc_sigla["Vl.Principal (Encargos)"]
                        .astype(str)
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False),
                        errors="coerce",
                    ).fillna(0.0)
                soma_inad = float(serie_inad.sum())
            consolidado_por_sigla.append((sigla, caminho_xlsx_motor, soma_inad))

            partes_dr.append(_ler_df_aba_xlsx_motor(caminho_xlsx_motor, "DADOS RECEBER", 6))
            partes_dp.append(_ler_df_aba_xlsx_motor(caminho_xlsx_motor, "DADOS RECEBIDOS", 6))
            partes_ra.append(_ler_df_aba_xlsx_motor(caminho_xlsx_motor, "DADOS GERAL", 7))
            partes_pend.append(_ler_df_aba_xlsx_motor(caminho_xlsx_motor, "PEND.PARCELAS", 4))
            if df_criterios_ref is None or (getattr(df_criterios_ref, "empty", True)):
                dc = _ler_df_aba_xlsx_motor(caminho_xlsx_motor, "CRITERIOS ANALISES", 0)
                if dc is not None and not dc.empty:
                    df_criterios_ref = dc

        df_dr_u = _concat_dfs_vertical(partes_dr)
        df_dp_u = _concat_dfs_vertical(partes_dp)
        df_ra_u = _concat_dfs_vertical(partes_ra)
        df_pend_u = _concat_dfs_vertical(partes_pend)
        if df_criterios_ref is None:
            df_criterios_ref = pd.DataFrame()

        tmp_apoio = os.path.join(pasta_temp_local, f"uau_lote_apoio_{uuid.uuid4().hex}.xlsx")
        try:
            with pd.ExcelWriter(tmp_apoio, engine="openpyxl") as wr:
                (df_dr_u if not df_dr_u.empty else pd.DataFrame()).to_excel(
                    wr, sheet_name="DADOS RECEBER", index=False
                )
                (df_dp_u if not df_dp_u.empty else pd.DataFrame()).to_excel(
                    wr, sheet_name="DADOS RECEBIDOS", index=False
                )
                (df_ra_u if not df_ra_u.empty else pd.DataFrame()).to_excel(
                    wr, sheet_name="DADOS GERAL", index=False, startrow=7
                )
                (df_pend_u if not df_pend_u.empty else pd.DataFrame()).to_excel(
                    wr, sheet_name="PEND.PARCELAS", index=False
                )
                (df_criterios_ref if not df_criterios_ref.empty else pd.DataFrame()).to_excel(
                    wr, sheet_name="CRITERIOS ANALISES", index=False
                )
            db_apoio = _data_base_de_primeiro_xlsx_motor(
                caminhos_motor_para_resumo[0] if caminhos_motor_para_resumo else ""
            )
            aplicar_estilo_excel(
                tmp_apoio,
                db_apoio,
                "LOTE — TODOS OS EMPREENDIMENTOS",
                "DADOS RECEBER",
                apenas_abas_apoio=True,
            )
            wb_apoio = load_workbook(tmp_apoio, data_only=False)
            try:
                _copiar_abas_ordenadas_para_destino(
                    wb_apoio,
                    wb_base,
                    [
                        "DADOS RECEBER",
                        "DADOS RECEBIDOS",
                        "DADOS GERAL",
                        "PEND.PARCELAS",
                        "CRITERIOS ANALISES",
                    ],
                    [
                        "DADOS RECEBER",
                        "DADOS RECEBIDOS",
                        "DADOS GERAL",
                        "PEND.PARCELAS",
                        "CRITERIOS ANALISES",
                    ],
                    titulos_base,
                )
            finally:
                wb_apoio.close()
        finally:
            _remover_seguro(tmp_apoio)

        partes_c = []
        for pth in caminhos_motor_para_resumo:
            dfc = consolidado_cache_por_path.get(pth)
            if dfc is not None and not dfc.empty:
                partes_c.append(dfc)
        df_resumo_lote = pd.DataFrame()
        titulo_rg: str | None = None
        if partes_c:
            df_resumo_lote = montar_dataframe_resumo_geral(pd.concat(partes_c, ignore_index=True))
        if not df_resumo_lote.empty:
            tmp_resumo = os.path.join(pasta_temp_local, f"uau_resumo_lote_{uuid.uuid4().hex}.xlsx")
            try:
                with pd.ExcelWriter(tmp_resumo, engine="openpyxl") as wr:
                    df_resumo_lote.to_excel(
                        wr, sheet_name=NOME_ABA_RESUMO_GERAL, index=False, startrow=7
                    )
                db0 = _data_base_de_primeiro_xlsx_motor(caminhos_motor_para_resumo[0])
                aplicar_estilo_arquivo_so_aba_resumo_geral(
                    tmp_resumo,
                    db0,
                    "LOTE — TODOS OS EMPREENDIMENTOS",
                )
                wb_r = load_workbook(tmp_resumo)
                try:
                    titulo_rg = _titulo_aba_unico(
                        wb_exec, (NOME_ABA_RESUMO_GERAL or "RESUMO")[:31], titulos_exec
                    )
                    _copiar_planilha_estilizada(wb_r[NOME_ABA_RESUMO_GERAL], wb_exec, titulo_rg)
                finally:
                    wb_r.close()
            finally:
                _remover_seguro(tmp_resumo)

        ordem_consolidados = sorted(
            consolidado_por_sigla,
            key=lambda x: (-float(x[2]), str(x[0]).upper()),
        )
        for sigla_ord, caminho_xlsx_ord, _ in ordem_consolidados:
            _anexar_somente_consolidado_por_sigla(
                wb_exec,
                caminho_xlsx_ord,
                sigla_ord,
                titulos_exec,
            )

        partes_es: List[pd.DataFrame] = []
        for pth_motor, ce_p in pares_motor_est:
            dfc = consolidado_cache_por_path.get(pth_motor)
            if dfc is None or dfc.empty:
                continue
            df_e = carregar_estoque_bruto(ce_p) if ce_p else carregar_estoque_bruto("")
            partes_es.append(montar_dataframe_consolidado_estoque(dfc, df_e))
        df_es_lote = pd.concat(partes_es, ignore_index=True) if partes_es else pd.DataFrame()
        if df_es_lote.empty:
            df_es_lote = pd.DataFrame(columns=COLUNAS_SAIDA_CONSOLIDADO_ESTOQUE)
        tmp_es = os.path.join(pasta_temp_local, f"uau_est_lote_{uuid.uuid4().hex}.xlsx")
        try:
            ind_es = calcular_indicadores_painel_consolidado_estoque(df_es_lote)
            with pd.ExcelWriter(tmp_es, engine="openpyxl") as wr:
                df_es_lote.to_excel(
                    wr,
                    sheet_name=NOME_ABA_CONSOLIDADO_ESTOQUE,
                    index=False,
                    startrow=CONSOLIDADO_ESTOQUE_PANDAS_STARTROW,
                )
            ref_motor = caminhos_motor_para_resumo[0] if caminhos_motor_para_resumo else ""
            db_es = _data_base_de_primeiro_xlsx_motor(ref_motor)
            aplicar_estilo_arquivo_so_aba_consolidado_estoque(
                tmp_es,
                db_es,
                "LOTE — TODOS OS EMPREENDIMENTOS",
                ind_es,
            )
            wb_e = load_workbook(tmp_es)
            try:
                titulo_es = _titulo_aba_unico(
                    wb_base, (NOME_ABA_CONSOLIDADO_ESTOQUE or "ESTOQUE")[:31], titulos_base
                )
                _copiar_planilha_estilizada(wb_e[NOME_ABA_CONSOLIDADO_ESTOQUE], wb_base, titulo_es)
            finally:
                wb_e.close()
        finally:
            _remover_seguro(tmp_es)

        if (
            NOME_ABA_RESUMO_GERAL in wb_exec.sheetnames
            and wb_exec.sheetnames[0] != NOME_ABA_RESUMO_GERAL
        ):
            try:
                idx_rg = wb_exec.sheetnames.index(NOME_ABA_RESUMO_GERAL)
                wb_exec.move_sheet(wb_exec[NOME_ABA_RESUMO_GERAL], offset=-idx_rg)
            except Exception:
                pass
        ordem_base = [
            "DADOS RECEBER",
            "DADOS RECEBIDOS",
            "DADOS GERAL",
            "PEND.PARCELAS",
            NOME_ABA_CONSOLIDADO_ESTOQUE,
            "CRITERIOS ANALISES",
        ]
        for idx, nome_aba in enumerate(ordem_base):
            if nome_aba in wb_base.sheetnames:
                try:
                    idx_atual = wb_base.sheetnames.index(nome_aba)
                    if idx_atual != idx:
                        wb_base.move_sheet(wb_base[nome_aba], offset=idx - idx_atual)
                except Exception:
                    pass

        destino_exec = os.path.join(pasta_saida, "CARTEIRAS GERAL.xlsx")
        destino_base = os.path.join(pasta_saida, "CARTEIRAS BANCO DE DADOS.xlsx")
        wb_exec.save(destino_exec)
        wb_base.save(destino_base)
        return (destino_exec, destino_base), time.perf_counter() - t0
    finally:
        for p in temporarios:
            _remover_seguro(p)
        for d in pastas_temp_workbook:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except OSError:
                pass


def processar_entrada_simples_ou_lote(
    caminhos_receber: Sequence[str],
    caminhos_recebidos: Sequence[str],
    caminho_saida_base: str,
    modo_geracao: str | None,
    caminhos_estoque: Sequence[str] | None = None,
) -> Tuple[str | Tuple[str, str], float]:
    """
    Compatível com fluxo antigo: 1+1 arquivos.
    Se um arquivo em cada lista e modo None ou vazio → delega ao processar_e_gerar_excel original.
    """
    cr = [p for p in caminhos_receber if p]
    cp = [p for p in caminhos_recebidos if p]
    modo = (modo_geracao or "").strip()
    if len(cr) == 1 and len(cp) == 1 and not modo:
        tmp_est_par: List[str] = []
        try:
            ce_par = _resolver_estoque_unificado_lote(caminhos_estoque or [], tmp_est_par)
            return processar_e_gerar_excel(
                cr[0], cp[0], caminho_saida_base, caminho_estoque=ce_par
            )
        finally:
            for p in tmp_est_par:
                _remover_seguro(p)
    if len(cr) == 1 and len(cp) == 1 and modo:
        # Um par explícito com modo: usa o fluxo de lote POR_EMPREENDIMENTO.
        return processar_lote_uau(cr, cp, caminho_saida_base, modo, caminhos_estoque=caminhos_estoque)
    if len(cr) >= 1 and len(cp) >= 1:
        if not modo:
            raise ProcessamentoUAUErro(
                etapa="validação",
                funcao="processar_entrada_simples_ou_lote",
                validacao="modo de geração",
                mensagem="Com vários arquivos, selecione o modo: Por empreendimento.",
                campo_ou_aba="modo_geracao",
            )
        return processar_lote_uau(cr, cp, caminho_saida_base, modo, caminhos_estoque=caminhos_estoque)
    raise ProcessamentoUAUErro(
        etapa="validação",
        funcao="processar_entrada_simples_ou_lote",
        validacao="arquivos",
        mensagem="Envie os arquivos TXT necessários.",
        campo_ou_aba="upload",
    )
