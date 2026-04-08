import os
from datetime import datetime
from flask import Flask, render_template, request, send_file, session, redirect, url_for
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from services.processador_uau import ProcessamentoUAUErro
from services.orquestrador_lote_uau import processar_entrada_simples_ou_lote

app = Flask(__name__)

app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-meu-financeiro-uau-altere-em-producao")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["OUTPUT_FOLDER"] = OUTPUT_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB (lote com vários TXT)

EXTENSAO_PERMITIDA = ".txt"


def _formatar_tempo_mm_ss(segundos: float) -> str:
    try:
        s = max(0, int(round(float(segundos))))
    except (TypeError, ValueError):
        s = 0
    m, sec = divmod(s, 60)
    return f"{m}:{sec:02d}"


def _agora_str() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def _estado_uploads():
    return {
        "receber": session.get("uploads_receber", []),
        "recebidos": session.get("uploads_recebidos", []),
        "estoque": session.get("upload_estoque", None),
    }


def _persistir_estado_uploads(estado):
    session["uploads_receber"] = estado.get("receber", [])
    session["uploads_recebidos"] = estado.get("recebidos", [])
    session["upload_estoque"] = estado.get("estoque")


def _remover_arquivo_local(caminho: str | None):
    if not caminho:
        return
    try:
        if os.path.isfile(caminho):
            os.remove(caminho)
    except OSError:
        app.logger.warning("Não foi possível remover arquivo local: %s", caminho)


def _salvar_upload(arquivo, nome_salvo: str) -> str:
    dest = os.path.join(app.config["UPLOAD_FOLDER"], nome_salvo)
    arquivo.save(dest)
    return os.path.abspath(dest)


def _anexar_em_lista(estado, chave_lista: str, arquivos, rotulo: str):
    erros = []
    atual = list(estado.get(chave_lista, []))
    inicio = len(atual)
    for idx, arq in enumerate(arquivos):
        nome_seguro, erro = validar_arquivo_enviado(arq, rotulo)
        if erro:
            erros.append(f"{rotulo} #{idx + 1}: {erro}")
            continue
        nome_unico = f"{chave_lista[:3]}_{inicio + idx:02d}_{nome_seguro}"
        caminho = _salvar_upload(arq, nome_unico)
        atual.append(
            {
                "nome": nome_seguro,
                "path": caminho,
                "anexado_em": _agora_str(),
                "status": "ATIVO",
            }
        )
    estado[chave_lista] = atual
    return erros


def validar_arquivo_enviado(arquivo, descricao_campo):
    if not arquivo:
        return None, f"O arquivo de {descricao_campo} não foi enviado."

    nome_original = (arquivo.filename or "").strip()
    if not nome_original:
        return None, f"Selecione o arquivo de {descricao_campo}."

    nome_seguro = secure_filename(nome_original)
    if not nome_seguro:
        return None, f"O nome do arquivo de {descricao_campo} é inválido."

    if not nome_seguro.lower().endswith(EXTENSAO_PERMITIDA):
        return None, f"O arquivo de {descricao_campo} deve estar no formato .txt."

    return nome_seguro, None


def _detalhes_processamento_uau_erro(exc: ProcessamentoUAUErro):
    try:
        texto = exc.formatar_relatorio_completo()
        linhas = [ln for ln in texto.splitlines() if ln is not None]
        if linhas:
            return linhas
    except Exception:
        pass
    out = [
        f"Função: {exc.funcao}",
        f"Validação: {exc.validacao}",
        f"Campo/Aba: {exc.campo_ou_aba or 'N/A'}",
        f"Mensagem: {exc.mensagem}",
    ]
    if exc.erro_tecnico is not None:
        out.append(f"Detalhe técnico: {type(exc.erro_tecnico).__name__}: {exc.erro_tecnico}")
    else:
        out.append("Detalhe técnico: sem exceção Python associada")
    return out


@app.route("/", methods=["GET", "POST"])
def index():
    estado = _estado_uploads()
    if request.method == "POST":
        acao_raw = (request.form.get("acao") or "processar").strip().lower()
        acao = acao_raw
        idx_acao = None
        if ":" in acao_raw:
            acao, _, idx_txt = acao_raw.partition(":")
            try:
                idx_acao = int(idx_txt)
            except ValueError:
                idx_acao = None

        if acao == "remover_receber":
            idx = idx_acao if idx_acao is not None else int(request.form.get("idx", "-1"))
            itens = list(estado["receber"])
            if 0 <= idx < len(itens):
                _remover_arquivo_local(itens[idx].get("path"))
                itens.pop(idx)
                estado["receber"] = itens
                _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        if acao == "remover_recebidos":
            idx = idx_acao if idx_acao is not None else int(request.form.get("idx", "-1"))
            itens = list(estado["recebidos"])
            if 0 <= idx < len(itens):
                _remover_arquivo_local(itens[idx].get("path"))
                itens.pop(idx)
                estado["recebidos"] = itens
                _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        if acao == "remover_estoque":
            item = estado.get("estoque")
            if item:
                _remover_arquivo_local(item.get("path"))
            estado["estoque"] = None
            _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        if acao == "substituir_receber":
            idx = idx_acao if idx_acao is not None else int(request.form.get("idx", "-1"))
            novo = request.files.get("arquivo_substituto")
            itens = list(estado["receber"])
            if 0 <= idx < len(itens) and novo and (novo.filename or "").strip():
                nome_seguro, erro = validar_arquivo_enviado(novo, "Contas a Receber")
                if erro:
                    return render_template("index.html", erro=erro, uploads_estado=estado)
                _remover_arquivo_local(itens[idx].get("path"))
                nome_unico = f"rec_{idx:02d}_{nome_seguro}"
                caminho = _salvar_upload(novo, nome_unico)
                itens[idx] = {"nome": nome_seguro, "path": caminho, "anexado_em": _agora_str(), "status": "ATIVO"}
                estado["receber"] = itens
                _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        if acao == "substituir_recebidos":
            idx = idx_acao if idx_acao is not None else int(request.form.get("idx", "-1"))
            novo = request.files.get("arquivo_substituto")
            itens = list(estado["recebidos"])
            if 0 <= idx < len(itens) and novo and (novo.filename or "").strip():
                nome_seguro, erro = validar_arquivo_enviado(novo, "Contas Recebidas")
                if erro:
                    return render_template("index.html", erro=erro, uploads_estado=estado)
                _remover_arquivo_local(itens[idx].get("path"))
                nome_unico = f"reb_{idx:02d}_{nome_seguro}"
                caminho = _salvar_upload(novo, nome_unico)
                itens[idx] = {"nome": nome_seguro, "path": caminho, "anexado_em": _agora_str(), "status": "ATIVO"}
                estado["recebidos"] = itens
                _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        if acao == "substituir_estoque":
            novo = request.files.get("arquivo_substituto")
            if novo and (novo.filename or "").strip():
                nome_seguro, erro = validar_arquivo_enviado(novo, "Relatório de Estoque")
                if erro:
                    return render_template("index.html", erro=erro, uploads_estado=estado)
                if estado.get("estoque"):
                    _remover_arquivo_local(estado["estoque"].get("path"))
                caminho = _salvar_upload(novo, f"est_00_{nome_seguro}")
                estado["estoque"] = {"nome": nome_seguro, "path": caminho, "anexado_em": _agora_str(), "status": "ATIVO"}
                _persistir_estado_uploads(estado)
            return redirect(url_for("index"))

        # ação principal de processamento
        lista_receber = [
            f for f in request.files.getlist("arquivo_receber")
            if f and (f.filename or "").strip()
        ]
        lista_recebidos = [
            f for f in request.files.getlist("arquivo_recebidos")
            if f and (f.filename or "").strip()
        ]

        erros_validacao = []
        erros_validacao.extend(_anexar_em_lista(estado, "receber", lista_receber, "Contas a Receber"))
        erros_validacao.extend(_anexar_em_lista(estado, "recebidos", lista_recebidos, "Contas Recebidas"))

        lista_estoque = [
            f for f in request.files.getlist("arquivo_estoque")
            if f and (f.filename or "").strip()
        ]
        if lista_estoque:
            ultimo = lista_estoque[-1]
            nome_es, erro_es = validar_arquivo_enviado(ultimo, "Relatório de Estoque")
            if erro_es:
                erros_validacao.append(erro_es)
            else:
                if estado.get("estoque"):
                    _remover_arquivo_local(estado["estoque"].get("path"))
                caminho_es = _salvar_upload(ultimo, f"est_00_{nome_es}")
                estado["estoque"] = {
                    "nome": nome_es,
                    "path": caminho_es,
                    "anexado_em": _agora_str(),
                    "status": "ATIVO",
                }

        if not estado["receber"]:
            erros_validacao.append("Envie ao menos um arquivo TXT de Contas a Receber.")
        if not estado["recebidos"]:
            erros_validacao.append("Envie ao menos um arquivo TXT de Contas Recebidas.")

        modo_geracao = (request.form.get("modo_geracao") or "").strip()
        if len(estado["receber"]) > 1 or len(estado["recebidos"]) > 1:
            if modo_geracao != "POR_EMPREENDIMENTO":
                erros_validacao.append(
                    "Com mais de um arquivo em algum campo, selecione o modo: "
                    "Por empreendimento."
                )

        if erros_validacao:
            return render_template(
                "index.html",
                erro="Não foi possível iniciar o processamento.",
                detalhes_erro=erros_validacao,
                uploads_estado=estado,
            )

        caminho_abs_receber = [x.get("path") for x in estado["receber"] if x.get("path")]
        caminho_abs_recebidos = [x.get("path") for x in estado["recebidos"] if x.get("path")]
        caminho_abs_estoque = [estado["estoque"]["path"]] if estado.get("estoque") and estado["estoque"].get("path") else []
        _persistir_estado_uploads(estado)

        caminho_saida_base = os.path.join(
            app.config["OUTPUT_FOLDER"],
            "consolidacao_uau.xlsx",
        )

        try:
            modo_param = modo_geracao if modo_geracao else None
            saida_processamento, tempo_execucao = processar_entrada_simples_ou_lote(
                caminho_abs_receber,
                caminho_abs_recebidos,
                caminho_saida_base,
                modo_param,
                caminhos_estoque=caminho_abs_estoque or None,
            )
        except ProcessamentoUAUErro as e:
            relatorio_txt = None
            try:
                relatorio_txt = e.formatar_relatorio_completo()
            except Exception:
                relatorio_txt = None
            if relatorio_txt:
                app.logger.error(
                    "ProcessamentoUAUErro — relatório completo:\n%s",
                    relatorio_txt,
                )
                detalhes_erro = [ln for ln in relatorio_txt.splitlines()]
            else:
                app.logger.error(
                    "ProcessamentoUAUErro (log fallback): etapa=%s função=%s validação=%s mensagem=%s",
                    e.etapa,
                    e.funcao,
                    e.validacao,
                    e.mensagem,
                    exc_info=True,
                )
                detalhes_erro = _detalhes_processamento_uau_erro(e)
            if e.erro_tecnico is not None:
                app.logger.error(
                    "ProcessamentoUAUErro — exceção Python associada: %s: %s",
                    type(e.erro_tecnico).__name__,
                    e.erro_tecnico,
                    exc_info=e.erro_tecnico,
                )
            return render_template(
                "index.html",
                erro=f"Erro na etapa de {e.etapa}.",
                detalhes_erro=detalhes_erro,
                uploads_estado=estado,
            )
        except ValueError as e:
            app.logger.exception("Erro de validação durante processamento")
            return render_template(
                "index.html",
                erro="Erro na etapa de validação dos dados.",
                detalhes_erro=[f"Mensagem técnica: {type(e).__name__}: {e}"],
                uploads_estado=estado,
            )
        except Exception as e:
            app.logger.exception("Erro inesperado durante processamento")
            return render_template(
                "index.html",
                erro="Erro na etapa de processamento.",
                detalhes_erro=[f"Mensagem técnica: {type(e).__name__}: {e}"],
                uploads_estado=estado,
            )

        caminho_saida_final = ""
        caminho_saida_base_opcional = None
        if isinstance(saida_processamento, tuple):
            caminho_saida_final = saida_processamento[0]
            if len(saida_processamento) > 1:
                caminho_saida_base_opcional = saida_processamento[1]
        else:
            caminho_saida_final = saida_processamento

        if not os.path.exists(caminho_saida_final):
            return render_template(
                "index.html",
                erro="O arquivo final não foi gerado.",
                detalhes_erro=[
                    "O processamento foi concluído sem gerar o arquivo de saída."
                ],
                uploads_estado=estado,
            )

        _persistir_estado_uploads(estado)
        basename_principal = os.path.basename(caminho_saida_final)
        caminho_base = (
            os.path.abspath(caminho_saida_base_opcional)
            if caminho_saida_base_opcional
            else os.path.join(app.config["OUTPUT_FOLDER"], "CARTEIRAS BANCO DE DADOS.xlsx")
        )
        basename_base = os.path.basename(caminho_base)
        session["download_consolidado_ok"] = True
        session["download_consolidado_basename"] = basename_principal
        session["download_base_ok"] = os.path.isfile(caminho_base)
        session["download_base_basename"] = basename_base if os.path.isfile(caminho_base) else None
        return render_template(
            "index.html",
            sucesso=True,
            tempo_execucao=tempo_execucao,
            tempo_execucao_mmss=_formatar_tempo_mm_ss(tempo_execucao),
            modo_geracao_exibicao=modo_geracao or "Padrão (par único)",
            qtd_receber=len(caminho_abs_receber),
            qtd_recebidos=len(caminho_abs_recebidos),
            download_base_disponivel=os.path.isfile(caminho_base),
            uploads_estado=estado,
        )

    return render_template("index.html", uploads_estado=estado)


@app.route("/baixar-planilha", methods=["GET"])
def baixar_planilha():
    if not session.get("download_consolidado_ok"):
        return redirect(url_for("index"))

    basename = os.path.basename(session.get("download_consolidado_basename") or "")
    if not basename.lower().endswith(".xlsx"):
        session.pop("download_consolidado_ok", None)
        session.pop("download_consolidado_basename", None)
        return redirect(url_for("index"))

    pasta_saida = os.path.realpath(app.config["OUTPUT_FOLDER"])
    caminho = os.path.realpath(os.path.join(pasta_saida, basename))
    if not caminho.startswith(pasta_saida + os.sep):
        session.pop("download_consolidado_ok", None)
        session.pop("download_consolidado_basename", None)
        return redirect(url_for("index"))

    if not os.path.isfile(caminho):
        session.pop("download_consolidado_ok", None)
        session.pop("download_consolidado_basename", None)
        return render_template(
            "index.html",
            erro="O arquivo consolidado não está mais disponível.",
            detalhes_erro=["Gere a planilha novamente enviando os arquivos."],
            uploads_estado=_estado_uploads(),
        )

    session.pop("download_consolidado_ok", None)
    session.pop("download_consolidado_basename", None)
    return send_file(caminho, as_attachment=True, download_name=basename)


@app.route("/baixar-base", methods=["GET"])
def baixar_base():
    if not session.get("download_base_ok"):
        return redirect(url_for("index"))

    basename = os.path.basename(session.get("download_base_basename") or "")
    if not basename.lower().endswith(".xlsx"):
        session.pop("download_base_ok", None)
        session.pop("download_base_basename", None)
        return redirect(url_for("index"))

    pasta_saida = os.path.realpath(app.config["OUTPUT_FOLDER"])
    caminho = os.path.realpath(os.path.join(pasta_saida, basename))
    if not caminho.startswith(pasta_saida + os.sep):
        session.pop("download_base_ok", None)
        session.pop("download_base_basename", None)
        return redirect(url_for("index"))

    if not os.path.isfile(caminho):
        session.pop("download_base_ok", None)
        session.pop("download_base_basename", None)
        return render_template(
            "index.html",
            erro="O arquivo base não está mais disponível.",
            detalhes_erro=["Gere a planilha novamente enviando os arquivos."],
            uploads_estado=_estado_uploads(),
        )

    session.pop("download_base_ok", None)
    session.pop("download_base_basename", None)
    return send_file(caminho, as_attachment=True, download_name=basename)


@app.errorhandler(RequestEntityTooLarge)
def tratar_arquivo_muito_grande(_erro):
    return render_template(
        "index.html",
        erro="O tamanho total dos arquivos excede o limite permitido (200 MB).",
        detalhes_erro=[
            "Reduza a quantidade ou o tamanho dos TXT e tente novamente."
        ],
        uploads_estado=_estado_uploads(),
    ), 413


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug_mode)
