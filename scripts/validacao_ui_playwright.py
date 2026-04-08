# -*- coding: utf-8 -*-
"""
Captura screenshots reais da interface (Flask + Playwright).
Uso: na raiz do projeto, com servidor NÃO em execução:
  .\\venv\\Scripts\\python.exe scripts\\validacao_ui_playwright.py
"""
from __future__ import annotations

import os
import sys
import threading
import time

# Raiz do projeto
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.chdir(ROOT)

from werkzeug.serving import make_server  # noqa: E402

PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"
OUT_DIR = os.path.join(ROOT, "outputs", "validacao_evidencias")
UPLOADS = os.path.join(ROOT, "uploads")

# Arquivos pequenos para POST rápido (1x1 e tempos curtos)
F_LTMAG_R = os.path.join(UPLOADS, "LTMAG_-_LOT.SPE_RESIDENCIAL_MAGALHAES_-_RECEBER.txt")
F_LTMAG_P = os.path.join(UPLOADS, "LTMAG_-_LOT.SPE_RESIDENCIAL_MAGALHAES_-_RECEBIDOS.txt")
F_NVLOT_R = os.path.join(UPLOADS, "NVLOT_-_LOT.RES.NILSON_VELOSO_-_RECEBER.txt")
F_NVLOT_P = os.path.join(UPLOADS, "NVLOT_-_LOT.RES.NILSON_VELOSO_-_RECEBIDOS.txt")


def _ensure_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def _run_server():
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    srv = make_server("127.0.0.1", PORT, flask_app, threaded=True)
    srv.serve_forever()


def main():
    _ensure_dir()
    t = threading.Thread(target=_run_server, daemon=True)
    t.start()
    time.sleep(1.2)

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1100, "height": 1400},
            locale="pt-BR",
        )
        page = context.new_page()

        page.goto(f"{BASE}/", wait_until="networkidle")
        page.wait_for_timeout(400)
        page.screenshot(path=os.path.join(OUT_DIR, "01_tela_inicial.png"), full_page=True)

        # Multiupload + modos visíveis (sem submeter)
        page.set_input_files("#arquivo_receber", [F_LTMAG_R, F_NVLOT_R])
        page.set_input_files("#arquivo_recebidos", [F_LTMAG_P, F_NVLOT_P])
        page.locator("label.modo-opt").filter(has=page.locator("#modo-uni")).click()
        page.wait_for_timeout(300)
        page.screenshot(path=os.path.join(OUT_DIR, "02_multiupload_e_modos.png"), full_page=True)

        # Bloco de instruções (TXT + passo a passo)
        page.locator("h2", has_text="Formato dos arquivos TXT").scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        page.screenshot(path=os.path.join(OUT_DIR, "03_bloco_instrucoes_txt_e_passos.png"), full_page=True)

        # Novo fluxo: 1x1 para monitor + sucesso (rápido)
        page.goto(f"{BASE}/", wait_until="networkidle")
        page.set_input_files("#arquivo_receber", [F_LTMAG_R])
        page.set_input_files("#arquivo_recebidos", [F_LTMAG_P])
        # Não esperar o POST terminar — capturar o monitor em andamento (m:ss).
        page.click('button[type="submit"]', no_wait_after=True)
        try:
            page.wait_for_selector("#monitor-processamento.ativo", timeout=8000)
        except Exception:
            pass
        page.wait_for_timeout(1200)
        page.screenshot(path=os.path.join(OUT_DIR, "04_monitor_tempo_mmss.png"), full_page=True)

        page.wait_for_selector("text=Processamento concluído", timeout=120000)
        page.wait_for_timeout(600)
        page.screenshot(path=os.path.join(OUT_DIR, "05_sucesso_e_download.png"), full_page=True)

        browser.close()

    print("OK — imagens em:", OUT_DIR)


if __name__ == "__main__":
    main()
