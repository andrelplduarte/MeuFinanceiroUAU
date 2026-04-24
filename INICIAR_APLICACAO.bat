@echo off
setlocal

cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
    echo Nao encontrei o Python do ambiente virtual em:
    echo %CD%\venv\Scripts\python.exe
    echo.
    echo Abra o projeto uma vez no Cursor/terminal e rode:
    echo python -m venv venv
    echo venv\Scripts\python.exe -m pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

echo Iniciando Meu Financeiro UAU...
echo.
echo Quando quiser parar a aplicacao, feche esta janela ou aperte CTRL+C.
echo.

start "" powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 2; Start-Process 'http://127.0.0.1:5000'"

"venv\Scripts\python.exe" app.py

echo.
echo Aplicacao encerrada.
pause
