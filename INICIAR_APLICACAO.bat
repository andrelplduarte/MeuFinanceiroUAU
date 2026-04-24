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

powershell -NoProfile -ExecutionPolicy Bypass -Command "$portaAberta = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue; if (-not $portaAberta) { Start-Process -FilePath (Join-Path $PWD 'venv\Scripts\pythonw.exe') -ArgumentList 'app.py' -WorkingDirectory $PWD; Start-Sleep -Seconds 2 }; Start-Process 'http://127.0.0.1:5000'"

exit /b 0
