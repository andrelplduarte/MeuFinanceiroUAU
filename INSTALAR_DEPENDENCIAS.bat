@echo off
setlocal

cd /d "%~dp0"

echo Preparando ambiente do Meu Financeiro UAU...
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo Python nao encontrado.
    echo Instale o Python 3.14 pelo site https://www.python.org/downloads/
    echo Marque a opcao "Add python.exe to PATH" durante a instalacao.
    echo.
    pause
    exit /b 1
)

if not exist "venv\Scripts\python.exe" (
    echo Criando ambiente virtual...
    python -m venv venv
    if errorlevel 1 (
        echo Falha ao criar o ambiente virtual.
        pause
        exit /b 1
    )
)

echo Atualizando pip...
"venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
    echo Falha ao atualizar o pip.
    pause
    exit /b 1
)

echo Instalando dependencias...
"venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo Falha ao instalar as dependencias.
    pause
    exit /b 1
)

echo.
echo Ambiente pronto.
echo Agora execute INICIAR_APLICACAO.bat para abrir o sistema.
pause
