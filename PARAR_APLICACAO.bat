@echo off
setlocal

echo Encerrando Central UAU...

powershell -NoProfile -ExecutionPolicy Bypass -Command "$conexoes = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue; if (-not $conexoes) { exit 0 }; $pids = $conexoes | Select-Object -ExpandProperty OwningProcess -Unique; foreach ($pidItem in $pids) { Stop-Process -Id $pidItem -Force -ErrorAction SilentlyContinue }"

echo Central UAU encerrada.
timeout /t 2 >nul
