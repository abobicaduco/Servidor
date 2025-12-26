@echo off
setlocal EnableDelayedExpansion

echo ========================================================
echo   INICIANDO SERVIDOR COM NODE.JS PORTATIL
echo ========================================================
echo.

REM 1. Verifica/Instala Node
if not exist "node_bin" (
    echo [!] Node.js nao encontrado. Tentando baixar com script Python...
    python setup_node.py
)

REM 2. Se falhou download, avisa mas continua
if not exist "node_bin" (
    echo [AVISO] Nao foi possivel baixar o Node.js.
    echo O servidor rodara em modo API-ONLY ^(sem interface visual^).
    goto :START_SERVER
)

REM 3. Configura Node se existe
echo [OK] Node.js detectado!
SET "PATH=%CD%\node_bin;%PATH%"

REM --- BYPASS SSL EMPRESARIAL ---
SET NODE_TLS_REJECT_UNAUTHORIZED=0
echo [SECURITY] SSL/TLS Verification Disabled for Node

REM 4. Instala Dependencias Frontend
if not exist "web_frontend\node_modules" (
    echo.
    echo [NPM] Instalando dependencias do frontend...
    cd web_frontend
    if exist "package-lock.json" del "package-lock.json"
    call npm install --no-audit --verbose
    cd ..
)

:START_SERVER
echo.
echo Iniciando Server.py...
python Server.py
pause
