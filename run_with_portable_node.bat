@echo off
setlocal EnableDelayedExpansion

echo ========================================================
echo   INICIANDO SERVIDOR COM NODE.JS PORTATIL
echo ========================================================
echo.

REM 1. Verifica Node.js (Incluido no repo em binaries/node)
if not exist "binaries\node" (
    echo [ERRO] Pasta binaries\node nao encontrada!
    echo Voce baixou o repositorio completo? Verifique se a pasta 'binaries' existe.
    echo O servidor rodara em modo API-ONLY.
    goto :PY_DEPS
)

REM 2. (Etapa de download removida pois ja esta incluso)

REM 2.1 Garante Dependencias Python (SSL Bypass)
:PY_DEPS
call install_requirements_proxy.bat

REM 3. Configura Node se existe
if exist "binaries\node" (
    echo [OK] Node.js Portable detectado!
    SET "PATH=%CD%\binaries\node;%PATH%"
)

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
