@echo off
echo ========================================================
echo   INSTALANDO DEPENDENCIAS PYTHON (MODO CORPORATIVO)
echo ========================================================
echo.
echo Tentando instalar bibliotecas Python ignorando SSL...
echo.

REM Lista de hosts confiaveis para ignorar erros de certificado
set TRUSTED=--trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

REM Instalação explicita
pip install fastapi uvicorn pandas pandas-gbq google-cloud-bigquery python-dotenv requests openssl-python pywin32 %TRUSTED%

if %errorlevel% neq 0 (
    echo.
    echo [ERRO] Falha ao instalar dependencias Python.
    echo Tente rodar este arquivo como Administrador ou verifique sua internet.
    pause
    exit /b 1
)

echo.
echo [SUCESSO] Dependencias Python instaladas!
