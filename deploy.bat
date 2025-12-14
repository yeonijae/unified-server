@echo off
setlocal

REM === Haniwon Unified Server 빌드 및 배포 스크립트 ===
REM 사용법: deploy.bat [서버경로]
REM 예시: deploy.bat \\192.168.0.173\haniwon

set "EXE_NAME=Haniwon-Unified-Server.exe"
set "DIST_PATH=%~dp0dist\%EXE_NAME%"

REM 서버 경로 설정 (인자가 없으면 기본값 사용)
if "%~1"=="" (
    set "SERVER_PATH=\\192.168.0.173\C$\Haniwon"
) else (
    set "SERVER_PATH=%~1"
)

echo.
echo ========================================
echo  Haniwon Unified Server Deploy Script
echo ========================================
echo.

REM 1. 빌드
echo [1/3] Building %EXE_NAME%...
pyinstaller --onefile --windowed --name "Haniwon-Unified-Server" --add-data "routes;routes" --add-data "services;services" app.py
if errorlevel 1 (
    echo [ERROR] Build failed!
    pause
    exit /b 1
)

REM 2. 빌드 결과 확인
if not exist "%DIST_PATH%" (
    echo [ERROR] Build output not found: %DIST_PATH%
    pause
    exit /b 1
)
echo [OK] Build completed: %DIST_PATH%

REM 3. 서버로 복사
echo.
echo [2/3] Deploying to %SERVER_PATH%...
if not exist "%SERVER_PATH%" (
    echo [WARN] Server path not accessible: %SERVER_PATH%
    echo [INFO] Skipping deployment. Please copy manually.
    echo        From: %DIST_PATH%
    goto :end
)

copy /Y "%DIST_PATH%" "%SERVER_PATH%\"
if errorlevel 1 (
    echo [ERROR] Copy failed!
    pause
    exit /b 1
)
echo [OK] Deployed successfully!

:end
echo.
echo [3/3] Done!
echo.
echo ========================================
echo  NOTE: Restart the server application
echo  to apply changes.
echo ========================================
echo.
pause
