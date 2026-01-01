@echo off
echo Building Haniwon Unified Server v3.1.2...
cd /d "%~dp0"
python -m PyInstaller --onefile --noconsole --name "Haniwon-Unified-Server-v3.1.2" --add-data "routes;routes" --add-data "services;services" gui.py --clean
echo.
echo Encrypting mssql_routes.py...
python encrypt_routes.py 2.6.6
echo.
echo Done!
pause
