@echo off
echo Building Haniwon Unified Server v1.2.0...
pyinstaller --onefile --windowed --name "Haniwon-Unified-Server-v1.2.0" ^
    --add-data "routes;routes" ^
    --add-data "services;services" ^
    app.py
echo Done!
pause
