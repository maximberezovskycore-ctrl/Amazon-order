@echo off
cd /d "%~dp0"
set USE_POSTGRES_JOBS=false
set JOB_STORE=memory
echo Starting Amazon Sourcing app on http://localhost:3000
node .\node_modules\next\dist\bin\next start
echo.
echo Server stopped. Press any key to close this window.
pause >nul
