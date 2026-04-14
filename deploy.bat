@echo off
echo ==============================================
echo GESIX Data Quality - Deployment Script
echo ==============================================

echo [1/3] Navigating to Frontend and building...
cd frontend
call npm install
if %ERRORLEVEL% neq 0 (
    echo Frontend npm install failed!
    exit /b %ERRORLEVEL%
)
call npm run build
if %ERRORLEVEL% neq 0 (
    echo Frontend build failed!
    exit /b %ERRORLEVEL%
)
cd ..

echo [2/3] Navigating to Backend and installing dependencies...
cd backend
python -m pip install -r requirements.txt
if %ERRORLEVEL% neq 0 (
    echo Backend dependencies installation failed!
    exit /b %ERRORLEVEL%
)

echo [3/3] Starting the Server...
python -m uvicorn app:app --host 0.0.0.0 --port 8000
