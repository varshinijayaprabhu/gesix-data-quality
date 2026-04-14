#!/bin/bash
set -e

echo "=============================================="
echo "GESIX Data Quality - Deployment Script"
echo "=============================================="

echo "[1/3] Navigating to Frontend and building..."
cd frontend
npm install
npm run build
cd ..

echo "[2/3] Navigating to Backend and installing dependencies..."
cd backend
python3 -m pip install -r requirements.txt

echo "[3/3] Starting the Server..."
python3 -m uvicorn app:app --host 0.0.0.0 --port 8000
