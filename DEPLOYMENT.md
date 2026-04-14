# GESIX Data Quality - Deployment Guide

This project consists of a React (Vite) frontend and a FastAPI backend. In production mode, the FastAPI backend acts as a single web server that serves both the API endpoints and the compiled frontend static files. This simplifies deployment to just running the backend server.

## Prerequisites
- **Node.js**: v18+ (Required for building the frontend)
- **Python**: 3.10+ (Required for the backend)

---

## Quick Start (Automated)

The easiest way to start the platform for deployment is using the provided scripts. These scripts will automatically install dependencies, build the frontend UI into static files, and launch the unified backend server.

### On Windows
Run the batch script from the project root:
```cmd
deploy.bat
```

### On Linux / macOS
Make the script executable and run it from the project root:
```bash
chmod +x deploy.sh
./deploy.sh
```

---

## Manual Deployment Process

If you prefer to run the deployment steps manually (for instance, in a CI/CD pipeline), follow these 2 steps:

### 1. Build the Frontend
Compile the React code into optimized static files.
```bash
cd frontend
npm install
npm run build
cd ..
```
*This command creates the `frontend/dist` directory containing the optimized static UI files, which the backend is configured to serve automatically.*

### 2. Start the Backend
Install the backend requirements and run the server using `uvicorn`.
```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

The application is now running! Open a browser and navigate to `http://localhost:8000`.

---

## Environment Configuration
Make sure the `.env` file in the `backend/` directory has all the necessary environment variables configured (e.g., Supabase keys). See `.env.example` in `backend/` for the required configuration formats.
