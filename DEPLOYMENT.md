# Deployment Guide - Gesix Data Quality

This guide explains how to deploy the Gesix Data Quality application for production use.

## Prerequisites

- Python 3.10+
- Node.js & npm (for building the frontend)

## Preparation

1.  **Backend Dependencies**:
    Install all required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Frontend Build**:
    Navigate to the `frontend` directory and build the production assets:
    ```bash
    cd frontend
    npm install
    npm run build
    ```
    This creates a `dist` folder which the backend will serve.

3.  **Environment Variables**:
    Create a `.env` file in the root directory based on `.env.example`:
    ```bash
    cp .env.example .env
    ```
    Adjust the `PORT` and `HOST` if necessary.

## Running the Application

In production, we use the `waitress` WSGI server (integrated into `app.py`).

1.  **Start the Server**:
    From the project root, run:
    ```bash
    python app.py
    ```

2.  **Verification**:
    The application will be available at `http://localhost:8080` (or your configured port).
    The logs will be stored in the `logs/` directory.

## Directory Structure (for Deployment)

```
.
├── app.py              # Production entry point
├── requirements.txt    # Python dependencies
├── .env                # Environment configuration
├── logs/               # Application logs
├── frontend/
│   └── dist/           # Built static assets
├── data/               # Data files (raw & processed)
└── src/                # Source code
```
