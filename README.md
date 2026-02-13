# Data Quality & Trustability Framework - Frontend & API Integration

This branch focuses on the **Frontend Visualization** and **API Infrastructure** for the Gesix Data Quality platform. It provides a modern React-based interface for interacting with the data quality pipeline and an asynchronous backend server to manage data processing.

## 🚀 Key Features (dev/dinesh)
- **React Dashboard**: A modern frontend for visualizing trustability metrics and data health.
- **Flask API Layer**: An asynchronous backend providing endpoints for data retrieval and pipeline execution.
- **CORS-Enabled Framework**: Seamless communication between the decoupled frontend and backend.
- **Dynamic Processing TRigger**: Ability to trigger data ingestion and QA cycles directly from the web interface.

## 🛠️ Tech Stack
- **Frontend**: React (Vite), CSS3, JavaScript (ES6+), React Router
- **Backend**: Flask, Flask-CORS
- **Build Tools**: npm, Vite

## 📁 Branch Structure
- `frontend/`: Full React application including source files and build configurations.
- `app.py`: Flask backend server implementing the API endpoints.
- `requirements.txt`: Updated dependencies for web and cross-origin support.

### 👥 Contributor: Dinesh
*   **Web Infrastructure Lead**: Built the React-based frontend application from scratch.
*   **API Architect**: Developed the Flask backend (`app.py`) to bridge the data pipeline with the web UI.
*   **Integration Expert**: Implemented cross-origin support and API-driven pipeline triggers.
*   **UI/UX Developer**: Designed the initial dashboard components for data visualization.

## 🚦 Getting Started (Local Development)

1. **Backend**:
   ```bash
   pip install -r requirements.txt
   python app.py
   ```

2. **Frontend**:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

---
*This branch focuses specifically on the interaction layer of the Gesix Data Quality project.*
