# Gesix Data Quality & Trustability Framework

The **Gesix Data Quality & Trustability Framework** is a production-grade solution designed to solve the critical problem of fragmented and unreliable real estate data. By integrating multiple sources—including Zillow-simulated APIs and city record scrapers—it creates a unified, high-integrity source of truth for property analysis.

The framework employs sophisticated **auto-remediation logic** to normalize addresses and fix inconsistencies, validated by a rigorous **7-Dimensional Trustability Engine**. It features a modern decoupled architecture with a Python/Flask backend and an interactive React frontend, providing stakeholders with real-time visibility into data health and integrity.

## 🚀 Key Features
- **Modern Full-Stack Architecture**: Decoupled React frontend and Flask-powered API.
- **Multi-Source Ingestion**: Unified processing of API responses (JSON), Scraped records (HTML), and legacy CSVs.
- **7-Dimensional QA**: Automated validation for Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, and Lineage.
- **Auto-Remediation Engine**: Intelligent address normalization and data sanitization.
- **Production Ready**: Served via Waitress WSGI with integrated logging and environment configuration.

## 🛠️ Tech Stack
- **Backend**: Python 3.10+, Flask, Waitress, Pandas, BeautifulSoup4
- **Frontend**: React (Vite), JavaScript (ES6+), Modern CSS
- **DevOps**: Dotenv configuration, Git Flow branching, Modular Architecture

---

## 👥 Team & Contributions

### **Varshini J | Lead & Architect**
*   **System Architecture**: Designed the modular E-T-L-Q (Extract, Transform, Load, Quality) engine.
*   **Ingestion Engine**: Developed the multi-source property data scraper (`scraper.py`).
*   **Data Remediation**: Engineered the automated address normalization and sanitization logic (`cleaner.py`).
*   **Pipeline Orchestration**: Built the master controller (`main.py`) for the core data lifecycle.
*   **Project Strategy**: Established the professional Git workflow and documentation standards.

### **Sangappa Arjun Malakappanavar | QA Specialist**
*   **7-Dimensional Framework**: Implemented the core validation engine to measure data health across 7 critical dimensions.
*   **Trustability Scoring**: Developed the algorithm for generating percentage-based data health scores.
*   **Dashboard Generator**: Engineered the backend reporting layer (`generator.py`) for quality summaries.

### **Dinesh | Full-Stack Engineer**
*   **Web Infrastructure**: Built the React frontend application using Vite for a modern dashboard experience.
*   **API Architecture**: Developed the Flask backend (`app.py`) to bridge the core pipeline with the web UI.
*   **System Integration**: Implemented CORS support and interactive API-driven pipeline triggers.

### **Shreyas | UI/UX Designer**
*   **UI Refinement**: Led the visual enhancement of the Landing and Dashboard pages for a premium feel.
*   **Design System**: Developed the custom CSS architecture and responsive layout components.
*   **Experience Optimization**: Focused on cohesive design language and smooth user transitions.

---

## 🚦 Quick Start

1. **Setup Environment**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize Config**:
   ```bash
   cp .env.example .env
   ```

3. **Run Production Server**:
   ```bash
   python app.py
   ```
   Access the dashboard at `http://localhost:8080`.

*Refer to [DEPLOYMENT.md](file:///c:/Users/Varshini%20J/Desktop/project%203%20-%20gesix%20solutions/DEPLOYMENT.md) for full deployment and build instructions.*
