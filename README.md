# Data Quality & Trustability Framework

A robust Python-based pipeline designed to ingest, unify, and remediate real estate data to ensure high-quality inputs for AI and Analytics models.

## 🚀 Key Features
- **Multi-Source Ingestion**: Supports API (Zillow-sim), Scraped City Records, and User Uploads.
- **Unified Data Schema**: Converts disparate JSON and HTML data into a standardized CSV format.
- **Auto-Remediation**: Normalizes addresses and flags missing/invalid data points.
- **Interactive Pipeline**: Dynamic date range filtering for targeted data analysis.

## 🛠️ Tech Stack
- **Language**: Python 3.x
- **Libraries**: Pandas, BeautifulSoup4, Requests
- **Architecture**: Modular Backend (Ingestion, QA, Remediation Layers)

## 📁 Project Structure
- `src/ingestion/`: Scrapers and format converters.
- `src/remediation/`: Data cleaning and auto-fix logic.
- `data/raw/`: Archived raw responses for data lineage.
- `data/processed/`: Unified and cleaned datasets.

## 🚦 Getting Started

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Pipeline**:
   ```bash
   python main.py
   ```

## 👥 Team
- **Person 1 (Lead A)**: Ingestion & Remediation Specialist.
- **Person 2 (Lead B)**: QA Architect & Reporting Lead (In Progress).
