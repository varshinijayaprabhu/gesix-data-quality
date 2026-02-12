# Data Quality & Trustability Framework

The Data Quality & Trustability Framework is a comprehensive Python-based solution designed to solve the critical problem of fragmented real estate data. By integrating multiple sources such as Zillow-simulated APIs and city record scrapers, it creates a unified source of truth for property analysis. The framework employs sophisticated remediation logic to automatically fix address inconsistencies and flag missing data points, ensuring high integrity for downstream AI models. It features a modular architecture that separates ingestion, quality assurance, and remediation into distinct, maintainable layers. Ultimately, this system provides the foundation for trustworthy data-driven decision-making in the competitive real estate market.

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

### 👥 Team & Contributions

#### **Lead: Varshini J**
*   **Project Architect**: Designed the modular E-T-L-Q (Extract, Transform, Load, Quality) architecture.
*   **Ingestion Engine**: Developed `scraper.py` for multi-source property data acquisition.
*   **Data Remediation**: Engineered `cleaner.py` for automated data sanitization and format standardization.
*   **Orchestration**: Built the master `main.py` controller and interactive CLI.

#### **Person 1: Varshini J**
*   **Branching Strategy**: Designed the professional Git workflow (`main`, `distributable`, `dev/`).
*   **Documentation Lead**: Authored the project roadmap and contributor guidelines.
*   **QA Consultant**: Verified pipeline logic and integration stability.

#### **Person 2: Sangappa Arjun Malakappanavar**
*   **QA Engine**: Implemented the **7-Dimensional Trustability Framework** (Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, Lineage).
*   **Reporting Layer**: Developed the `generator.py` dashboard for executive quality summaries.
*   **Trustability Scoring**: Engineered the percentage-based data health scoring algorithm.

## 🚦 Getting Started

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Pipeline**:
   ```bash
   python main.py
   ```
