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

## � Meet the Team

### Lead 1 (Project Architect & Primary Developer)
Lead 1 is the core visionary and technical lead responsible for the foundation of the framework.
- **Architecture Design**: Conceptualized and modularized the pipeline into decoupled Ingest-Unify-Remediate layers.
- **Ingestion & Transformation**: Developed the multi-source `scraper.py` and the `converter.py` logic for universal schema mapping.
- **Auto-Remediation Engine**: Engineered the `cleaner.py` module to handle deterministic address normalization and null flagging.
- **Orchestration**: Developed the interactive `main.py` CLI, allowing for dynamic date-range filtering and pipeline control.

### Contributor 1 - Varshini (Workflow & Documentation Specialist)
Varshini leads the project's collaboration strategy and professional documentation standards.
- **Branching Strategy Lead**: Designed and implemented the professional `main -> distributable -> dev` Git workflow to ensure production stability.
- **Documentation Architect**: Authored the comprehensive Project Guides, including `contributor_guide.md` and `branching_guide.md`.
- **Quality Standards**: Responsible for maintaining the repository's professional structure and ensuring contributor compliance.

## 🚦 Getting Started

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Pipeline**:
   ```bash
   python main.py
   ```
