# System Architecture: Data Quality & Trustability Framework

This document outlines the technical design of the framework to ensure data integrity for AI applications.

## 1. Data Flow Logic
The pipeline follows a linear "Ingest -> Unify -> Clean -> Score" architecture:

1.  **Ingestion Layer (`scraper.py`)**: Fetches raw data from various sources (APIs, Scrapers). Data is saved in its original format in `data/raw/` for auditability.
2.  **Transformation Layer (`converter.py`)**: Maps source-specific keys to a unified internal schema. Combines data into a single CSV.
3.  **Remediation Layer (`cleaner.py`)**: Applies deterministic cleaning rules (address normalization, null flagging).

## 2. Directory Layout
```text
project-root/
├── data/               # Persistent data storage
│   ├── raw/            # Immutable raw data captures
│   └── processed/      # Unified and cleaned outputs
├── src/
│   ├── ingestion/      # Data acquisition logic
│   ├── remediation/    # Data cleaning logic
│   ├── qa/             # (Lead B) Validation logic
│   └── reporting/      # (Lead B) Metric visualization
└── main.py             # Pipeline orchestrator
```

## 3. Trustability Scoring (Planned)
The upcoming QA Engine will evaluate datasets across 7 dimensions:
- Completeness
- Accuracy
- Validity
- Consistency
- Uniqueness
- Integrity
- Lineage
