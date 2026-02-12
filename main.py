import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Adding src to path so we can import our modules
sys.path.append(os.path.join(os.getcwd(), "src"))

from ingestion.scraper import PropertyIngestor
from ingestion.converter import DataConverter
from remediation.cleaner import DataCleaner
from qa.validator import DataValidator
from reporting.generator import ReportGenerator

def run_pipeline():
    print("\n" + "="*50)
    print("      REAL ESTATE DATA QUALITY PIPELINE (MASTER)")
    print("="*50 + "\n")

    try:
        # Step 1: Ingestion
        print("[STEP 1/5] Starting Data Ingestion...")
        ingestor = PropertyIngestor()
        
        # Capture user dates for the entire pipeline
        print("\nPipeline Configuration:")
        print("Format: YYYY-MM-DD (e.g., 2026-02-01)")
        user_start = input("  Enter Start Date [Enter for default]: ")
        user_end = input("  Enter End Date   [Enter for default]: ")
        
        # Fallbacks
        if not user_start: user_start = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
        if not user_end: user_end = datetime.now().strftime("%Y-%m-%d")
        
        ingestor.fetch_api_data("https://api.rapidapi.com/zillow-sim", start_date=user_start, end_date=user_end)
        ingestor.scrape_city_records("https://city-council.gov/property-tax", start_date=user_start, end_date=user_end)
        
        # Step 2: Unification/Conversion
        print("\n[STEP 2/5] Transforming Raw Data to Unified CSV...")
        converter = DataConverter()
        structured_file = converter.unify_to_csv()

        # Step 3: Remediation/Cleaning
        print("\n[STEP 3/5] Running Data Remediation (Auto-Fix)...")
        cleaner = DataCleaner()
        cleaned_file = cleaner.run_remediation()

        # Step 4: Quality Assurance (Consolidated 7-Dimensions)
        print("\n[STEP 4/5] Executing Advanced QA Engine...")
        validator = DataValidator()
        quality_report = validator.validate(cleaned_file)

        # Step 5: Final Reporting
        print("\n[STEP 5/5] Generating Data Quality Dashboard...")
        reporter = ReportGenerator()
        reporter.generate_summary(quality_report)
        reporter.save_report(quality_report)

        print("\n" + "="*50)
        print("✅ PIPELINE EXECUTION FINISHED!")
        print(f"Final Dataset: data/processed/cleaned_data.csv")
        print("="*50 + "\n")

    except Exception as e:
        print(f"\n[❌] PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    run_pipeline()