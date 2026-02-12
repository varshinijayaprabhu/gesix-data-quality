import sys
import os

# Adding src to path so we can import our modules
sys.path.append(os.path.join(os.getcwd(), "src"))

from ingestion.scraper import PropertyIngestor
from ingestion.converter import DataConverter
from remediation.cleaner import DataCleaner

def run_pipeline():
    print("\n" + "="*50)
    print("      REAL ESTATE DATA QUALITY PIPELINE (MEMBER 1)")
    print("="*50 + "\n")

    try:
        # Step 1: Ingestion
        print("[STEP 1/3] Starting Data Ingestion...")
        ingestor = PropertyIngestor()
        
        # Capture user dates for the entire pipeline
        print("\nPipeline Configuration:")
        print("Format: YYYY-MM-DD (e.g., 2026-02-01)")
        user_start = input("  Enter Start Date [Enter for default]: ")
        user_end = input("  Enter End Date   [Enter for default]: ")
        
        # Fallbacks (logic matching scraper)
        from datetime import datetime, timedelta
        if not user_start: user_start = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
        if not user_end: user_end = datetime.now().strftime("%Y-%m-%d")
        
        raw_api = ingestor.fetch_api_data("https://api.rapidapi.com/zillow-sim", start_date=user_start, end_date=user_end)
        raw_city = ingestor.scrape_city_records("https://city-council.gov/property-tax", start_date=user_start, end_date=user_end)
        
        if not raw_api and not raw_city:
            print("[!] Pipeline aborted: No data fetched.")
            return

        # Step 2: Unification/Conversion
        print("\n[STEP 2/3] Transforming Raw Data to Unified CSV...")
        converter = DataConverter()
        converter.unify_to_csv()

        # Step 3: Remediation/Cleaning
        print("\n[STEP 3/5] Running Data Remediation (Auto-Fix)...")
        cleaner = DataCleaner()
        cleaned_file = cleaner.run_remediation()

        # Step 4: Quality Assurance (Initial Pass)
        print("\n[STEP 4/5] Executing Advanced QA Engine (Initial Pass)...")
        from qa.validator import DataValidator
        validator = DataValidator()
        quality_report = validator.validate(cleaned_file)
        
        # Step 5: Smart Feedback Loop (Dynamic Pass)
        # If score is below 95%, trigger targeted remediation
        if quality_report['overall_trustability'] < 95.0:
            print("\n[SMART LOOP] Trustability < 95%. Triggering Feedback-Driven Remediation...")
            cleaner.targeted_remediation(quality_report)
            
            print("[SMART LOOP] Re-validating dataset...")
            quality_report = validator.validate(cleaned_file)

        # Step 6: Final Reporting
        print("\n[STEP 5/5] Generating Final Data Quality Dashboard...")
        from reporting.generator import ReportGenerator
        reporter = ReportGenerator()
        reporter.generate_summary(quality_report)
        reporter.save_report(quality_report)
        reporter.save_html_report(quality_report)

        print("\n" + "="*50)
        print("✅ PIPELINE EXECUTION FINISHED!")
        print(f"Final Dataset: data/processed/cleaned_data.csv")
        print("="*50 + "\n")

    except Exception as e:
        print(f"\n[❌] PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    run_pipeline()
