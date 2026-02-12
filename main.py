import sys
import os
import webbrowser
from datetime import datetime, timedelta

# Adding src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from ingestion.scraper import PropertyIngestor
from ingestion.converter import DataConverter
from remediation.cleaner import DataCleaner
from qa.report_generator import generate_quality_report
from qa.scoring import calculate_trustability_score 

def run_pipeline():
    print("\n" + "="*50)
    print("      REAL ESTATE DATA QUALITY PIPELINE (MEMBER 1)")
    print("="*50 + "\n")

    try:
        # Step 1: Ingestion
        print("[STEP 1/4] Starting Data Ingestion...")
        ingestor = PropertyIngestor()
        
        user_start = input("  Enter Start Date [YYYY-MM-DD]: ")
        user_end = input("  Enter End Date   [YYYY-MM-DD]: ")
        
        if not user_start: user_start = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
        if not user_end: user_end = datetime.now().strftime("%Y-%m-%d")
        
        ingestor.fetch_api_data("https://api.rapidapi.com/zillow-sim", start_date=user_start, end_date=user_end)
        ingestor.scrape_city_records("https://city-council.gov/property-tax", start_date=user_start, end_date=user_end)

        # Step 2: Unification
        print("\n[STEP 2/4] Transforming Raw Data to Unified CSV...")
        converter = DataConverter()
        structured_file = converter.unify_to_csv() 
        df = pd.read_csv(structured_file)

        # Step 3: QA Engine
        print("\n[STEP 3/4] Running 7-Dimensional QA Engine...")
        
        # Force report generation and capture results
        results = generate_quality_report(df)
        score, status = calculate_trustability_score(results)
        
        print(f"\n>>> TRUSTABILITY REPORT <<<")
        print(f"Final Score: {score}%")
        print(f"Health Status: {status}")
        
        # Step 4: Remediation (Triggers if score < 90)
        if score < 90:
            print("\n[STEP 4/4] Running Data Remediation (Auto-Fix)...")
            cleaner = DataCleaner()
            cleaner.run_remediation()
            print(f"✅ Remediation Complete. Cleaned Dataset: cleaned_data.csv")
        
        print("\n" + "="*50)
        print("✅ PIPELINE EXECUTION FINISHED!")
        print("="*50 + "\n")

    except Exception as e:
        print(f"\n[❌] PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    import pandas as pd # Required for the unify step
    run_pipeline()