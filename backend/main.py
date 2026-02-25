import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Set up module path
sys.path.append(os.path.join(BASE_DIR, "src"))

from ingestion.scraper import UniversalIngestor
from ingestion.converter import DataConverter
from remediation.cleaner import DataCleaner

def purge_old_results():
    """Security Fix: Clears all previous analysis results to prevent stale data visibility."""
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    processed_dir = os.path.join(BASE_DIR, "data", "processed")
    reports_dir = os.path.join(BASE_DIR, "data", "reports")
    
    for d in [raw_dir, processed_dir, reports_dir]:
        if os.path.exists(d):
            for f in os.listdir(d):
                # Don't delete .gitkeep or other hidden files
                if f.startswith('.'):
                    continue
                try:
                    os.remove(os.path.join(d, f))
                except:
                    pass
    print("[*] Secure Purge: All old raw, processed, and report records cleared.")

def purge_raw_files():
    """Security Fix: Erases sensitive raw uploaded/API data immediately after processing."""
    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    if os.path.exists(raw_dir):
        for f in os.listdir(raw_dir):
            if f.startswith('.'):
                continue
            try:
                os.remove(os.path.join(raw_dir, f))
            except:
                pass
    print("[*] Secure Purge: Temporary Raw Data permanently erased.")

def run_pipeline(start_date=None, end_date=None, source_type="api", source_url=None, file_path=None, api_key=None):
    # Security: Wipe previous results immediately
    purge_old_results()

    print("\n" + "="*50)
    print("      UNIVERSAL DATA QUALITY PIPELINE")
    print(f"      MODE: {source_type.upper()}")
    print("="*50 + "\n")

    # Normalize dates (handle empty strings from UI)
    start_date = start_date if start_date else None
    end_date = end_date if end_date else None

    try:
        sys._last_step = "Ingestion"
        print(f"[STEP 1/3] Starting Generic Data Ingestion ({source_type})...")
        ingestor = UniversalIngestor()
        
        raw_path = None
        if source_type == "api":
            raw_path = ingestor.fetch_api_data(source_url, start_date, end_date, api_key)
        elif source_type == "scraping":
            raw_path = ingestor.scrape_city_records(source_url, start_date, end_date)
        elif source_type == "upload":
            raw_path = ingestor.handle_user_upload(file_path)
        elif source_type == "pdf":
            raw_path = ingestor.handle_pdf_upload(file_path)
        elif source_type == "docx":
            raw_path = ingestor.handle_docx_upload(file_path)
        elif source_type == "json_upload":
            raw_path = ingestor.handle_json_upload(file_path)
        elif source_type == "xlsx_upload":
            raw_path = ingestor.handle_xlsx_upload(file_path)
        elif source_type == "zip_upload":
            raw_path = ingestor.handle_zip_upload(file_path)
        elif source_type == "xml_upload":
            raw_path = ingestor.handle_xml_upload(file_path)
        elif source_type == "parquet_upload":
            raw_path = ingestor.handle_parquet_upload(file_path)
        elif source_type == "others_upload":
            raw_path = ingestor.handle_other_upload(file_path)
        else:
            print(f"[!] Invalid source type: {source_type}")
            return {"status": "Error", "error": "Invalid source type", "total_records": 0, "overall_trustability": 0}

        if not raw_path:
            print("[!] Ingestion failed or no data found.")
            return {"status": "No Data Found for this period", "total_records": 0, "overall_trustability": 0, "dimensions": {}}

        sys._last_step = "Unification"
        print("\n[STEP 2/3] Transforming Raw Data to Unified Parquet Hub...")
        converter = DataConverter()
        hub_path = converter.unify_to_parquet(source_filter=source_type)
        
        if not hub_path:
            print("[!] Unification failed. No data found for processing.")
            return {"status": "Unification Failed", "total_records": 0, "overall_trustability": 0}

        sys._last_step = "Remediation"
        print("\n[STEP 3/5] Running Data Remediation (Auto-Fix)...")
        cleaner = DataCleaner()
        cleaned_file = cleaner.run_remediation()
        
        if not cleaned_file:
            print("[!] Remediation failed.")
            return {"status": "Remediation Failed", "total_records": 0, "overall_trustability": 0}

        sys._last_step = "Validation"
        print("\n[STEP 4/5] Executing Advanced QA Engine (Initial Pass)...")
        from qa.validator import DataValidator
        validator = DataValidator()
        quality_report = validator.validate(cleaned_file)
        
        if not quality_report:
            print("[!] QA Engine returned no report.")
            return {"status": "QA Failed", "total_records": 0, "overall_trustability": 0}

        print(f"[*] QA Complete: Trustability = {quality_report.get('overall_trustability')}%")

        # Step 5: Smart Feedback Loop (Dynamic Pass)
        if quality_report.get('overall_trustability', 0) < 95.0:
            print(f"\n[SMART LOOP] Trustability ({quality_report.get('overall_trustability')}%) < 95%. Triggering Feedback-Driven Remediation...")
            cleaner.targeted_remediation(quality_report)
            print("[SMART LOOP] Re-validating dataset...")
            quality_report = validator.validate(cleaned_file)
            print(f"[*] Post-Loop Trustability = {quality_report.get('overall_trustability')}%")

        # Step 6: Final Reporting Handled by GUI
        print("\n[STEP 5/5] Finalizing QA Payload for the Dashboard...")

        print("\n" + "="*50)
        print("[OK] PIPELINE EXECUTION FINISHED!")
        print(f"Unified Hub: data/processed/raw_structured.parquet")
        print(f"Cleaned Hub: data/processed/cleaned_data.parquet")
        print("="*50 + "\n")
        
        # Security: Immediately wipe raw uploaded/API data so it doesn't stay on disk while idle
        purge_raw_files()
        
        return quality_report

    except Exception as e:
        import traceback
        print(f"\n[FAIL] PIPELINE FAILED at Step {getattr(sys, '_last_step', 'Unknown')}")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        traceback.print_exc()
        sys._last_step = "Error"
        return {"status": "Error", "error": str(e), "total_records": 0, "overall_trustability": 0}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Gesix Data Quality Pipeline")
    parser.add_argument("--source", default="api", help="Source type (api, scraping, upload, etc.)")
    parser.add_argument("--url", help="Source URL for API or Scraping")
    parser.add_argument("--file", help="Local file path for uploads")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--key", help="API authentication key/token")
    
    args = parser.parse_args()
    
    run_pipeline(
        start_date=args.start,
        end_date=args.end,
        source_type=args.source,
        source_url=args.url,
        file_path=args.file,
        api_key=args.key
    )
