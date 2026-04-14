import sys
import os
from datetime import datetime
from typing import Optional, Dict, Any, TYPE_CHECKING
from paths import get_workspace_dir

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Set up module path so runtime imports work when running from the CLI
sys.path.append(os.path.join(BASE_DIR, "src"))

# Dynamic import helper - visible to both Pylance and runtime
def _import_module(module_path: str, class_name: str) -> Any:
    """Helper to import classes dynamically"""
    import importlib
    try:
        # Try package style first
        module = importlib.import_module(f"src.{module_path}")
    except ImportError:
        # Fallback to direct import
        module = importlib.import_module(module_path)
    return getattr(module, class_name)

if TYPE_CHECKING:
    # Pylance-friendly imports - these resolve correctly for type checking
    from src.ingestion.scraper import UniversalIngestor
    from src.ingestion.converter import DataConverter
    from src.remediation.cleaner import DataCleaner
    from src.qa.validator import DataValidator
else:
    # Runtime imports - use dynamic import to avoid Pylance errors
    UniversalIngestor = _import_module("ingestion.scraper", "UniversalIngestor")
    DataConverter = _import_module("ingestion.converter", "DataConverter")  
    DataCleaner = _import_module("remediation.cleaner", "DataCleaner")


def purge_old_results() -> None:
    """Purge previous analysis results to save disk space."""
    import os
    import shutil
    workspace = get_workspace_dir()
    processed_dir = workspace["processed"]
    if os.path.exists(processed_dir):
        for filename in os.listdir(processed_dir):
            file_path = os.path.join(processed_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
    print("[*] Secure Purge: Stale Analysis Results permanently erased.")


def purge_raw_files() -> None:
    """Purge temporary raw files to save disk space."""
    import os
    workspace = get_workspace_dir()
    raw_dir = workspace["raw"]
    temp_dir = workspace["temp"]
    
    for directory in [raw_dir, temp_dir]:
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename == ".gitkeep":
                    continue
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')
    print("[*] Secure Purge: Temporary Raw Data permanently erased.")


def run_pipeline(start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 source_type: str = "api",
                 source_url: Optional[str] = None,
                 file_path: Optional[str] = None,
                 api_key: Optional[str] = None) -> Dict[str, Any]:
    # Security: Wipe previous results immediately
    purge_old_results()

    print("\n" + "=" * 50)
    print("      UNIVERSAL DATA QUALITY PIPELINE")
    print(f"      MODE: {source_type.upper()}")
    print("=" * 50 + "\n")

    # Normalize dates (handle empty strings from UI)
    start_date = start_date if start_date else None
    end_date = end_date if end_date else None

    try:
        setattr(sys, "_last_step", "Ingestion")
        print(f"[STEP 1/3] Starting Generic Data Ingestion ({source_type})...")
        ingestor = UniversalIngestor()

        raw_path = None
        if source_type == "api":
            raw_path = ingestor.fetch_api_data(source_url, start_date, end_date, api_key)
        elif source_type in ["scraping", "scraper"]:  # Support both names
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

        setattr(sys, "_last_step", "Unification")
        print("\n[STEP 2/3] Transforming Raw Data to Unified Parquet Hub...")
        converter = DataConverter()
        hub_path = converter.unify_to_parquet(source_filter=source_type)

        if not hub_path:
            print("[!] Unification failed. No data found for processing.")
            return {"status": "Unification Failed", "total_records": 0, "overall_trustability": 0}

        setattr(sys, "_last_step", "Remediation")
        print("\n[STEP 3/5] Running Data Remediation (Auto-Fix)...")
        cleaner = DataCleaner()
        cleaned_file = cleaner.run_remediation()

        if not cleaned_file:
            print("[!] Remediation failed.")
            return {"status": "Remediation Failed", "total_records": 0, "overall_trustability": 0}

        sys._last_step = "Validation"
        print("\n[STEP 4/5] Executing Advanced QA Engine (Initial Pass)...")
        DataValidator = _import_module("qa.validator", "DataValidator")
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

        print("\n" + "=" * 50)
        print("[OK] PIPELINE EXECUTION FINISHED!")
        print(f"Unified Hub: {hub_path}")
        print(f"Cleaned Hub: {cleaned_file}")
        print("=" * 50 + "\n")

        # Return a rich payload including artifact paths for the caller to upload
        result_payload = quality_report.copy()
        result_payload.update({
            "hub_path": hub_path,
            "cleaned_path": cleaned_file,
            "status": "completed"
        })
        return result_payload

    except Exception as e:
        import traceback
        print(f"\n[FAIL] PIPELINE FAILED at Step {getattr(sys, '_last_step', 'Unknown')}")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        traceback.print_exc()
        setattr(sys, "_last_step", "Error")
        return {"status": "Error", "error": str(e), "total_records": 0, "overall_trustability": 0}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Data Quality and Trustability Pipeline")
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
