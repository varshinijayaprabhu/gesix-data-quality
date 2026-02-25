import requests
import os
import shutil
import pandas as pd
import json
import logging
from datetime import datetime, timedelta

# Set up logging for ingestion
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Basic console handler if not already configured upstream
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

class UniversalIngestor:
    """
    The Universal Ingestion Engine for the Data Quality Framework.
    
    Responsible for acquiring raw data from disparate streams 
    including Live APIs, Web Scraping, and File Uploads.
    """
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.raw_dir = os.path.join(base_dir, "data", "raw")
        
        if not os.path.exists(self.raw_dir):
            os.makedirs(self.raw_dir)
            logger.info(f"Created raw data directory at {self.raw_dir}")

    def _save_raw(self, content, source_name, extension="html"):
        """Utility to save raw data with a timestamp for Lineage tracking."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.{extension}"
        filepath = os.path.join(self.raw_dir, filename)
        
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(str(content))
            logger.info(f"Raw data archived: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save raw data {filename}: {e}")
            return None

    def fetch_api_data(self, api_url: str, start_date: str = None, end_date: str = None, api_key: str = None) -> str:
        """
        Fetches data from a provided API URL. Handles optional date filtering and Authentication keys.
        """
        if not api_url or str(api_url).strip() == "":
            logger.error("API Error: No URL provided.")
            return None

        logger.info(f"Calling External API: {api_url}")
        try:
            from datetime import timezone
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            if api_key:
                # Standard Auth Headers (Bearer / x-api-key)
                headers["Authorization"] = f"Bearer {api_key}"
                headers["x-api-key"] = api_key
                
                # Special Case: OpenWeatherMap requires the key in the URL as 'appid'
                if "openweathermap.org" in api_url and "appid=" not in api_url:
                    connector = "&" if "?" in api_url else "?"
                    api_url = f"{api_url}{connector}appid={api_key}"
                    logger.info("Auto-Injected OpenWeatherMap appid into URL.")
                
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            content = response.json()
            
            # API can return a list or a dict with a 'data' key
            # Also check 'works' for OpenLibrary example
            if isinstance(content, list):
                records = content
            else:
                records = content.get("data") or content.get("works")
                if records is None:
                    # Treat the single generic JSON object as the record itself
                    records = [content]
            
            if not isinstance(records, list):
                records = [records]

            # Apply date filtering if requested
            if start_date or end_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.min
                end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.max
                
                # Make naive for comparison by default, will adapt if item is aware
                filtered = []
                for r in records:
                    # Heuristic for finding a date field
                    date_val = next((v for k, v in r.items() if any(x in k.lower() for x in ["date", "time", "created"])), None)
                    if date_val:
                        try:
                            item_dt = datetime.fromisoformat(str(date_val).replace('Z', '+00:00'))
                            
                            # Robust comparison: Convert everything to UTC if needed
                            if item_dt.tzinfo is not None:
                                s_dt = pd.Timestamp(start_dt).tz_localize('UTC')
                                e_dt = pd.Timestamp(end_dt).tz_localize('UTC')
                            else:
                                s_dt, e_dt = start_dt, end_dt
                                
                            if s_dt <= item_dt <= e_dt:
                                filtered.append(r)
                        except ValueError as ve:
                            logger.warning(f"Skipping date filter for record due to unparseable date '{date_val}': {ve}")
                            filtered.append(r)
                        except Exception as e:
                            logger.error(f"Unexpected error parsing date '{date_val}': {e}")
                            filtered.append(r)
                    else:
                        filtered.append(r)
                records = filtered
                logger.info(f"API Filtered: {len(records)} records found.")
            else:
                # Default logic: Last 15 records
                records = records[:15]
                logger.info(f"Fetching baseline records (Limit: {len(records)}).")
            
            mock_json = json.dumps({"data": records})
            return self._save_raw(mock_json, "api_data", "json")
        except requests.exceptions.HTTPError as he:
            logger.error(f"HTTP API Error: {he}")
            return None
        except Exception as e:
            import traceback
            logger.error(f"Unexpected API Error: {e}\n{traceback.format_exc()}")
            return None

    def scrape_city_records(self, url: str, start_date: str = None, end_date: str = None) -> str:
        """
        Fetches HTML content from a provided URL.
        """
        if not url:
            logger.error("Scraper Error: No URL provided.")
            return None

        logger.info(f"Scraping External URL: {url}")
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # For simplicity, we save the raw HTML. The converter handles parsing.
            return self._save_raw(response.text, "web_scrape", "html")
        except Exception as e:
            logger.error(f"Scraper Error: {e}")
            return None

    def handle_file_upload(self, local_file_path: str) -> str:
        """
        Universally handles user file uploads regardless of extension.
        Directly copies the file to the raw directory with a lineage timestamp
        to avoid unnecessary memory overhead.
        """
        if not local_file_path:
            logger.error("Upload Error: No file path provided.")
            return None
            
        logger.info(f"Processing uploaded file: {local_file_path}")
        try:
            if not os.path.exists(local_file_path):
                logger.error(f"Upload Error: File not found at {local_file_path}")
                return None
                
            # Extract extension or default to bin
            _, ext = os.path.splitext(local_file_path)
            ext = ext.lstrip('.').lower() if ext else "bin"
            
            # Provide a descriptive prefix based on file type
            prefix = f"{ext}_upload" if ext != "bin" else "universal_upload"
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{prefix}_{timestamp}.{ext}"
            filepath = os.path.join(self.raw_dir, filename)
            
            # Efficiently copy the file without loading it into pandas/memory
            shutil.copy2(local_file_path, filepath)
            logger.info(f"File archived successfully: {filepath}")
            
            return filepath
            
        except PermissionError:
            logger.error(f"Upload Error: Permission denied to read {local_file_path} or write to {filepath}")
            return None
        except Exception as e:
            logger.error(f"Upload Error handling {local_file_path}: {e}")
            return None

    # Backward compatibility mappings for older scripts that might still call specific methods
    def handle_user_upload(self, p): return self.handle_file_upload(p)
    def handle_pdf_upload(self, p): return self.handle_file_upload(p)
    def handle_docx_upload(self, p): return self.handle_file_upload(p)
    def handle_json_upload(self, p): return self.handle_file_upload(p)
    def handle_xlsx_upload(self, p): return self.handle_file_upload(p)
    def handle_zip_upload(self, p): return self.handle_file_upload(p)
    def handle_xml_upload(self, p): return self.handle_file_upload(p)
    def handle_parquet_upload(self, p): return self.handle_file_upload(p)
    def handle_other_upload(self, local_file_path):
        """Forces 'universal_' prefix so the converter's parse_other_upload can find it."""
        if not local_file_path or not os.path.exists(local_file_path):
            logger.error("Upload Error: No file path provided or file not found.")
            return None
        _, ext = os.path.splitext(local_file_path)
        ext = ext.lstrip('.').lower() if ext else "bin"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"universal_{timestamp}.{ext}"
        filepath = os.path.join(self.raw_dir, filename)
        shutil.copy2(local_file_path, filepath)
        logger.info(f"Universal file archived: {filepath}")
        return filepath
