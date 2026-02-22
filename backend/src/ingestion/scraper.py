import requests
import os
import pandas as pd
import json
from datetime import datetime, timedelta

class UniversalIngestor:
    """
    The Universal Ingestion Engine for the Data Quality Framework.
    
    Responsible for acquiring raw data from disparate streams 
    including Live APIs, Web Scraping, and Manual CSV Uploads.
    """
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.raw_dir = os.path.join(base_dir, "data", "raw")
        
        if not os.path.exists(self.raw_dir):
            os.makedirs(self.raw_dir)

    def _save_raw(self, content, source_name, extension="html"):
        """Utility to save raw data with a timestamp for Lineage tracking."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{source_name}_{timestamp}.{extension}"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(str(content))
        
        print(f"[+] Raw data archived: {filepath}")
        return filepath

    def fetch_api_data(self, api_url: str, start_date: str = None, end_date: str = None, api_key: str = None) -> str:
        """
        Fetches data from a provided API URL. Handles optional date filtering and Authentication keys.
        """
        if not api_url or str(api_url).strip() == "":
            print("[!] API Error: No URL provided.")
            return None

        print(f"[*] Calling External API: {api_url}")
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
                    print(f"[*] Auto-Injected OpenWeatherMap appid into URL.")
                
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
                        except: 
                            filtered.append(r)
                    else:
                        filtered.append(r)
                records = filtered
                print(f"[*] API Filtered: {len(records)} records found.")
            else:
                # Default logic: Last 15 records
                records = records[:15]
                print(f"[*] Fetching baseline records (Limit: {len(records)}).")
            
            mock_json = json.dumps({"data": records})
            return self._save_raw(mock_json, "api_data", "json")
        except Exception as e:
            import traceback
            print(f"[!] API Error: {e}")
            traceback.print_exc()
            return None

    def scrape_city_records(self, url: str, start_date: str = None, end_date: str = None) -> str:
        """
        Fetches HTML content from a provided URL.
        """
        if not url:
            print("[!] Scraper Error: No URL provided.")
            return None

        print(f"[*] Scraping External URL: {url}")
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # For simplicity, we save the raw HTML. The converter handles parsing.
            return self._save_raw(response.text, "web_scrape", "html")
        except Exception as e:
            print(f"[!] Scraper Error: {e}")
            return None

    def handle_user_upload(self, local_file_path):
        """
        Processes any user-uploaded CSV file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Processing user upload: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                # Use encoding_errors='replace' for robustness
                df = pd.read_csv(local_file_path, encoding="utf-8", encoding_errors="replace")
                return self._save_raw(df.to_csv(index=False), "user_upload", "csv")
            else:
                print("[!] Error: File not found.")
                return None
        except Exception as e:
            print(f"[!] Upload Error: {e}")
            return None

    def handle_pdf_upload(self, local_file_path):
        """
        Archives a user-uploaded PDF file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving PDF: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"pdf_upload_{timestamp}.pdf"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] PDF archived: {filepath}")
                return filepath
            else:
                print("[!] Error: PDF file not found.")
                return None
        except Exception as e:
            print(f"[!] PDF Archiving Error: {e}")
            return None

    def handle_docx_upload(self, local_file_path):
        """
        Archives a user-uploaded Word document.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving DOCX: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"docx_upload_{timestamp}.docx"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] DOCX archived: {filepath}")
                return filepath
            else:
                print("[!] Error: DOCX file not found.")
                return None
        except Exception as e:
            print(f"[!] DOCX Archiving Error: {e}")
            return None

    def handle_json_upload(self, local_file_path):
        """
        Archives a user-uploaded JSON file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving JSON: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"json_upload_{timestamp}.json"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] JSON archived: {filepath}")
                return filepath
            else:
                print("[!] Error: JSON file not found.")
                return None
        except Exception as e:
            print(f"[!] JSON Archiving Error: {e}")
            return None

    def handle_xlsx_upload(self, local_file_path):
        """
        Archives a user-uploaded Excel file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving XLSX: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"xlsx_upload_{timestamp}.xlsx"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] XLSX archived: {filepath}")
                return filepath
            else:
                print("[!] Error: XLSX file not found.")
                return None
        except Exception as e:
            print(f"[!] XLSX Archiving Error: {e}")
            return None

    def handle_zip_upload(self, local_file_path):
        """
        Archives a user-uploaded ZIP file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving ZIP: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"zip_upload_{timestamp}.zip"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] ZIP archived: {filepath}")
                return filepath
            else:
                print("[!] Error: ZIP file not found.")
                return None
        except Exception as e:
            print(f"[!] ZIP Archiving Error: {e}")
            return None

    def handle_xml_upload(self, local_file_path):
        """
        Archives a user-uploaded XML file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving XML: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"xml_upload_{timestamp}.xml"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] XML archived: {filepath}")
                return filepath
            else:
                print("[!] Error: XML file not found.")
                return None
        except Exception as e:
            print(f"[!] XML Archiving Error: {e}")
            return None

    def handle_parquet_upload(self, local_file_path):
        """
        Archives a user-uploaded Parquet file.
        """
        if not local_file_path:
            return None
            
        print(f"[*] Archiving Parquet: {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"parquet_upload_{timestamp}.parquet"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] Parquet archived: {filepath}")
                return filepath
            else:
                print("[!] Error: Parquet file not found.")
                return None
        except Exception as e:
            print(f"[!] Parquet Archiving Error: {e}")
            return None

    def handle_other_upload(self, local_file_path):
        """
        Archives any user-uploaded file that doesn't fit standard categories.
        """
        if not local_file_path:
            return None
            
        ext = local_file_path.split('.')[-1].lower() if '.' in local_file_path else "bin"
        print(f"[*] Archiving Universal Format ({ext}): {local_file_path}...")
        try:
            if os.path.exists(local_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"universal_{timestamp}.{ext}"
                filepath = os.path.join(self.raw_dir, filename)
                
                import shutil
                shutil.copy2(local_file_path, filepath)
                print(f"[+] File archived via Universal Route: {filepath}")
                return filepath
            else:
                print("[!] Error: File not found.")
                return None
        except Exception as e:
            print(f"[!] Universal Archiving Error: {e}")
            return None
