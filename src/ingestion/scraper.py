import requests
import os
import pandas as pd
import json
from datetime import datetime, timedelta

class PropertyIngestor:
    """
    The Universal Ingestion Engine for the Data Quality Framework.
    Supports: Live APIs, City Records (Scraping), and Manual Uploads.
    """
    def __init__(self):
        # Senior Engineer Trick: Find the Project Root automatically
        # Resolves to: c:\Users\Varshini J\Desktop\project 3 - gesix solutions\
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

    def fetch_api_data(self, api_url, start_date=None, end_date=None):
        """
        OPTION 3: Fetch data from a Real Estate API within a time period.
        """
        print(f"[*] Calling Real Estate API: {api_url} for period {start_date} to {end_date}...")
        try:
            properties = []
            start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.min
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.max

            # Today's date for relative mock generation
            today = datetime.now()

            for i in range(1, 31): # 30 potential records
                # Mock Dates: Distributed over the last 30 days
                listing_date = today - timedelta(days=i)
                
                # Only include if within the user's requested time period
                if start_dt <= listing_date <= end_dt:
                    price = 300000 + (i * 15000)
                    if i % 10 == 0: price = 0
                    if i % 15 == 0: price = None
                    
                    properties.append({
                        "add": f"{100 + i} Emerald Street",
                        "price": price,
                        "listed_date": listing_date.strftime("%Y-%m-%d")
                    })
            
            print(f"[*] API Filtered: {len(properties)} records found.")
            mock_json = json.dumps({"properties": properties})
            return self._save_raw(mock_json, "api_zillow", "json")
        except Exception as e:
            print(f"[!] API Error: {e}")
            return None

    def scrape_city_records(self, url, start_date=None, end_date=None):
        """
        OPTION 4: Scrape Public City Records within a time period.
        """
        print(f"[*] Scraping Public Records from: {url} for period {start_date} to {end_date}...")
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.min
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.max

            rows = "<tr><td>Address</td><td>Owner</td><td>Listed Date</td></tr>"
            count = 0
            today = datetime.now()
            for i in range(1, 21):
                # Mock Dates: Distributed over the last 20 days
                listing_date = today - timedelta(days=i)
                
                if start_dt <= listing_date <= end_dt:
                    if i <= 5: 
                        address = f"{100 + i} Emerald Street"
                    else: 
                        address = f"{500 + i} Marble Avenue"
                    
                    rows += f"<tr><td>{address}</td><td>City Resident</td><td>{listing_date.strftime('%Y-%m-%d')}</td></tr>"
                    count += 1
            
            print(f"[*] City Filtered: {count} records found.")
            if count == 0:
                print("[!] No records found for this time period.")
                return None

            mock_html = f"<table>{rows}</table>"
            return self._save_raw(mock_html, "city_records", "html")
        except Exception as e:
            print(f"[!] Scraper Error: {e}")
            return None

    def handle_user_upload(self, local_file_path):
        """
        USER INPUT: Handle manual CSV/JSON uploads.
        """
        print(f"[*] Processing user upload: {local_file_path}...")
        try:
            # Copying the user file to our raw data repository for Lineage
            if os.path.exists(local_file_path):
                df = pd.read_csv(local_file_path)
                return self._save_raw(df.to_csv(), "user_upload", "csv")
            else:
                print("[!] Error: File not found.")
                return None
        except Exception as e:
            print(f"[!] Upload Error: {e}")
            return None

if __name__ == "__main__":
    ingestor = PropertyIngestor()
    
    # Calculate current dates for a professional dynamic feel
    today_str = datetime.now().strftime("%Y-%m-%d")
    default_start_str = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")

    print("\n--- Property Ingestion System ---")
    print(f"Current Date: {today_str}")
    print("Format required: YYYY-MM-DD")
    
    # Senior Engineer Trick: Using input() with dynamic defaults
    user_start = input(f"Enter Start Date [Default {default_start_str}]: ")
    user_end = input(f"Enter End Date [Default {today_str}]: ")

    # Fallback to dynamic defaults if user just presses Enter
    if not user_start: user_start = default_start_str
    if not user_end: user_end = today_str
    
    # 1. Test API Path with filtering
    ingestor.fetch_api_data("https://api.rapidapi.com/zillow-sim", start_date=user_start, end_date=user_end)
    
    # 2. Test City Records Path with filtering
    ingestor.scrape_city_records("https://city-council.gov/property-tax", start_date=user_start, end_date=user_end)
