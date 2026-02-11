import os
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

class DataConverter:
    """
    Member 1's Task: The Converter.
    Unifies disparate raw formats (JSON, HTML) into a single Structured CSV.
    """
    def __init__(self):
        # Discover base directory
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.raw_dir = os.path.join(self.base_dir, "data", "raw")
        self.output_dir = os.path.join(self.base_dir, "data", "processed")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _get_latest_file(self, prefix, extension):
        """Finds the most recent file for a given source for Lineage."""
        files = [f for f in os.listdir(self.raw_dir) if f.startswith(prefix) and f.endswith(extension)]
        if not files:
            return None
        return os.path.join(self.raw_dir, sorted(files)[-1])

    def parse_api_json(self):
        """Extracts property data from the Zillow API JSON."""
        file_path = self._get_latest_file("api_zillow", "json")
        if not file_path:
            return []
            
        print(f"[*] Parsing JSON: {os.path.basename(file_path)}...")
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Mapping API keys to our Unified Schema
        standardized = []
        for prop in data.get("properties", []):
            standardized.append({
                "address": prop.get("add"),
                "price": prop.get("price"),
                "listed_date": prop.get("listed_date"),
                "source": "Zillow_API",
                "ingested_at": datetime.now().isoformat()
            })
        return standardized

    def parse_city_html(self):
        """Scrapes property data from the City Records HTML table."""
        file_path = self._get_latest_file("city_records", "html")
        if not file_path:
            return []

        print(f"[*] Parsing HTML: {os.path.basename(file_path)}...")
        with open(file_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
        
        table_rows = soup.find_all("tr")[1:] # Skip header row
        standardized = []
        for row in table_rows:
            cols = row.find_all("td")
            standardized.append({
                "address": cols[0].text,
                "price": None,
                "listed_date": cols[2].text if len(cols) > 2 else None,
                "source": "City_Records",
                "ingested_at": datetime.now().isoformat()
            })
        return standardized

    def unify_to_csv(self):
        """Combines all sources and saves to /data/processed/raw_structured.csv."""
        all_data = self.parse_api_json() + self.parse_city_html()
        
        if not all_data:
            print("[!] No data found to convert.")
            return

        df = pd.DataFrame(all_data)
        output_path = os.path.join(self.output_dir, "raw_structured.csv")
        df.to_csv(output_path, index=False)
        
        print(f"[+] Success! Unified dataset created at: {output_path}")
        print(df)
        return output_path

if __name__ == "__main__":
    converter = DataConverter()
    converter.unify_to_csv()
