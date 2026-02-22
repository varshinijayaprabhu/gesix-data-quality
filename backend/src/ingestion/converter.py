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
        """Finds the most recent file for a given source using modification time."""
        files = [f for f in os.listdir(self.raw_dir) if f.startswith(prefix) and f.endswith(extension)]
        if not files:
            return None
        # Use full path for mtime comparison
        full_paths = [os.path.join(self.raw_dir, f) for f in files]
        return max(full_paths, key=os.path.getmtime)

    def _flatten_dict(self, d, parent_key='', sep='_'):
        """Helper to flatten nested dictionaries for CSV compatibility."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def parse_api_json(self):
        """Extracts and flattens keys from the API JSON dynamically."""
        file_path = self._get_latest_file("api_data", "json")
        if not file_path:
            return []
            
        print(f"[*] Parsing Dynamic JSON (Flattened): {os.path.basename(file_path)}...")
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[!] API JSON Parsing Error: {e}")
            return []
        
        # Assume 'data' or 'properties' key might contain the list
        records = data.get("data") or data.get("properties") or data
        if not isinstance(records, list): 
            records = [records]

        standardized = []
        for rec in records:
            if isinstance(rec, dict):
                entry = self._flatten_dict(rec)
            else:
                entry = {"value": rec}
            
            entry["source"] = "API_Source"
            entry["ingested_at"] = datetime.now().isoformat()
            standardized.append(entry)
        return standardized

    def parse_json_upload(self):
        """Extracts and flattens keys from uploaded JSON files."""
        file_path = self._get_latest_file("json_upload", "json")
        if not file_path:
            return []
            
        print(f"[*] Parsing JSON Upload (Flattened): {os.path.basename(file_path)}...")
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[!] JSON Upload Parsing Error: {e}")
            return []

        # Try to find the list of records
        records = None
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Common keys for data lists
            for key in ['data', 'properties', 'results', 'items', 'records', 'features']:
                if key in data and isinstance(data[key], list):
                    records = data[key]
                    break
            # Fallback: treat the whole dict as one record if no list found
            if records is None:
                records = [data]
        
        if not records:
            records = []
            
        print(f"[*] JSON Upload: Found {len(records)} records.")

        standardized = []
        for rec in records:
            if isinstance(rec, dict):
                entry = self._flatten_dict(rec)
            else:
                entry = {"value": rec}
            
            entry["source"] = "JSON_Upload"
            entry["ingested_at"] = datetime.now().isoformat()
            standardized.append(entry)
        return standardized

    def parse_xlsx_upload(self):
        """Extracts data from uploaded Excel files using Pandas."""
        file_path = self._get_latest_file("xlsx_upload", "xlsx")
        if not file_path:
            return []
            
        print(f"[*] Parsing Excel Upload: {os.path.basename(file_path)}...")
        try:
            # Read the first sheet by default
            df = pd.read_excel(file_path)
            
            # Simple sanitization: replace NaN with empty string
            df = df.fillna("")
            
            standardized = []
            for _, row in df.iterrows():
                entry = row.to_dict()
                entry["source"] = "XLSX_Upload"
                entry["ingested_at"] = datetime.now().isoformat()
                standardized.append(entry)
                
            return standardized
        except Exception as e:
            print(f"[!] XLSX Parsing Error: {e}")
            return []

    def parse_xml_upload(self):
        """Extracts data from uploaded XML files using Pandas read_xml or simple ElementTree."""
        file_path = self._get_latest_file("xml_upload", "xml")
        if not file_path:
            return []
            
        print(f"[*] Parsing XML Upload: {os.path.basename(file_path)}...")
        try:
            # Pandas read_xml is very robust. We explicitly set encoding for robustness.
            df = pd.read_xml(file_path, encoding="utf-8")
            
            # Simple sanitization: replace NaN with empty string
            df = df.fillna("")
            
            standardized = []
            for _, row in df.iterrows():
                entry = row.to_dict()
                entry["source"] = "XML_Upload"
                entry["ingested_at"] = datetime.now().isoformat()
                standardized.append(entry)
                
            return standardized
        except Exception as e:
            print(f"[!] XML Parsing Error: {e}")
            try:
                import xml.etree.ElementTree as ET
                # For fallback, we must be careful with encoding.
                # ET.parse can take a file-like object with encoding set if we use a stream.
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    tree = ET.parse(f)
                root = tree.getroot()
                records = []
                for child in root:
                    record = {}
                    for subchild in child:
                        record[subchild.tag] = subchild.text
                    record["source"] = "XML_Upload"
                    record["ingested_at"] = datetime.now().isoformat()
                    records.append(record)
                return records
            except Exception as e2:
                print(f"[!] XML Fallback Parsing Error: {e2}")
                return []

    def parse_parquet_upload(self):
        """Standardized Parquet Parser: Reads uploaded binary columnar data."""
        file_path = self._get_latest_file("parquet_upload", "parquet")
        if not file_path:
            return []
            
        print(f"[*] Parsing Parquet Upload: {os.path.basename(file_path)}...")
        try:
            # We use fastparquet or pyarrow engine via pandas
            df = pd.read_parquet(file_path)
            
            # Sanitization
            df = df.fillna("")
            
            standardized = []
            for _, row in df.iterrows():
                entry = row.to_dict()
                entry["source"] = "Parquet_Upload"
                entry["ingested_at"] = datetime.now().isoformat()
                standardized.append(entry)
                
            return standardized
        except Exception as e:
            print(f"[!] Parquet Parsing Error: {e}")
            return []

    def parse_other_upload(self):
        """Universal Format Parser (Heuristic Sensing Engine)."""
        # Get latest universal upload or any file with local prefix
        file_path = self._get_latest_file("universal_", "")
        if not file_path:
            return []
            
        print(f"[*] Heuristic Sensing on: {os.path.basename(file_path)}...")
        try:
            # Step 1: Delimeter Sensing
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                head = f.read(2048) # Sample first 2kb
            
            # Simple heuristic: counts common delims
            delims = [',', ';', '|', '\t']
            found_delim = None
            for d in delims:
                if head.count(d) > 10: 
                    found_delim = d
                    break
            
            if found_delim:
                print(f"[*] Detected Table structure (Delim: '{found_delim}')")
                # Use encoding_errors='replace' to handle non-UTF-8 characters gracefully
                df = pd.read_csv(file_path, sep=found_delim, encoding="utf-8", encoding_errors="replace", on_bad_lines="skip")
                df = df.fillna("")
                standardized = []
                for _, row in df.iterrows():
                    entry = row.to_dict()
                    entry["source"] = "Universal_Heuristic"
                    entry["ingested_at"] = datetime.now().isoformat()
                    standardized.append(entry)
                return standardized

            # Step 2: Key-Value Sensing
            import re
            lines = head.splitlines()
            kv_map = {}
            for line in lines:
                match = re.match(r"^([\w\s_-]+)[:=](.*)$", line.strip())
                if match:
                    key, val = match.groups()
                    kv_map[key.strip().lower().replace(" ", "_")] = val.strip()
            
            if len(kv_map) >= 3:
                print(f"[*] Detected Key-Value structure ({len(kv_map)} fields)")
                # For KV files, we treat the whole file as one record for now
                # In a more advanced version, we'd look for repeating patterns
                entry = kv_map.copy()
                entry["source"] = "Universal_Heuristic_KV"
                entry["sensed_format"] = "Key-Value Pairs"
                entry["filename"] = os.path.basename(file_path)
                entry["ingested_at"] = datetime.now().isoformat()
                return [entry]

            # Step 3: Paragraph/Line Sensing (Default)
            print("[*] Falling back to Paragraph/Line Sensing")
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                full_content = f.read().strip()
            
            if not full_content:
                return []

            # Strategy: If it looks like a list or logs, split by lines. 
            # If it has chunks of text, split by double newlines.
            if "\n\n" in full_content:
                blocks = [b.strip() for b in full_content.split("\n\n") if b.strip()]
            else:
                blocks = [l.strip() for l in full_content.splitlines() if l.strip()]
                
            standardized = []
            for block in blocks:
                standardized.append({
                    "raw_content": block,
                    "filename": os.path.basename(file_path),
                    "source": "Universal_Heuristic_Text",
                    "sensed_format": "Unstructured Text Blocks",
                    "ingested_at": datetime.now().isoformat()
                })
            return standardized

        except Exception as e:
            print(f"[!] Universal Parsing Error: {e}")
            return []

    def parse_zip_upload(self):
        """Extracts and scans multimedia health from uploaded ZIP archives."""
        import zipfile
        from PIL import Image
        import io
        
        file_path = self._get_latest_file("zip_upload", "zip")
        if not file_path:
            return []
            
        print(f"[*] Scanning Multimedia ZIP: {os.path.basename(file_path)}...")
        standardized = []
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                for file_info in zf.infolist():
                    if file_info.is_dir() or "__MACOSX" in file_info.filename or file_info.filename.startswith("._"):
                        continue
                        
                    filename = file_info.filename
                    ext = filename.split('.')[-1].lower() if '.' in filename else ""
                    size_kb = round(file_info.file_size / 1024, 2)
                    
                    # Basic record for all files
                    record = {
                        "filename": filename,
                        "extension": ext,
                        "size_kb": size_kb,
                        "status": "Healthy",
                        "multimedia_type": "Other",
                        "resolution": "—",
                        "source": "ZIP_Archive",
                        "ingested_at": datetime.now().isoformat()
                    }
                    
                    # Multimedia detection
                    if ext in ['jpg', 'jpeg', 'png', 'svg', 'webp', 'gif']:
                        record["multimedia_type"] = "Image"
                        try:
                            with zf.open(filename) as f:
                                img_data = f.read()
                                img = Image.open(io.BytesIO(img_data))
                                width, height = img.width, img.height
                                record["resolution"] = f"{width}x{height}"
                                
                                # Suitability Logic based on Megapixels
                                megapixels = (width * height) / 1_000_000
                                if megapixels >= 3.0:
                                    record["suitability"] = "Professional Print"
                                elif megapixels >= 0.5:
                                    record["suitability"] = "AI Training / HD"
                                elif megapixels >= 0.1:
                                    record["suitability"] = "Standard Web"
                                else:
                                    record["suitability"] = "Low Quality"
                                    record["status"] = "Warning (Low Res)"
                                    
                        except Exception:
                            record["status"] = "Corrupted/Invalid"
                            record["suitability"] = "Unusable"
                    else:
                        record["suitability"] = "N/A (Non-Image)"
                        
                    standardized.append(record)
                    
            print(f"[+] ZIP Scan Complete: {len(standardized)} items found.")
            return standardized
        except Exception as e:
            print(f"[!] ZIP Parsing Error: {e}")
            return []

    def parse_city_html(self):
        """Standardized HTML Table Parser: Extracts data from all tables on the page."""
        file_path = self._get_latest_file("web_scrape", "html")
        if not file_path:
            return []

        print(f"[*] Parsing Dynamic HTML (Standardized): {os.path.basename(file_path)}...")
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                soup = BeautifulSoup(f.read(), "html.parser")
            
            all_tables = soup.find_all("table")
            if not all_tables:
                return []

            standardized = []
            for table in all_tables:
                # Find all rows in this table
                rows = table.find_all("tr")
                if not rows: continue

                # Try to get headers from the first row or <thead>
                header_row = table.find("thead").find("tr") if table.find("thead") else rows[0]
                headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
                
                # If the first row was the header, start data from index 1. 
                # Otherwise (if no clear header was found but we used rows[0]), we might duplicate data,
                # but we'll filter it out if headers and row values are identical.
                data_rows = rows[1:] if not table.find("thead") else rows
                
                for row in data_rows:
                    cols = row.find_all(["td", "th"])
                    if not cols or len(cols) == 0: continue
                    
                    entry = {}
                    for i in range(min(len(headers), len(cols))):
                        key = headers[i] if headers[i] else f"Col_{i}"
                        entry[key] = cols[i].get_text(strip=True)
                    
                    if entry and any(entry.values()):
                        entry["source"] = "Web_Scrape"
                        entry["ingested_at"] = datetime.now().isoformat()
                        standardized.append(entry)
            
            # Fallback: If no tables produced records, try finding lists (li items)
            if not standardized:
                print("[*] No tables found, checking for list items (li)...")
                lists = soup.find_all(["ul", "ol"])
                for lst in lists:
                    items = lst.find_all("li")
                    if len(items) < 5: continue # Ignore small navigation lists
                    
                    for li in items:
                        text = li.get_text(strip=True)
                        if text:
                            entry = {"Item_Content": text}
                            entry["source"] = "Web_Scrape"
                            entry["ingested_at"] = datetime.now().isoformat()
                            standardized.append(entry)
                
            # Simple deduplication or limiting if we have way too many records
            return standardized[:100]
        except Exception as e:
            print(f"[!] HTML Parsing Error: {e}")
            return []

    def parse_user_csv(self):
        """Extracts all columns from manually uploaded CSVs."""
        file_path = self._get_latest_file("user_upload", "csv")
        if not file_path:
            return []
            
        print(f"[*] Parsing User CSV: {os.path.basename(file_path)}...")
        try:
            # Use encoding_errors='replace' for robustness
            df = pd.read_csv(file_path, encoding="utf-8", encoding_errors="replace")
            df = df.fillna("")
            
            standardized = []
            for _, row in df.iterrows():
                entry = row.to_dict()
                entry["source"] = "User_Upload"
                entry["ingested_at"] = datetime.now().isoformat()
                standardized.append(entry)
                
            return standardized
        except Exception as e:
            print(f"[!] CSV Parsing Error: {e}")
            return []

    def parse_pdf_document(self):
        """Extracts structured data (tables, KV pairs) from uploaded PDF documents."""
        file_path = self._get_latest_file("pdf_upload", "pdf")
        if not file_path:
            return []
            
        print(f"[*] Parsing PDF Document (Structured): {os.path.basename(file_path)}...")
        standardized = []
        try:
            import pdfplumber
            import re
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    # 1. Try Table Extraction (Enhanced Strategy)
                    tables = page.extract_tables({
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text",
                        "snap_tolerance": 3,
                    })
                    if not tables:
                        # Fallback to default if text strategy fails
                        tables = page.extract_tables()
                    for table in tables:
                        if len(table) > 1:
                            headers = [str(h).strip() if h else f"Col_{j}" for j, h in enumerate(table[0])]
                            for row in table[1:]:
                                entry = {headers[j]: (str(val).strip() if val else "") for j, val in enumerate(row) if j < len(headers)}
                                if any(entry.values()): # Skip completely empty rows
                                    entry["source"] = "PDF_Table"
                                    entry["page"] = i + 1
                                    entry["ingested_at"] = datetime.now().isoformat()
                                    standardized.append(entry)

                    # 2. Key-Value Sensing (Regex) - for unstructured areas
                    text = page.extract_text()
                    if text:
                        # Find patterns like "Label: Value" or "Label : Value"
                        kv_pairs = re.findall(r'^\s*([\w\s]+)\s*:\s*(.+)$', text, re.MULTILINE)
                        if kv_pairs:
                            kv_entry = {k.strip().replace(' ', '_'): v.strip() for k, v in kv_pairs}
                            kv_entry["source"] = "PDF_KV_Pairs"
                            kv_entry["page"] = i + 1
                            kv_entry["ingested_at"] = datetime.now().isoformat()
                            standardized.append(kv_entry)
                        
                        # 3. Fallback: Page Text (Split by lines for granular record count)
                        if not tables and not kv_pairs:
                            lines = [line.strip() for line in text.split('\n') if line.strip()]
                            for line_idx, line_content in enumerate(lines):
                                standardized.append({
                                    "Content": line_content,
                                    "Page_Number": i + 1,
                                    "Line_Number": line_idx + 1,
                                    "source": "PDF_Text_Line",
                                    "ingested_at": datetime.now().isoformat()
                                })
        except Exception as e:
            print(f"[!] PDF Parsing Error: {e}")
            
        return standardized

    def parse_docx_document(self):
        """Extracts structured data (tables, KV pairs) from uploaded Word documents."""
        file_path = self._get_latest_file("docx_upload", "docx")
        if not file_path:
            return []
            
        print(f"[*] Parsing DOCX Document (Structured): {os.path.basename(file_path)}...")
        standardized = []
        try:
            from docx import Document
            import re
            doc = Document(file_path)
            
            # 1. Extract Tables
            for table in doc.tables:
                if len(table.rows) > 1:
                    headers = [cell.text.strip() if cell.text.strip() else f"Col_{j}" for j, cell in enumerate(table.rows[0].cells)]
                    for row in table.rows[1:]:
                        entry = {headers[j]: row.cells[j].text.strip() for j in range(min(len(headers), len(row.cells)))}
                        if any(entry.values()):
                            entry["source"] = "DOCX_Table"
                            entry["ingested_at"] = datetime.now().isoformat()
                            standardized.append(entry)
            
            # 2. Key-Value Sensing (Regex) from paragraphs
            for para in doc.paragraphs:
                text = para.text.strip()
                if text:
                    # Find patterns like "Label: Value"
                    match = re.match(r'^([^:]+)\s*:\s*(.+)$', text)
                    if match:
                        label, val = match.groups()
                        kv_entry = {label.strip().replace(' ', '_'): val.strip()}
                        kv_entry["source"] = "DOCX_KV_Pairs"
                        kv_entry["ingested_at"] = datetime.now().isoformat()
                        standardized.append(kv_entry)
            
            # 3. Fallback: Full text if no structure found
            if not standardized:
                full_text = "\n".join([p.text for p in doc.paragraphs if p.text.strip()])
                if full_text:
                    standardized.append({
                        "Content": full_text,
                        "source": "DOCX_Text",
                        "ingested_at": datetime.now().isoformat()
                    })
        except Exception as e:
            print(f"[!] DOCX Parsing Error: {e}")
            
        return standardized

    def unify_to_parquet(self, source_filter=None):
        """Combines all sources and saves to /data/processed/raw_structured.parquet."""
        all_data = []
        
        if source_filter == "api" or source_filter is None:
            all_data += self.parse_api_json()
        if source_filter == "scraping" or source_filter is None:
            all_data += self.parse_city_html()
        if source_filter == "upload" or source_filter is None:
            all_data += self.parse_user_csv()
        if source_filter == "pdf" or source_filter is None:
            all_data += self.parse_pdf_document()
        if source_filter == "docx" or source_filter is None:
            all_data += self.parse_docx_document()
        if source_filter == "json_upload" or source_filter is None:
            all_data += self.parse_json_upload()
        if source_filter == "xlsx_upload" or source_filter is None:
            all_data += self.parse_xlsx_upload()
        if source_filter == "zip_upload" or source_filter is None:
            all_data += self.parse_zip_upload()
        if source_filter == "xml_upload" or source_filter is None:
            all_data += self.parse_xml_upload()
        if source_filter == "parquet_upload" or source_filter is None:
            all_data += self.parse_parquet_upload()
        if source_filter == "others_upload" or source_filter is None:
            all_data += self.parse_other_upload()
        
        if not all_data:
            print("[!] No data found to convert.")
            return
        
        # Pandas handle alignment of different column sets automatically
        df = pd.DataFrame(all_data)
        
        # Primary Hub: Parquet (for performance)
        # Security/Format Fix: Force all heterogeneous Excel/JSON/API native objects (like datetime) 
        # into strings before Parquet serialization to prevent schema coercion crashes.
        df = df.astype(str)
        
        output_path = os.path.join(self.output_dir, "raw_structured.parquet")
        df.to_parquet(output_path, index=False)
        
        # Secondary Export: CSV (for user convenience/Excel)
        csv_path = os.path.join(self.output_dir, "raw_structured.csv")
        df.to_csv(csv_path, index=False)
        
        print(f"[+] Success! Unified Parquet Hub created at: {output_path}")
        print(f"[*] Secondary CSV Export available at: {csv_path}")
        return output_path

if __name__ == "__main__":
    converter = DataConverter()
    converter.unify_to_csv()
