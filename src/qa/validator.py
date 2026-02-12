import pandas as pd
import os
from datetime import datetime

class DataValidator:
    """
    Advanced QA Engine: Evaluates data using the 7 Dimensions of Trustability.
    Focuses on: Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, Lineage.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def validate(self, file_path):
        print(f"[*] QA Engine: Performing 7-Dimensional Analysis on {os.path.basename(file_path)}...")
        
        if not os.path.exists(file_path):
            print(f"[!] Error: {file_path} not found.")
            return None
            
        df = pd.read_csv(file_path)
        total = len(df)
        if total == 0:
            return None

        # --- 7 DIMENSIONS CALCULATION ---
        
        # 1. Completeness: % of rows where critical fields (address, price) aren't null or remediated
        price_complete = df['price'].notna().sum()
        address_complete = df['address'].notna().sum()
        dim_completeness = ((price_complete + address_complete) / (2 * total)) * 100

        # 2. Accuracy: % of prices within realistic market range (e.g., $50k to $10M)
        # We check if 'remediation_notes' contains "invalid" for prices
        invalid_prices = df[df['remediation_notes'].str.contains("Price invalid", na=False)].shape[0]
        dim_accuracy = ((total - invalid_prices) / total) * 100

        # 3. Validity: % of dates matching YYYY-MM-DD format
        valid_dates = pd.to_datetime(df['listed_date'], format='%Y-%m-%d', errors='coerce').notna().sum()
        dim_validity = (valid_dates / total) * 100

        # 4. Consistency: % of records where listed_date is not in the future
        try:
            dates = pd.to_datetime(df['listed_date'], errors='coerce')
            consistent_dates = (dates <= datetime.now()).sum()
            dim_consistency = (consistent_dates / total) * 100
        except:
            dim_consistency = 0

        # 5. Uniqueness: % of records with unique addresses
        unique_addresses = df['address'].nunique()
        dim_uniqueness = (unique_addresses / total) * 100

        # 6. Integrity: % of records matching the expected schema type (e.g., price is numeric)
        # Since CSV reads everything as strings potentially, we check if price can be float
        integrity_check = pd.to_numeric(df['price'], errors='coerce').notna().sum()
        dim_integrity = (integrity_check / total) * 100

        # 7. Lineage: % of records with a traceable source (source column exists and is not empty)
        # Assuming DataConverter adds 'source'
        if 'source' in df.columns:
            lineage_count = df['source'].notna().sum()
        else:
            lineage_count = 0
        dim_lineage = (lineage_count / total) * 100

        # FINAL AGGREGATED SCORE
        overall_score = (dim_completeness + dim_accuracy + dim_validity + 
                         dim_consistency + dim_uniqueness + dim_integrity + dim_lineage) / 7

        report = {
            "total_records": total,
            "overall_trustability": round(overall_score, 2),
            "dimensions": {
                "Completeness": round(dim_completeness, 2),
                "Accuracy": round(dim_accuracy, 2),
                "Validity": round(dim_validity, 2),
                "Consistency": round(dim_consistency, 2),
                "Uniqueness": round(dim_uniqueness, 2),
                "Integrity": round(dim_integrity, 2),
                "Lineage": round(dim_lineage, 2)
            }
        }
        
        return report
