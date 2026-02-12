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
        
        # 1. Completeness: % of rows where critical fields aren't null
        price_complete = df['price'].notna().sum()
        address_complete = df['address'].notna().sum()
        dim_completeness = ((price_complete + address_complete) / (2 * total)) * 100

        # 2. Accuracy: % of prices within realistic market range
        # Check if 'remediation_notes' flags price invalidity
        invalid_prices = 0
        if 'remediation_notes' in df.columns:
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
        duplicate_mask = df.duplicated(subset=['address'], keep='first')
        dim_uniqueness = ((total - duplicate_mask.sum()) / total) * 100
        duplicate_indices = df[duplicate_mask].index.tolist()

        # 6. Integrity: % of records matching the expected schema type
        integrity_mask = pd.to_numeric(df['price'], errors='coerce').notna()
        dim_integrity = (integrity_mask.sum() / total) * 100
        integrity_fail_indices = df[~integrity_mask].index.tolist()

        # 7. Lineage: % of records with a traceable source
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
            },
            "issue_metadata": {
                "duplicate_indices": duplicate_indices,
                "integrity_fail_indices": integrity_fail_indices
            }
        }
        
        return report
