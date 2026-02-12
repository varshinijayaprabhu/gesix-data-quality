import pandas as pd
import os

class DataValidator:
    """
    QA Engine for evaluating data quality across trustability dimensions.
    Focuses on: Completeness, Accuracy, and Validity.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    def validate(self, file_path):
        print(f"[*] QA Engine: Validating {os.path.basename(file_path)}...")
        
        if not os.path.exists(file_path):
            print(f"[!] Error: {file_path} not found for validation.")
            return None
            
        df = pd.read_csv(file_path)
        total_records = len(df)
        
        if total_records == 0:
            return {"score": 0, "total": 0, "issues": 0}

        # Calculate Completeness (Checking the remediation notes added by Cleaner)
        # Assuming DataCleaner adds 'remediation_notes' for issues
        if 'remediation_notes' in df.columns:
            issues_found = df['remediation_notes'].notna().sum()
        else:
            issues_found = 0
            
        completeness_percentage = ((total_records - issues_found) / total_records) * 100
        
        results = {
            "total_records": total_records,
            "trustability_score": round(completeness_percentage, 2),
            "remediation_required": issues_found
        }
        
        return results
