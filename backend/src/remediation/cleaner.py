import pandas as pd
import os

class DataCleaner:
    """
    Member 1's Task: The Remediation Engine.
    Takes 'Dirty' unified data and applies auto-fixes and standardization.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.input_file = os.path.join(self.base_dir, "data", "processed", "raw_structured.parquet")
        self.output_file = os.path.join(self.base_dir, "data", "processed", "cleaned_data.parquet")

    def load_data(self):
        if not os.path.exists(self.input_file):
            print(f"[!] Error: Target file {self.input_file} not found.")
            return None
        return pd.read_parquet(self.input_file)

    def generic_cleanup(self, df):
        """Universal string normalization: Trims whitespace and normalizes case."""
        print("[*] Running Universal String Normalization...")
        # For object-type columns in Parquet, we still perform string cleanup
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            if col not in ['source', 'ingested_at']: # Skip metadata
                df[col] = df[col].astype(str).str.strip()
        return df

    def handle_missing_values(self, df):
        """
        Remediation Strategy: 
        Flags any row containing nulls or zero-values in numeric columns.
        """
        print("[*] Flagging missing or suspicious zero-values...")
        # Check all numeric columns for 0 or NaN
        num_cols = df.select_dtypes(include=['number']).columns
        # Avoid errors if no numeric columns
        if not num_cols.empty:
            mask = df[num_cols].isna().any(axis=1) | (df[num_cols] == 0).any(axis=1)
        else:
            mask = df.isna().any(axis=1) # Fallback to all columns if no numeric found
            
        if 'remediation_notes' not in df.columns:
            df['remediation_notes'] = ""
            
        df.loc[mask, 'remediation_notes'] += "Suspicious null or zero value detected; "
        return df

    def run_remediation(self):
        df = self.load_data()
        if df is None: return

        # Apply Universal Fixes
        df = self.generic_cleanup(df)
        df = self.handle_missing_values(df)

        # UNIVERSAL DEDUPLICATION: Remove full-row duplicates
        initial_len = len(df)
        df = df.drop_duplicates(keep='first')
        
        if len(df) < initial_len:
            print(f"[*] Deduped: Removed {initial_len - len(df)} full-row duplicates.")

        # Save the "Cleaned" version as Parquet
        df.to_parquet(self.output_file, index=False)
        # Also maintain a CSV version for reports/previews
        csv_output = self.output_file.replace(".parquet", ".csv")
        df.to_csv(csv_output, index=False)
        
        print(f"[+] Success! Universal cleaned dataset created at: {self.output_file}")
        return self.output_file

    def targeted_remediation(self, feedback):
        """
        Surgical fixes based on QA feedback.
        """
        print("[*] Running Targeted Remediation Pass...")
        if not os.path.exists(self.output_file):
            return self.run_remediation()
            
        # Use Parquet for the hub records to avoid encoding issues with binary files
        df = pd.read_parquet(self.output_file)
        initial_count = len(df)
        
        if not feedback or 'issue_metadata' not in feedback:
            return self.output_file

        metadata = feedback['issue_metadata']
        
        # Action A: Remove Duplicates identified by QA
        if metadata.get('duplicate_indices'):
            indices_to_drop = [i for i in metadata['duplicate_indices'] if i in df.index]
            if indices_to_drop:
                print(f"    [-] Removing {len(indices_to_drop)} duplicate records...")
                df = df.drop(index=indices_to_drop)
            
        # Action B: Remove Corrupt/Integrity Failure records
        if metadata.get('integrity_fail_indices'):
            targets = [i for i in metadata['integrity_fail_indices'] if i in df.index]
            if targets:
                print(f"    [-] Removing {len(targets)} records with integrity failures...")
                df = df.drop(index=targets)

        df.to_parquet(self.output_file, index=False)
        final_count = len(df)
        print(f"[+] Targeted Fixes Complete. Records reduced from {initial_count} to {final_count}.")
        return self.output_file

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run_remediation()
