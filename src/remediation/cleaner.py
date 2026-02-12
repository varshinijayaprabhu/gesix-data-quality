import pandas as pd
import os

class DataCleaner:
    """
    Member 1's Task: The Remediation Engine.
    Takes 'Dirty' unified data and applies auto-fixes and standardization.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.input_file = os.path.join(self.base_dir, "data", "processed", "raw_structured.csv")
        self.output_file = os.path.join(self.base_dir, "data", "processed", "cleaned_data.csv")

    def load_data(self):
        if not os.path.exists(self.input_file):
            print(f"[!] Error: Target file {self.input_file} not found.")
            return None
        return pd.read_csv(self.input_file)

    def standardize_addresses(self, df):
        """Removes whitespace and standardizes common abbreviations (Format Validity)."""
        print("[*] Standardizing address formats (Title Case + Abbreviations)...")
        # Ensure string type and strip
        df['address'] = df['address'].astype(str).str.strip().str.title()
        
        # Consistent mapping for abbreviations
        abbrev_map = {
            'St': 'Street',
            'St.': 'Street',
            'Ave': 'Avenue',
            'Ave.': 'Avenue',
            'Rd': 'Road',
            'Rd.': 'Road',
            'Blvd': 'Boulevard',
            'Ln': 'Lane'
        }
        
        for abbrev, full in abbrev_map.items():
            # Use regex with word boundaries to avoid partial matches (e.g., "Stone" -> "Streete")
            df['address'] = df['address'].str.replace(rf'\b{abbrev}\b', full, regex=True)
            
        return df

    def handle_missing_prices(self, df):
        """
        Remediation Strategy: 
        If price is null or 0, we flag it. For now, we fill with 'PENDING'.
        (This will be improved after Person 2's QA Engine identifies which rows failed).
        """
        print("[*] Handling missing or invalid price points...")
        # Using 0 or NaN as criteria for 'Missing'
        mask = (df['price'].isna()) | (df['price'] == 0)
        df.loc[mask, 'remediation_notes'] = "Price invalid or missing - flagged for review"
        return df

    def run_remediation(self):
        df = self.load_data()
        if df is None: return

        # Apply Fixes
        df = self.standardize_addresses(df)
        df = self.handle_missing_prices(df)

        # MANDATORY DEDUPLICATION: Ensure "U" flag is cleared even if score high
        initial_len = len(df)
        df = df.drop_duplicates(subset=['address', 'listed_date'], keep='first')
        if len(df) < initial_len:
            print(f"[*] Deduped: Removed {initial_len - len(df)} duplicate records based on Address+Date.")

        # Save the "Cleaned" version
        df.to_csv(self.output_file, index=False)
        print(f"[+] Success! Cleaned dataset created at: {self.output_file}")
        print(df)
        return self.output_file

    def targeted_remediation(self, feedback):
        """
        SMART FEEDBACK LOOP: Applies surgical fixes based on QA Engine feedback.
        """
        print("[*] Running Targeted Remediation Pass...")
        df = pd.read_csv(self.output_file)
        initial_count = len(df)
        
        if not feedback or 'issue_metadata' not in feedback:
            return self.output_file

        metadata = feedback['issue_metadata']
        
        # Action A: Remove Duplicates identified by QA
        if metadata.get('duplicate_indices'):
            print(f"    [-] Removing {len(metadata['duplicate_indices'])} duplicate records...")
            df = df.drop(index=metadata['duplicate_indices'])
            
        # Action B: Remove Corrupt/Integrity Failure records
        if metadata.get('integrity_fail_indices'):
            # Filter valid indices that still exist
            targets = [i for i in metadata['integrity_fail_indices'] if i in df.index]
            if targets:
                print(f"    [-] Removing {len(targets)} records with integrity failures...")
                df = df.drop(index=targets)

        df.to_csv(self.output_file, index=False)
        final_count = len(df)
        print(f"[+] Targeted Fixes Complete. Records reduced from {initial_count} to {final_count}.")
        return self.output_file

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run_remediation()
