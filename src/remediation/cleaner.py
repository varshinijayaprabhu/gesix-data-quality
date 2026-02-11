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
        print("[*] Standardizing address formats...")
        df['address'] = df['address'].str.strip()
        df['address'] = df['address'].str.replace('St.', 'Street', regex=False)
        df['address'] = df['address'].str.replace('Ave.', 'Avenue', regex=False)
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

        # Save the "Cleaned" version
        df.to_csv(self.output_file, index=False)
        print(f"[+] Success! Cleaned dataset created at: {self.output_file}")
        print(df)
        return self.output_file

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run_remediation()
