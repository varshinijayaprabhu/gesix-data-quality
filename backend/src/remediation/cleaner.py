import pandas as pd
import os
import logging
import numpy as np

# Set up logging for remediation
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

class DataCleaner:
    """
    Member 1's Task: The Remediation Engine.
    Takes 'Dirty' unified data and applies auto-fixes, standardization,
    and surgical QA corrections based on Great Expectations output.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.input_file = os.path.join(self.base_dir, "data", "processed", "raw_structured.parquet")
        self.output_file = os.path.join(self.base_dir, "data", "processed", "cleaned_data.parquet")

    def load_data(self):
        if not os.path.exists(self.input_file):
            logger.error(f"Target file {self.input_file} not found. Cannot run Remediation.")
            return None
        return pd.read_parquet(self.input_file)

    def generic_cleanup(self, df):
        """Universal Cleanup: Drops emptiness, normalizes strings, standardizes dates."""
        logger.info("Running Advanced Universal Cleanup (Trims, Dates, Numerics)...")
        
        # 1. Drop completely empty rows or columns (useless data)
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)
        
        # 2. String Normalization
        str_cols = df.select_dtypes(include=['object', 'string']).columns
        for col in str_cols:
            if col not in ['source', 'ingested_at', 'remediation_notes']:
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings with actual NaN for better null handling downstream
                df[col] = df[col].replace({'': np.nan, '—': np.nan, 'None': np.nan, 'nan': np.nan})

        # 3. Numeric Coercion (if a string column should clearly be numeric)
        for col in df.columns:
            if col not in ['source', 'ingested_at', 'remediation_notes']:
                # Try to convert to numeric, if more than 80% successfully convert, keep it
                try:
                    num_series = pd.to_numeric(df[col], errors='coerce')
                    if num_series.notna().sum() / len(df) > 0.8:
                        df[col] = num_series
                except Exception:
                    pass

        # 4. Datetime Standardization
        date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'timestamp', '_at'])]
        for col in date_cols:
            if col != 'ingested_at':
                try:
                    # Coerce invalid dates to NaT (Not a Time)
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    logger.warning(f"Could not standardize date column '{col}': {e}")
                    
        return df

    def handle_missing_values(self, df):
        """
        Remediation Strategy: 
        Safely flags any row containing nulls or suspicious outliers.
        """
        logger.info("Flagging structural anomalies (nulls/zeros/corrupt cells)...")
        
        # Ensure remediation_notes exists and is a clean string column
        if 'remediation_notes' not in df.columns:
            df['remediation_notes'] = ""
        else:
            # Fill existing NaNs with empty string so concatenation doesn't fail
            df['remediation_notes'] = df['remediation_notes'].fillna("").astype(str)
            
        # Check all numeric columns for exactly 0 or NaN
        num_cols = df.select_dtypes(include=['number', 'float64', 'int64']).columns
        if len(num_cols) > 0:
            mask = df[num_cols].isna().any(axis=1) | (df[num_cols] == 0).any(axis=1)
        else:
            # If no numerics, check for NaNs across all non-metadata columns
            check_cols = [c for c in df.columns if c not in ['source', 'ingested_at', 'remediation_notes']]
            mask = df[check_cols].isna().any(axis=1) if check_cols else pd.Series(False, index=df.index)
            
        # Safely apply the flag to the masked rows without throwing a TypeError
        if mask.any():
            df.loc[mask, 'remediation_notes'] = df.loc[mask, 'remediation_notes'].apply(
                lambda x: x + "Suspicious null or zero value detected; " if not "Suspicious null" in str(x) else x
            )
            
        return df

    def run_remediation(self):
        """Phase 1: Generative cleaning before the QA Pipeline takes over."""
        df = self.load_data()
        if df is None or df.empty:
            logger.warning("No data loaded. Remediation aborted.")
            return None

        # Apply Universal Fixes
        df = self.generic_cleanup(df)
        df = self.handle_missing_values(df)

        # UNIVERSAL DEDUPLICATION: Remove exact full-row duplicates
        initial_len = len(df)
        df = df.drop_duplicates(keep='first')
        
        if len(df) < initial_len:
            logger.info(f"Universal Dedup: Removed {initial_len - len(df)} full-row background duplicates.")

        # Save the "Cleaned" version as Parquet for the GE QA Engine
        df.to_parquet(self.output_file, index=False)
        
        # Maintain a CSV version for human inspection/exports
        csv_output = self.output_file.replace(".parquet", ".csv")
        df.to_csv(csv_output, index=False)
        
        logger.info(f"Success! Phase 1 Cleanup Dataset created at: {self.output_file}")
        return self.output_file

    def targeted_remediation(self, feedback):
        """
        Phase 2: Surgical fixes based on strict Great Expectations QA feedback.
        """
        logger.info("Executing Phase 2: Targeted Great Expectations Remediation...")
        if not os.path.exists(self.output_file):
            return self.run_remediation()
            
        try:
            df = pd.read_parquet(self.output_file)
        except Exception as e:
            logger.error(f"Failed to load dataset for targeted remediation: {e}")
            return None
            
        initial_count = len(df)
        
        if not feedback or 'issue_metadata' not in feedback:
            logger.info("No actionable feedback provided by QA engine. Skipping targeted repair.")
            return self.output_file

        metadata = feedback['issue_metadata']
        
        # Action A: Remove Strict GE Duplicates
        if metadata.get('duplicate_indices'):
            indices_to_drop = [i for i in metadata['duplicate_indices'] if i in df.index]
            if indices_to_drop:
                logger.info(f"Removing {len(indices_to_drop)} QA-identified duplicate records...")
                df = df.drop(index=indices_to_drop)
            
        # Action B: Remove GE Schema/Integrity Failures
        if metadata.get('integrity_fail_indices'):
            targets = [i for i in metadata['integrity_fail_indices'] if i in df.index]
            if targets:
                logger.info(f"Removing {len(targets)} structural anomaly records...")
                df = df.drop(index=targets)

        # Save Final State
        df.to_parquet(self.output_file, index=False)
        
        final_count = len(df)
        logger.info(f"Targeted Fixes Complete. Records reduced from {initial_count} to {final_count}.")
        return self.output_file

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run_remediation()
