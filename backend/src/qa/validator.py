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
        # [FUTURE TODO: Great Expectations Integration]
        # self.ge_context = gx.get_context(context_root_dir=os.path.join(self.base_dir, "gx"))

    def _run_great_expectations_suite(self, df):
        """
        [FUTURE TODO: Great Expectations Integration]
        The next developer will implement this method to run Great Expectations.
        
        Args:
            df (pd.DataFrame): The dataframe to validate.
        
        Returns:
            dict: A dictionary mapping the 7 dimensions to their GE validation scores.
                  Example: {"Completeness": 95.0, "Accuracy": 88.5, ...}
        """
        # Example GE workflow for the next developer:
        # 1. Create a GE PandasDataset from the dataframe
        #    dataset = ge.from_pandas(df)
        # 2. Run an expectation suite
        #    validation_result = dataset.validate(expectation_suite="gesix_universal_suite")
        # 3. Parse validation_result to calculate dimension scores
        #    ...
        
        # For now, return None so the pipeline falls back to the native Pandas logic
        print("[*] Great Expectations not yet active. Falling back to native Pandas heuristics.")
        return None

    def validate(self, file_path):
        print(f"[*] QA Engine: Performing 7-Dimensional Analysis on {os.path.basename(file_path)}...")
        
        if not os.path.exists(file_path):
            print(f"[!] Error: {file_path} not found.")
            return None
            
        # Support both Parquet (New Hub) and CSV (Legacy/Uploads)
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        else:
            # Use encoding_errors='replace' for robustness during analysis
            df = pd.read_csv(file_path, encoding="utf-8", encoding_errors="replace")
            
        total = len(df)
        if total == 0:
            return {
                "total_records": 0,
                "overall_trustability": 0.0,
                "status": "No Data Found for this period",
                "dimensions": {d: 0.0 for d in ["Completeness", "Accuracy", "Validity", "Consistency", "Uniqueness", "Integrity", "Lineage"]},
                "issue_metadata": {"duplicate_indices": [], "integrity_fail_indices": []}
            }

        # --- [FUTURE TODO: GREAT EXPECTATIONS INTEGRATION] ---
        # Next developer: Uncomment the block below to run Great Expectations.
        # If GE returns a valid score dict, use it. Otherwise, fall back to native heuristics.
        #
        # ge_scores = self._run_great_expectations_suite(df)
        # if ge_scores:
        #     # Merge GE scores into the report directly here...
        #     pass
        # -----------------------------------------------------

        # --- 7 DIMENSIONS CALCULATION (Domain Agnostic / Native Pandas Fallback) ---
        
        # 1. Completeness: Average % of non-null values across ALL columns
        dim_completeness = (df.notna().sum().sum() / df.size) * 100

        # 2. Accuracy: Statistical consistency & Outlier detection
        # Check numerical columns for extreme outliers (Z-score > 3)
        num_cols = df.select_dtypes(include=['number']).columns
        outlier_count = 0
        for col in num_cols:
            if total > 1:
                z_scores = (df[col] - df[col].mean()) / df[col].std()
                outlier_count += (z_scores.abs() > 3).sum()
        dim_accuracy = ((df.size - outlier_count) / df.size) * 100

        # 3. Validity: % of values that match their column's dominant data type
        # (For this simplified version, we'll check non-nulls vs total size)
        dim_validity = (df.notna().sum().sum() / df.size) * 100

        # 4. Consistency: Date sequence checks (if date-like columns exist)
        date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'timestamp', '_at'])]
        consistent_dates = 0
        total_date_cells = 0
        for col in date_cols:
            try:
                dates = pd.to_datetime(df[col], errors='coerce')
                valid_dates = dates.dropna()
                if valid_dates.empty: continue
                
                total_date_cells += len(valid_dates)
                
                # Robust comparison: Convert both sides to UTC
                now = pd.Timestamp.now(tz='UTC')
                compare_dates = valid_dates.dt.tz_localize('UTC') if valid_dates.dt.tz is None else valid_dates.dt.tz_convert('UTC')
                
                consistent_dates += (compare_dates <= now).sum()
            except Exception as e:
                print(f"[!] Consistency Check Error on {col}: {e}")
                continue
        
        dim_consistency = (consistent_dates / total_date_cells * 100) if total_date_cells > 0 else 100

        # 5. Uniqueness: % of records that are not full-row duplicates
        duplicate_mask = df.duplicated(keep='first')
        dim_uniqueness = ((total - duplicate_mask.sum()) / total) * 100
        duplicate_indices = df[duplicate_mask].index.tolist()

        # 6. Integrity: Schema structural consistency
        # Check if any row has more/fewer columns than header (CSV parsing usually handles this, 
        # so we check if critical source/ingested_at metadata exists)
        has_metadata = all(col in df.columns for col in ['source', 'ingested_at'])
        dim_integrity = 100.0 if has_metadata else 50.0
        integrity_fail_indices = [] # Logic could be added to check specific row corruption

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
            "status": "Success",
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
