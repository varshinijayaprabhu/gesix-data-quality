import pandas as pd
import os
from datetime import datetime
import great_expectations as ge
import warnings

class DataValidator:
    """
    Advanced QA Engine: Evaluates data using the 7 Dimensions of Trustability
    powered by the Great Expectations Framework.
    Focuses on: Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, Lineage.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def _run_great_expectations_suite(self, df):
        """
        Dynamically builds and runs a Great Expectations suite against the unified Parquet dataframe.
        Routes the success ratios of specific expectations to their corresponding Quality Dimensions.
        
        Args:
            df (pd.DataFrame): The dataframe to validate.
        
        Returns:
            dict: A dictionary mapping the 7 dimensions to their GE validation scores.
                  Example: {"Completeness": 95.0, "Accuracy": 88.5, ...}
        """
        if df.empty:
            return None
            
        # GE v1.0+ Ephemeral Context API
        ctx = ge.get_context(mode="ephemeral")
        
        # We use a try-except block here because if the datasource already exists in the context, it throws an error
        try:
            ds = ctx.data_sources.add_pandas("pandas_datasource")
        except Exception:
            ds = ctx.get_datasource("pandas_datasource")
            
        da = ds.add_dataframe_asset("df_asset")
        batch_request = da.build_batch_request(options={"dataframe": df})
        
        # Suppress the Expectation Deprecation Warning coming from GE core code
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dataset = ctx.get_validator(batch_request=batch_request, create_expectation_suite_with_name="gesix_suite")
            
            total_rows = len(df)
            
            # We will collect success percentages for each dimension
            dimension_scores = {
                "Completeness": [],
                "Accuracy": [],
                "Validity": [],
                "Consistency": [],
                "Uniqueness": [],
                "Integrity": [],
                "Lineage": []
            }
            
            # 1. COMPLETENESS: Expect columns to not be null
            for col in df.columns:
                res = dataset.expect_column_values_to_not_be_null(column=col, result_format="SUMMARY")
                if res.success or res.result.get("unexpected_percent") is not None:
                    percent_complete = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                    dimension_scores["Completeness"].append(percent_complete)

            # 2. ACCURACY & 3. VALIDITY: Type checking and Outlier bounds
            num_cols = df.select_dtypes(include=['number']).columns
            for col in num_cols:
                # Accuracy: Values within statistical bounds (using rough 3 std dev proxy)
                if total_rows > 1:
                    mean = df[col].mean()
                    std = df[col].std()
                    if pd.notna(mean) and pd.notna(std) and std != 0:
                        min_val = mean - (3 * std)
                        max_val = mean + (3 * std)
                        res = dataset.expect_column_values_to_be_between(column=col, min_value=min_val, max_value=max_val, result_format="SUMMARY")
                        percent_accurate = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                        dimension_scores["Accuracy"].append(percent_accurate)
                
                # Validity: Type matching
                res = dataset.expect_column_values_to_be_of_type(column=col, type_="int") if pd.api.types.is_integer_dtype(df[col]) else dataset.expect_column_values_to_be_of_type(column=col, type_="float")
                dimension_scores["Validity"].append(100.0 if res.success else 0.0)
                
            text_cols = df.select_dtypes(include=['object', 'string']).columns
            for col in text_cols:
                res = dataset.expect_column_values_to_be_of_type(column=col, type_="str")
                dimension_scores["Validity"].append(100.0 if res.success else 0.0)

            # 4. CONSISTENCY: Date parseable formats
            date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'timestamp', '_at'])]
            for col in date_cols:
                res = dataset.expect_column_values_to_be_dateutil_parseable(column=col, result_format="SUMMARY")
                percent_consistent = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                dimension_scores["Consistency"].append(percent_consistent)

            # 5. UNIQUENESS: Row-level duplication
            try:
                res = dataset.expect_compound_columns_to_be_unique(column_list=list(df.columns), result_format="SUMMARY")
                unexpected_count = res.result.get("unexpected_count", 0)
                percent_unique = ((total_rows - unexpected_count) / total_rows) * 100.0 if total_rows > 0 else 100.0
                dimension_scores["Uniqueness"].append(percent_unique)
            except Exception:
                dimension_scores["Uniqueness"].append(100.0)

            # 6. INTEGRITY: Schema matches the baseline
            expected_meta_cols = ['source', 'ingested_at']
            for expected_col in expected_meta_cols:
                res = dataset.expect_column_to_exist(column=expected_col)
                dimension_scores["Integrity"].append(100.0 if res.success else 0.0)

            # 7. LINEAGE: Source Tracking
            if 'source' in df.columns:
                res = dataset.expect_column_values_to_not_be_null(column='source', result_format="SUMMARY")
                percent_lineage = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                dimension_scores["Lineage"].append(percent_lineage)
            else:
                dimension_scores["Lineage"].append(0.0)

        # Calculate final averages for each dimension
        final_scores = {}
        for dim, scores in dimension_scores.items():
            if scores:
                final_scores[dim] = sum(scores) / len(scores)
            else:
                final_scores[dim] = 100.0 # Default to perfect if no metrics applied to this dataset geometry
                
        # Also grab duplicate indices for the frontend report metadata (this is easier via pandas)
        duplicate_indices = df[df.duplicated(keep='first')].index.tolist()

        return final_scores, duplicate_indices        
        # 1. COMPLETENESS: Expect columns to not be null
        for col in df.columns:
            res = dataset.expect_column_values_to_not_be_null(column=col, result_format="SUMMARY")
            if res.success or res.result.get("unexpected_percent") is not None:
                percent_complete = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                dimension_scores["Completeness"].append(percent_complete)

        # 2. ACCURACY & 3. VALIDITY: Type checking and Outlier bounds
        num_cols = df.select_dtypes(include=['number']).columns
        for col in num_cols:
            # Accuracy: Values within statistical bounds (using rough 3 std dev proxy)
            if total_rows > 1:
                mean = df[col].mean()
                std = df[col].std()
                if pd.notna(mean) and pd.notna(std) and std != 0:
                    min_val = mean - (3 * std)
                    max_val = mean + (3 * std)
                    res = dataset.expect_column_values_to_be_between(column=col, min_value=min_val, max_value=max_val, result_format="SUMMARY")
                    percent_accurate = 100.0 - float(res.result.get("unexpected_percent", 0.0))
                    dimension_scores["Accuracy"].append(percent_accurate)
            
            # Validity: Type matching
            res = dataset.expect_column_values_to_be_of_type(column=col, type_="int") if pd.api.types.is_integer_dtype(df[col]) else dataset.expect_column_values_to_be_of_type(column=col, type_="float")
            # Type checks are usually pass/fail per column in GE
            dimension_scores["Validity"].append(100.0 if res.success else 0.0)
            
        text_cols = df.select_dtypes(include=['object', 'string']).columns
        for col in text_cols:
            res = dataset.expect_column_values_to_be_of_type(column=col, type_="str")
            dimension_scores["Validity"].append(100.0 if res.success else 0.0)

        # 4. CONSISTENCY: Date parseable formats
        date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'timestamp', '_at'])]
        for col in date_cols:
            res = dataset.expect_column_values_to_be_dateutil_parseable(column=col, result_format="SUMMARY")
            percent_consistent = 100.0 - float(res.result.get("unexpected_percent", 0.0))
            dimension_scores["Consistency"].append(percent_consistent)

        # 5. UNIQUENESS: Row-level duplication
        # GE doesn't have an out-of-the-box expect_table_rows_to_be_unique for pandas as easily as columns
        # So we use a compound column expectation on all columns, or fallback to hashing
        try:
            res = dataset.expect_compound_columns_to_be_unique(column_list=list(df.columns), result_format="SUMMARY")
            # If there's an unexpected_percent in the result, use it. Otherwise calculate based on unexpected_count
            unexpected_count = res.result.get("unexpected_count", 0)
            percent_unique = ((total_rows - unexpected_count) / total_rows) * 100.0 if total_rows > 0 else 100.0
            dimension_scores["Uniqueness"].append(percent_unique)
        except Exception:
            dimension_scores["Uniqueness"].append(100.0)

        # 6. INTEGRITY: Schema matches the baseline
        expected_meta_cols = ['source', 'ingested_at']
        for expected_col in expected_meta_cols:
            res = dataset.expect_column_to_exist(column=expected_col)
            dimension_scores["Integrity"].append(100.0 if res.success else 0.0)

        # 7. LINEAGE: Source Tracking
        if 'source' in df.columns:
            res = dataset.expect_column_values_to_not_be_null(column='source', result_format="SUMMARY")
            percent_lineage = 100.0 - float(res.result.get("unexpected_percent", 0.0))
            dimension_scores["Lineage"].append(percent_lineage)
        else:
            dimension_scores["Lineage"].append(0.0)

        # Calculate final averages for each dimension
        final_scores = {}
        for dim, scores in dimension_scores.items():
            if scores:
                final_scores[dim] = sum(scores) / len(scores)
            else:
                final_scores[dim] = 100.0 # Default to perfect if no metrics applied to this dataset geometry
                
        # Also grab duplicate indices for the frontend report metadata (this is easier via pandas)
        duplicate_indices = df[df.duplicated(keep='first')].index.tolist()

        return final_scores, duplicate_indices

    def validate(self, file_path):
        print(f"[*] QA Engine: Performing Great Expectations Integration Analysis on {os.path.basename(file_path)}...")
        
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

        # --- GREAT EXPECTATIONS EXECUTION ---
        ge_results = self._run_great_expectations_suite(df)
        
        if ge_results:
            ge_scores, duplicate_indices = ge_results
            
            # Extract individual dimensions formatted by GE
            dim_completeness = ge_scores.get("Completeness", 0)
            dim_accuracy = ge_scores.get("Accuracy", 0)
            dim_validity = ge_scores.get("Validity", 0)
            dim_consistency = ge_scores.get("Consistency", 0)
            dim_uniqueness = ge_scores.get("Uniqueness", 0)
            dim_integrity = ge_scores.get("Integrity", 0)
            dim_lineage = ge_scores.get("Lineage", 0)
            integrity_fail_indices = []
            
        else:
            # Major error fallback (should never hit if df has rows)
            dim_completeness = dim_accuracy = dim_validity = dim_consistency = dim_uniqueness = dim_integrity = dim_lineage = 0.0
            duplicate_indices = []
            integrity_fail_indices = []

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
