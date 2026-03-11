import pandas as pd
import os
from datetime import datetime
import great_expectations as ge
import warnings
from .rule_engine import RuleEngine

class DataValidator:
    """
    Advanced QA Engine: Evaluates data using the 7 Dimensions of Trustability
    powered by the Great Expectations Framework.
    Focuses on: Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, Lineage.
    """
    
    def _safe_float(self, value, default=0.0):
        """Safely convert value to float, handling None and string cases."""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.rule_engine = RuleEngine()

    def _run_great_expectations_suite(self, df, rule_engine=None):
        """
        Dynamically builds and runs a Great Expectations suite against the dataframe.
        Routes the success ratios of specific expectations to their corresponding Quality Dimensions.

        Args:
            df (pd.DataFrame): The dataframe to validate.

        Returns:
            tuple: (final_scores dict, duplicate_indices list)
                   final_scores maps each of the 7 dimensions to a 0–100 float score.
        """
        if df.empty:
            return None

        # GE v1.0+ Ephemeral Context (no filesystem required)
        ctx = ge.get_context(mode="ephemeral")

        try:
            ds = ctx.data_sources.add_pandas("pandas_datasource")
        except Exception:
            ds = ctx.get_datasource("pandas_datasource")

        da = ds.add_dataframe_asset("df_asset")
        batch_request = da.build_batch_request(options={"dataframe": df})

        # Suppress GE internal DeprecationWarning and the result_format UserWarning
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            warnings.filterwarnings("ignore", category=UserWarning, module="great_expectations")

            dataset = ctx.get_validator(
                batch_request=batch_request,
                create_expectation_suite_with_name="dqt_suite"
            )

            total_rows = len(df)

            dimension_scores = {
                "Completeness": [],
                "Accuracy": [],
                "Validity": [],
                "Consistency": [],
                "Uniqueness": [],
                "Integrity": [],
                "Lineage": []
            }
            
            # Metadata columns to exclude from data quality checks
            metadata_cols = {'source', 'ingested_at', 'remediation_notes'}
            data_cols = [c for c in df.columns if c not in metadata_cols]

            # Placeholder values that should be treated as INCOMPLETE/MISSING data
            # These are common representations of "no real value" that shouldn't be counted as valid
            PLACEHOLDER_VALUES = {
                '', ' ', 'nan', 'NaN', 'NAN', 'null', 'NULL', 'Null',
                'none', 'None', 'NONE', 'n/a', 'N/A', 'NA', 'na',
                'unknown', 'Unknown', 'UNKNOWN', 'undefined', 'Undefined',
                '-', '--', '---', '.', '..', '...', '?', '??',
                'tbd', 'TBD', 'pending', 'Pending', 'PENDING',
                'blank', 'Blank', 'BLANK', 'empty', 'Empty', 'EMPTY',
                '#N/A', '#NA', '#VALUE!', '#REF!', '#DIV/0!', '#NULL!',  # Excel errors
                'missing', 'Missing', 'MISSING', 'not available', 'Not Available',
            }

            # 1. COMPLETENESS: Direct pandas calculation (more reliable than GE)
            #    Calculate % non-null AND non-placeholder for each DATA column
            for col in data_cols:
                # Count actual nulls
                null_count = df[col].isna().sum()
                
                # Count placeholder values (treated as missing/incomplete data)
                placeholder_count = 0
                if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                    # Convert to string and check for placeholders
                    str_values = df[col].fillna('__ACTUAL_NULL__').astype(str).str.strip()
                    
                    # Check each value against placeholder set
                    is_placeholder = str_values.isin(PLACEHOLDER_VALUES)
                    placeholder_count = is_placeholder.sum()
                
                total_missing = null_count + placeholder_count
                complete_count = total_rows - total_missing
                completeness_pct = (complete_count / total_rows) * 100.0 if total_rows > 0 else 0.0
                dimension_scores["Completeness"].append(completeness_pct)
                
                # Log columns with missing/placeholder values for transparency
                if total_missing > 0:
                    print(f"    [Completeness] '{col}': {null_count} nulls + {placeholder_count} placeholders = {total_missing} incomplete ({100-completeness_pct:.1f}% incomplete)")

            num_cols = [c for c in df.select_dtypes(include=['number']).columns if c not in metadata_cols]

            # 2. ACCURACY: Values within 3-sigma statistical bounds
            for col in num_cols:
                if total_rows > 1:
                    mean = df[col].mean()
                    std = df[col].std()
                    if pd.notna(mean) and pd.notna(std) and std != 0:
                        res = dataset.expect_column_values_to_be_between(
                            column=col,
                            min_value=mean - (3 * std),
                            max_value=mean + (3 * std),
                            result_format="SUMMARY"
                        )
                        unexpected_pct = res.result.get("unexpected_percent") if res.result else 0.0
                        dimension_scores["Accuracy"].append(100.0 - self._safe_float(unexpected_pct, 0.0))

            # 3. VALIDITY: Column values match their actual pandas dtype + custom rules
            #    Use df[col].dtype.name (e.g. "int64", "float64") so GE gets the correct type string.
            #    Also applies configurable business rules from validation_rules.json
            
            type_validity_scores = []
            
            for col in num_cols:
                dtype_name = df[col].dtype.name  # e.g. "int64", "float64", "Int64"
                res = dataset.expect_column_values_to_be_of_type(column=col, type_=dtype_name)
                type_validity_scores.append(100.0 if res.success else 0.0)

            text_cols = [c for c in df.select_dtypes(include=['object', 'string']).columns if c not in metadata_cols]
            for col in text_cols:
                # pandas string/object columns report dtype.name as "object"
                res = dataset.expect_column_values_to_be_of_type(column=col, type_="object")
                type_validity_scores.append(100.0 if res.success else 0.0)
            
            # Apply custom validation rules + auto format detection from rule_engine
            custom_rules_score = 100.0
            auto_detected_cols = 0
            if rule_engine:
                rules_result = rule_engine.validate_dataframe(df, exclude_cols=metadata_cols)
                custom_rules_score = rules_result['overall_compliance']
                auto_detected_cols = rules_result.get('auto_detected_columns', 0)
                
                configured_rules_count = rules_result['columns_validated'] - auto_detected_cols
                if rules_result['columns_validated'] > 0:
                    print(f"    [Validity] Format validation: {custom_rules_score:.1f}% compliance ({configured_rules_count} configured + {auto_detected_cols} auto-detected)")
            
            # Combine type checks and format rules for final Validity score
            # Type validity: average of all type checks
            type_validity_avg = (sum(type_validity_scores) / len(type_validity_scores)) if type_validity_scores else 100.0
            
            # Final Validity = weighted average (50% type checks + 50% format rules)
            # Format rules include both configured rules AND auto-detected format compliance
            if rule_engine:
                final_validity = (type_validity_avg + custom_rules_score) / 2
                dimension_scores["Validity"].append(final_validity)
            else:
                # No rule engine - use type validity only
                dimension_scores["Validity"].extend(type_validity_scores)


            # 4. CONSISTENCY: Now calculated as the average of:
            #    - Date column consistency (average of all date-like columns)
            #    - Completeness (average completeness score)
            #    - Uniqueness (average uniqueness score)
            #    - Lineage (average lineage score)
            #    - Accuracy (average accuracy score)

            # Date-like columns consistency
            date_cols = [c for c in df.columns if any(x in c.lower() for x in ['date', 'time', 'timestamp', '_at'])]
            date_consistency_scores = []
            for col in date_cols:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    valid_pct = (df[col].notna().sum() / total_rows) * 100.0 if total_rows > 0 else 100.0
                    date_consistency_scores.append(valid_pct)
                elif df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                    res = dataset.expect_column_values_to_be_dateutil_parseable(column=col, result_format="SUMMARY")
                    unexpected_pct = res.result.get("unexpected_percent") if res.result else 0.0
                    date_consistency_scores.append(100.0 - self._safe_float(unexpected_pct, 0.0))
            avg_date_consistency = (sum(date_consistency_scores) / len(date_consistency_scores)) if date_consistency_scores else 100.0

            # Completeness, Uniqueness, Lineage, Accuracy
            avg_completeness = (sum(dimension_scores["Completeness"]) / len(dimension_scores["Completeness"])) if dimension_scores["Completeness"] else 100.0
            avg_uniqueness = (sum(dimension_scores["Uniqueness"]) / len(dimension_scores["Uniqueness"])) if dimension_scores["Uniqueness"] else 100.0
            avg_lineage = (sum(dimension_scores["Lineage"]) / len(dimension_scores["Lineage"])) if dimension_scores["Lineage"] else 100.0
            avg_accuracy = (sum(dimension_scores["Accuracy"]) / len(dimension_scores["Accuracy"])) if dimension_scores["Accuracy"] else 100.0

            # Consistency is the average of these five
            consistency_final = (avg_date_consistency + avg_completeness + avg_uniqueness + avg_lineage + avg_accuracy) / 5
            dimension_scores["Consistency"] = [consistency_final]

            # 5. UNIQUENESS: No fully-duplicate rows (based on DATA columns only)
            #    Metadata columns (source, ingested_at) have unique timestamps per row,
            #    so we exclude them to find actual data duplicates
            duplicate_count = df[data_cols].duplicated().sum()
            percent_unique = ((total_rows - duplicate_count) / total_rows) * 100.0 if total_rows > 0 else 100.0
            dimension_scores["Uniqueness"].append(percent_unique)
            
            if duplicate_count > 0:
                print(f"    [Uniqueness] {duplicate_count} duplicate rows detected ({100-percent_unique:.1f}% duplicates)")

            # 6. INTEGRITY: Required metadata columns must exist
            for expected_col in ['source', 'ingested_at']:
                res = dataset.expect_column_to_exist(column=expected_col)
                dimension_scores["Integrity"].append(100.0 if res.success else 0.0)

            # --- INTEGRITY CALCULATION ---
            # Integrity is now calculated as:
            # 1. Required metadata columns check (source, ingested_at)
            # 2. Average of other quality dimensions (Accuracy, Validity, Uniqueness, Consistency)
            
            # Metadata existence checks
            for expected_col in ['source', 'ingested_at']:
                res = dataset.expect_column_to_exist(column=expected_col)
                dimension_scores["Integrity"].append(100.0 if res.success else 0.0)

            # Average of Accuracy, Validity, Uniqueness, Consistency
            avg_from_other_dims = 0.0
            other_dims = ["Accuracy", "Validity", "Uniqueness", "Consistency"]
            for dim in other_dims:
                scores = dimension_scores.get(dim, [])
                avg_from_other_dims += (sum(scores) / len(scores)) if scores else 100.0
            
            avg_from_other_dims = avg_from_other_dims / len(other_dims)
            dimension_scores["Integrity"].append(avg_from_other_dims)

            # 7. LINEAGE: Data continuity - checks for blank/empty rows
            #    A blank row = a row where ALL data columns are empty/null/placeholder
            #    This checks if data forms a continuous chain without gaps
            def is_row_blank(row):
                """Check if a row has ALL data columns blank/null/placeholder."""
                for val in row:
                    if pd.notna(val):
                        str_val = str(val).strip()
                        # If any value is NOT a placeholder, row is not blank
                        if str_val and str_val not in PLACEHOLDER_VALUES:
                            return False
                return True  # All values are blank/null/placeholder
            
            # Count blank rows in data columns only
            data_df = df[data_cols]
            blank_row_count = data_df.apply(is_row_blank, axis=1).sum()
            blank_row_pct = (blank_row_count / total_rows) * 100.0 if total_rows > 0 else 0.0
            lineage_score = 100.0 - blank_row_pct
            dimension_scores["Lineage"].append(lineage_score)
            
            if blank_row_count > 0:
                print(f"    [Lineage] {blank_row_count} blank rows detected ({blank_row_pct:.1f}% of data)")

        # Average all collected scores per dimension.
        # Dimensions with no applicable columns (e.g. no date cols → no Consistency data)
        # default to 100.0 so the overall score is not penalised for missing column types.
        final_scores = {
            dim: (sum(scores) / len(scores)) if scores else 100.0
            for dim, scores in dimension_scores.items()
        }

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

        # Pre-validation data quality summary
        metadata_cols = {'source', 'ingested_at', 'remediation_notes'}
        data_cols = [c for c in df.columns if c not in metadata_cols]
        
        print(f"[*] Data Summary: {total} rows × {len(data_cols)} data columns")
        total_nulls = df[data_cols].isna().sum().sum()
        print(f"[*] Total Null Values: {total_nulls}")
        
        if total_nulls > 0:
            print("[*] Columns with nulls:")
            for col in data_cols:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    print(f"    - {col}: {null_count} nulls ({null_count/total*100:.1f}%)")

        # --- GREAT EXPECTATIONS EXECUTION ---
        ge_results = self._run_great_expectations_suite(df, rule_engine=self.rule_engine)
        
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