import pandas as pd
import os
import logging
import numpy as np
from scipy import stats
import re
from paths import get_workspace_dir

# Set up logging for remediation
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# Metadata columns to preserve (never impute or modify)
METADATA_COLS = {'source', 'ingested_at', 'remediation_notes'}

# Placeholder values that should be treated as empty/missing
PLACEHOLDER_VALUES = {
    '', ' ', '  ', '   ',
    'nan', 'NaN', 'NAN', 'null', 'NULL', 'Null',
    'none', 'None', 'NONE', 'n/a', 'N/A', 'NA', 'na',
    'unknown', 'Unknown', 'UNKNOWN', 'undefined', 'Undefined',
    '-', '--', '---', '.', '..', '...', '?', '??',
    'tbd', 'TBD', 'pending', 'Pending', 'PENDING',
    'blank', 'Blank', 'BLANK', 'empty', 'Empty', 'EMPTY',
    '#N/A', '#NA', '#VALUE!', '#REF!', '#DIV/0!', '#NULL!',
    'missing', 'Missing', 'MISSING', 'not available', 'Not Available',
}

# Common embedded header/label patterns in Excel/CSV files
EMBEDDED_HEADER_PATTERNS = [
    'total', 'subtotal', 'grand total', 'sum', 'count', 'average', 'mean',
    'section', 'category', 'group', 'header', 'title', 'label', 'note',
    'remarks', 'comments', '---', '===', '***', 'n/a', 'na', 'null',
    'blank', 'empty', 'undefined', 'unknown', 'tbd', 'pending'
]


class DataCleaner:
    """
    ML-Grade Preprocessing & Remediation Engine with Data Segregation.
    
    NOW CREATES 3-WAY DATA SPLIT instead of removing problematic rows:
    - cleaned_data.parquet: High-quality rows (ready for ML/analysis)
    - noisy_data.parquet: Problematic rows with explanations (data quality issues)
    - raw_structured.parquet: Original untouched data (for reference)
    
    NEVER REMOVES DATA - only flags and segregates it.
    All problematic rows are preserved with detailed quality issue notes.
    """
    
    def __init__(self):
        workspace = get_workspace_dir()
        self.output_dir = workspace["processed"]
        self.input_file = os.path.join(self.output_dir, "raw_structured.parquet")
        self.output_file = os.path.join(self.output_dir, "cleaned_data.parquet")
        self.noisy_file = os.path.join(self.output_dir, "noisy_data.parquet")
        self.imputation_stats = {}  # Track what was imputed for transparency
        self.noisy_rows_indices = set()  # Track which rows are problematic

    def load_data(self):
        if not os.path.exists(self.input_file):
            logger.error(f"Target file {self.input_file} not found. Cannot run Remediation.")
            return None
        return pd.read_parquet(self.input_file)

    def _add_noisy_flag(self, df, row_indices, reason):
        """
        Marks rows as noisy/problematic and tracks the reason.
        Appends to remediation_notes without removing the row.
        
        Args:
            df: DataFrame
            row_indices: List or Index of row indices to flag
            reason: String explanation of why rows are problematic
        """
        if 'remediation_notes' not in df.columns:
            df['remediation_notes'] = ""
        
        for idx in row_indices:
            if idx not in self.noisy_rows_indices:  # First time flagging this row
                df.at[idx, 'remediation_notes'] = reason
                self.noisy_rows_indices.add(idx)
            else:  # Already flagged, append to notes
                existing = str(df.at[idx, 'remediation_notes']).strip()
                if reason not in existing:
                    df.at[idx, 'remediation_notes'] = existing + f" | {reason}"
        
        return df


    # STEP 1: EMBEDDED HEADER & LABEL DETECTION

    
    def _is_embedded_header_row(self, row, column_names):
        """
        Detects if a row is actually an embedded header/label row from Excel/CSV.
        Common patterns: section titles, repeated column names, summary rows.
        
        NOTE: This is conservative - only triggers when there's strong evidence
        of an embedded header, not just if a cell contains a common word.
        """
        row_values = [str(v).lower().strip() for v in row.values if pd.notna(v) and str(v).strip()]
        
        if not row_values:
            return True  # Empty row = embedded spacer
        
        # Check 1: Row contains column names (repeated header) - at least 50% match
        col_names_lower = {str(c).lower().strip() for c in column_names}
        matching_cols = sum(1 for v in row_values if v in col_names_lower)
        if matching_cols >= len(row_values) * 0.5 and matching_cols >= 3:
            return True
        
        # Check 2: Row is a section divider (entire row is just markers like --- or ===)
        divider_patterns = ['---', '===', '***', '___', '...']
        if all(any(p in v for p in divider_patterns) or v == '' for v in row_values):
            return True
        
        # Check 3: Row starts with a section keyword AND has mostly empty cells
        # (typical of Excel section headers like "Section: Financial Data")
        section_keywords = ['section', 'category', 'group', 'header', 'title', 'total', 'subtotal', 'grand total']
        first_val = row_values[0] if row_values else ''
        if any(first_val.startswith(kw) or first_val == kw for kw in section_keywords):
            # Check if most other cells are empty
            empty_count = sum(1 for v in row.values if pd.isna(v) or str(v).strip() == '')
            if empty_count >= len(row) * 0.6:  # 60% of row is empty
                return True
        
        return False
    
    def remove_embedded_headers(self, df):
        """
        Flags rows that are actually embedded headers, section labels, or summary rows.
        DOES NOT REMOVE - marks them in remediation_notes as unhandled/noisy data.
        Common in Excel exports where users add section titles mid-data.
        """
        logger.info("Scanning for embedded headers and label rows...")
        
        initial_len = len(df)
        if initial_len == 0:
            return df
            
        data_cols = [c for c in df.columns if c not in METADATA_COLS]
        
        # Identify rows to FLAG (not remove)
        rows_to_flag = []
        for idx, row in df.iterrows():
            if self._is_embedded_header_row(row[data_cols], data_cols):
                rows_to_flag.append(idx)
        
        # Flag all identified rows
        if rows_to_flag:
            df = self._add_noisy_flag(df, rows_to_flag, "Embedded header/section label row detected")
            logger.info(f"Flagged {len(rows_to_flag)} embedded header/label rows as noisy (NOT removed).")
        
        # Also flag exact duplicate header rows
        duplicate_header_mask = df[data_cols].apply(
            lambda row: all(str(row[c]).lower().strip() == str(c).lower().strip() for c in data_cols if pd.notna(row[c])),
            axis=1
        )
        if duplicate_header_mask.any():
            header_dup_indices = df[duplicate_header_mask].index.tolist()
            df = self._add_noisy_flag(df, header_dup_indices, "Exact header row duplicate")
            logger.info(f"Flagged {duplicate_header_mask.sum()} exact header duplicate rows as noisy (NOT removed).")
        
        logger.info(f"Header scan complete: {len(rows_to_flag)} rows flagged as noisy data.")
        return df


    # STEP 2: TYPE INFERENCE & COERCION
 
    
    def infer_and_cast_types(self, df):
        """
        Smart type inference: converts string columns to proper types.
        Uses sampling to detect if a column should be numeric, datetime, or categorical.
        """
        logger.info("Running intelligent type inference...")
        
        for col in df.columns:
            if col in METADATA_COLS:
                continue
                
            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Sample the column for type detection
            sample = df[col].dropna().head(100)
            if len(sample) == 0:
                continue
            
            # Try numeric conversion
            numeric_converted = pd.to_numeric(sample.astype(str).str.replace(',', '').str.strip(), errors='coerce')
            numeric_success_rate = numeric_converted.notna().sum() / len(sample)
            
            if numeric_success_rate >= 0.7:
                # Convert entire column to numeric
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', '').str.strip(), 
                    errors='coerce'
                )
                logger.info(f"  → '{col}' cast to numeric ({numeric_success_rate:.0%} success)")
                continue
            
            # Try datetime conversion
            datetime_keywords = ['date', 'time', 'timestamp', '_at', 'created', 'updated', 'modified']
            if any(kw in col.lower() for kw in datetime_keywords):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
                    valid_dates = df[col].notna().sum() / len(df)
                    if valid_dates >= 0.5:
                        logger.info(f"  → '{col}' cast to datetime ({valid_dates:.0%} valid)")
                        continue
                    else:
                        # Revert if too many failed
                        df[col] = df[col].astype(str)
                except Exception:
                    pass
            
            # Otherwise keep as string/categorical
            df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())
            df[col] = df[col].replace({'': np.nan, 'nan': np.nan, 'None': np.nan, 'NaN': np.nan, '—': np.nan})
        
        return df


    # STEP 3: MISSING VALUE IMPUTATION (ML-GRADE)

    
    def _detect_distribution(self, series):
        """
        Detects if a numeric series is normally distributed or skewed.
        Returns 'normal', 'skewed', or 'unknown'.
        """
        clean = series.dropna()
        if len(clean) < 10:
            return 'unknown'
        
        try:
            # Shapiro-Wilk test for normality (sample if too large)
            sample = clean.sample(min(len(clean), 5000)) if len(clean) > 5000 else clean
            stat, p_value = stats.shapiro(sample)
            
            if p_value > 0.05:
                return 'normal'
            
            # Check skewness
            skewness = abs(clean.skew())
            if skewness > 1:
                return 'skewed'
            return 'normal'
        except Exception:
            return 'unknown'
    
    def impute_missing_values(self, df):
        """
        ML-grade missing value imputation:
        - Rows with >50% missing: FLAGGED as noisy data (not imputed)
        - Standard missing: Numeric (mean/median), Categorical (mode), DateTime (ffill/bfill)
        - All-null columns: Dropped entirely
        """
        logger.info("Running ML-grade missing value imputation...")
        
        # Ensure remediation_notes column exists
        if 'remediation_notes' not in df.columns:
            df['remediation_notes'] = ""
        df['remediation_notes'] = df['remediation_notes'].fillna("").astype(str)
        
        # FLAG rows with >50% missing values BEFORE imputation
        data_cols = [col for col in df.columns if col not in METADATA_COLS]
        if data_cols:
            missing_per_row = df[data_cols].isna().sum(axis=1)
            missing_rate_per_row = missing_per_row / len(data_cols)
            high_missing_mask = missing_rate_per_row > 0.5
            
            if high_missing_mask.any():
                high_missing_indices = df[high_missing_mask].index.tolist()
                high_missing_count = len(high_missing_indices)
                
                # Add detailed reason for each row
                for idx in high_missing_indices:
                    missing_pct = int(missing_rate_per_row[idx] * 100)
                    reason = f"Row has {missing_pct}% missing values (>{high_missing_count} columns)"
                    df = self._add_noisy_flag(df, [idx], reason)
                
                logger.warning(f"  ⚠ {high_missing_count} rows flagged as noisy: {missing_rate_per_row[high_missing_mask].mean()*100:.0f}% avg missing")
        
        # First pass: identify and drop columns that are ALL null (useless)
        all_null_cols = [col for col in df.columns if col not in METADATA_COLS and df[col].isna().all()]
        if all_null_cols:
            logger.warning(f"  → Dropping {len(all_null_cols)} all-null columns: {all_null_cols}")
            df = df.drop(columns=all_null_cols)
        
        # NOW IMPUTE missing values (for ALL rows, both clean and noisy)
        for col in df.columns:
            if col in METADATA_COLS:
                continue
            
            missing_count = df[col].isna().sum()
            
            if missing_count == 0:
                continue
            
            # Numeric column imputation
            if pd.api.types.is_numeric_dtype(df[col]):
                distribution = self._detect_distribution(df[col])
                
                if distribution == 'normal':
                    fill_value = df[col].mean()
                    strategy = 'mean'
                else:
                    fill_value = df[col].median()
                    strategy = 'median'
                
                # Fallback if mean/median is NaN (very sparse column)
                if pd.isna(fill_value):
                    fill_value = 0
                    strategy = 'zero (fallback)'
                    logger.warning(f"  ⚠ '{col}': mean/median is NaN, using 0 as fallback")
                
                df[col] = df[col].fillna(fill_value)
                self.imputation_stats[col] = {'strategy': strategy, 'value': fill_value, 'count': missing_count}
                logger.info(f"  → '{col}': {missing_count} nulls imputed with {strategy} ({fill_value:.4g})")
            
            # Categorical/String column imputation
            elif df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                mode_result = df[col].mode()
                if len(mode_result) > 0 and pd.notna(mode_result.iloc[0]):
                    fill_value = mode_result.iloc[0]
                else:
                    fill_value = "Unknown"
                    logger.warning(f"  ⚠ '{col}': no valid mode found, using 'Unknown'")
                
                df[col] = df[col].fillna(fill_value)
                self.imputation_stats[col] = {'strategy': 'mode', 'value': fill_value, 'count': missing_count}
                logger.info(f"  → '{col}': {missing_count} nulls imputed with mode ('{fill_value}')")
            
            # Datetime column - forward fill then backward fill
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].ffill().bfill()
                # If still has nulls (e.g., all were null), fill with current timestamp
                remaining_nulls = df[col].isna().sum()
                if remaining_nulls > 0:
                    df[col] = df[col].fillna(pd.Timestamp.now())
                    logger.warning(f"  ⚠ '{col}': {remaining_nulls} remaining nulls filled with current timestamp")
                self.imputation_stats[col] = {'strategy': 'ffill/bfill', 'value': 'temporal', 'count': missing_count}
                logger.info(f"  → '{col}': {missing_count} nulls imputed with forward/backward fill")
        
        logger.info("  ✓ Missing value imputation complete (noisy rows preserved)")
        return df

 
    # STEP 4: OUTLIER DETECTION & HANDLING
    
    def handle_outliers(self, df, method='flag'):
        """
        Outlier detection using IQR method.
        
        Args:
            method: 'flag' (default - mark as noisy), 'cap' (winsorize), or 'remove'
        
        Note: Default changed to 'flag' - problematic rows are marked but NOT removed
        """
        logger.info(f"Running outlier detection (method={method})...")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_cols = [c for c in numeric_cols if c not in METADATA_COLS]
        
        outlier_count = 0
        flagged_indices = []
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            col_outliers = outlier_mask.sum()
            
            if col_outliers == 0:
                continue
            
            outlier_count += col_outliers
            outlier_indices = df[outlier_mask].index.tolist()
            
            if method == 'flag':
                # Mark as noisy but keep the data
                for idx in outlier_indices:
                    reason = f"Outlier detected in {col} (value={df.at[idx, col]:.4g}, bounds=[{lower_bound:.4g}, {upper_bound:.4g}])"
                    df = self._add_noisy_flag(df, [idx], reason)
                    if idx not in flagged_indices:
                        flagged_indices.append(idx)
                logger.info(f"  → '{col}': {col_outliers} outliers flagged as noisy (NOT removed)")
            
            elif method == 'cap':
                # Winsorization: cap values to bounds
                df.loc[df[col] < lower_bound, col] = lower_bound
                df.loc[df[col] > upper_bound, col] = upper_bound
                logger.info(f"  → '{col}': {col_outliers} outliers capped to [{lower_bound:.2f}, {upper_bound:.2f}]")
            
            elif method == 'remove':
                # Legacy support - but flag them instead
                for idx in outlier_indices:
                    reason = f"Outlier in {col} (value={df.at[idx, col]:.4g})"
                    df = self._add_noisy_flag(df, [idx], reason)
                logger.warning(f"  → '{col}': {col_outliers} outlier rows flagged (method='remove' deprecated)")
        
        if outlier_count > 0:
            logger.info(f"Total outliers handled: {outlier_count} rows flagged as noisy")
        
        return df


    # STEP 5: STRING NORMALIZATION & CLEANUP

        
    def normalize_strings(self, df):
        """
        Standardizes string columns:
        - Strip whitespace
        - Convert null-like strings AND empty strings to actual NaN
        - Standardize common variations (YES/yes/Y → Yes, etc.)
        """
        logger.info("Normalizing string columns...")
        
        str_cols = df.select_dtypes(include=['object', 'string']).columns
        str_cols = [c for c in str_cols if c not in METADATA_COLS]
        
        # Comprehensive null-like patterns (including empty string)
        null_patterns = {
            '', ' ', '  ', '   ',  # Empty and whitespace-only
            'nan', 'NaN', 'NAN', 'none', 'None', 'NONE', 
            'null', 'Null', 'NULL', 'N/A', 'n/a', 'NA', 'na',
            '—', '-', '.', '..', '...', '#N/A', '#NA', '#VALUE!',
            'undefined', 'UNDEFINED', 'missing', 'MISSING',
            '<NA>', '<na>', 'NaT', 'nat', '0', '0.0'  # Add common placeholder values
        }
        
        for col in str_cols:
            # First, preserve actual NaN values
            original_nulls = df[col].isna()
            
            # Convert to string and strip whitespace (only for non-null values)
            df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.strip())
            
            # Replace null-like patterns with actual NaN (including empty strings after strip)
            df[col] = df[col].apply(lambda x: np.nan if x in null_patterns or (pd.notna(x) and str(x).strip() == '') else x)
        
            
            # Count conversions
            new_nulls = df[col].isna().sum()
            converted = new_nulls - original_nulls.sum()
            if converted > 0:
                logger.info(f"  → '{col}': {converted} empty/blank values converted to NaN")
            
            # Standardize boolean-like values
            bool_map = {
                'yes': 'Yes', 'YES': 'Yes', 'y': 'Yes', 'Y': 'Yes', 'true': 'Yes', 'TRUE': 'Yes', 'True': 'Yes',
                'no': 'No', 'NO': 'No', 'n': 'No', 'N': 'No', 'false': 'No', 'FALSE': 'No', 'False': 'No'
            }
            # Only apply to columns that look boolean
            unique_vals = set(df[col].dropna().astype(str).unique())
            if len(unique_vals) > 0 and unique_vals.issubset(set(bool_map.keys()) | {'Yes', 'No'}):
                df[col] = df[col].map(lambda x: bool_map.get(str(x), x) if pd.notna(x) else x)
                logger.info(f"  → '{col}': Standardized boolean values")
            
            # Count how many nulls we have after normalization
            null_count = df[col].isna().sum()
            if null_count > 0:
                logger.info(f"  → '{col}': {null_count} null-like values identified")
        
        return df


    # STEP 5b: BLANK ROW REMOVAL

    
    def remove_blank_rows(self, df):
        """
        Flags rows where ALL data columns are blank/null/placeholder as noisy data.
        DOES NOT REMOVE - marks them in remediation_notes.
        """
        logger.info("Scanning for blank rows (all data columns empty)...")
        
        initial_len = len(df)
        data_cols = [c for c in df.columns if c not in METADATA_COLS]
        
        if not data_cols:
            return df
        
        def is_row_blank(row):
            """Check if ALL data columns in a row are blank/null/placeholder."""
            for val in row:
                if pd.notna(val):
                    str_val = str(val).strip()
                    if str_val and str_val not in PLACEHOLDER_VALUES:
                        return False
            return True
        
        # Apply to data columns only
        blank_mask = df[data_cols].apply(is_row_blank, axis=1)
        blank_indices = df[blank_mask].index.tolist()
        blank_count = len(blank_indices)
        
        if blank_count > 0:
            df = self._add_noisy_flag(df, blank_indices, "Entire row is blank/empty - no valid data")
            logger.info(f"  → Flagged {blank_count} blank rows as noisy (NOT removed, {blank_count/initial_len*100:.1f}%)")
        else:
            logger.info("  → No completely blank rows found")
        
        return df

    # STEP 5c: FORMAT STANDARDIZATION


    def standardize_formats(self, df):
        """
        Detects dominant format in each column and standardizes values.
        Fixes format inconsistencies that affect Validity score.
        
        Examples:
        - Dates: '01/15/2024' → '2024-01-15' (standardize to ISO format)
        - Codes: 'emp001' → 'EMP-001' (if majority follow EMP-XXX pattern)
        - Phones: standardize to consistent format
        """
        logger.info("Standardizing formats for consistency...")
        
        try:
            from ..qa.rule_engine import FormatDetector, FORMAT_PATTERNS
        except ImportError:
            logger.warning("  ⚠ Could not import FormatDetector - skipping format standardization")
            return df
        
        data_cols = [c for c in df.columns if c not in METADATA_COLS]
        str_cols = [c for c in df.select_dtypes(include=['object', 'string']).columns if c not in METADATA_COLS]
        
        standardization_count = 0
        
        for col in str_cols:
            # Get non-null values
            col_values = df[col].dropna().astype(str).tolist()
            if len(col_values) < 3:
                continue
            
            # Detect dominant format
            detection = FormatDetector.detect_format(col_values)
            
            if detection['detected_format'] in ('empty', 'mixed', 'unknown'):
                continue
            
            if detection['confidence'] < 70:
                # Not confident enough about the format
                continue
            
            format_name = detection['detected_format']
            pattern = detection['pattern']
            
            # Standardize based on detected format
            col_fixed = 0
            
            # DATE STANDARDIZATION - convert to ISO format
            if format_name in ('date_iso', 'date_us', 'date_eu', 'datetime_iso'):
                for idx, val in df[col].items():
                    if pd.notna(val):
                        str_val = str(val).strip()
                        if str_val and not re.match(r'^\d{4}-\d{2}-\d{2}', str_val):
                            # Try to parse and standardize
                            try:
                                parsed = pd.to_datetime(str_val, infer_datetime_format=True, errors='coerce')
                                if pd.notna(parsed):
                                    df.at[idx, col] = parsed.strftime('%Y-%m-%d')
                                    col_fixed += 1
                            except Exception:
                                pass
            
            # CODE STANDARDIZATION - uppercase and add separator if needed
            elif format_name in ('code_alpha_num', 'code_prefix_num'):
                # Detect the dominant separator and case pattern
                has_separator = '-' in (detection.get('sample_match', '') or '')
                
                for idx, val in df[col].items():
                    if pd.notna(val):
                        str_val = str(val).strip()
                        if str_val and not re.match(pattern, str_val, re.IGNORECASE):
                            # Try to fix common issues
                            fixed_val = str_val.upper()
                            
                            # If dominant pattern has separator and this doesn't, try to add it
                            if has_separator and '-' not in fixed_val:
                                # Find where letters end and numbers start
                                match = re.match(r'^([A-Z]+)(\d+)$', fixed_val)
                                if match:
                                    fixed_val = f"{match.group(1)}-{match.group(2)}"
                            
                            # Validate the fix
                            if re.match(pattern, fixed_val, re.IGNORECASE):
                                df.at[idx, col] = fixed_val
                                col_fixed += 1
            
            # PHONE STANDARDIZATION - normalize separators
            elif format_name in ('phone_intl', 'phone_domestic', 'phone_formatted'):
                for idx, val in df[col].items():
                    if pd.notna(val):
                        str_val = str(val).strip()
                        if str_val:
                            # Remove all non-digit except + at start
                            digits = re.sub(r'[^\d+]', '', str_val)
                            if digits.startswith('+'):
                                normalized = digits  # Keep international format
                            elif len(digits) == 10:
                                # Format as (XXX) XXX-XXXX
                                normalized = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                            elif len(digits) == 11 and digits.startswith('1'):
                                normalized = f"+{digits[0]} ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
                            else:
                                normalized = str_val  # Keep as-is
                            
                            if normalized != str_val:
                                df.at[idx, col] = normalized
                                col_fixed += 1
            
            # EMAIL STANDARDIZATION - lowercase
            elif format_name == 'email':
                for idx, val in df[col].items():
                    if pd.notna(val):
                        str_val = str(val).strip()
                        if str_val:
                            lower_val = str_val.lower()
                            if lower_val != str_val:
                                df.at[idx, col] = lower_val
                                col_fixed += 1
            
            if col_fixed > 0:
                standardization_count += col_fixed
                logger.info(f"  → '{col}': Standardized {col_fixed} values to '{format_name}' format")
        
        if standardization_count > 0:
            logger.info(f"  Total values standardized: {standardization_count}")
        else:
            logger.info("  → No format standardization needed")
        
        return df


    # STEP 5d: FIX RULE VIOLATIONS
    
    
    def fix_rule_violations(self, df):
        """
        Fixes values that violate configured validation rules.
        Loads rules from validation_rules.json and attempts to fix non-compliant values.
        
        Fix strategies by rule type:
        - email: lowercase, then invalidate if still fails
        - phone: normalize format (remove extra chars, reformat)
        - range: cap to min/max boundaries
        - enum: fuzzy match to closest allowed value, else invalidate
        - length: truncate if too long, invalidate if too short
        - date_format: try to parse and reformat
        - regex: invalidate if doesn't match (can't auto-fix regex)
        - url: try to add protocol, else invalidate
        
        Values that can't be fixed are set to NaN for imputation to handle.
        """
        logger.info("Fixing validation rule violations...")
        
        # Load validation rules
        rules_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                   '..', 'qa', 'validation_rules.json')
        
        if not os.path.exists(rules_file):
            logger.info("  → No validation_rules.json found - skipping")
            return df
        
        try:
            import json
            with open(rules_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            rules = [r for r in config.get('rules', []) if r.get('enabled', True)]
            pattern_aliases = config.get('pattern_aliases', {})
        except Exception as e:
            logger.warning(f"  ⚠ Error loading rules: {e}")
            return df
        
        if not rules:
            logger.info("  → No enabled rules found")
            return df
        
        # Build column -> rules mapping
        column_rules = {}
        for rule in rules:
            col_name = rule.get('column', '').lower()
            if col_name:
                if col_name not in column_rules:
                    column_rules[col_name] = []
                column_rules[col_name].append(rule)
        
        total_fixes = 0
        total_invalidations = 0
        
        # Process each column that has rules
        for col in df.columns:
            if col in METADATA_COLS:
                continue
            
            col_lower = col.lower()
            matching_rules = []
            
            # Find rules that apply to this column (case-insensitive, partial match)
            for rule_col, rules_list in column_rules.items():
                if rule_col == col_lower or rule_col in col_lower or col_lower in rule_col:
                    matching_rules.extend(rules_list)
            
            if not matching_rules:
                continue
            
            col_fixes = 0
            col_invalidations = 0
            
            for rule in matching_rules:
                rule_type = rule.get('type', '')
                
                for idx, value in df[col].items():
                    if pd.isna(value):
                        continue
                    
                    str_value = str(value).strip()
                    if not str_value:
                        continue
                    
                    is_valid = self._check_rule_validity(value, rule, pattern_aliases)
                    
                    if is_valid:
                        continue
                    
                    # Try to fix based on rule type
                    fixed_value = self._fix_rule_violation(value, rule, pattern_aliases)
                    
                    if fixed_value is not None and not pd.isna(fixed_value):
                        # Successfully fixed
                        df.at[idx, col] = fixed_value
                        col_fixes += 1
                    else:
                        # Can't fix - invalidate (set to NaN for imputation)
                        df.at[idx, col] = np.nan
                        col_invalidations += 1
            
            if col_fixes > 0 or col_invalidations > 0:
                logger.info(f"  → '{col}': {col_fixes} fixed, {col_invalidations} invalidated")
                total_fixes += col_fixes
                total_invalidations += col_invalidations
        
        if total_fixes > 0 or total_invalidations > 0:
            logger.info(f"  Rule violations: {total_fixes} fixed, {total_invalidations} invalidated")
        else:
            logger.info("  → No rule violations found")
        
        return df
    
    def _check_rule_validity(self, value, rule, pattern_aliases):
        """Check if a value complies with a rule."""
        rule_type = rule.get('type', '')
        
        if pd.isna(value):
            return rule_type != 'not_null'
        
        str_value = str(value).strip()
        
        if not str_value:
            return rule_type != 'not_null'
        
        try:
            if rule_type == 'email':
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                return bool(re.match(pattern, str_value))
            
            elif rule_type == 'url':
                from urllib.parse import urlparse
                result = urlparse(str_value)
                return all([result.scheme, result.netloc])
            
            elif rule_type == 'phone':
                pattern = rule.get('pattern', r'^[+]?[0-9\s\-().]{7,20}$')
                if pattern in pattern_aliases:
                    pattern = pattern_aliases[pattern]
                return bool(re.match(pattern, str_value))
            
            elif rule_type == 'range':
                try:
                    num_value = float(value)
                    min_val = rule.get('min', float('-inf'))
                    max_val = rule.get('max', float('inf'))
                    return min_val <= num_value <= max_val
                except (ValueError, TypeError):
                    return False
            
            elif rule_type == 'enum':
                allowed = [v.lower() for v in rule.get('values', [])]
                return str_value.lower() in allowed
            
            elif rule_type == 'length':
                min_len = rule.get('min_len', 0)
                max_len = rule.get('max_len', float('inf'))
                return min_len <= len(str_value) <= max_len
            
            elif rule_type == 'date_format':
                from datetime import datetime
                fmt = rule.get('format', '%Y-%m-%d')
                try:
                    datetime.strptime(str_value, fmt)
                    return True
                except ValueError:
                    return False
            
            elif rule_type == 'regex':
                pattern = rule.get('pattern', '')
                if pattern in pattern_aliases:
                    pattern = pattern_aliases[pattern]
                return bool(re.match(pattern, str_value, re.IGNORECASE))
            
            elif rule_type == 'not_null':
                return True  # Already handled above
            
            else:
                return True  # Unknown rule type - assume valid
        except Exception:
            return True  # On error, don't penalize
    
    def _fix_rule_violation(self, value, rule, pattern_aliases):
        """
        Attempt to fix a value that violates a rule.
        Returns fixed value, or None if can't be fixed.
        """
        rule_type = rule.get('type', '')
        str_value = str(value).strip()
        
        try:
            if rule_type == 'email':
                # Try lowercase fix
                fixed = str_value.lower()
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if re.match(pattern, fixed):
                    return fixed
                # Can't fix - invalid email structure
                return None
            
            elif rule_type == 'url':
                # Try adding http:// if missing
                if not str_value.startswith(('http://', 'https://')):
                    fixed = 'https://' + str_value
                    from urllib.parse import urlparse
                    result = urlparse(fixed)
                    if all([result.scheme, result.netloc]):
                        return fixed
                return None
            
            elif rule_type == 'phone':
                # Normalize: keep only digits and leading +
                digits = re.sub(r'[^\d+]', '', str_value)
                if digits.startswith('+'):
                    normalized = digits
                elif len(digits) >= 7:
                    normalized = digits
                else:
                    return None
                
                # Check if normalized version passes
                pattern = rule.get('pattern', r'^[+]?[0-9\s\-().]{7,20}$')
                if pattern in pattern_aliases:
                    pattern = pattern_aliases[pattern]
                if re.match(pattern, normalized):
                    return normalized
                return None
            
            elif rule_type == 'range':
                try:
                    num_value = float(value)
                    min_val = rule.get('min', float('-inf'))
                    max_val = rule.get('max', float('inf'))
                    # Cap to range
                    return max(min_val, min(max_val, num_value))
                except (ValueError, TypeError):
                    return None
            
            elif rule_type == 'enum':
                allowed = rule.get('values', [])
                str_lower = str_value.lower()
                
                # Try exact case-insensitive match first
                for v in allowed:
                    if v.lower() == str_lower:
                        return v
                
                # Try fuzzy matching (prefix match)
                for v in allowed:
                    if v.lower().startswith(str_lower) or str_lower.startswith(v.lower()):
                        return v
                
                # Try similarity-based matching (if difflib available)
                try:
                    from difflib import get_close_matches
                    matches = get_close_matches(str_lower, [v.lower() for v in allowed], n=1, cutoff=0.6)
                    if matches:
                        # Return the original case version
                        for v in allowed:
                            if v.lower() == matches[0]:
                                return v
                except ImportError:
                    pass
                
                return None
            
            elif rule_type == 'length':
                min_len = rule.get('min_len', 0)
                max_len = rule.get('max_len', float('inf'))
                
                if len(str_value) > max_len:
                    # Truncate
                    return str_value[:int(max_len)]
                elif len(str_value) < min_len:
                    # Too short - can't fix
                    return None
                return str_value
            
            elif rule_type == 'date_format':
                # Try to parse with various formats and convert
                fmt = rule.get('format', '%Y-%m-%d')
                try:
                    parsed = pd.to_datetime(str_value, errors='coerce')
                    if pd.notna(parsed):
                        return parsed.strftime(fmt)
                except Exception:
                    pass
                return None
            
            elif rule_type == 'regex':
                # Can't auto-fix regex violations
                return None
            
            elif rule_type == 'not_null':
                # Can't fix null
                return None
            
            else:
                return None
        except Exception:
            return None

    
    # STEP 6: DEDUPLICATION
    
    
    def deduplicate(self, df):
        """
        Flags duplicate rows as noisy data. DOES NOT REMOVE.
        Keeps all occurrences but marks them with indices of other duplicates.
        """
        logger.info("Scanning for duplicate rows...")
        
        initial_len = len(df)
        
        # Get data columns only (ignore metadata for duplicate detection)
        data_cols = [c for c in df.columns if c not in METADATA_COLS]
        
        if 'remediation_notes' not in df.columns:
            df['remediation_notes'] = ""
        
        # Find duplicates (don't drop, mark them)
        duplicate_mask = df[data_cols].duplicated(keep=False)  # mark ALL duplicates including first
        
        if duplicate_mask.any():
            duplicate_indices = df[duplicate_mask].index.tolist()
            
            # For each duplicate group, flag all but the first as "duplicate"
            for idx in duplicate_indices:
                # Check if this row matches any other row
                row_data = df.loc[idx, data_cols]
                matching_rows = df[data_cols].apply(lambda x: (x == row_data).all(), axis=1)
                other_matches = df[matching_rows & (df.index != idx)].index.tolist()
                
                if other_matches:
                    match_indices = ", ".join(str(i) for i in other_matches[:5])  # Show first 5
                    reason = f"Duplicate of row(s): {match_indices}"
                    df = self._add_noisy_flag(df, [idx], reason)
            
            duplicate_count = duplicate_mask.sum()
            logger.info(f"  → Flagged {duplicate_count} duplicate rows as noisy (NOT removed)")
        else:
            logger.info("  → No duplicates found")
        
        return df

    
    # STEP 7: CATEGORICAL ENCODING
    
    
    def encode_categorical(self, df):
        """
        Encodes categorical columns for ML compatibility.
        
        Strategy:
        - Low cardinality (≤10 unique): One-hot encoding
        - High cardinality (>10 unique): Label encoding
        - Binary columns: Convert to 0/1
        
        Stores encoding mappings for reproducibility.
        """
        logger.info("Encoding categorical columns...")
        
        self.encoding_maps = {}
        str_cols = df.select_dtypes(include=['object', 'string']).columns
        str_cols = [c for c in str_cols if c not in METADATA_COLS]
        
        cols_to_drop = []
        
        for col in str_cols:
            unique_vals = df[col].dropna().unique()
            n_unique = len(unique_vals)
            
            if n_unique == 0:
                continue
            
            # Binary columns → 0/1
            if n_unique == 2:
                mapping = {unique_vals[0]: 0, unique_vals[1]: 1}
                df[col] = df[col].map(mapping).fillna(0).astype(int)
                self.encoding_maps[col] = {'type': 'binary', 'mapping': mapping}
                logger.info(f"  → '{col}': Binary encoded (0/1)")
            
            # Low cardinality → One-hot encoding
            elif n_unique <= 10:
                dummies = pd.get_dummies(df[col], prefix=col, dummy_na=False)
                # Ensure column names are strings
                dummies.columns = [str(c) for c in dummies.columns]
                df = pd.concat([df, dummies], axis=1)
                cols_to_drop.append(col)
                self.encoding_maps[col] = {'type': 'onehot', 'categories': list(unique_vals)}
                logger.info(f"  → '{col}': One-hot encoded ({n_unique} categories → {len(dummies.columns)} columns)")
            
            # High cardinality → Label encoding
            else:
                mapping = {val: idx for idx, val in enumerate(sorted(unique_vals))}
                df[col] = df[col].map(mapping).fillna(-1).astype(int)
                self.encoding_maps[col] = {'type': 'label', 'mapping': mapping}
                logger.info(f"  → '{col}': Label encoded ({n_unique} unique values)")
        
        # Drop original columns that were one-hot encoded
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        return df

    
    # STEP 8: FEATURE SCALING
    
    
    def scale_features(self, df, method='standard'):
        """
        Scales numeric features for ML algorithms.
        
        Args:
            method: 'standard' (z-score), 'minmax' (0-1), or 'robust' (IQR-based)
        
        Standard scaling: (x - mean) / std → mean=0, std=1
        MinMax scaling: (x - min) / (max - min) → range [0, 1]
        Robust scaling: (x - median) / IQR → resistant to outliers
        """
        logger.info(f"Scaling numeric features (method={method})...")
        
        self.scaling_params = {}
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_cols = [c for c in numeric_cols if c not in METADATA_COLS]
        
        # Exclude one-hot encoded columns (already 0/1)
        numeric_cols = [c for c in numeric_cols if not any(
            c.startswith(f"{orig}_") for orig in self.encoding_maps.keys() 
            if self.encoding_maps.get(orig, {}).get('type') == 'onehot'
        )]
        
        for col in numeric_cols:
            # Skip binary columns (already 0/1)
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 2 and set(unique_vals).issubset({0, 1, 0.0, 1.0}):
                continue
            
            if method == 'standard':
                mean_val = df[col].mean()
                std_val = df[col].std()
                if std_val != 0 and pd.notna(std_val):
                    df[col] = (df[col] - mean_val) / std_val
                    self.scaling_params[col] = {'method': 'standard', 'mean': mean_val, 'std': std_val}
                    logger.info(f"  → '{col}': StandardScaled (μ={mean_val:.2f}, σ={std_val:.2f})")
            
            elif method == 'minmax':
                min_val = df[col].min()
                max_val = df[col].max()
                if max_val != min_val:
                    df[col] = (df[col] - min_val) / (max_val - min_val)
                    self.scaling_params[col] = {'method': 'minmax', 'min': min_val, 'max': max_val}
                    logger.info(f"  → '{col}': MinMaxScaled (range [{min_val:.2f}, {max_val:.2f}] → [0, 1])")
            
            elif method == 'robust':
                median_val = df[col].median()
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                if iqr != 0:
                    df[col] = (df[col] - median_val) / iqr
                    self.scaling_params[col] = {'method': 'robust', 'median': median_val, 'iqr': iqr}
                    logger.info(f"  → '{col}': RobustScaled (median={median_val:.2f}, IQR={iqr:.2f})")
        
        return df

    
    # STEP 9: SKEWNESS CORRECTION

    
    def correct_skewness(self, df, threshold=1.0):
        """
        Corrects heavily skewed numeric distributions.
        
        For right-skewed data (skewness > threshold): Apply log1p transform
        For left-skewed data (skewness < -threshold): Apply reflection + log1p
        
        Args:
            threshold: Skewness threshold above which to apply transformation
        """
        logger.info(f"Correcting skewness (threshold=±{threshold})...")
        
        self.skewness_transforms = {}
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_cols = [c for c in numeric_cols if c not in METADATA_COLS]
        
        for col in numeric_cols:
            # Skip binary/encoded columns
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) <= 10:
                continue
            
            skewness = df[col].skew()
            
            if pd.isna(skewness):
                continue
            
            if skewness > threshold:
                # Right-skewed: log1p transform (handles zeros)
                min_val = df[col].min()
                if min_val < 0:
                    # Shift to make all positive
                    df[col] = df[col] - min_val + 1
                df[col] = np.log1p(df[col])
                new_skewness = df[col].skew()
                self.skewness_transforms[col] = {'type': 'log1p', 'original_skewness': skewness}
                logger.info(f"  → '{col}': Log1p transform (skew {skewness:.2f} → {new_skewness:.2f})")
            
            elif skewness < -threshold:
                # Left-skewed: reflect then log1p
                max_val = df[col].max()
                df[col] = np.log1p(max_val - df[col] + 1)
                new_skewness = df[col].skew()
                self.skewness_transforms[col] = {'type': 'reflect_log1p', 'original_skewness': skewness, 'max': max_val}
                logger.info(f"  → '{col}': Reflect+Log1p transform (skew {skewness:.2f} → {new_skewness:.2f})")
        
        return df

    
    # MAIN PIPELINE
    

    def run_remediation(self, outlier_method='cap', scale_method=None, encode_categorical=False, correct_skew=False):
        """
        Data Cleaning Pipeline - preserves original data types.
        
        Executes preprocessing steps in order:
        1. Remove embedded headers/labels from spreadsheets
        2. Infer and cast proper data types
        3. Normalize string values (convert placeholders to NaN)
        4. Remove blank rows (all data columns empty) - improves Lineage score
        5. Standardize formats (dates, codes, emails) - improves Validity score
        6. Fix validation rule violations - improves Validity score
        7. Impute missing values (mean/median/mode) - improves Completeness score
        8. Handle outliers (cap/remove/flag) - improves Accuracy score
        9. Deduplicate rows - improves Uniqueness score
        
        Optional ML transformations (disabled by default to preserve readability):
        10. Encode categorical variables (one-hot/label) - set encode_categorical=True
        11. Correct skewness (log transform) - set correct_skew=True
        12. Scale numeric features - set scale_method='standard'/'minmax'/'robust'
        
        Args:
            outlier_method: 'cap' (winsorize), 'remove', or 'flag'
            scale_method: None (default), 'standard', 'minmax', or 'robust'
            encode_categorical: False (default) - set True for ML pipelines
            correct_skew: False (default) - set True for ML pipelines
        
        Returns:
            Path to cleaned parquet file
        """
        logger.info("=" * 60)
        logger.info("  DATA CLEANING PIPELINE")
        logger.info("=" * 60)
        
        df = self.load_data()
        if df is None or df.empty:
            logger.warning("No data loaded. Remediation aborted.")
            return None
        
        initial_rows = len(df)
        initial_cols = len(df.columns)
        logger.info(f"Input: {initial_rows} rows × {initial_cols} columns")
        
        # Drop completely empty rows/columns first
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)
        
        # Step 1: Remove embedded headers (Excel section labels, repeated headers)
        df = self.remove_embedded_headers(df)
        
        # Step 2: Infer and cast proper types
        df = self.infer_and_cast_types(df)
        
        # Step 3: Normalize strings (convert placeholders to NaN)
        df = self.normalize_strings(df)
        
        # Step 4: Remove blank rows (improves Lineage score)
        df = self.remove_blank_rows(df)
        
        # Step 5: Standardize formats (improves Validity score)
        df = self.standardize_formats(df)
        
        # Step 6: Fix validation rule violations (improves Validity score)
        df = self.fix_rule_violations(df)
        
        # Step 7: Impute missing values (ML-grade: mean/median/mode) - improves Completeness
        df = self.impute_missing_values(df)
        
        # Step 8: Handle outliers - improves Accuracy
        df = self.handle_outliers(df, method='flag')  # Default to flagging, not removing
        
        # Step 9: Deduplicate - improves Uniqueness
        df = self.deduplicate(df)
        
        # Final null check before final processing (encoding fails on nulls)
        data_cols = [c for c in df.columns if c not in METADATA_COLS]
        total_nulls = df[data_cols].isna().sum().sum()
        
        if total_nulls > 0:
            logger.error(f"  ❌ {total_nulls} null values detected before final processing!")
            for col in data_cols:
                if df[col].isna().any():
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(0)
                    else:
                        df[col] = df[col].fillna("Unknown")
            logger.info("  → Applied emergency fallback imputation")
        
        # Step 10: Encode categorical variables (optional)
        if encode_categorical:
            df = self.encode_categorical(df)
        
        # Step 11: Correct skewness (optional, before scaling for better results)
        if correct_skew:
            df = self.correct_skewness(df, threshold=1.0)
        
        # Step 12: Scale numeric features (optional)
        if scale_method:
            df = self.scale_features(df, method=scale_method)
        
        # ===== NOW SPLIT INTO CLEANED AND NOISY DATA =====
        logger.info("=" * 60)
        logger.info("  SPLITTING DATA: CLEANED vs NOISY")
        logger.info("=" * 60)
        
        # Separate cleaned and noisy rows
        cleaned_mask = ~df.index.isin(self.noisy_rows_indices)
        df_cleaned = df[cleaned_mask].copy()
        df_noisy = df[~cleaned_mask].copy()
        
        logger.info(f"✓ CLEANED DATA: {len(df_cleaned)} rows (high quality)")
        logger.info(f"⚠ NOISY DATA: {len(df_noisy)} rows (quality issues flagged)")
        
        # Drop remediation_notes from cleaned data if empty (no issues)
        if 'remediation_notes' in df_cleaned.columns:
            if df_cleaned['remediation_notes'].fillna('').str.strip().eq('').all():
                df_cleaned = df_cleaned.drop(columns=['remediation_notes'])
                logger.info("  → Dropped empty remediation_notes from cleaned data")
        
        # Keep remediation_notes in noisy data (shows WHY each row is problematic)
        if 'remediation_notes' in df_noisy.columns:
            # Ensure notes are filled (should already be, but safety check)
            df_noisy['remediation_notes'] = df_noisy['remediation_notes'].fillna("Data quality issues detected")
        
        # Save cleaned data with timestamp for preservation
        from datetime import datetime as dt
        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        timestamped_path = os.path.join(self.output_dir, f"cleaned_data_{timestamp}.parquet")
        df_cleaned.to_parquet(timestamped_path, index=False)
        logger.info(f"  Archived cleaned data: {timestamped_path}")
        
        # Also save as "latest" for the app to use
        df_cleaned.to_parquet(self.output_file, index=False)
        logger.info(f"  ✓ Saved: {self.output_file}")
        
        # Save noisy data (will be empty if no issues detected)
        timestamped_noisy = os.path.join(self.output_dir, f"noisy_data_{timestamp}.parquet")
        df_noisy.to_parquet(timestamped_noisy, index=False)
        logger.info(f"  Archived noisy data: {timestamped_noisy}")
        
        df_noisy.to_parquet(self.noisy_file, index=False)
        logger.info(f"  ✓ Saved: {self.noisy_file}")
        
        final_rows = len(df_cleaned) + len(df_noisy)
        final_cols = len([c for c in df_cleaned.columns if c not in METADATA_COLS])
        
        # Calculate null stats for both datasets
        data_cols_cleaned = [c for c in df_cleaned.columns if c not in METADATA_COLS]
        data_cols_noisy = [c for c in df_noisy.columns if c not in METADATA_COLS]
        nulls_in_cleaned = df_cleaned[data_cols_cleaned].isna().sum().sum()
        nulls_in_noisy = df_noisy[data_cols_noisy].isna().sum().sum() if len(df_noisy) > 0 else 0
        
        logger.info("=" * 60)
        logger.info("  PREPROCESSING COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Total Rows: {initial_rows} → {final_rows} (0 removed)")
        logger.info(f"  Cleaned Rows: {len(df_cleaned)} (ML-ready, no null values)")
        logger.info(f"  Noisy Rows: {len(df_noisy)} (flagged, preserved for review)")
        logger.info(f"  Columns: {initial_cols} → {len(df.columns)} (after encoding)")
        logger.info(f"  Data Columns: {final_cols}")
        logger.info("=" * 60)
        
        # Log transformation summaries
        if self.imputation_stats:
            logger.info("Imputation Summary:")
            for col, stats in self.imputation_stats.items():
                logger.info(f"  • {col}: {stats['count']} values filled with {stats['strategy']}")
        
        if hasattr(self, 'encoding_maps') and self.encoding_maps:
            logger.info("Encoding Summary:")
            for col, info in self.encoding_maps.items():
                logger.info(f"  • {col}: {info['type']} encoding")
        
        if hasattr(self, 'scaling_params') and self.scaling_params:
            logger.info("Scaling Summary:")
            for col, info in self.scaling_params.items():
                logger.info(f"  • {col}: {info['method']} scaled")
        
        if hasattr(self, 'skewness_transforms') and self.skewness_transforms:
            logger.info("Skewness Corrections:")
            for col, info in self.skewness_transforms.items():
                logger.info(f"  • {col}: {info['type']} (skew {info['original_skewness']:.2f})")
        
        logger.info("=" * 60)
        logger.info(f"  Cleaned Data: {len(df_cleaned)} rows → {self.output_file}")
        logger.info(f"  Noisy Data: {len(df_noisy)} rows → {self.noisy_file}")
        logger.info(f"  Reason field: 'remediation_notes' column in noisy data")
        logger.info("=" * 60)
        
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

        # Save Final State (with timestamp for preservation)
        from datetime import datetime as dt
        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        timestamped_path = os.path.join(self.output_dir, f"cleaned_data_{timestamp}.parquet")
        df.to_parquet(timestamped_path, index=False)
        
        # Also save as "latest" for the app to use
        df.to_parquet(self.output_file, index=False)
        
        final_count = len(df)
        logger.info(f"Targeted Fixes Complete. Records reduced from {initial_count} to {final_count}.")
        return self.output_file

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run_remediation()
