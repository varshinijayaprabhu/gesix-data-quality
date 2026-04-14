import os
import numpy as np
import pandas as pd
import logging
from paths import get_workspace_dir

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)


_NULL_LIKE_STRINGS = {
    '', ' ', '  ', '   ',
    'nan', 'NaN', 'NAN',
    'none', 'None', 'NONE',
    'null', 'Null', 'NULL',
    'N/A', 'n/a', 'NA', 'na',
    '—', '#N/A', '#NA', '#VALUE!',
    'undefined', 'UNDEFINED',
    'missing', 'MISSING',
    '<NA>', '<na>', 'NaT', 'nat',
}

# Metadata columns to drop before profiling
_META_COLS = {'remediation_notes', 'ingested_at', 'source'}


class DataProfiler:
    def __init__(self):
        workspace = get_workspace_dir()
        self.output_dir = workspace["processed"]

    def _prepare_for_profiling(self, df: pd.DataFrame) -> pd.DataFrame:
      
        df = df.copy()

        # 1. Drop metadata columns
        df = df.drop(columns=[c for c in _META_COLS if c in df.columns])

        for col in df.columns:
            # 2. Null-like strings → NaN
            df[col] = df[col].apply(
                lambda x: np.nan
                if isinstance(x, str) and x.strip() in _NULL_LIKE_STRINGS
                else x
            )

            # Skip columns that are already non-object dtype
            if not pd.api.types.is_object_dtype(df[col]):
                continue

            # 3. Try numeric conversion
            sample = df[col].dropna().head(100)
            if len(sample) > 0:
                numeric_test = pd.to_numeric(
                    sample.astype(str).str.replace(',', '', regex=False).str.strip(),
                    errors='coerce',
                )
                success_rate = numeric_test.notna().sum() / len(sample)
                if success_rate >= 0.7:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce',
                    )
                    continue

            # 4. Try datetime conversion for date-like column names
            date_hints = ['date', 'time', 'timestamp', '_at', 'created', 'updated', 'modified']
            if any(hint in col.lower() for hint in date_hints):
                try:
                    converted = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
                    if converted.notna().sum() / len(df) >= 0.5:
                        df[col] = converted
                        continue
                except Exception:
                    pass

            # 5. Strip whitespace on remaining string columns
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(_NULL_LIKE_STRINGS, np.nan)

        return df

    def generate(self, df: pd.DataFrame, title: str = "Data Quality Profile", output_filename: str = "eda_profile.html") -> str | None:
        try:
            from ydata_profiling import ProfileReport
        except ImportError:
            logger.warning("ydata-profiling is not installed.")
            return None

        if df is None or df.empty:
            return None

        # Restore proper types before profiling (fixes raw-data issues)
        df = self._prepare_for_profiling(df)

        try:
            profile = ProfileReport(
                df,
                title=title,
                minimal=True,          
                progress_bar=False,
                correlations=None,    
                missing_diagrams=None, 
                interactions=None,    
                samples=None,          
            )

            output_path = os.path.join(self.output_dir, output_filename)
            profile.to_file(output_path)
            logger.info(f"DataProfiler: EDA report saved → {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"DataProfiler: Failed — {e}")
            return None

    def generate_from_parquet(self, parquet_path: str) -> str | None:
        if not os.path.exists(parquet_path):
            return None
        try:
            df = pd.read_parquet(parquet_path)
            return self.generate(df)
        except Exception as e:
            logger.error(f"DataProfiler: Could not read parquet — {e}")
            return None