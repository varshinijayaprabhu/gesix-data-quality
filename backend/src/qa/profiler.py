import os
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


class DataProfiler:
    """
    Automatic EDA Profiler powered by ydata-profiling.
    Generates a rich HTML report alongside the 7-dimension QA scores,
    giving AI teams a complete picture of the dataset at a glance.

    Output: data/processed/eda_profile.html
    """

    def __init__(self):
        workspace = get_workspace_dir()
        self.output_dir = workspace["processed"]

    def generate(self, df: pd.DataFrame, title: str = "Data Quality Profile") -> str | None:
        """
        Runs ydata-profiling on the supplied DataFrame and saves an HTML report.

        Args:
            df:    The cleaned DataFrame to profile.
            title: Title shown at the top of the HTML report.

        Returns:
            Path to the generated HTML file, or None on failure.
        """
        try:
            from ydata_profiling import ProfileReport
        except ImportError:
            logger.warning(
                "ydata-profiling is not installed. "
                "Run 'pip install ydata-profiling' to enable EDA reports."
            )
            return None

        if df is None or df.empty:
            logger.warning("DataProfiler: Empty DataFrame supplied. Skipping profile generation.")
            return None

        logger.info(f"DataProfiler: Generating EDA report for {len(df)} records...")

        try:
            profile = ProfileReport(
                df,
                title=title,
                # Minimal mode keeps report generation fast for large datasets
                minimal=len(df) > 5000,
                explorative=True,
                # Disable progress bar for cleaner server logs
                progress_bar=False,
                # Correlation methods to include
                correlations={
                    "pearson": {"calculate": True},
                    "spearman": {"calculate": False},
                    "kendall": {"calculate": False},
                    "phi_k": {"calculate": False},
                    "cramers": {"calculate": False},
                },
            )

            output_path = os.path.join(self.output_dir, "eda_profile.html")
            profile.to_file(output_path)
            logger.info(f"DataProfiler: EDA report saved → {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"DataProfiler: Failed to generate profile — {e}")
            return None

    def generate_from_parquet(self, parquet_path: str) -> str | None:
        """Convenience wrapper: loads a parquet file then calls generate()."""
        if not os.path.exists(parquet_path):
            logger.error(f"DataProfiler: Parquet file not found at {parquet_path}")
            return None
        try:
            df = pd.read_parquet(parquet_path)
            return self.generate(df)
        except Exception as e:
            logger.error(f"DataProfiler: Could not read parquet — {e}")
            return None