"""
Sales Data Pipeline - Pandas/Lightweight Version
=================================================

A lightweight version of the pipeline using pandas instead of PySpark.
Useful for development, testing, or smaller datasets.

Usage:
    python run_pipeline_pandas.py
"""

import os
import re
import json
import glob
import logging
from datetime import datetime
from typing import List, Dict, Tuple

import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    SOURCE_PATH = "/Workspace/Users/artisadikaj99@gmail.com/project/"
    OUTPUT_PATH = "./output"
    FILE_PATTERN = "sales_*.csv"
    SALES_COLUMN_PATTERN = r"^([A-Z][a-z]{2})_sales$"
    IDENTIFIER_COLUMNS = [
        "salesperson_id", "salesperson_name",
        "sales_state_id", "sales_state_name",
        "store_id", "store_name", "year"
    ]


MONTH_TO_NUMBER = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger("SalesPipeline")


# =============================================================================
# PIPELINE FUNCTIONS
# =============================================================================

def discover_files(source_path: str, pattern: str) -> List[str]:
    """Find all CSV files matching the pattern."""
    search_pattern = os.path.join(source_path, pattern)
    files = glob.glob(search_pattern)
    logger.info(f"Discovered {len(files)} files")
    return files


def classify_columns(columns: List[str]) -> Tuple[List[str], List[str]]:
    """Separate identifier columns from sales columns."""
    pattern = re.compile(Config.SALES_COLUMN_PATTERN)
    sales_cols = [c for c in columns if pattern.match(c)]
    id_cols = [c for c in columns if not pattern.match(c)]
    return id_cols, sales_cols


def read_and_transform(file_path: str) -> pd.DataFrame:
    """Read CSV and transform to normalized format."""
    file_name = os.path.basename(file_path)
    logger.info(f"Processing: {file_name}")

    # Read CSV
    df = pd.read_csv(file_path)
    logger.info(f"  Read {len(df):,} records with {len(df.columns)} columns")

    # Classify columns
    id_cols, sales_cols = classify_columns(df.columns.tolist())
    logger.info(f"  Found {len(id_cols)} identifier cols, {len(sales_cols)} sales cols")

    # Add metadata
    df['source_file'] = file_name
    df['load_timestamp'] = datetime.now()

    # Unpivot (melt) sales columns
    id_vars = id_cols + ['source_file', 'load_timestamp']
    melted = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=sales_cols,
        var_name='month_col',
        value_name='sales_amount'
    )

    # Extract month name from column name (e.g., "Jan_sales" -> "Jan")
    melted['month'] = melted['month_col'].str.replace('_sales', '', regex=False)

    # Add month number and quarter
    melted['month_number'] = melted['month'].map(MONTH_TO_NUMBER)
    melted['quarter'] = ((melted['month_number'] - 1) // 3) + 1

    # Drop the temporary column
    melted = melted.drop(columns=['month_col'])

    logger.info(f"  Transformed to {len(melted):,} records (normalized)")

    return melted


def validate_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate data and separate valid/invalid records."""
    # Check for nulls in key columns
    required_cols = ['salesperson_id', 'store_id', 'year', 'sales_amount']
    null_mask = df[required_cols].isnull().any(axis=1)

    # Check for negative sales
    negative_mask = df['sales_amount'] < 0

    # Combine validation
    invalid_mask = null_mask | negative_mask

    valid_df = df[~invalid_mask].copy()
    invalid_df = df[invalid_mask].copy()

    if len(invalid_df) > 0:
        logger.warning(f"  Found {len(invalid_df)} invalid records")

    return valid_df, invalid_df


def save_output(df: pd.DataFrame, output_path: str):
    """Save the transformed data."""
    # Create output directory
    os.makedirs(output_path, exist_ok=True)

    # Save as Parquet (preferred)
    parquet_path = os.path.join(output_path, "sales_normalized.parquet")
    df.to_parquet(parquet_path, index=False, engine='pyarrow')
    logger.info(f"Saved Parquet: {parquet_path}")

    # Also save as CSV for easy viewing
    csv_path = os.path.join(output_path, "sales_normalized.csv")
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved CSV: {csv_path}")


def generate_summary(df: pd.DataFrame) -> Dict:
    """Generate summary statistics."""
    summary = {
        "total_records": len(df),
        "unique_salespeople": df['salesperson_id'].nunique(),
        "unique_stores": df['store_id'].nunique(),
        "unique_states": df['sales_state_id'].nunique(),
        "months_covered": sorted(df['month'].unique().tolist()),
        "total_sales": float(df['sales_amount'].sum()),
        "avg_sales": float(df['sales_amount'].mean()),
        "min_sales": float(df['sales_amount'].min()),
        "max_sales": float(df['sales_amount'].max()),
    }
    return summary


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run the pipeline."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("SALES DATA PIPELINE - PANDAS VERSION")
    logger.info("=" * 60)

    # Discover files
    files = discover_files(Config.SOURCE_PATH, Config.FILE_PATTERN)

    if not files:
        logger.error("No files found!")
        return

    # Process each file
    all_dfs = []
    for file_path in files:
        try:
            df = read_and_transform(file_path)
            valid_df, invalid_df = validate_data(df)
            all_dfs.append(valid_df)
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")

    # Combine all DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Combined total: {len(combined_df):,} records")

        # Save output
        save_output(combined_df, Config.OUTPUT_PATH)

        # Generate summary
        summary = generate_summary(combined_df)

        # Print summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Files Processed: {len(files)}")
        logger.info(f"Total Records: {summary['total_records']:,}")
        logger.info(f"Unique Salespeople: {summary['unique_salespeople']:,}")
        logger.info(f"Unique Stores: {summary['unique_stores']:,}")
        logger.info(f"Months Covered: {', '.join(summary['months_covered'])}")
        logger.info(f"Total Sales: ${summary['total_sales']:,.2f}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        # Save summary
        summary['duration_seconds'] = duration
        summary['files_processed'] = len(files)
        summary['timestamp'] = datetime.now().isoformat()

        summary_path = os.path.join(Config.OUTPUT_PATH, "summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary saved: {summary_path}")


if __name__ == "__main__":
    main()
