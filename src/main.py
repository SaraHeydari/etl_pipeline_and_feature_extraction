"""
Main entry point for the Nordic Data Pipeline.

This script orchestrates the full pipeline:
1. ETL: Load and clean customer and transaction data
2. Feature Engineering: Compute RFM metrics and business flags

Usage:
    uv run python src/main.py
"""

import config
from etl import run_etl
from features import run_feature_engineering


def main() -> None:
    """Run the complete data pipeline."""
    print("=" * 60)
    print("NORDIC DATA PIPELINE")
    print("=" * 60)

    # Step 1: ETL
    print("\n" + "=" * 60)
    print("STEP 1: ETL")
    print("=" * 60)

    customers, transactions = run_etl(
        customers_path=config.CUSTOMERS_FILE,
        transactions_path=config.TRANSACTIONS_FILE,
        output_dir=config.PROCESSED_DATA_DIR,
    )

    # Step 2: Feature Engineering
    print("\n" + "=" * 60)
    print("STEP 2: FEATURE ENGINEERING")
    print("=" * 60)

    features = run_feature_engineering(
        customers=customers,
        transactions=transactions,
        output_dir=config.PROCESSED_DATA_DIR,
    )

    # Final summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\nOutput files in: {config.PROCESSED_DATA_DIR}")
    print("  - customers_cleaned.csv")
    print("  - transactions_cleaned.csv")
    print("  - customer_features.csv")


if __name__ == "__main__":
    main()
