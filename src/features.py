"""
Feature engineering module for customer analytics.

This module computes:
- RFM (Recency, Frequency, Monetary) metrics per customer
- Behavioral flags (high-value, churning)
- Customer segmentation indicators
"""

from datetime import date
from pathlib import Path

import polars as pl

import config


def compute_rfm_features(
    transactions: pl.DataFrame,
    reference_date: date | None = None,
) -> pl.DataFrame:
    """Compute RFM (Recency, Frequency, Monetary) features per customer.

    Args:
        transactions: Cleaned transactions DataFrame
        reference_date: Date to calculate recency from (defaults to max transaction date)

    Returns:
        DataFrame with one row per customer and RFM features
    """
    # Use max transaction date as reference if not provided
    if reference_date is None:
        reference_date = transactions["timestamp"].max().date()
    
    # Calculate interevent times (time between consecutive transactions) per customer
    transactions_sorted = transactions.sort(["customer_id", "timestamp"])
    interevent_stats = (
        transactions_sorted
        .with_columns(
            # Calculate difference from previous transaction timestamp for same customer
            (pl.col("timestamp") - pl.col("timestamp").shift(1).over("customer_id"))
            .dt.total_days()
            .alias("interevent_days")
        )
        .filter(pl.col("interevent_days").is_not_null())  # Remove first transaction per customer
        .group_by("customer_id")
        .agg(
            pl.col("interevent_days").mean().alias("mean_interevent_days"),
            pl.col("interevent_days").std().alias("std_interevent_days"),
        )
    )
    
    rfm_added = transactions.group_by("customer_id").agg(
        # === Monetary Features ===
        pl.col("amount_in_eur").sum().round(2).alias("total_spend"),
        pl.col("amount_in_eur").mean().round(2).alias("avg_transaction_amount"),
        pl.col("amount_in_eur").std().round(2).alias("std_transaction_amount"),
        pl.col("amount_in_eur").min().alias("min_transaction_amount"),
        pl.col("amount_in_eur").max().alias("max_transaction_amount"),
        # === Frequency Features ===
        pl.col("transaction_id").count().alias("transaction_count"),
        # === Recency Features ===
        pl.col("timestamp").max().alias("last_transaction_date"),
        pl.col("timestamp").min().alias("first_transaction_date"),
        # === Category Preferences ===
        pl.col("category").mode().first().alias("preferred_category"),
        # === Currency (most used) ===
        pl.col("currency").mode().first().alias("preferred_currency"),
    ).with_columns(
        # Calculate days since last transaction
        (pl.lit(reference_date) - pl.col("last_transaction_date").dt.date())
        .dt.total_days()
        .alias("days_since_last_transaction"),
        # Calculate customer tenure (days between first and last transaction)
        (
            pl.col("last_transaction_date").dt.date()
            - pl.col("first_transaction_date").dt.date()
        )
        .dt.total_days()
        .alias("customer_tenure_days"),
    ).join(interevent_stats, on="customer_id", how="left")
    
    return rfm_added


def add_customer_flags(
    features: pl.DataFrame,
    high_value_percentile: float | None = None,
    churn_days: int | None = None,
) -> pl.DataFrame:
    """Add business flags to customer features.

    Flags added:
    - is_high_value: Customer is in top percentile by total spend
    - is_churning: Customer has not transacted in churn_days
    - is_churning_2: Customer's days_since_last_transaction > mean + 2*std of interevent times
    - has_single_transaction: Customer made only one transaction

    Args:
        features: DataFrame with RFM features
        high_value_percentile: Percentile threshold for high-value (default from config)
        churn_days: Days of inactivity to consider churning (default from config)

    Returns:
        DataFrame with additional flag columns
    """
    # Use config defaults if not explicitly provided
    if high_value_percentile is None:
        high_value_percentile = config.HIGH_VALUE_PERCENTILE
    if churn_days is None:
        churn_days = config.CHURN_DAYS
    
    # Calculate high-value threshold
    high_value_threshold = features["total_spend"].quantile(high_value_percentile)

    return features.with_columns(
        # High value: top percentile by spend
        (pl.col("total_spend") >= high_value_threshold).alias("is_high_value"),
        # Churning: no activity in N days
        (pl.col("days_since_last_transaction") >= churn_days).alias("is_churning"),
        # Churning (z-score based): days since last transaction > mean + CHURN_Z_SCORE_THRESHOLD*std of interevent times
        (pl.col("days_since_last_transaction") > (pl.col("mean_interevent_days") + config.CHURN_Z_SCORE_THRESHOLD * pl.col("std_interevent_days")))
        .alias("is_churning_2"),
        # Single transaction customers (potential onboarding targets)
        (pl.col("transaction_count") == 1).alias("has_single_transaction"),
    )


def enrich_with_customer_data(
    features: pl.DataFrame,
    customers: pl.DataFrame,
) -> pl.DataFrame:
    """Join customer features with customer master data.

    Args:
        features: DataFrame with customer features and flags
        customers: Cleaned customers DataFrame

    Returns:
        Enriched DataFrame with customer attributes
    """
    return features.join(
        customers.select(["customer_id", "country", "signup_date", "email"]),
        on="customer_id",
        how="left",
    )


def compute_feature_summary(features: pl.DataFrame) -> dict:
    """Compute summary statistics for the feature set.

    Args:
        features: DataFrame with customer features

    Returns:
        Dictionary with summary statistics
    """
    return {
        "total_customers": features.height,
        "high_value_customers": features.filter(pl.col("is_high_value")).height,
        "churning_customers": features.filter(pl.col("is_churning")).height,
        "churning_customers_based_on_z_score": features.filter(pl.col("is_churning_2")).height,
        "single_transaction_customers": features.filter(
            pl.col("has_single_transaction")
        ).height,
        "total_spend_stats": {
            "min": round(features["total_spend"].min(), 2),
            "max": round(features["total_spend"].max(), 2),
            "mean": round(features["total_spend"].mean(), 2),
            "median": round(features["total_spend"].median(), 2),
        },
        "transaction_count_stats": {
            "min": features["transaction_count"].min(),
            "max": features["transaction_count"].max(),
            "mean": round(features["transaction_count"].mean(), 2),
            "median": features["transaction_count"].median(),
        },
        "avg_days_since_last_transaction": round(
            features["days_since_last_transaction"].mean(), 1
        ),
    }


def run_feature_engineering(
    customers: pl.DataFrame,
    transactions: pl.DataFrame,
    output_dir: Path,
    reference_date: date | None = None,
    high_value_percentile: float | None = None,
    churn_days: int | None = None,
) -> pl.DataFrame:
    """Run the complete feature engineering pipeline.

    Args:
        customers: Cleaned customers DataFrame
        transactions: Cleaned transactions DataFrame
        output_dir: Directory for output files
        reference_date: Date for recency calculation (defaults to max transaction date)
        high_value_percentile: Percentile for high-value flag (default from config)
        churn_days: Days threshold for churn flag (default from config)

    Returns:
        DataFrame with all customer features
    """
    # Use config defaults if not explicitly provided
    if high_value_percentile is None:
        high_value_percentile = config.HIGH_VALUE_PERCENTILE
    if churn_days is None:
        churn_days = config.CHURN_DAYS
    
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Computing RFM features...")
    features = compute_rfm_features(transactions, reference_date)
    print(f"  Computed features for {features.height} customers")

    print("\nAdding business flags...")
    features = add_customer_flags(features, high_value_percentile, churn_days)

    print("\nEnriching with customer data...")
    features = enrich_with_customer_data(features, customers)

    # Reorder columns for better readability
    column_order = [
        # Identifiers
        "customer_id",
        "email",
        "country",
        "signup_date",
        # Monetary
        "total_spend",
        "avg_transaction_amount",
        "std_transaction_amount",
        "min_transaction_amount",
        "max_transaction_amount",
        # Frequency
        "transaction_count",
        # Recency
        "first_transaction_date",
        "last_transaction_date",
        "days_since_last_transaction",
        "customer_tenure_days",
        # Interevent statistics
        "mean_interevent_days",
        "std_interevent_days",
        # Preferences
        "preferred_category",
        "preferred_currency",
        # Flags
        "is_high_value",
        "is_churning",
        "is_churning_2",
        "has_single_transaction",
    ]
    features = features.select(column_order)

    # Print summary
    print("\nFeature summary:")
    summary = compute_feature_summary(features)
    print(f"  Total customers with transactions: {summary['total_customers']}")
    print(f"  High-value customers: {summary['high_value_customers']}")
    print(f"  Churning customers, last transaction more than 50 days ago: {summary['churning_customers']}")
    print(f"  Churning customers, z-score larger than 2: {summary['churning_customers_based_on_z_score']}")
    print(f"  Single-transaction customers: {summary['single_transaction_customers']}")
    print(
        f"  Avg days since last transaction: {summary['avg_days_since_last_transaction']}"
    )

    # Save features
    print("\nSaving features...")
    output_path = output_dir / "customer_features.csv"
    features.write_csv(output_path)
    print(f"  Saved: {output_path}")

    return features


if __name__ == "__main__":
    # Load cleaned data
    print("Loading cleaned data...")
    customers = pl.read_csv(config.CUSTOMERS_CLEANED_FILE)
    transactions = pl.read_csv(
        config.TRANSACTIONS_CLEANED_FILE,
        try_parse_dates=True,
    )

    # Run feature engineering
    run_feature_engineering(
        customers=customers,
        transactions=transactions,
        output_dir=config.PROCESSED_DATA_DIR,
    )
