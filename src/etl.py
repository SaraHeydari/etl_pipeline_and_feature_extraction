"""
ETL module for cleaning customer and transaction data.

This module provides functions to:
- Load raw CSV data
- Clean and standardize customer records
- Clean and standardize transaction records
- Validate data quality
"""

from pathlib import Path
import polars as pl

import config


def load_customers(path: Path) -> pl.DataFrame:
    """Load customers CSV file.

    Args:
        path: Path to customers.csv
        path is a Path object, not a string
    Returns:
        Polars DataFrame including raw customer data
    """
    return pl.read_csv(
        path,
        schema_overrides={
            "customer_id": pl.Int64,
            "country": pl.String,
            "signup_date": pl.String,
            "email": pl.String,
        },
    )


def load_transactions(path: Path) -> pl.DataFrame:
    """Load transactions CSV file.

    Args:
        path: Path to transactions.csv

    Returns:
        Raw DataFrame with transaction data
    """
    return pl.read_csv(
        path,
        schema_overrides={
            "transaction_id": pl.Int64,
            "customer_id": pl.Int64,
            "amount": pl.Float64,
            "currency": pl.String,
            "timestamp": pl.String,
            "category": pl.String,
        },
    )


def clean_customers(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and standardize customer data.

    Transformations:
    - Standardize country codes to uppercase
    - Parse signup_date to Date type
    - Lowercase email addresses
    - Remove rows with null customer_id
    - Filter to valid Nordic countries
    - Remove duplicate customer_ids (keep first)

    Args:
        df: Raw customer DataFrame

    Returns:
        Cleaned customer DataFrame
    """
    initial_rows = df.height

    # Check for duplicate customer_ids
    unique_ids = df["customer_id"].n_unique()
    if unique_ids < initial_rows:
        duplicate_count = initial_rows - unique_ids
        print(f"  WARNING: {duplicate_count} duplicate customer_ids found")

    cleaned = (
        df.with_columns(
            pl.col("country").str.to_uppercase().alias("country"),
            pl.col("signup_date").str.to_date("%Y-%m-%d").alias("signup_date"),
            pl.col("email").str.to_lowercase().alias("email"),
        )
        .filter(
            pl.col("customer_id").is_not_null(),
            pl.col("country").is_in(config.VALID_COUNTRIES),
        )
        .unique(subset=["customer_id"], keep="first")
        .sort("customer_id")
    )

    # Report filtering statistics
    final_rows = cleaned.height
    removed_rows = initial_rows - final_rows
    if removed_rows > 0:
        removed_pct = (removed_rows / initial_rows) * 100
        print(f"  Removed {removed_rows} rows ({removed_pct:.1f}% of original)")

    return cleaned


def clean_transactions(df: pl.DataFrame) -> pl.DataFrame:
    """Clean and standardize transaction data.

    Transformations:
    - Standardize currency to uppercase, fill nulls with "NA"
    - Standardize category to lowercase, fill nulls/empty with "NA"
    - Parse timestamp to Datetime type
    - Remove rows with invalid amounts (null or <= 0)
    - Remove rows with null customer_id or transaction_id
    - Remove duplicate transaction_ids (keep first)

    Args:
        df: Raw transaction DataFrame

    Returns:
        Cleaned transaction DataFrame
    """
    initial_rows = df.height

    # Check for duplicate transaction_ids
    unique_ids = df["transaction_id"].n_unique()
    if unique_ids < initial_rows:
        duplicate_count = initial_rows - unique_ids
        print(f"  WARNING: {duplicate_count} duplicate transaction_ids found")

    cleaned = (
        df.with_columns(
            # Standardize currency: uppercase, handle nulls
            pl.when(pl.col("currency").is_null() | (pl.col("currency") == ""))
            .then(pl.lit("NA"))
            .otherwise(pl.col("currency").str.to_uppercase())
            .alias("currency"),
            # Standardize category: lowercase, handle nulls and empty strings
            pl.when(pl.col("category").is_null() | (pl.col("category") == ""))
            .then(pl.lit("NA"))
            .otherwise(pl.col("category").str.to_lowercase())
            .alias("category"),
            # Parse timestamp
            pl.col("timestamp")
            .str.to_datetime("%Y-%m-%d %H:%M:%S")
            .alias("timestamp"),
        )
        .filter(
            pl.col("transaction_id").is_not_null(),
            pl.col("customer_id").is_not_null(),
            pl.col("amount").is_not_null(),
            pl.col("amount") > 0,
        )
        .unique(subset=["transaction_id"], keep="first")
        .sort("transaction_id")
    )

    # Report filtering statistics
    final_rows = cleaned.height
    removed_rows = initial_rows - final_rows
    if removed_rows > 0:
        removed_pct = (removed_rows / initial_rows) * 100
        print(f"  Removed {removed_rows} rows ({removed_pct:.1f}% of original)")

    return cleaned


def validate_customers(df: pl.DataFrame) -> dict:
    """Validate cleaned customer data and return quality metrics.

    Args:
        df: Cleaned customer DataFrame

    Returns:
        Dictionary with validation metrics
    """
    total_rows = df.height

    return {
        "total_customers": total_rows,
        "countries": sorted(df["country"].unique().to_list()),
        "country_distribution": df.group_by("country")
        .len()
        .sort("country")
        .to_dicts(),
        "signup_date_range": {
            "min": str(df["signup_date"].min()),
            "max": str(df["signup_date"].max()),
        },
        "null_emails": df["email"].null_count(),
        "duplicate_emails": total_rows - df["email"].n_unique(),
    }


def validate_transactions(df: pl.DataFrame) -> dict:
    """Validate cleaned transaction data and return quality metrics.

    Args:
        df: Cleaned transaction DataFrame

    Returns:
        Dictionary with validation metrics
    """
    return {
        "total_transactions": df.height,
        "unique_customers": df["customer_id"].n_unique(),
        "currencies": sorted(df["currency"].unique().to_list()),
        "currency_distribution": df.group_by("currency")
        .len()
        .sort("len", descending=True)
        .to_dicts(),
        "categories": sorted(df["category"].unique().to_list()),
        "category_distribution": df.group_by("category")
        .len()
        .sort("len", descending=True)
        .to_dicts(),
        "amount_stats": {
            "min": round(df["amount"].min(), 2),
            "max": round(df["amount"].max(), 2),
            "mean": round(df["amount"].mean(), 2),
            "median": round(df["amount"].median(), 2),
        },
        "timestamp_range": {
            "min": str(df["timestamp"].min()),
            "max": str(df["timestamp"].max()),
        },
        "na_currency_count": df.filter(pl.col("currency") == "NA").height,
        "na_category_count": df.filter(pl.col("category") == "NA").height,
    }


def infer_currency_from_country(
    transactions: pl.DataFrame, customers: pl.DataFrame
) -> pl.DataFrame:
    """Infer currency for transactions with NA currency based on customer country.

    Maps Nordic countries to their currencies:
    - DK (Denmark) -> DKK
    - SE (Sweden) -> SEK
    - NO (Norway) -> NOK
    - FI (Finland) -> EUR

    Args:
        transactions: Cleaned transactions DataFrame
        customers: Cleaned customers DataFrame with country information

    Returns:
        DataFrame with inferred currencies filled in
    """
    # Join transactions with customers to get country information
    transactions_with_country = transactions.join(
        customers.select(["customer_id", "country"]),
        on="customer_id",
        how="left",
    )

    # Infer currency for NA values based on country
    result = transactions_with_country.with_columns(
        pl.when(pl.col("currency") == "NA")
        .then(pl.col("country").replace_strict(config.COUNTRY_CURRENCY_MAP, default="NA"))
        .otherwise(pl.col("currency"))
        .alias("currency")
    ).drop("country")

    na_count_before = transactions.filter(pl.col("currency") == "NA").height
    na_count_after = result.filter(pl.col("currency") == "NA").height
    inferred_count = na_count_before - na_count_after
    
    if inferred_count > 0:
        print(f"  Inferred currency for {inferred_count} transactions based on customer country")

    return result


def remove_orphan_transactions(
    transactions: pl.DataFrame, customers: pl.DataFrame
) -> pl.DataFrame:
    """Filter out transactions that reference non-existent customers.

    Args:
        transactions: Cleaned transactions DataFrame
        customers: Cleaned customers DataFrame

    Returns:
        DataFrame of transactions with valid customer_id
    """
    valid_customer_ids = customers["customer_id"].unique().to_list()
    return transactions.filter(pl.col("customer_id").is_in(valid_customer_ids))


def add_amount_in_eur(transactions: pl.DataFrame) -> pl.DataFrame:
    """Add amount_in_eur column by converting amounts using constant exchange rates.

    Conversion rates to EUR (approximate):
    - EUR: 1.0 (no conversion)
    - DKK: 0.134 (Danish Krone)
    - SEK: 0.091 (Swedish Krona)
    - NOK: 0.088 (Norwegian Krone)

    Args:
        transactions: Cleaned transactions DataFrame with amount and currency columns

    Returns:
        DataFrame with added amount_in_eur column
    """
    # Add amount_in_eur column using the conversion rates
    result = transactions.with_columns(
        (pl.col("amount") * pl.col("currency").replace_strict(config.CONVERSION_RATES, default=None)).round(2)
        .alias("amount_in_eur")
    )

    return result

def run_etl(
    customers_path: Path,
    transactions_path: Path,
    output_dir: Path,
    infer_missing_currency: bool | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the complete ETL pipeline.

    Args:
        customers_path: Path to raw customers.csv
        transactions_path: Path to raw transactions.csv
        output_dir: Directory for cleaned output files
        infer_missing_currency: If True, infer currency from customer country for NA values

    Returns:
        Tuple of (cleaned_customers, cleaned_transactions)
    """
    # Use config default if not explicitly provided
    if infer_missing_currency is None:
        infer_missing_currency = config.INFER_MISSING_CURRENCY
    
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load raw data
    print("Loading raw data...")
    raw_customers = load_customers(customers_path)
    raw_transactions = load_transactions(transactions_path)
    print(f"  Loaded {raw_customers.height} customers")
    print(f"  Loaded {raw_transactions.height} transactions")

    # Clean data
    print("\nCleaning data, first step...")
    clean_cust = clean_customers(raw_customers)
    clean_txn = clean_transactions(raw_transactions)
    print(f"  Cleaned customers: {clean_cust.height} rows")
    print(f"  Cleaned transactions: {clean_txn.height} rows")

    # Validate
    print("\nValidating data...")
    cust_validation = validate_customers(clean_cust)
    txn_validation = validate_transactions(clean_txn)

    print(f"  Customer countries: {cust_validation['countries']}")
    print(f"  Transaction currencies: {txn_validation['currencies']}")
    print(f"  Transactions with NA currency: {txn_validation['na_currency_count']}")
    print(f"  Transactions with NA category: {txn_validation['na_category_count']}")

    # Cleaning step 2:
    # Infer missing currency from customer country if flag is set to True
    if infer_missing_currency:
        print("\nCleaning data, second step...")
        print("  Inferring missing currencies...")
        clean_txn = infer_currency_from_country(clean_txn, clean_cust)

        txn_validation = validate_transactions(clean_txn)
        print(f"  Number of transactions with NA currency after inference: {txn_validation['na_currency_count']}")

    # Remove orphan transactions that reference non-existent customers
    clean_txn = remove_orphan_transactions(clean_txn, clean_cust)
    if clean_txn.height < raw_transactions.height:
        print(
            f"  WARNING: {raw_transactions.height - clean_txn.height} transactions reference non-existent customers. These transactions have been removed."
        )

    # Add amount_in_eur column so the amounts are comparable across currencies
    print("\nAdding amount_in_eur column to transactions...")
    clean_txn = add_amount_in_eur(clean_txn)

    # Save cleaned data as CSV
    print("\nSaving cleaned data...")
    customers_output = output_dir / "customers_cleaned.csv"
    transactions_output = output_dir / "transactions_cleaned.csv"

    clean_cust.write_csv(customers_output)
    clean_txn.write_csv(transactions_output)
    print(f"  Saved: {customers_output}")
    print(f"  Saved: {transactions_output}")

    return clean_cust, clean_txn


if __name__ == "__main__":
    run_etl(
        customers_path=config.CUSTOMERS_FILE,
        transactions_path=config.TRANSACTIONS_FILE,
        output_dir=config.PROCESSED_DATA_DIR,
    )
