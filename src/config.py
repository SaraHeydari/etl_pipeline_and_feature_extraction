"""
Configuration file for the Nordic Data Pipeline.

This module centralizes all configuration parameters including:
- Data paths
- Currency mappings and conversion rates
- Feature engineering thresholds
- ETL pipeline settings
"""

from pathlib import Path

# =============================================================================
# DATA PATHS
# =============================================================================

# Base directory (project root)
BASE_DIR = Path(__file__).parent.parent

# Raw data directory
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# Processed data directory
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"

# Input file paths
CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
TRANSACTIONS_FILE = RAW_DATA_DIR / "transactions.csv"

# Output file paths
CUSTOMERS_CLEANED_FILE = PROCESSED_DATA_DIR / "customers_cleaned.csv"
TRANSACTIONS_CLEANED_FILE = PROCESSED_DATA_DIR / "transactions_cleaned.csv"
CUSTOMER_FEATURES_FILE = PROCESSED_DATA_DIR / "customer_features.csv"


# =============================================================================
# ETL SETTINGS
# =============================================================================

# Valid Nordic country codes
VALID_COUNTRIES = {"DK", "FI", "SE", "NO"}

# Country to currency mapping
COUNTRY_CURRENCY_MAP = {
    "DK": "DKK",  # Denmark -> Danish Krone
    "SE": "SEK",  # Sweden -> Swedish Krona
    "NO": "NOK",  # Norway -> Norwegian Krone
    "FI": "EUR",  # Finland -> Euro
}

# Currency conversion rates to EUR (approximate, should be updated with real-time rates in production)
CONVERSION_RATES = {
    "EUR": 1.0,    # Euro (base currency)
    "DKK": 0.134,  # Danish Krone
    "SEK": 0.094,  # Swedish Krona
    "NOK": 0.087,  # Norwegian Krone
}

# Whether to infer missing currency from customer country
INFER_MISSING_CURRENCY = True


# =============================================================================
# FEATURE ENGINEERING SETTINGS
# =============================================================================

# High-value customer threshold (percentile)
HIGH_VALUE_PERCENTILE = 0.80  # Top 20%

# Churn detection threshold (days of inactivity)
CHURN_DAYS = 50  # No activity in 50+ days considered churning

# Z-score threshold for personalized churn detection
CHURN_Z_SCORE_THRESHOLD = 2  # Number of standard deviations above mean interevent time
