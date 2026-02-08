# Nordic Data & AI Pipeline

A data engineering and AI pipeline for Nordic customer analytics, built as part of a consulting assignment.

## Overview

This project demonstrates:
1. **ETL Pipeline** - Cleaning and standardizing customer and transaction data
2. **Feature Engineering** - Computing RFM metrics and business flags

## Quick Start

### Prerequisites

- Python 3.11+
- [UV](https://docs.astral.sh/uv/) package manager

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd nordic-data-pipeline

# Install dependencies with UV
uv sync

# Install dev dependencies (for linting and testing)
uv sync --all-extras
```

### Prepare Data

Place your raw data files in the `data/raw/` directory:
```
data/raw/
├── customers.csv
└── transactions.csv
```

### Running the Pipeline

```bash
# Run the full pipeline (ETL + Feature Engineering)
uv run python src/main.py

# Run individual steps (for development/debugging)
uv run python src/etl.py
uv run python src/features.py
```

### Development Commands

```bash
# Lint code (check for issues)
uv run ruff check .

# Auto-fix lint issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Run both lint and format
uv run ruff check --fix . && uv run ruff format .

# Run tests
uv run pytest
```

## Project Structure

```
nordic-data-pipeline/
├── src/
│   ├── config.py           # Configuration settings
│   ├── etl.py              # Data cleaning functions
│   ├── features.py         # Feature engineering
│   └── main.py             # Pipeline orchestration
├── data/
│   ├── raw/                # Input: original CSV files
│   └── processed/          # Output: cleaned CSV files
├── tests/
│   └── test_etl.py         # Unit tests
├── pyproject.toml          # Dependencies & Ruff config
└── README.md
```

## Data Pipeline

### Input Data

| File | Description | Records |
|------|-------------|---------|
| `customers.csv` | Customer master data | ~5,000 |
| `transactions.csv` | Transaction history | ~50,000 |

### Data Quality Issues Addressed

**For customers.csv**
- Standardize country codes to uppercase
- Standardize email addresses to Lowercase
- Remove rows with null customer_id
- Only kepp rows with valid country codes (the 4 Nordic countries)
- Remove duplicate customer_ids (keep the first entry)

**For transactions.csv**
- Standardize currency to uppercase, fill nulls with "NA"
- Standardize category to lowercase, fill nulls/empty with "NA"
- Remove rows with invalid amounts (null or <= 0)
- Remove rows with null customer_id or null transaction_id
- Remove rows with duplicate transaction_ids (keep first)
- Remove orphan transactions (transactions with customer IDs that do not exist in preprocessed customers list)
- Optionally, infer currency for transactions with NA currency based on customer country (configurable via `config.py`).
- For comparability, add amount_in_eur column which includes the transaction values in euros. Conversion rates are defined in `config.py` (should be replaced with time-dependent rates in production)

### Output Files

| File | Description |
|------|-------------|
| `customers_cleaned.csv` | Cleaned customer data |
| `transactions_cleaned.csv` | Cleaned transaction data |
| `customer_features.csv` | RFM features and flags per customer |

### Customer Features

| Feature | Description | Business Use |
|---------|-------------|--------------|
| `total_spend` | Sum of transaction amounts (EUR) | Customer value |
| `transaction_count` | Number of transactions | Engagement |
| `avg_transaction_amount` | Mean transaction value (EUR) | Spending pattern |
| `std_transaction_amount` | Standard deviation of transaction amounts (EUR) | Spending consistency |
| `min_transaction_amount` | Minimum transaction amount | Spending range |
| `max_transaction_amount` | Maximum transaction amount | Spending range |
| `first_transaction_date` | Date of first transaction | Customer age |
| `last_transaction_date` | Date of most recent transaction | Recency |
| `days_since_last_transaction` | Days since last activity | Churn risk |
| `customer_tenure_days` | Days between first and last transaction | Customer lifetime |
| `mean_interevent_days` | Average days between consecutive transactions | Transaction frequency pattern |
| `std_interevent_days` | Standard deviation of interevent times | Transaction regularity |
| `preferred_category` | Most frequent category | Personalization |
| `preferred_currency` | Most used currency | Regional preference |
| `is_high_value` | Top 20% by spend | VIP identification |
| `is_churning` | No activity in 50+ days | Retention targeting |
| `is_churning_2` | Inactivity > mean + 2σ of interevent times | Personalized churn detection |
| `has_single_transaction` | Customer made only one transaction | Onboarding opportunity |

## Configuration

All pipeline parameters are centralized in `src/config.py`:

**Paths:**
- Input/output directories
- File names for raw and processed data

**ETL Settings:**
- Valid country codes (DK, FI, SE, NO)
- Country-to-currency mapping
- Currency conversion rates to EUR
- Flag to enable/disable currency inference

**Feature Engineering:**
- High-value percentile threshold (default: 0.80 = top 20%)
- Churn detection threshold (default: 50 days)
- Z-score threshold for personalized churn (default: 2 standard deviations)

To modify behavior, edit values in `config.py` rather than changing code.

## Key Assumptions

1. **Missing values**: Currency and category nulls are marked as "NA" (not filtered out)
2. **Reference date**: Recency calculated from the max transaction date in the data
3. **High-value threshold**: 80th percentile of total spend (configurable)
4. **Churn threshold (simple)**: 50 days without activity (configurable)
5. **Churn threshold (personalized)**: Time since last transaction > mean + 2σ of interevent times (configurable)
6. **Valid countries**: Only Nordic countries (DK, FI, SE, NO)


## What I Would Improve with More Time:

1. **Data validation** - Add schema validation with Pandera or Pydantic
2. **Logging** - Structured logging instead of print statements
3. **Incremental processing** - Support for incremental loads
4. **More tests** - Edge cases, integration tests
5. **Time-dependent exchange rates** - Replace static rates with API or historical data

## Technologies

- **Polars** - Fast DataFrame library (Rust-based)
- **Ruff** - Fast Python linter and formatter
- **UV** - Fast Python package manager
- **Pytest** - Testing framework
