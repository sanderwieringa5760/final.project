# ClearSpend Data Platform

A data engineering pipeline built for ClearSpend, a fintech company that processes credit and debit card transactions across the US. The goal was to take four messy raw CSV exports and turn them into a clean, queryable data warehouse that finance, customer, and merchant teams can actually use.



## Requirements

- Python 3.8+
- SQL Server Express (the scripts connect to `.\SQLEXPRESS`)
- ODBC Driver 17 for SQL Server

Install Python packages:

```
pip install pyodbc pandas sqlalchemy word2number
```



## The datasets

The pipeline takes four raw CSV files as input:

- `cards_data.csv` — Cards linked to customers: brand, type, credit limit, issuer info
- `users_data.csv` — Customer demographics: age, income, debt, employment
- `transactions_data.csv` — Every card transaction: amount, merchant, date, chip usage
- `mcc_data.csv` — Merchant Category Code reference: maps MCC codes to industry descriptions



## How to run it

### Option A — Run the entire pipeline at once (recommended)

```
python pipeline.py
```

Runs all 15 scripts in the correct order. At the start it asks:
- `t` — test mode: loads only 1000 transactions (fast, for verifying everything works)
- `f` — full mode: loads the entire dataset

If any script fails, the pipeline stops immediately so broken data does not flow into the next layer.

### Option B — Run each step manually

Each step depends on the previous one completing successfully.

**Step 1 — Ingestion**
```
python Ingestion/ingestion.ddl.py       # create database + tables
python Ingestion/ingestion.load.py      # load CSVs into ingestion schema
```
Creates the database and loads the four raw CSVs into the `ingestion` schema as-is.

**Step 2 — Transformation**
```
python Transformation/transformation.mcc_data.py
python Transformation/transformation.cards_data.py
python Transformation/transformation.users_data.py
python Transformation/transformation.transactions_data.py
```
Cleans each dataset and writes the result to the `transformation` schema.

**Step 3 — Curated (data warehouse)**
```
python Curated/curated_dim_mcc.py
python Curated/curated_dim_customer.py
python Curated/curated_dim_cards.py
python Curated/curated_dim_date.py
python Curated/curated_dim_merchants.py
python Curated/curated_dim_fact_transactions.py   # must run last
```
Builds the star schema inside the `curated` schema. All dimensions must be loaded before the fact table.

**Step 4 — Marts**
```
python Marts/mart_finance.py
python Marts/mart_customer.py
python Marts/mart_merchant.py
```
Creates the aggregated tables in the `mart` schema for each business team.

### Mart Governance & Query Guide (optional)

```
python Marts/mart_governance.py
```
A CLI tool that helps you navigate the mart tables. Select your team and a business question, and it tells you which mart table to query, which columns to focus on, how to interpret the results, and gives you a ready-to-run SQL query.



## What got cleaned

The raw data had a lot of issues. Here is what was fixed in the transformation layer:

- **Amounts and currency** — dollar signs, commas, shorthand like `$24k` all converted to plain decimals
- **Card brand/type** — dozens of misspellings and abbreviations normalised (e.g. `V`, `Vissa`, `V!sa` → `Visa`)
- **Employment status** — messy values like `Empl0yed`, `Un-employed`, `Studnt` corrected
- **Education level** — variants like `Bachelor's Degree`, `BA/BS`, `Bachelors` all unified
- **Dates** — expiry dates and account open dates parsed from `Mon-YY` format into `MM-YYYY`
- **Bank names** — abbreviations like `Bk of America`, `Chase Bk` expanded to full names
- **States** — full state names converted to two-letter codes
- **Duplicates** — exact duplicate rows removed; duplicate card numbers deduplicated
- **Nulls** — missing merchant state, zip, and error fields filled with `N/A`



## Database structure

All data lives in one SQL Server database called `final_project`, split across four schemas:

| Schema | Purpose |
|---|---|
| `ingestion` | Raw data exactly as it came from the CSVs |
| `transformation` | Cleaned and typed version of the same data |
| `curated` | Star schema — one fact table joined to five dimension tables |
| `mart` | Pre-aggregated tables built for specific business questions |

### Star schema (curated layer)

The fact table is `curated.fact_transactions`. Each row is one transaction, with foreign keys pointing to:

- `dim_customer` — who made the transaction
- `dim_cards` — which card was used
- `dim_merchants` — where it happened
- `dim_mcc` — what industry/category
- `dim_date` — when it happened

### Marts

**Finance** (`mart_finance.py`)
- `mart.finance_monthly` — revenue and refund rate by month
- `mart.finance_by_state` — revenue broken down by state
- `mart.finance_by_category` — revenue by merchant category

**Customer analytics** (`mart_customer.py`)
- `mart.customer_ltv` — lifetime spend per customer
- `mart.customer_channel` — online vs in-store behaviour
- `mart.customer_cards` — how many cards each customer has
- `mart.suspicious_transactions` — flagged transactions (errors, dark web cards, high-value outliers)

**Merchant partnerships** (`mart_merchant.py`)
- `mart.merchant_performance` — transaction volume and revenue per merchant
- `mart.industry_growth` — revenue by industry per year
- `mart.merchant_errors` — error rates per merchant
- `mart.revenue_by_geography` — revenue by state and city
