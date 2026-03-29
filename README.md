# ClearSpend Data Platform

A data engineering pipeline built for ClearSpend, a fintech company that processes credit and debit card transactions across the US. The goal was to take four messy raw CSV exports and turn them into a clean, queryable data warehouse that finance, customer, and merchant teams can actually use.



## What this project does

Raw data comes in → gets loaded into SQL Server → gets cleaned → gets modelled into a star schema → gets summarised into business-ready tables.


CSV files
   ↓
Ingestion  (raw data, no changes)
   ↓
Transformation  (cleaning, standardising, type casting)
   ↓
Curated  (star schema: facts + dimensions)
   ↓
Marts  (aggregated tables per business team)




## Requirements

- Python 3.8+
- SQL Server Express (the scripts connect to `.\SQLEXPRESS`)
- ODBC Driver 17 for SQL Server

Python packages:

pip install pyodbc pandas sqlalchemy word2number




## How to run it

Run the scripts in this order. Each step depends on the previous one.

**Step 1 — Ingestion**

python Ingestion/ingestion.ddl.py
python Ingestion/ingestion.load.py

Creates the database and loads the four raw CSVs into the `ingestion` schema as-is.

**Step 2 — Transformation**

python Transformation/transformation.cards_data.py
(Cleans "cards_data.csv")

python Transformation/transformation.users_data.py
(Cleans "users_data.csv")

python Transformation/transformation.transactions_data.py
(Cleans "transaction_data.csv")

python Transformation/transformation.mcc_data.py
(Cleans "mcc_data.csv")

Cleans each dataset and writes the result to the `transformation` schema.

**Step 3 — Curated (data warehouse)**

python Curated/curated_dim_customer.py (costumer dimension)
python Curated/curated_dim_cards.py (cards dimension)
python Curated/curated_dim_mcc.py (mcc dimension)
python Curated/curated_dim_merchants.py (merchants dimension)
python Curated/curated_dim_date.py (date dimension)
python Curated/curated_dim_fact_transactions.py (fact transaction dimension)

Builds the star schema inside the `curated` schema. The dimensions need to be loaded before the fact table.

**Step 4 — Marts**

python Marts/mart_finance.py
python Marts/mart_customer.py
python Marts/mart_merchant.py

Creates the aggregated tables in the `mart` schema for each business team.



## The datasets

Here is what each file in the dataset contains

`cards_data.csv`: Cards linked to customers — brand, type, credit limit, issuer info
`users_data.csv`: Customer demographics — age, income, debt, employment
`transactions_data.csv`: Every card transaction — amount, merchant, date, chip usage
`mcc_data.csv`: Merchant Category Code reference — maps MCC codes to industry descriptions



## Database structure

All data lives in one SQL Server database called `final_project`, split across four schemas:

| Schema | Purpose |

`ingestion`: Raw data exactly as it came from the CSVs
`transformation`: Cleaned and typed version of the same data
`curated`: Star schema — one fact table joined to five dimension tables
`mart`: Pre-aggregated tables built for specific business questions

### Star schema (curated layer)

The fact table is `curated.dim_fact_transactions`. Each row is one transaction, with foreign keys pointing to:

`dim_customer`: who made the transaction
`dim_cards`: which card was used
`dim_merchants`: where it happened
`dim_mcc`: what industry/category
`dim_date`: when it happened

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
