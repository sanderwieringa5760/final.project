import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

#--------------------------
# READ SQL
#--------------------------


conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="final_project",
    user="postgres",
    password="aaaa"
)

cur = conn.cursor()

# -----------------------------------
# Get date range from transactions — use SQL to avoid loading all rows
# -----------------------------------
result = pd.read_sql("""
    SELECT
        CAST(MIN(date) AS DATE) AS min_date,
        CAST(MAX(date) AS DATE) AS max_date
    FROM transformation.transactions_data
    WHERE date IS NOT NULL
""", con=conn)

min_date = result["min_date"].iloc[0]
max_date = result["max_date"].iloc[0]

print(f"Transaction date range: {min_date} → {max_date}")

# -----------------------------------
# Generate one row per calendar day in the range
# -----------------------------------
date_range = pd.date_range(start=min_date, end=max_date, freq="D")

dim_date = pd.DataFrame({
    "full_date":   date_range.date,              # Python date (no time component)
    "year":        date_range.year,
    "quarter":     date_range.quarter,
    "month":       date_range.month,
    "month_name":  date_range.strftime("%B"),    # January, February, ...
    "day":         date_range.day,
    "day_of_week": date_range.dayofweek + 1,     # 1=Monday, 7=Sunday
    "day_name":    date_range.strftime("%A"),    # Monday, Tuesday, ...
    "is_weekend":  (date_range.dayofweek >= 5).astype(int),  # 1 if Sat/Sun
})

dim_date.insert(0, "date_key", dim_date.index + 1)

# Convert full_date to proper datetime for SQL Server compatibility
dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

print(f"dim_date: {len(dim_date)} rows")
print(dim_date.head())
#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/final_project"
)

dim_date.to_sql(
    name="dim_date",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into curated.dim_date")

cur.close()
conn.close()