import pyodbc
import pandas as pd
import urllib
from sqlalchemy import create_engine


# READ SQL

conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=final_project;"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

cur = conn.cursor()


# Get date range from transactions — use SQL to avoid loading all rows

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


# Generate one row per calendar day in the range

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
    "week_of_year": date_range.isocalendar().week.values,    # ISO week 1-53
    "is_weekend":  (date_range.dayofweek >= 5).astype(int),  # 1 if Sat/Sun
})

dim_date.insert(0, "date_key", dim_date.index + 1)

# Convert full_date to proper datetime for SQL Server compatibility
dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

print(f"dim_date: {len(dim_date)} rows")
print(dim_date.head())

# WRITE TO SQL

cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

dim_date.to_sql(
    name="dim_date",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("Cleaned data loaded into curated.dim_date")

engine.dispose()
cur.close()
conn.close()
