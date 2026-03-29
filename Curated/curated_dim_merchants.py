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


# Load source from transformation layer

df = pd.read_sql("""
    SELECT DISTINCT
        merchant_id,
        merchant_city,
        merchant_state,
        zip
    FROM transformation.transactions_data
    WHERE merchant_id IS NOT NULL
""", con=conn)


# Build dim_merchants

dim_merchants = pd.DataFrame({
    "merchant_id":    df["merchant_id"],
    "merchant_city":  df["merchant_city"],
    "merchant_state": df["merchant_state"],
    "zip":            df["zip"],
})

dim_merchants = dim_merchants.drop_duplicates(
    subset=["merchant_id", "merchant_city", "merchant_state"], keep="first"
).sort_values(
    ["merchant_id", "merchant_city", "merchant_state"]
).reset_index(drop=True)

dim_merchants.insert(0, "merchant_key", dim_merchants.index + 1)

print(f"dim_merchants: {len(dim_merchants)} rows")
print(dim_merchants.head())


# WRITE TO SQL

cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

dim_merchants.to_sql(
    name="dim_merchants",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_merchants loaded into curated.dim_merchants")

engine.dispose()
cur.close()
conn.close()
