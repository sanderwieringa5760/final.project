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

#--------------------------
# Load source from transformation layer
#--------------------------
df = pd.read_sql("""
    SELECT DISTINCT
        merchant_id,
        merchant_city,
        merchant_state
    FROM transformation.transactions_data
    WHERE merchant_id IS NOT NULL
""", con=conn)

#--------------------------
# Build dim_merchants
#--------------------------
dim_merchants = pd.DataFrame({
    "merchant_id":    df["merchant_id"],
    "merchant_city":  df["merchant_city"],
    "merchant_state": df["merchant_state"],
})

dim_merchants = dim_merchants.sort_values(
    ["merchant_id", "merchant_city", "merchant_state"]
).reset_index(drop=True)

dim_merchants.insert(0, "merchant_key", dim_merchants.index + 1)

print(f"dim_merchants: {len(dim_merchants)} rows")
print(dim_merchants.head())

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/final_project"
)

dim_merchants.to_sql(
    name="dim_merchants",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_merchants loaded into curated.dim_merchants")

cur.close()
conn.close()