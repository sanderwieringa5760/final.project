import psycopg2
import pandas as pd
from sqlalchemy import create_engine

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

df = pd.read_sql("SELECT code, description FROM transformation.mcc_data;", con=conn)

df["code"] = pd.to_numeric(df["code"], errors="coerce")
df = df.dropna(subset=["code"])
df["code"] = df["code"].astype(int)

# keep one row per mcc_code
df = df.drop_duplicates(subset=["code"], keep="first")

#--------------------------
# Build dim_mcc
#--------------------------
dim_mcc = pd.DataFrame({
    "mcc_code": df["code"],
    "description": df["description"],
})

dim_mcc = dim_mcc.sort_values("mcc_code").reset_index(drop=True)
dim_mcc.insert(0, "mcc_key", dim_mcc.index + 1)

print(f"dim_mcc: {len(dim_mcc)} rows")
print(dim_mcc.head())

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/final_project"
)

dim_mcc.to_sql(
    name="dim_mcc",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_mcc loaded into curated.dim_mcc")

cur.close()
conn.close()