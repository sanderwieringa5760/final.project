import pyodbc
import pandas as pd
import urllib
from sqlalchemy import create_engine

#--------------------------
# READ SQL
#--------------------------
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
cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

dim_mcc.to_sql(
    name="dim_mcc",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_mcc loaded into curated.dim_mcc")

engine.dispose()
cur.close()
conn.close()