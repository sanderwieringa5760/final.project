import pyodbc
import pandas as pd
from word2number import w2n

conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()

# ---------------
# get data
# ---------------

# Create transformation schema if it doesn't exist
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'transformation')
    BEGIN
        EXEC('CREATE SCHEMA transformation');
    END
""")

# Read from ingestion layer
df = pd.read_sql_query(sql="SELECT * FROM ingestion.mcc_data", con=conn)

# ---------------
# clean data
# ---------------

# df = df.where(df.notna(), other=None)
# df["card_brand"] = df["card_brand"].fillna("N/A")
# df["card_type"] = df["card_type"].fillna("N/A")
# df["credit_limit"] = df["credit_limit"].fillna(0.0)
df["code"] = df["code"].str.replace('"', '') # make normal numbers, remove "
df["code"] = df["code"].str.replace('MCC', '') # remove MCC
df = df.iloc[:-2] # remove final rows of comments
df["description"] = df["description"].str.lstrip()
df["notes"] = df["notes"].fillna("N/A")
df["updated_by"] = df["updated_by"].fillna("N/A")




# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.mcc_data")
cursor.execute("""
    CREATE TABLE transformation.mcc_data (
    code        VARCHAR(50),
    description VARCHAR(255),
    notes       VARCHAR(500),
    updated_by  VARCHAR(100)    )   
""")

# Keep only the columns matching the target table
df = df[["code", "description", "notes", "updated_by"]]

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.mcc_data VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.mcc_data")

cursor.close()
conn.close()
