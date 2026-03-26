import pyodbc
import pandas as pd

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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.transactions_data", con=conn)

# ---------------
# clean data
# ---------------

# df = df.where(df.notna(), other=None)
											
# column 1 id
# column 2 date
# column 3 client_id
# column 4 card_id
# column 5 amount
def parse_amount(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    val = str(val).strip().replace("$", "").replace(",", "")
    try:
        return float(val)
    except ValueError:
        return 0.0
# column 6 use_chip
# column 7 merchant_id
# column 8 merchant_city
# column 9 merchant_state
df["merchant_state"] = df["merchant_state"].fillna("N/A")
# column 10 zip
df["zip"] = df["zip"].fillna("N/A")
# column 11 mcc 
# column 12 errors
df["errors"] = df["errors"].fillna("N/A")

# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.transactions_data")
cursor.execute("""
    CREATE TABLE transformation.transactions_data (
    id              INT,
    date            VARCHAR(50),
    client_id       INT,
    card_id         INT,
    amount          DECIMAL(18,2),
    use_chip        VARCHAR(50),
    merchant_id     INT,
    merchant_city   VARCHAR(100),
    merchant_state  VARCHAR(50),
    zip             VARCHAR(10),
    mcc             INT,
    errors          VARCHAR(255)
    )
""")

# Keep only the columns matching the target table
df = df[["id", "date", "client_id", "card_id", "amount", "use_chip", "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors"]]

# Cast numeric columns (ingestion stores everything as strings)
for col in ["id", "client_id", "card_id", "merchant_id", "mcc"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
df["amount"] = df["amount"].apply(parse_amount)

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
total = len(df)
batch_size = 10000
last_pct = -1
rows_inserted = 0

import math
records = [
    [None if (v is None or (isinstance(v, float) and math.isnan(v))) else v for v in row]
    for row in df.values.tolist()
]

for i in range(0, total, batch_size):
    batch = records[i:i + batch_size]
    cursor.executemany(f"INSERT INTO transformation.transactions_data VALUES ({placeholders})", batch)
    rows_inserted += len(batch)

    pct = int(rows_inserted / total * 100)
    if pct != last_pct:
        print(f"Progress: {pct}% ({rows_inserted}/{total})", flush=True)
        last_pct = pct

print(f"Loaded {total} cleaned rows into transformation.transactions_data")

cursor.close()
conn.close()
