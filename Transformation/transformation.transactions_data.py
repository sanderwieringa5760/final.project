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
cursor.fast_executemany = True  # critical for bulk insert speed

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
df = pd.read_sql_query("SELECT * FROM ingestion.transactions_data", conn)

# ---------------
# clean data
# ---------------

# remove fully duplicate rows (all columns must match)
before = len(df)
df = df.drop_duplicates()
removed = before - len(df)
if removed > 0:
    print(f"Removed {removed} duplicate row(s). {len(df)} rows remaining.")
else:
    print("No duplicate rows found.")

# column 1 id
# column 2 date
# column 3 client_id
# column 4 card_id
# column 5 amount
# Vectorized amount parsing (replaces slow row-by-row .apply())
df["amount"] = (
    df["amount"]
    .astype(str)
    .str.strip()
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
# colums 6 use_chip
# column 7 merchant_id
df["merchant_id"] = df["merchant_id"].astype(str).str.zfill(7)
# column 8 merchant_city
df["merchant_city"] = df["merchant_city"].replace("ONLINE", "Online") 
# column 9 merchant_state
df["merchant_state"] = df["merchant_state"].fillna("N/A").replace("", "N/A")
# column 10 zip
df["zip"] = df["zip"].fillna("N/A").replace("", "N/A")
df["zip"] = df["zip"].astype(str).str.replace(".0", "", regex=False)
# column 11 mcc
# column 12 errors
df["errors"] = df["errors"].fillna("N/A").replace("", "N/A")

# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.transactions_data")
cursor.execute("""
    CREATE TABLE transformation.transactions_data (
    id              INT,
    date            VARCHAR(20),
    client_id       INT,
    card_id         INT,
    amount          DECIMAL(18,2),
    use_chip        VARCHAR(30),
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
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Convert NaN → None in one vectorized pass (replaces slow row-by-row list comprehension)
records = df.where(df.notna(), other=None).values.tolist()

total = len(records)
batch_size = 50000
placeholders = ",".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.transactions_data VALUES ({placeholders})"

rows_inserted = 0
last_pct = -1

for i in range(0, total, batch_size):
    batch = records[i:i + batch_size]
    cursor.executemany(insert_sql, batch)
    rows_inserted += len(batch)
    pct = int(rows_inserted / total * 100)
    if pct != last_pct:
        print(f"Progress: {pct}% ({rows_inserted}/{total})", flush=True)
        last_pct = pct

print(f"Loaded {total} cleaned rows into transformation.transactions_data")

cursor.close()
conn.close()
