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
cursor.fast_executemany = True

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

# Select only needed columns upfront — avoids loading unused data
df = pd.read_sql_query("""
    SELECT id, [date], client_id, card_id, amount, use_chip,
           merchant_id, merchant_city, merchant_state, zip, mcc, errors
    FROM ingestion.transactions_data
""", conn)

# ---------------
# clean data
# ---------------

# Remove duplicates first to reduce work on remaining transforms
before = len(df)
df = df.drop_duplicates()
removed = before - len(df)
if removed > 0:
    print(f"Removed {removed} duplicate row(s). {len(df)} rows remaining.")
else:
    print("No duplicate rows found.")

# column 1 id / column 3 client_id / column 4 card_id / column 7 merchant_id / column 11 mcc
for col in ["id", "client_id", "card_id", "merchant_id", "mcc"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.dropna(subset=["id"])
df["id"] = df["id"].astype(int)

# column 2 date
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

# column 5 amount
df["amount"] = (
    df["amount"]
    .astype(str).str.strip()
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

# column 6 use_chip
use_chip_map = {
    "Swipe Transaction": "In-Store",
    "Online Transaction": "Online",
    "Chip Transaction": "In-Store",
    "Chip Card Transaction": "In-Store",
    "Swipe": "In-Store",
    "Online": "Online",
}
df["use_chip"] = df["use_chip"].str.strip().replace(use_chip_map).fillna("Unknown")

# column 8 merchant_city
df["merchant_city"] = (df["merchant_city"].str.strip() 
.str.replace(r"\s+", " ", regex=True).replace("ONLINE", "Online")
)
# column 9 merchant_state
df["merchant_state"] = df["merchant_state"].fillna("N/A").replace("", "N/A").str.strip()
# column 10 zip
df["zip"] = df["zip"].fillna("N/A").replace("", "N/A")
mask = df["zip"] != "N/A"
df.loc[mask, "zip"] = df.loc[mask, "zip"].astype(str).str.strip().str.replace(r"\.\d+$", "", regex=True)
# column 12 errors
df["errors"] = df["errors"].fillna("N/A").replace("", "N/A").str.strip()

# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.transactions_data")
cursor.execute("""
    CREATE TABLE transformation.transactions_data (
    id              INT,
    date            DATE,
    client_id       INT,
    card_id         INT,
    amount          DECIMAL(18,2),
    use_chip        VARCHAR(20),
    merchant_id     INT,
    merchant_city   VARCHAR(100),
    merchant_state  VARCHAR(50),
    zip             VARCHAR(10),
    mcc             INT,
    errors          VARCHAR(255)
    )
""")

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
        print(f"Progress: {pct}% ({rows_inserted:,}/{total:,})", flush=True)
        last_pct = pct

print(f"Loaded {total:,} cleaned rows into transformation.transactions_data")

cursor.close()
conn.close()
