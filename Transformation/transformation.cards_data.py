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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.cards_data", con=conn)

# ---------------
# clean data
# ---------------

# df = df.where(df.notna(), other=None)
df["card_brand"] = df["card_brand"].fillna("N/A")
df["card_type"] = df["card_type"].fillna("N/A")
df["credit_limit"] = df["credit_limit"].fillna(0.0)

def parse_credit_limit(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    val = str(val).strip().replace("$", "").replace(",", "")
    if val.lower().endswith("k"):
        try:
            return float(val[:-1]) * 1_000
        except ValueError:
            pass
    if val.lower().endswith("m"):
        try:
            return float(val[:-1]) * 1_000_000
        except ValueError:
            pass
    try:
        return float(val)
    except ValueError:
        pass
    try:
        return float(w2n.word_to_num(val))
    except Exception:
        return 0.0

df["credit_limit"] = df["credit_limit"].apply(parse_credit_limit)

# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.cards_data")
cursor.execute("""
    CREATE TABLE transformation.cards_data (
    id                      INT,
    client_id               INT,
    card_brand              VARCHAR(50),
    card_type               VARCHAR(50),
    card_number             VARCHAR(50),
    expires                 VARCHAR(10),
    cvv                     INT,
    has_chip                VARCHAR(5),
    num_cards_issued        INT,
    credit_limit            DECIMAL(18,2),
    acct_open_date          VARCHAR(20),
    year_pin_last_changed   INT,
    card_on_dark_web        VARCHAR(5),
    issuer_bank_name        VARCHAR(100),
    issuer_bank_state       VARCHAR(50),
    issuer_bank_type        VARCHAR(50),
    issuer_risk_rating      VARCHAR(20)    )
""")

# Keep only the columns matching the target table
df = df[["id", "client_id", "card_brand", "card_type", "card_number", "expires", "cvv", "has_chip", "num_cards_issued", "credit_limit", "acct_open_date", "year_pin_last_changed", "card_on_dark_web", "issuer_bank_name", "issuer_bank_state", "issuer_bank_type", "issuer_risk_rating"]]

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.cards_data VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.cards_data")

cursor.close()
conn.close()
