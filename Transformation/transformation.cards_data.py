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
df["card_brand"] = df["card_brand"].str.replace(" ", "", regex=False) # remove all spaces
df["card_brand"] = df["card_brand"].replace("unknown", "N/A")
df["card_brand"] = df["card_brand"].replace("-", "")

df["card_brand"] = df["card_brand"].replace("MASTERCARD", "Mastercard")
df["card_brand"] = df["card_brand"].replace("MasterCard", "Mastercard")

df["card_brand"] = df["card_brand"].replace("VISA", "Visa")
df["card_brand"] = df["card_brand"].replace("V", "Visa")
df["card_brand"] = df["card_brand"].replace("Vissa", "Visa")
df["card_brand"] = df["card_brand"].replace("VVisa", "Visa")
df["card_brand"] = df["card_brand"].replace("visa-card", "Visa")
df["card_brand"] = df["card_brand"].replace("V!sa", "Visa")
df["card_brand"] = df["card_brand"].replace("vis", "Visa")
df["card_brand"] = df["card_brand"].replace("Vis", "Visa")

df["card_brand"] = df["card_brand"].replace("Amex", "American Express")
df["card_brand"] = df["card_brand"].replace("amex", "American Express") 
df["card_brand"] = df["card_brand"].replace("AMEX", "American Express") 

df["card_type"] = df["card_type"].fillna("N/A")
df["card_type"] = df["card_type"].str.replace(" ", "", regex=False) # remove all spaces
df["card_type"] = df["card_type"].replace("unknown", "N/A")
df["card_type"] = df["card_type"].replace("-", "")

df["card_type"] = df["card_type"].replace("DB", "Debit") 
df["card_type"] = df["card_type"].replace("DEB", "Debit") 
df["card_type"] = df["card_type"].replace("D", "Debit")
df["card_type"] = df["card_type"].replace("Deibt", "Debit")
df["card_type"] = df["card_type"].replace("Debiit", "Debit") 
df["card_type"] = df["card_type"].replace("BankDebit", "Debit")
df["card_type"] = df["card_type"].replace("Debti", "Debit") 
df["card_type"] = df["card_type"].replace("DebitCard", "Debit")
df["card_type"] = df["card_type"].replace("Debit(Prepayed)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debit(Prepaid)Card", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debit(Prepaid)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debti(Prepaid)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debti(Prepiad)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("PrepaidDebit", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("debit(prepaid)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DEBIT(PREPAID)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DeBiT(PrePaid))", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debit(PREPAID)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DebitPrepaid", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debit(Prepiad)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DeBiT(PrePaid)", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Debit(Prepiad)", "Debit (Prepaid)")

df["card_type"] = df["card_type"].replace("DP", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DPP", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("PPD", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("DB-PP", "Debit (Prepaid)")
df["card_type"] = df["card_type"].replace("Prepaid", "Debit (Prepaid)")



df["card_type"] = df["card_type"].replace("CC", "Credit") 
df["card_type"] = df["card_type"].replace("CR", "Credit") 
df["card_type"] = df["card_type"].replace("cedit", "Credit")
df["card_type"] = df["card_type"].replace("Cedit", "Credit")
df["card_type"] = df["card_type"].replace("Credt", "Credit")
df["card_type"] = df["card_type"].replace("Crdeit", "Credit")
df["card_type"] = df["card_type"].replace("Card-Credit", "Credit")
df["card_type"] = df["card_type"].replace("CreditCard", "Credit") 
df["card_type"] = df["card_type"].replace("CRED", "Credit") 
















df["card_type"] = df["card_type"].fillna("N/A")

df = df.drop_duplicates(subset=['card_number'], keep='last')
df["credit_limit"] = df["credit_limit"].fillna(0.0) # missing becomes 0
df["issuer_bank_name"] = df["issuer_bank_name"].str.lstrip() # remove emtpy spaces at start of bank names

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
