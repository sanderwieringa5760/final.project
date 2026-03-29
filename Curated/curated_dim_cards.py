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

df = pd.read_sql("SELECT * FROM transformation.cards_data;", con=conn)


# Build dim_cards
# Natural key: card_id
# client_id links to dim_customer.customer_id

dim_cards = pd.DataFrame({
    "card_id":                df["id"],
    "client_id":              df["client_id"],
    "card_brand":             df["card_brand"],
    "card_type":              df["card_type"],
    "card_number":            df["card_number"],
    "expires":                df["expires"],
    "cvv":                    df["cvv"],
    "has_chip":               df["has_chip"],
    "num_cards_issued":       df["num_cards_issued"],
    "credit_limit":           df["credit_limit"],
    "acct_open_date":         df["acct_open_date"],
    "year_pin_last_changed":  df["year_pin_last_changed"],
    "card_on_dark_web":       df["card_on_dark_web"],
    "issuer_bank_name":       df["issuer_bank_name"],
    "issuer_bank_state":      df["issuer_bank_state"],
    "issuer_bank_type":       df["issuer_bank_type"],
    "issuer_risk_rating":     df["issuer_risk_rating"],
})

dim_cards = dim_cards.sort_values("card_id").reset_index(drop=True)
dim_cards.insert(0, "card_key", dim_cards.index + 1)

print(f"dim_cards: {len(dim_cards)} rows")
print(dim_cards.head())


# WRITE TO SQL

cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

dim_cards.to_sql(
    name="dim_cards",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_cards loaded into curated.dim_cards")

engine.dispose()
cur.close()
conn.close()
