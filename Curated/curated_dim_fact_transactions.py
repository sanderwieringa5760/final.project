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

CHUNK_SIZE = 200000

# Load dimension tables
dim_customers = pd.read_sql("""
    SELECT customer_key, customer_id
    FROM curated.dim_customer
""", con=conn)

dim_cards = pd.read_sql("""
    SELECT card_key, card_id
    FROM curated.dim_cards
""", con=conn)

dim_merchants = pd.read_sql("""
    SELECT merchant_key, merchant_id, merchant_city, merchant_state
    FROM curated.dim_merchants
""", con=conn)

dim_date = pd.read_sql("""
    SELECT date_key, full_date
    FROM curated.dim_date
""", con=conn)

dim_mcc = pd.read_sql("""
    SELECT mcc_key, mcc_code
    FROM curated.dim_mcc
""", con=conn)

#--------------------------
# DATA CLEANING / BUILD STEPS
#--------------------------
dim_date["full_date"] = pd.to_datetime(dim_date["full_date"]).dt.date

total_rows = pd.read_sql("""
    SELECT COUNT(*) AS cnt
    FROM transformation.transactions_data
""", con=conn)["cnt"].iloc[0]

print(f"Total transaction rows: {total_rows:,}")

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

cur.execute("DROP TABLE IF EXISTS curated.fact_transactions;")
conn.commit()

first_chunk = True
offset = 0
processed = 0

while offset < total_rows:
    query = f"""
        SELECT
            id,
            date,
            client_id,
            card_id,
            merchant_id,
            merchant_city,
            merchant_state,
            mcc,
            amount,
            use_chip,
            errors
        FROM transformation.transactions_data
        ORDER BY id
        OFFSET {offset} ROWS
        FETCH NEXT {CHUNK_SIZE} ROWS ONLY
    """

    chunk = pd.read_sql(query, con=conn)

    if chunk.empty:
        break

    #-----------------------------------
    # Transform current chunk
    #-----------------------------------
    chunk["full_date"] = pd.to_datetime(chunk["date"], errors="coerce").dt.date
    chunk["mcc"] = pd.to_numeric(chunk["mcc"], errors="coerce")
    chunk["is_refund"] = (chunk["amount"] < 0).astype(int)

    # Join customer_key
    fact_chunk = chunk.merge(
        dim_customers,
        how="left",
        left_on="client_id",
        right_on="customer_id"
    )

    # Join card_key
    fact_chunk = fact_chunk.merge(
        dim_cards,
        how="left",
        left_on="card_id",
        right_on="card_id"
    )

    # Join merchant_key
    fact_chunk = fact_chunk.merge(
        dim_merchants,
        how="left",
        on=["merchant_id", "merchant_city", "merchant_state"]
    )

    # Join date_key
    fact_chunk = fact_chunk.merge(
        dim_date,
        how="left",
        on="full_date"
    )

    # Join mcc_key
    fact_chunk = fact_chunk.merge(
        dim_mcc,
        how="left",
        left_on="mcc",
        right_on="mcc_code"
    )

    # Keep only fact table columns
    fact_chunk = fact_chunk[
        [
            "id",
            "date_key",
            "customer_key",
            "card_key",
            "merchant_key",
            "mcc_key",
            "amount",
            "use_chip",
            "errors",
            "is_refund"
        ]
    ].copy()

    fact_chunk = fact_chunk.rename(columns={
        "id": "transaction_id"
    })

    # Write chunk
    fact_chunk.to_sql(
        name="fact_transactions",
        con=engine,
        schema="curated",
        if_exists="replace" if first_chunk else "append",
        index=False
    )

    processed += len(fact_chunk)
    offset += CHUNK_SIZE
    first_chunk = False

    print(f"... {processed:,} fact rows loaded")

print(f"fact_transactions complete: {processed:,} rows")

engine.dispose()
cur.close()
conn.close()