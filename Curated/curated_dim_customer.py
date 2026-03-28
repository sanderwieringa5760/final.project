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

#--------------------------
# Load source from transformation layer
#--------------------------
df = pd.read_sql("SELECT * FROM transformation.users_data;", con=conn)

#--------------------------
# Build dim_customer
# Natural key: customer_id
#--------------------------
dim_customers = pd.DataFrame({
    "customer_id":        df["id"],
    "gender":             df["gender"],
    "birth_year":         df["birth_year"],
    "birth_month":        df["birth_month"],
    "current_age":        df["current_age"],
    "retirement_age":     df["retirement_age"],
    "address":            df["address"],
    "latitude":           df["latitude"],
    "longitude":          df["longitude"],
    "per_capita_income":  df["per_capita_income"],
    "yearly_income":      df["yearly_income"],
    "total_debt":         df["total_debt"],
    "credit_score":       df["credit_score"],
    "num_credit_cards":   df["num_credit_cards"],
    "employment_status":  df["employment_status"],
    "education_level":    df["education_level"],
})

dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
dim_customers.insert(0, "customer_key", dim_customers.index + 1)

print(f"dim_customers: {len(dim_customers)} rows")
print(dim_customers.head())

#--------------------------
# WRITE TO SQL
#--------------------------
cur.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    BEGIN EXEC('CREATE SCHEMA curated') END
""")
conn.commit()

dim_customers.to_sql(
    name="dim_customer",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_customer loaded into curated.dim_customer")

engine.dispose()
cur.close()
conn.close()