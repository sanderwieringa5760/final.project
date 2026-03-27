import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

#--------------------------
# READ SQL
#--------------------------
conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="final_project",
    user="postgres",
    password="aaaa"
)

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
cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
conn.commit()

engine = create_engine(
    "postgresql+psycopg2://postgres:aaaa@127.0.0.1:5432/final_project"
)

dim_customers.to_sql(
    name="dim_customer",
    con=engine,
    schema="curated",
    if_exists="replace",
    index=False
)

print("dim_customer loaded into curated.dim_customer")

cur.close()
conn.close()