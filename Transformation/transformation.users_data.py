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
df = pd.read_sql_query(sql="SELECT * FROM ingestion.users_data", con=conn)

# ---------------
# clean data
# ---------------

# column 1  id
# column 2  current_age
# column 3  retirement_age
# column 4  birth_year
# column 5  birth_month
# column 6  gender
# column 7  address
# column 8  latitude
# column 9  longitude
# column 10 per_capita_income
# column 11 yearly_income
# column 12 total_debt
def parse_currency(val):
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

df["per_capita_income"] = df["per_capita_income"].apply(parse_currency)
df["yearly_income"] = df["yearly_income"].apply(parse_currency)
df["total_debt"] = df["total_debt"].apply(parse_currency)
# column 13 credit_score
# column 14 num_credit_cards
# column 15 employment_status
# column 16 education_level
df["education_level"] = df["education_level"].str.lstrip() # remove emtpy spaces at start and end of eduction levels
df["education_level"] = df["education_level"].str.replace('BACHELOR DEGREE', 'Bachelor') # remove MCC
df["education_level"] = df["education_level"].str.replace('DOCTORATE', 'Doctorate') 
df["education_level"] = df["education_level"].str.replace('associate degree', 'Associate') 
df["education_level"] = df["education_level"].str.replace('ASSOCIATE DEGREE', 'Associate') 
df["education_level"] = df["education_level"].str.replace('high school', 'High School') 



# ---------------
# load data
# ---------------

# Drop and recreate the table in transformation schema
cursor.execute("DROP TABLE IF EXISTS transformation.users_data")
cursor.execute("""
    CREATE TABLE transformation.users_data (
    id                  INT,
    current_age         INT,
    retirement_age      INT,
    birth_year          INT,
    birth_month         INT,
    gender              VARCHAR(50),
    address             VARCHAR(255),
    latitude            DECIMAL(18,6),
    longitude           DECIMAL(18,6),
    per_capita_income   DECIMAL(18,2),
    yearly_income       DECIMAL(18,2),
    total_debt          DECIMAL(18,2),
    credit_score        INT,
    num_credit_cards    INT,
    employment_status   VARCHAR(100),
    education_level     VARCHAR(100)
    )
""")

# Keep only the columns matching the target table
df = df[["id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", "address", "latitude", "longitude", "per_capita_income", "yearly_income", "total_debt", "credit_score", "num_credit_cards", "employment_status", "education_level"]]

# Insert cleaned data into transformation layer
placeholders = ",".join(["?" for _ in range(len(df.columns))])
for _, row in df.iterrows():
    values = [None if pd.isna(v) else v for v in row]
    cursor.execute(f"INSERT INTO transformation.users_data VALUES ({placeholders})", values)

print(f"Loaded {len(df)} cleaned rows into transformation.users_data")

cursor.close()
conn.close()
