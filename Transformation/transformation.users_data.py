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

# remove fully duplicate rows (all columns must match)
before = len(df)
df = df.drop_duplicates()
removed = before - len(df)
if removed > 0:
    print(f"Removed {removed} duplicate row(s). {len(df)} rows remaining.")
else:
    print("No duplicate rows found.")

# column 1  id
# column 2  current_age
today = pd.Timestamp.today()
df["birth_year"] = pd.to_numeric(df["birth_year"], errors="coerce")
df["birth_month"] = pd.to_numeric(df["birth_month"], errors="coerce")
df["current_age"] = df.apply(
    lambda row: today.year - row["birth_year"] - (1 if today.month < row["birth_month"] else 0)
    if pd.notna(row["birth_year"]) and pd.notna(row["birth_month"]) else None,
    axis=1
)

# column 3  retirement_age
# column 4  birth_year
# column 5  birth_month
# column 6  gender
# column 7  address
df["address"] = df["address"].str.strip().str.replace(r"\s+", " ", regex=True).str.title()
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
df["employment_status"] = df["employment_status"].str.lstrip() # remove emtpy spaces at start and end of eduction levels
df["employment_status"] = df["employment_status"].replace("Studnt", "Retired")
df["employment_status"] = df["employment_status"].replace("student", "Student")
df["employment_status"] = df["employment_status"].replace("Un-employed", "Unemployd")
df["employment_status"] = df["employment_status"].replace("Unemployd", "Unemployd")
df["employment_status"] = df["employment_status"].replace("unemployed", "Unemployed")
df["employment_status"] = df["employment_status"].replace("Empl0yed", "Retired")
df["employment_status"] = df["employment_status"].replace("employed", "Employed")
df["employment_status"] = df["employment_status"].replace("SELF-EMPLOYED", "Self-Employed")
df["employment_status"] = df["employment_status"].replace("SELF EMPLOYED", "Retired")
df["employment_status"] = df["employment_status"].replace("Self-Employd", "Retired")
df["employment_status"] = df["employment_status"].replace("Self Employed", "Retired")
df["employment_status"] = df["employment_status"].replace("Retird", "Retired")
df["employment_status"] = df["employment_status"].replace("Ret.", "Retired")

# column 16 education_level
df["education_level"] = df["education_level"].str.strip().str.replace(r"\s+", " ", regex=True) # remove extra spaces

df["education_level"] = df["education_level"].replace("high school", "High School")
df["education_level"] = df["education_level"].replace("highschool", "High School")
df["education_level"] = df["education_level"].replace("Highschool", "High School")
df["education_level"] = df["education_level"].replace("HIGH SCHOOL", "High School")
df["education_level"] = df["education_level"].replace("HS", "High School")

df["education_level"] = df["education_level"].replace("associate degree", "Associate Degree")
df["education_level"] = df["education_level"].replace("Associate Degree Degree", "Associate Degree")
df["education_level"] = df["education_level"].replace("Associate Degree Deg", "Associate Degree")
df["education_level"] = df["education_level"].replace("Associate", "Associate Degree")
df["education_level"] = df["education_level"].replace("ASSOCIATE DEGREE", "Associate Degree")
df["education_level"] = df["education_level"].replace("Assoc Degree", "Associate Degree")
df["education_level"] = df["education_level"].replace("Associate deg.", "Associate Degree")

df["education_level"] = df["education_level"].replace("BACHELOR DEGREE", "Bachelor Degree")
df["education_level"] = df["education_level"].replace("Bachelor Degrees", "Bachelor Degree")
df["education_level"] = df["education_level"].replace("Bachelor", "Bachelor Degree")
df["education_level"] = df["education_level"].replace("Bachelor's Degree", "Bachelor Degree")
df["education_level"] = df["education_level"].replace("BA/BS", "Bachelor Degree")
df["education_level"] = df["education_level"].replace("Bachelors", "Bachelor Degree")

df["education_level"] = df["education_level"].replace("Masters", "Masters Degree")
df["education_level"] = df["education_level"].replace("masters degree", "Masters Degree")
df["education_level"] = df["education_level"].replace("master degree", "Masters Degree")
df["education_level"] = df["education_level"].replace("Master Degree", "Masters Degree")
df["education_level"] = df["education_level"].replace("MASTER DEGREE", "Masters Degree")
df["education_level"] = df["education_level"].replace("Masters Degree Degree", "Masters Degree")
df["education_level"] = df["education_level"].replace("MS/MA", "Masters Degree")
df["education_level"] = df["education_level"].replace("Master's Degree", "Masters Degree")

df["education_level"] = df["education_level"].replace("DOCTORATE", "Doctorate")

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
    latitude            DECIMAL(18,2),
    longitude           DECIMAL(18,2),
    per_capita_income   DECIMAL(18,0),
    yearly_income       DECIMAL(18,0),
    total_debt          DECIMAL(18,0),
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
