import pyodbc

# 1. Connect to 'master' to create the database
conn_master = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=master;"
    r"Trusted_Connection=yes;"
)
conn_master.autocommit = True
cursor_master = conn_master.cursor()

cursor_master.execute("""
    IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'final_project')
    BEGIN   
        CREATE DATABASE final_project;
    END
""")
print("Database final_project created or already exists!")

cursor_master.close()
conn_master.close()

# 2. Connect to the new 'final_project' to create schema and tables
conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()

# df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_cust_info", con=conn)

# Create schema
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ingestion')
    BEGIN
        EXEC('CREATE SCHEMA ingestion');
    END
""")
print("Schema created or already exists!")

# Drop and create tables - All VARCHAR for speed, convert in SQL later
tables_sql = {
    "cards_data": """
        CREATE TABLE ingestion.cards_data (
            id VARCHAR(50),
            client_id VARCHAR(50),
            card_brand VARCHAR(50),
            card_type VARCHAR(50),
            card_number VARCHAR(25),
            expires VARCHAR(20),
            cvv VARCHAR(10),
            has_chip VARCHAR(10),
            num_cards_issued VARCHAR(10),
            credit_limit VARCHAR(25),
            acct_open_date VARCHAR(20),
            year_pin_last_changed VARCHAR(10),
            card_on_dark_web VARCHAR(10),
            issuer_bank_name VARCHAR(100),
            issuer_bank_state VARCHAR(50),
            issuer_bank_type VARCHAR(50),
            issuer_risk_rating VARCHAR(20)
        )
    """,
    "mcc_data": """
        CREATE TABLE ingestion.mcc_data (
            code VARCHAR(255),
            description VARCHAR(255),
            notes VARCHAR(255),
            updated_by VARCHAR(255)
        )
    """,
    "transactions_data": """
        CREATE TABLE ingestion.transactions_data (
            id INT,
            [date] VARCHAR(20),
            client_id INT,
            card_id INT,
            amount VARCHAR(25),
            use_chip VARCHAR(30),
            merchant_id INT,
            merchant_city VARCHAR(100),
            merchant_state VARCHAR(50),
            zip VARCHAR(10),
            mcc INT,
            errors VARCHAR(255)
        )
    """,
    "users_data": """
        CREATE TABLE ingestion.users_data (
            id VARCHAR(50),
            current_age VARCHAR(10),
            retirement_age VARCHAR(10),
            birth_year VARCHAR(10),
            birth_month VARCHAR(10),
            gender VARCHAR(20),
            address VARCHAR(255),
            latitude VARCHAR(25),
            longitude VARCHAR(25),
            per_capita_income VARCHAR(25),
            yearly_income VARCHAR(25),
            total_debt VARCHAR(25),
            credit_score VARCHAR(10),
            num_credit_cards VARCHAR(10),
            employment_status VARCHAR(50),
            education_level VARCHAR(50)
        )
    """
}

for table_name, create_sql in tables_sql.items():
    cursor.execute(f"DROP TABLE IF EXISTS ingestion.{table_name}")
    cursor.execute(create_sql)
    print(f"Table ingestion.{table_name} created!")

cursor.close()
conn.close()