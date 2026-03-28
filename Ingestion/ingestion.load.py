import os
import csv
import itertools
import pyodbc

# ---- TEST MODE: limit transactions to 1000 rows, load all other tables in full ----
TEST_MODE = False   # Change to False to load all rows for every table
ROW_LIMIT = 1000
TRANSACTIONS_TABLE = "ingestion.transactions_data"
# -----------------------------------------------------------------------------------

# Connect to SQL Server database
conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = False
cursor = conn.cursor()

DATASET_DIR = r"C:\Users\Gebruiker\Desktop\Data Engeneering\Dataset-final-project"

BATCH_SIZE = 300000

# Define tables and their CSV file paths
tables = [
    {
        "table": "ingestion.cards_data",
        "file": os.path.join(DATASET_DIR, "cards_data.csv"),
    },
    {
        "table": "ingestion.mcc_data",
        "file": os.path.join(DATASET_DIR, "mcc_data.csv"),
    },
    {
        "table": "ingestion.transactions_data",
        "file": os.path.join(DATASET_DIR, "transactions_data.csv"),
    },
    {
        "table": "ingestion.users_data",
        "file": os.path.join(DATASET_DIR, "users_data.csv"),
    }
]

# ------------------
# Load data into tables
# ------------------

def count_rows(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1  # exclude header

def load_table(table, file_path):
    total_rows = count_rows(file_path)
    limit_this_table = TEST_MODE and table == TRANSACTIONS_TABLE
    effective_rows = min(ROW_LIMIT, total_rows) if limit_this_table else total_rows
    print(f"\nLoading {os.path.basename(file_path)} into {table} ({effective_rows} of {total_rows} rows)...")

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        if limit_this_table:
            reader = itertools.islice(reader, ROW_LIMIT)

        placeholders = ", ".join("?" * len(headers))
        columns = ", ".join(f"[{col}]" for col in headers)
        insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        batch = []
        rows_loaded = 0

        for row in reader:
            batch.append(row)

            if len(batch) == BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                rows_loaded += len(batch)
                batch = []
                pct = (rows_loaded / total_rows) * 100
                print(f"  {rows_loaded}/{total_rows} rows ({pct:.1f}%)")

        # Insert any remaining rows
        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            rows_loaded += len(batch)
            pct = (rows_loaded / total_rows) * 100
            print(f"  {rows_loaded}/{total_rows} rows ({pct:.1f}%)")

    print(f"  Done: {table}")

for entry in tables:
    load_table(entry["table"], entry["file"])

cursor.close()
conn.close()

