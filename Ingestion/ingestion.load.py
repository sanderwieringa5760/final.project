import os
import csv
import pyodbc

# Connect to SQL Server database
conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = False
cursor = conn.cursor()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BATCH_SIZE = 300000

# Define tables and their CSV file paths
tables = [
    {
        "table": "ingestion.cards_data",
        "file": os.path.join(BASE_DIR, "Dataset-final-project", "cards_data.csv"),
    },
    {
        "table": "ingestion.mcc_data",
        "file": os.path.join(BASE_DIR, "Dataset-final-project", "mcc_data.csv"),
    },
    {
        "table": "ingestion.transactions_data",
        "file": os.path.join(BASE_DIR, "Dataset-final-project", "transactions_data.csv"),
    },
    {
        "table": "ingestion.users_data",
        "file": os.path.join(BASE_DIR, "Dataset-final-project", "users_data.csv"),
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
    print(f"\nLoading {os.path.basename(file_path)} into {table} ({total_rows} rows)...")

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

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

