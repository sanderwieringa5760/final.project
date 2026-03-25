import pyodbc
import csv

# Connect to your SQL Server database
conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()

# Define tables and their CSV file paths
tables = [
    {
        "table": "ingestion.cards_data",
        "file": "C:\\Users\\sjonn\\Documents\\UM\\data engineering\\final_project\\Dataset-final-project\\cards_data.csv",
        "columns": 17
    },
    {
        "table": "ingestion.mcc_data",
        "file": "C:\\Users\\sjonn\\Documents\\UM\\data engineering\\final_project\\Dataset-final-project\\mcc_data.csv",
        "columns": 4
    },
    {
        "table": "ingestion.transactions_data",
        "file": "C:\\Users\\sjonn\\Documents\\UM\\data engineering\\final_project\\Dataset-final-project\\transactions_data.csv",
        "columns": 12
    },
    {
        "table": "ingestion.users_data",
        "file": "C:\\Users\\sjonn\\Documents\\UM\\data engineering\\final_project\\Dataset-final-project\\users_data.csv",
        "columns": 16
    }
]

for t in tables:
    table_name = t["table"]
    file_path = t["file"]
    placeholders = ",".join(["?" for _ in range(t["columns"])])

    # Truncate table before loading
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Read CSV and insert rows
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        rows = 0
        batch_rows = []
        batch_size = 1000 if table_name == "ingestion.transactions_data" else 1
        
        for row in reader:
            # Only convert empty strings to None, keep everything else as-is
            row = [None if val == "" else val for val in row]
            batch_rows.append(row)
            
            # Execute batch or single row
            if len(batch_rows) >= batch_size:
                cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", batch_rows)
                rows += len(batch_rows)
                batch_rows = []
        
        # Insert remaining rows
        if batch_rows:
            cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", batch_rows)
            rows += len(batch_rows)

    print(f"Loaded {rows} rows into {table_name}")

cursor.close()
conn.close()
print("All tables loaded!")