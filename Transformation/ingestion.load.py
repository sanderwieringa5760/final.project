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

# ------------------
# make tables
# ------------------

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

# ------------------
# Load data into tables
# ------------------
for t in tables:
    table_name = t["table"]
    file_path = t["file"]
    placeholders = ",".join(["?" for _ in range(t["columns"])])

    # Truncate table before loading
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Read CSV and insert rows
    # Pre-count lines for progress tracking (only for large tables)
    total_rows = None
    if table_name == "ingestion.transactions_data":
        with open(file_path, "r", encoding="utf-8") as f:
            total_rows = sum(1 for _ in f) - 1  # subtract header

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        rows = 0
        batch_rows = []
        batch_size = 1000 if table_name == "ingestion.transactions_data" else 1
        last_pct = -1

# If doesn't work delete from here
        # Pre-compute column indexes once outside the loop
        card_number_index = header.index("card_number") if table_name == "ingestion.cards_data" else None
        amount_index = header.index("amount") if table_name == "ingestion.transactions_data" else None

        for row in reader:
            row = [None if val == "" else val for val in row]

            # Fix card_number precision corruption (only for cards_data)
            if card_number_index is not None and row[card_number_index] is not None and "." in row[card_number_index]:
                row[card_number_index] = row[card_number_index].split(".")[0]

            # Strip $ from amount (only for transactions_data)
            if amount_index is not None and row[amount_index] is not None:
                row[amount_index] = row[amount_index].replace("$", "")

            batch_rows.append(row)
# if doesn't work delete until here

            # Execute batch or single row
            if len(batch_rows) >= batch_size:
                cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", batch_rows)
                rows += len(batch_rows)
                batch_rows = []

                if total_rows:
                    pct = int(rows / total_rows * 100)
                    if pct != last_pct:
                        print(f"  {table_name}: {pct}% ({rows}/{total_rows})", flush=True)
                        last_pct = pct

        # Insert remaining rows
        if batch_rows:
            cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", batch_rows)
            rows += len(batch_rows)

    print(f"Loaded {rows} rows into {table_name}")

cursor.close()
conn.close()
print("All tables loaded!")