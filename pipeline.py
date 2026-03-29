import subprocess
import sys
import os


# ClearSpend — End-to-End Pipeline Runner

# Runs all 15 scripts in the correct order:
#   1. Ingestion   — creates the database and loads raw CSVs into SQL Server
#   2. Transformation — cleans and standardises each dataset
#   3. Curated     — builds the star schema (dimensions + fact table)
#   4. Marts       — creates aggregated tables for each business team

# If any script fails, the pipeline stops immediately so you
# don't run the next layer on broken data.


#  Mode selection 
# t = test mode (only loads 1000 transactions, fast for checking everything works)
# f = full mode (loads all 1.3M transactions)
mode = input("Run in test mode or full mode? (t/f): ").strip().lower()
if mode not in ("t", "f"):
    print("Invalid input. Enter t or f.")
    sys.exit(1)

load_script = os.path.join(os.path.dirname(__file__), "Ingestion", "ingestion.load.py")
with open(load_script, "r") as f:
    content = f.read()
if mode == "t":
    content = content.replace("TEST_MODE = False", "TEST_MODE = True")
else:
    content = content.replace("TEST_MODE = True", "TEST_MODE = False")
with open(load_script, "w") as f:
    f.write(content)

print(f"\nRunning in {'TEST MODE (1000 transactions)' if mode == 't' else 'FULL MODE (all data)'}.\n")


scripts = [
    #  Layer 1: Ingestion 
    # Creates the database schema and loads raw CSV files as-is into SQL Server
    ("Ingestion",       "ingestion.ddl.py"),           # create database + tables
    ("Ingestion",       "ingestion.load.py"),           # load CSVs into ingestion schema

    # Layer 2: Transformation
    # Cleans, standardises, and type-casts each dataset
    ("Transformation",  "transformation.mcc_data.py"),          # clean MCC reference data
    ("Transformation",  "transformation.cards_data.py"),         # clean cards data
    ("Transformation",  "transformation.users_data.py"),         # clean customer data
    ("Transformation",  "transformation.transactions_data.py"),  # clean transactions

    # Layer 3: Curated (star schema)
    # Builds dimension tables first, then the fact table last
    ("Curated",         "curated_dim_mcc.py"),                   # MCC dimension
    ("Curated",         "curated_dim_customer.py"),              # customer dimension
    ("Curated",         "curated_dim_cards.py"),                 # cards dimension
    ("Curated",         "curated_dim_date.py"),                  # date dimension
    ("Curated",         "curated_dim_merchants.py"),             # merchants dimension
    ("Curated",         "curated_dim_fact_transactions.py"),     # fact table (joins all dims)

    # Layer 4: Marts
    # Pre-aggregated tables built for each business team
    ("Marts",           "mart_customer.py"),    # customer analytics team
    ("Marts",           "mart_finance.py"),     # finance team
    ("Marts",           "mart_merchant.py"),    # merchant partnerships team
]

total = len(scripts)

for i, (folder, script) in enumerate(scripts, 1):
    path = os.path.join(os.path.dirname(__file__), folder, script)
    print(f"[{i}/{total}] {folder}/{script}")
    result = subprocess.run([sys.executable, path])
    if result.returncode != 0:
        print(f"\n  FAILED: {folder}/{script}")
        print("  Pipeline stopped. Fix the error above and re-run.")
        sys.exit(1)
    print(f"  completed.\n")

print("")
print("  Pipeline complete!")

