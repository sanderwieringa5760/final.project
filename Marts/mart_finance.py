import pyodbc

conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=.\SQLEXPRESS;"
    r"DATABASE=final_project;"
    r"Trusted_Connection=yes;"
)
conn.autocommit = True
cursor = conn.cursor()

# Create mart schema if not exists
cursor.execute("""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart')
    BEGIN EXEC('CREATE SCHEMA mart') END
""")


# 1. Revenue by month
#    Answers: "What is our total revenue by month?"
#             "What percentage of transactions are refunds?"

cursor.execute("DROP TABLE IF EXISTS mart.finance_monthly")
cursor.execute("""
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)        AS total_revenue,
        SUM(CASE WHEN f.is_refund = 1 THEN ABS(f.amount) ELSE 0 END)   AS total_refund_amount,
        SUM(f.is_refund)                                                AS refund_count,
        CAST(
            SUM(f.is_refund) * 100.0 / COUNT(*)
        AS DECIMAL(5,2))                                                AS refund_pct
    INTO mart.finance_monthly
    FROM curated.fact_transactions f
    JOIN curated.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
""")
print("mart.finance_monthly created")


# 2. Revenue by state
#    Answers: "Which states generate the most revenue?"

cursor.execute("DROP TABLE IF EXISTS mart.finance_by_state")
cursor.execute("""
    SELECT
        m.merchant_state,
        COUNT(*)                                                    AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)    AS total_revenue,
        SUM(f.is_refund)                                            AS refund_count
    INTO mart.finance_by_state
    FROM curated.fact_transactions f
    JOIN curated.dim_merchants m ON f.merchant_key = m.merchant_key
    WHERE m.merchant_state != 'N/A'
    GROUP BY m.merchant_state
""")
print("mart.finance_by_state created")


# 3. Revenue by merchant category (MCC)
#    Answers: "Which merchant categories drive the highest spending?"

cursor.execute("DROP TABLE IF EXISTS mart.finance_by_category")
cursor.execute("""
    SELECT
        mc.mcc_code,
        mc.description                                              AS category,
        COUNT(*)                                                    AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)    AS total_revenue,
        AVG(CASE WHEN f.is_refund = 0 THEN f.amount END)           AS avg_transaction_amount
    INTO mart.finance_by_category
    FROM curated.fact_transactions f
    JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
    GROUP BY mc.mcc_code, mc.description
""")
print("mart.finance_by_category created")

cursor.close()
conn.close()
print("Finance mart complete.")
