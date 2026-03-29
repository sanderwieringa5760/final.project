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


# 1. Merchant performance (volume + revenue)
#    Answers: "Which merchants generate the highest transaction volume?"

cursor.execute("DROP TABLE IF EXISTS mart.merchant_performance")
cursor.execute("""
    SELECT
        m.merchant_key,
        m.merchant_id,
        m.merchant_city,
        m.merchant_state,
        mc.description                                              AS category,
        COUNT(*)                                                    AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)    AS total_revenue,
        SUM(f.is_refund)                                            AS refund_count,
        AVG(CASE WHEN f.is_refund = 0 THEN f.amount END)           AS avg_transaction_amount
    INTO mart.merchant_performance
    FROM curated.fact_transactions f
    JOIN curated.dim_merchants m ON f.merchant_key = m.merchant_key
    LEFT JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
    GROUP BY m.merchant_key, m.merchant_id, m.merchant_city, m.merchant_state, mc.description
""")
print("mart.merchant_performance created")

# 2. Industry growth by year
#    Answers: "What industries are growing the fastest?"

cursor.execute("DROP TABLE IF EXISTS mart.industry_growth")
cursor.execute("""
    SELECT
        d.year,
        mc.mcc_code,
        mc.description                                              AS category,
        COUNT(*)                                                    AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)    AS total_revenue
    INTO mart.industry_growth
    FROM curated.fact_transactions f
    JOIN curated.dim_date d ON f.date_key = d.date_key
    JOIN curated.dim_mcc mc ON f.mcc_key = mc.mcc_key
    GROUP BY d.year, mc.mcc_code, mc.description
""")
print("mart.industry_growth created")


# 3. Merchant error rates
#    Answers: "Which merchants have the highest error rates?"

cursor.execute("DROP TABLE IF EXISTS mart.merchant_errors")
cursor.execute("""
    SELECT
        m.merchant_key,
        m.merchant_id,
        m.merchant_city,
        m.merchant_state,
        COUNT(*)                                                                AS total_transactions,
        SUM(CASE WHEN f.errors != 'N/A' THEN 1 ELSE 0 END)                     AS error_count,
        CAST(
            SUM(CASE WHEN f.errors != 'N/A' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2))                                                        AS error_rate_pct,
        -- most common error type for this merchant
        STRING_AGG(CASE WHEN f.errors != 'N/A' THEN f.errors END, ', ')        AS error_types
    INTO mart.merchant_errors
    FROM curated.fact_transactions f
    JOIN curated.dim_merchants m ON f.merchant_key = m.merchant_key
    GROUP BY m.merchant_key, m.merchant_id, m.merchant_city, m.merchant_state
""")
print("mart.merchant_errors created")


# 4. Revenue by geography (state + city)
#    Answers: "How is revenue distributed geographically?"

cursor.execute("DROP TABLE IF EXISTS mart.revenue_by_geography")
cursor.execute("""
    SELECT
        m.merchant_state,
        m.merchant_city,
        COUNT(*)                                                    AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)    AS total_revenue,
        SUM(f.is_refund)                                            AS refund_count,
        AVG(CASE WHEN f.is_refund = 0 THEN f.amount END)           AS avg_transaction_amount
    INTO mart.revenue_by_geography
    FROM curated.fact_transactions f
    JOIN curated.dim_merchants m ON f.merchant_key = m.merchant_key
    WHERE m.merchant_state != 'N/A'
    GROUP BY m.merchant_state, m.merchant_city
""")
print("mart.revenue_by_geography created")

cursor.close()
conn.close()
print("Merchant partnerships mart complete.")
