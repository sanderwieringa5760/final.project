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


# 1. Customer lifetime value
#    Answers: "What is the lifetime value of each customer?"

cursor.execute("DROP TABLE IF EXISTS mart.customer_ltv")
cursor.execute("""
    SELECT
        c.customer_key,
        c.customer_id,
        c.gender,
        c.current_age,
        c.employment_status,
        c.education_level,
        c.yearly_income,
        c.total_debt,
        c.credit_score,
        COUNT(*)                                                        AS total_transactions,
        SUM(CASE WHEN f.is_refund = 0 THEN f.amount ELSE 0 END)        AS lifetime_spend,
        SUM(CASE WHEN f.is_refund = 1 THEN ABS(f.amount) ELSE 0 END)   AS total_refunded,
        AVG(CASE WHEN f.is_refund = 0 THEN f.amount END)               AS avg_transaction_amount,
        MIN(d.full_date)                                                AS first_transaction_date,
        MAX(d.full_date)                                                AS last_transaction_date
    INTO mart.customer_ltv
    FROM curated.fact_transactions f
    JOIN curated.dim_customer c ON f.customer_key = c.customer_key
    JOIN curated.dim_date d ON f.date_key = d.date_key
    GROUP BY
        c.customer_key, c.customer_id, c.gender, c.current_age,
        c.employment_status, c.education_level, c.yearly_income,
        c.total_debt, c.credit_score
""")
print("mart.customer_ltv created")


# 2. Online vs in-store behaviour per customer
#    Answers: "How do customers behave online vs in-store?"

cursor.execute("DROP TABLE IF EXISTS mart.customer_channel")
cursor.execute("""
    SELECT
        c.customer_key,
        c.customer_id,
        SUM(CASE WHEN f.use_chip = 'Online Transaction' THEN 1 ELSE 0 END)         AS online_transactions,
        SUM(CASE WHEN f.use_chip = 'Chip Transaction'   THEN 1 ELSE 0 END)         AS chip_transactions,
        SUM(CASE WHEN f.use_chip = 'Swipe Transaction'  THEN 1 ELSE 0 END)         AS swipe_transactions,
        SUM(CASE WHEN f.use_chip = 'Online Transaction' THEN f.amount ELSE 0 END)  AS online_spend,
        SUM(CASE WHEN f.use_chip != 'Online Transaction' THEN f.amount ELSE 0 END) AS instore_spend,
        CAST(
            SUM(CASE WHEN f.use_chip = 'Online Transaction' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2))                                                            AS online_pct
    INTO mart.customer_channel
    FROM curated.fact_transactions f
    JOIN curated.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.customer_id
""")
print("mart.customer_channel created")


# 3. Active cards per customer
#    Answers: "How many active cards does a typical customer have?"

cursor.execute("DROP TABLE IF EXISTS mart.customer_cards")
cursor.execute("""
    SELECT
        c.customer_key,
        c.customer_id,
        COUNT(dc.card_key)                                              AS total_cards,
        SUM(CASE WHEN dc.has_chip  = 'Yes' THEN 1 ELSE 0 END)          AS cards_with_chip,
        SUM(CASE WHEN dc.card_on_dark_web = 'Yes' THEN 1 ELSE 0 END)   AS dark_web_cards,
        AVG(dc.credit_limit)                                            AS avg_credit_limit,
        SUM(dc.credit_limit)                                            AS total_credit_limit,
        -- card type breakdown
        SUM(CASE WHEN dc.card_type = 'Credit'         THEN 1 ELSE 0 END) AS credit_cards,
        SUM(CASE WHEN dc.card_type = 'Debit'          THEN 1 ELSE 0 END) AS debit_cards,
        SUM(CASE WHEN dc.card_type = 'Debit (Prepaid)' THEN 1 ELSE 0 END) AS prepaid_cards
    INTO mart.customer_cards
    FROM curated.dim_customer c
    LEFT JOIN curated.dim_cards dc ON c.customer_id = dc.client_id
    GROUP BY c.customer_key, c.customer_id
""")
print("mart.customer_cards created")


# 4. Suspicious transaction flags
#    Answers: "Can we identify suspicious transaction patterns?"
#    Flags: transaction errors, dark-web-linked cards, high-value outliers
#    (amount > customer average + 3 standard deviations)

cursor.execute("DROP TABLE IF EXISTS mart.suspicious_transactions")
cursor.execute("""
    WITH customer_stats AS (
        SELECT
            customer_key,
            AVG(amount)   AS avg_amount,
            STDEV(amount) AS std_amount
        FROM curated.fact_transactions
        WHERE is_refund = 0
        GROUP BY customer_key
    )
    SELECT
        f.transaction_id,
        f.customer_key,
        f.card_key,
        d.full_date,
        f.amount,
        cs.avg_amount           AS customer_avg_amount,
        f.use_chip,
        f.errors,
        dc.card_on_dark_web,
        CASE
            WHEN dc.card_on_dark_web = 'Yes'
                THEN 'Dark Web Card'
            WHEN f.errors != 'N/A'
                THEN 'Transaction Error'
            WHEN f.amount > cs.avg_amount + 3 * cs.std_amount
                THEN 'High Value Outlier'
            ELSE 'Other'
        END AS flag_reason
    INTO mart.suspicious_transactions
    FROM curated.fact_transactions f
    JOIN curated.dim_date d     ON f.date_key = d.date_key
    JOIN curated.dim_cards dc   ON f.card_key = dc.card_key
    JOIN customer_stats cs      ON f.customer_key = cs.customer_key
    WHERE dc.card_on_dark_web = 'Yes'
       OR f.errors != 'N/A'
       OR f.amount > cs.avg_amount + 3 * cs.std_amount
""")
print("mart.suspicious_transactions created")

cursor.close()
conn.close()
print("Customer analytics mart complete.")
