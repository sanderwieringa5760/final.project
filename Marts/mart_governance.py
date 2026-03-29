# mart_governance.py
# ClearSpend — Mart Governance & Query Guide
#
# A CLI tool that helps users navigate the mart tables.
# Select your team, pick a business question, and get:
#   - which mart table answers it
#   - which columns to focus on
#   - how to interpret the output
#   - a ready-to-run SQL query


# -------------------------------------------------------
# Knowledge base: team → questions → mart guidance
# -------------------------------------------------------

GUIDE = {
    "Finance Team": [
        {
            "question": "What is our total revenue by month?",
            "mart":     "mart.finance_monthly",
            "columns":  ["year", "month_name", "total_revenue", "total_refund_amount"],
            "focus":    "Order by year and month to see how revenue trends over time.",
            "interpretation": (
                "Each row is one calendar month. 'total_revenue' is the sum of all "
                "non-refund transactions. Compare month-over-month to spot growth or dips."
            ),
            "sql": (
                "SELECT year, month_name, total_revenue, total_refund_amount\n"
                "FROM mart.finance_monthly\n"
                "ORDER BY year, month;"
            ),
        },
        {
            "question": "What percentage of transactions are refunds?",
            "mart":     "mart.finance_monthly",
            "columns":  ["year", "month_name", "refund_count", "refund_pct"],
            "focus":    "Look at 'refund_pct' — this is the refund rate per month as a percentage.",
            "interpretation": (
                "A healthy refund rate is typically under 5%. Months with a spike in "
                "'refund_pct' may indicate a product issue, fraud wave, or data problem."
            ),
            "sql": (
                "SELECT year, month_name, total_transactions, refund_count, refund_pct\n"
                "FROM mart.finance_monthly\n"
                "ORDER BY refund_pct DESC;"
            ),
        },
        {
            "question": "Which states generate the most revenue?",
            "mart":     "mart.finance_by_state",
            "columns":  ["merchant_state", "total_revenue", "total_transactions"],
            "focus":    "Rank by 'total_revenue' to see which states drive the most value.",
            "interpretation": (
                "High-revenue states are your most important markets. Cross-reference "
                "'total_transactions' — a state with many low-value transactions is different "
                "from one with fewer high-value transactions."
            ),
            "sql": (
                "SELECT TOP 10 merchant_state, total_transactions, total_revenue\n"
                "FROM mart.finance_by_state\n"
                "ORDER BY total_revenue DESC;"
            ),
        },
        {
            "question": "Which merchant categories drive the highest spending?",
            "mart":     "mart.finance_by_category",
            "columns":  ["category", "total_revenue", "avg_transaction_amount", "total_transactions"],
            "focus":    "Compare 'total_revenue' and 'avg_transaction_amount' together.",
            "interpretation": (
                "A category with high 'avg_transaction_amount' but low 'total_transactions' "
                "means big but rare purchases. High volume + high revenue categories are the "
                "most strategically important."
            ),
            "sql": (
                "SELECT TOP 10 category, total_transactions, total_revenue, avg_transaction_amount\n"
                "FROM mart.finance_by_category\n"
                "ORDER BY total_revenue DESC;"
            ),
        },
    ],

    "Customer Analytics Team": [
        {
            "question": "What is the lifetime value of each customer?",
            "mart":     "mart.customer_ltv",
            "columns":  ["customer_id", "lifetime_spend", "total_transactions", "avg_transaction_amount", "first_transaction_date", "last_transaction_date"],
            "focus":    "Rank by 'lifetime_spend' to identify your highest-value customers.",
            "interpretation": (
                "'lifetime_spend' is the total amount spent excluding refunds. "
                "Combine with 'avg_transaction_amount' to distinguish high-frequency "
                "low-spend customers from low-frequency high-spend ones. "
                "The date columns show customer tenure."
            ),
            "sql": (
                "SELECT TOP 10 customer_id, lifetime_spend, total_transactions,\n"
                "       avg_transaction_amount, first_transaction_date, last_transaction_date\n"
                "FROM mart.customer_ltv\n"
                "ORDER BY lifetime_spend DESC;"
            ),
        },
        {
            "question": "How do customers behave online vs in-store?",
            "mart":     "mart.customer_channel",
            "columns":  ["customer_id", "online_pct", "online_transactions", "chip_transactions", "swipe_transactions", "online_spend", "instore_spend"],
            "focus":    "Use 'online_pct' to segment customers into online-first vs in-store-first.",
            "interpretation": (
                "'online_pct' = percentage of that customer's transactions made online. "
                "100% means fully online, 0% means fully in-store. "
                "Chip vs swipe breakdown shows card usage habits for in-store visits."
            ),
            "sql": (
                "SELECT TOP 10 customer_id, online_transactions, chip_transactions,\n"
                "       swipe_transactions, online_spend, instore_spend, online_pct\n"
                "FROM mart.customer_channel\n"
                "ORDER BY online_pct DESC;"
            ),
        },
        {
            "question": "How many active cards does a typical customer have?",
            "mart":     "mart.customer_cards",
            "columns":  ["customer_id", "total_cards", "credit_cards", "debit_cards", "prepaid_cards", "avg_credit_limit", "dark_web_cards"],
            "focus":    "Look at the distribution of 'total_cards' and card type breakdown.",
            "interpretation": (
                "Most customers will have 2–5 cards. 'dark_web_cards' flags customers "
                "whose cards have been found on the dark web — a fraud risk indicator. "
                "'avg_credit_limit' gives a sense of customer financial profile."
            ),
            "sql": (
                "SELECT customer_id, total_cards, credit_cards, debit_cards,\n"
                "       prepaid_cards, avg_credit_limit, dark_web_cards\n"
                "FROM mart.customer_cards\n"
                "ORDER BY total_cards DESC;"
            ),
        },
        {
            "question": "Can we identify suspicious transaction patterns?",
            "mart":     "mart.suspicious_transactions",
            "columns":  ["transaction_id", "customer_key", "flag_reason", "amount", "customer_avg_amount", "errors", "card_on_dark_web"],
            "focus":    "Group by 'flag_reason' first to see the breakdown of suspicion types.",
            "interpretation": (
                "Three flag types: 'Dark Web Card' (card linked to known breach), "
                "'Transaction Error' (error field is not N/A), "
                "'High Value Outlier' (amount > customer average + 3 std deviations). "
                "Dark web flags are the most serious and should be reviewed first."
            ),
            "sql": (
                "SELECT flag_reason, COUNT(*) AS cnt\n"
                "FROM mart.suspicious_transactions\n"
                "GROUP BY flag_reason\n"
                "ORDER BY cnt DESC;\n\n"
                "-- Drill into a specific flag type:\n"
                "SELECT TOP 20 transaction_id, customer_key, amount,\n"
                "       customer_avg_amount, errors, card_on_dark_web, flag_reason\n"
                "FROM mart.suspicious_transactions\n"
                "WHERE flag_reason = 'Dark Web Card'\n"
                "ORDER BY amount DESC;"
            ),
        },
    ],

    "Merchant Partnerships Team": [
        {
            "question": "Which merchants generate the highest transaction volume?",
            "mart":     "mart.merchant_performance",
            "columns":  ["merchant_id", "merchant_city", "merchant_state", "category", "total_transactions", "total_revenue", "avg_transaction_amount"],
            "focus":    "Sort by 'total_transactions' for volume, or 'total_revenue' for value.",
            "interpretation": (
                "High-volume merchants are your most active partners. A merchant with "
                "high volume but low 'avg_transaction_amount' operates in low-ticket categories "
                "(e.g. grocery). High avg + low volume = premium/infrequent purchases."
            ),
            "sql": (
                "SELECT TOP 10 merchant_id, merchant_city, merchant_state,\n"
                "       category, total_transactions, total_revenue\n"
                "FROM mart.merchant_performance\n"
                "ORDER BY total_transactions DESC;"
            ),
        },
        {
            "question": "What industries are growing the fastest?",
            "mart":     "mart.industry_growth",
            "columns":  ["year", "category", "total_transactions", "total_revenue"],
            "focus":    "Compare the same category across years to calculate year-over-year growth.",
            "interpretation": (
                "Each row is one industry (MCC category) for one year. "
                "To find growth, compare a category's 'total_revenue' in year N vs year N-1. "
                "Categories appearing in later years that weren't in earlier years are emerging."
            ),
            "sql": (
                "SELECT year, category, total_transactions, total_revenue\n"
                "FROM mart.industry_growth\n"
                "ORDER BY year, total_revenue DESC;"
            ),
        },
        {
            "question": "Which merchants have the highest error rates?",
            "mart":     "mart.merchant_errors",
            "columns":  ["merchant_id", "merchant_city", "merchant_state", "total_transactions", "error_count", "error_rate_pct", "error_types"],
            "focus":    "Sort by 'error_rate_pct' — only look at merchants with meaningful volume.",
            "interpretation": (
                "'error_rate_pct' is the share of that merchant's transactions that had an error. "
                "A merchant with 2 transactions and 1 error shows 50% — filter by "
                "'total_transactions > 10' to avoid small-sample noise. "
                "'error_types' lists the actual error messages seen."
            ),
            "sql": (
                "SELECT TOP 10 merchant_id, merchant_city, merchant_state,\n"
                "       total_transactions, error_count, error_rate_pct, error_types\n"
                "FROM mart.merchant_errors\n"
                "WHERE total_transactions > 10\n"
                "ORDER BY error_rate_pct DESC;"
            ),
        },
        {
            "question": "How is revenue distributed geographically?",
            "mart":     "mart.revenue_by_geography",
            "columns":  ["merchant_state", "merchant_city", "total_transactions", "total_revenue", "avg_transaction_amount"],
            "focus":    "Start at state level, then drill into city for a specific state.",
            "interpretation": (
                "Each row is a unique city+state combination. "
                "Use 'total_revenue' for overall geographic importance. "
                "'avg_transaction_amount' shows whether a city has high-value or high-frequency spend. "
                "Online transactions are excluded (merchant_state = N/A is filtered out)."
            ),
            "sql": (
                "-- State level summary:\n"
                "SELECT TOP 10 merchant_state, merchant_city, total_transactions, total_revenue\n"
                "FROM mart.revenue_by_geography\n"
                "ORDER BY total_revenue DESC;\n\n"
                "-- Drill into a specific state (example: TX):\n"
                "SELECT merchant_city, total_transactions, total_revenue\n"
                "FROM mart.revenue_by_geography\n"
                "WHERE merchant_state = 'TX'\n"
                "ORDER BY total_revenue DESC;"
            ),
        },
    ],
}

TEAMS = list(GUIDE.keys())


# -------------------------------------------------------
# Helper functions
# -------------------------------------------------------

def print_separator():
    print("-" * 55)

def prompt_choice(prompt, max_value):
    """Ask the user for a number between 1 and max_value. Re-prompts on invalid input."""
    while True:
        raw = input(prompt).strip()
        if raw.isdigit() and 1 <= int(raw) <= max_value:
            return int(raw)
        print(f"  Please enter a number between 1 and {max_value}.")

def show_teams():
    print("\nSelect your team:")
    for i, team in enumerate(TEAMS, 1):
        print(f"  {i}. {team}")

def show_questions(team_name):
    questions = GUIDE[team_name]
    print(f"\n{team_name} — business questions:")
    for i, entry in enumerate(questions, 1):
        print(f"  {i}. {entry['question']}")

def show_guidance(entry):
    """Print the full guidance for a selected question."""
    print_separator()
    print(f"  Question : {entry['question']}")
    print(f"  Mart     : {entry['mart']}")
    print(f"  Columns  : {', '.join(entry['columns'])}")
    print_separator()
    print(f"  Focus\n  {entry['focus']}")
    print()
    print(f"  How to interpret\n  {entry['interpretation']}")
    print_separator()
    print("  SQL query:")
    print()
    for line in entry["sql"].splitlines():
        print(f"    {line}")
    print_separator()


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def main():
    print("=" * 55)
    print("  ClearSpend — Mart Governance & Query Guide")
    print("=" * 55)

    # Step 1: choose a team
    show_teams()
    team_choice = prompt_choice("\nEnter team number: ", len(TEAMS))
    team_name = TEAMS[team_choice - 1]

    # Step 2: show questions for that team
    show_questions(team_name)
    question_choice = prompt_choice("\nEnter question number: ", len(GUIDE[team_name]))
    entry = GUIDE[team_name][question_choice - 1]

    # Step 3: show guidance + SQL
    show_guidance(entry)

if __name__ == "__main__":
    main()
