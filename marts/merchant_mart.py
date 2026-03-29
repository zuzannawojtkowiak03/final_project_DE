import psycopg2

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "curated"
SCHEMA_TARGET = "merchant_mart"


def merchant_mart():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")

    # Create merchant revenue summary
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.merchant_volume CASCADE;")
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.merchant_volume AS
        SELECT
            dm.merchant_key,
            dm.merchant_id,
            dm.merchant_city,
            dm.merchant_state,
            ROUND(SUM(ft.amount)::NUMERIC, 2) AS total_revenue,
            COUNT(*)                          AS transaction_count,
            ROUND(AVG(ft.amount)::NUMERIC, 2) AS avg_transaction
        FROM {SCHEMA_SOURCE}.fact_transactions ft
        JOIN {SCHEMA_SOURCE}.dim_merchants dm
          ON ft.merchant_key = dm.merchant_key
        WHERE ft.is_refund = 'No'
          AND ft.amount > 0
        GROUP BY dm.merchant_key, dm.merchant_id, dm.merchant_city, dm.merchant_state
        ORDER BY transaction_count DESC;
    """)

    # Create yearly revenue growth by MCC
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.merchant_industry_growth CASCADE;")
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.merchant_industry_growth AS
        WITH yearly_revenue AS (
            SELECT
                dd.year,
                dmcc.mcc_code,
                dmcc.description AS mcc_description,
                SUM(ft.amount)   AS total_revenue
            FROM {SCHEMA_SOURCE}.fact_transactions ft
            JOIN {SCHEMA_SOURCE}.dim_date dd
              ON ft.date_key = dd.date_key
            JOIN {SCHEMA_SOURCE}.dim_mcc dmcc
              ON ft.mcc_key = dmcc.mcc_key
            WHERE ft.is_refund = 'No'
              AND ft.amount > 0
            GROUP BY dd.year, dmcc.mcc_code, dmcc.description
        ),
        lagged AS (
            SELECT
                year,
                mcc_code,
                mcc_description,
                total_revenue,
                LAG(total_revenue) OVER (PARTITION BY mcc_code ORDER BY year) AS prev_year_revenue
            FROM yearly_revenue
        )
        SELECT
            year,
            mcc_code,
            mcc_description,
            ROUND(total_revenue::NUMERIC, 2)     AS total_revenue,
            ROUND(prev_year_revenue::NUMERIC, 2) AS prev_year_revenue,
            CASE
                WHEN prev_year_revenue IS NOT NULL AND prev_year_revenue <> 0
                THEN ROUND((100.0 * (total_revenue - prev_year_revenue) / prev_year_revenue)::NUMERIC, 2)
                ELSE NULL
            END AS yoy_growth_pct
        FROM lagged
        ORDER BY year DESC, yoy_growth_pct DESC NULLS LAST;
    """)

    # Create merchant error rate summary
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.merchant_error_rates CASCADE;")
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.merchant_error_rates AS
        SELECT
            dm.merchant_key,
            dm.merchant_id,
            dm.merchant_city,
            dm.merchant_state,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN ft.errors IS NOT NULL THEN 1 ELSE 0 END) AS error_transaction_count,
            ROUND(
                (
                    100.0 * SUM(CASE WHEN ft.errors IS NOT NULL THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0)
                )::NUMERIC,
                2
            ) AS error_rate_pct
        FROM {SCHEMA_SOURCE}.fact_transactions ft
        JOIN {SCHEMA_SOURCE}.dim_merchants dm
          ON ft.merchant_key = dm.merchant_key
        GROUP BY dm.merchant_key, dm.merchant_id, dm.merchant_city, dm.merchant_state
        HAVING COUNT(*) >= 10
        ORDER BY error_rate_pct DESC, total_transactions DESC;
    """)

    # Create geographic revenue summary
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.merchant_geo_revenue CASCADE;")
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.merchant_geo_revenue AS
        SELECT
            COALESCE(dm.merchant_state, 'Unknown') AS merchant_state,
            ROUND(SUM(ft.amount)::NUMERIC, 2)      AS total_revenue,
            COUNT(*)                               AS transaction_count,
            COUNT(DISTINCT dm.merchant_id)         AS merchant_count,
            ROUND(
                (
                    SUM(ft.amount) / NULLIF(COUNT(DISTINCT dm.merchant_id), 0)
                )::NUMERIC,
                2
            ) AS avg_revenue_per_merchant
        FROM {SCHEMA_SOURCE}.fact_transactions ft
        JOIN {SCHEMA_SOURCE}.dim_merchants dm
          ON ft.merchant_key = dm.merchant_key
        WHERE ft.is_refund = 'No'
          AND ft.amount > 0
        GROUP BY COALESCE(dm.merchant_state, 'Unknown')
        ORDER BY total_revenue DESC;
    """)

    cur.close()
    conn.close()
    print("All merchant marts loaded")


if __name__ == "__main__":
    merchant_mart()
