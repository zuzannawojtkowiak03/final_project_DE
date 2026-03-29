import psycopg2

DB_NAME = "project"
USER = "postgres"
PASSWORD = "Password"
HOST = "127.0.0.1"
PORT = "5432"

SCHEMA_SOURCE = "curated"
SCHEMA_TARGET = "finance_mart"


def finance_mart():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create target schema
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")

    # Drop old views and tables
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_finance_by_channel CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_finance_revenue_by_mcc CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_finance_revenue_by_state CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_finance_refund_summary CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_finance_monthly_revenue CASCADE;")

    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.fact_finance_transactions CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_channel CASCADE;")

    # Create channel dimension
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.dim_channel (
            channel_key     INT          PRIMARY KEY,
            source_use_chip VARCHAR(100) UNIQUE,
            channel_group   VARCHAR(50)
        );
    """)
    cur.execute(f"""
        INSERT INTO {SCHEMA_TARGET}.dim_channel (channel_key, source_use_chip, channel_group)
        SELECT
            ROW_NUMBER() OVER (ORDER BY src.use_chip_coalesced) AS channel_key,
            src.use_chip_coalesced AS source_use_chip,
            CASE
                WHEN src.use_chip_coalesced = 'Online Transaction' THEN 'Online'
                WHEN src.use_chip_coalesced = '__UNKNOWN__'        THEN 'Unknown'
                ELSE 'In-Store'
            END AS channel_group
        FROM (
            SELECT DISTINCT COALESCE(use_chip, '__UNKNOWN__') AS use_chip_coalesced
            FROM {SCHEMA_SOURCE}.fact_transactions
        ) src;
    """)

    # Create core finance fact table
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.fact_finance_transactions AS
        SELECT
            ft.transaction_id,
            ft.date_key,
            ft.merchant_key,
            ft.mcc_key,
            ch.channel_key,
            CASE WHEN ft.amount > 0 THEN ft.amount ELSE 0 END          AS gross_revenue,
            CASE WHEN ft.amount < 0 THEN ABS(ft.amount) ELSE 0 END     AS refund_amount,
            ft.amount                                                   AS net_amount,
            1                                                           AS transaction_count,
            CASE WHEN ft.amount < 0 THEN 1 ELSE 0 END                  AS refund_flag,
            CASE
                WHEN ft.errors IS NOT NULL
                 AND ft.errors <> ''
                 AND ft.errors <> 'No Error'
                THEN 1 ELSE 0
            END                                                         AS error_flag
        FROM {SCHEMA_SOURCE}.fact_transactions ft
        JOIN {SCHEMA_TARGET}.dim_channel ch
          ON COALESCE(ft.use_chip, '__UNKNOWN__') = ch.source_use_chip;
    """)

    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_finance_transactions
        ADD PRIMARY KEY (transaction_id);
    """)

    # Add foreign key
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_finance_transactions
        ADD CONSTRAINT fk_finance_channel
        FOREIGN KEY (channel_key) REFERENCES {SCHEMA_TARGET}.dim_channel(channel_key);
    """)

    # Create indexes
    cur.execute(f"CREATE INDEX idx_fft_date     ON {SCHEMA_TARGET}.fact_finance_transactions(date_key);")
    cur.execute(f"CREATE INDEX idx_fft_merchant ON {SCHEMA_TARGET}.fact_finance_transactions(merchant_key);")
    cur.execute(f"CREATE INDEX idx_fft_mcc      ON {SCHEMA_TARGET}.fact_finance_transactions(mcc_key);")
    cur.execute(f"CREATE INDEX idx_fft_channel  ON {SCHEMA_TARGET}.fact_finance_transactions(channel_key);")

    # Create monthly revenue view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_finance_monthly_revenue AS
        SELECT
            d.year,
            d.month,
            d.month_name,
            ROUND(SUM(f.gross_revenue)::NUMERIC, 2) AS total_revenue,
            SUM(f.transaction_count)                AS transaction_count,
            ROUND((AVG(CASE WHEN f.gross_revenue > 0 THEN f.gross_revenue END))::NUMERIC, 2) AS avg_transaction
        FROM {SCHEMA_TARGET}.fact_finance_transactions f
        JOIN {SCHEMA_SOURCE}.dim_date d
          ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month;
    """)

    # Create refund summary view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_finance_refund_summary AS
        SELECT
            d.year,
            d.month,
            d.month_name,
            SUM(f.transaction_count)                 AS total_transactions,
            SUM(f.refund_flag)                       AS refund_count,
            ROUND(SUM(f.refund_amount)::NUMERIC, 2)  AS refund_amount,
            ROUND(
                (100.0 * SUM(f.refund_flag)
                / NULLIF(SUM(f.transaction_count), 0))::NUMERIC,
                2
            ) AS refund_pct_by_count,
            ROUND(
                (100.0 * SUM(f.refund_amount)
                / NULLIF(SUM(f.gross_revenue), 0))::NUMERIC,
                2
            ) AS refund_pct_by_amount
        FROM {SCHEMA_TARGET}.fact_finance_transactions f
        JOIN {SCHEMA_SOURCE}.dim_date d
          ON f.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month;
    """)

    # Create revenue by state view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_finance_revenue_by_state AS
        SELECT
            COALESCE(m.merchant_state, 'Unknown')    AS merchant_state,
            ROUND(SUM(f.gross_revenue)::NUMERIC, 2)  AS gross_revenue,
            ROUND(SUM(f.refund_amount)::NUMERIC, 2)  AS refund_amount,
            ROUND(SUM(f.net_amount)::NUMERIC, 2)     AS net_revenue,
            SUM(f.transaction_count)                 AS transaction_count,
            ROUND((AVG(CASE WHEN f.gross_revenue > 0 THEN f.gross_revenue END))::NUMERIC, 2) AS avg_transaction
        FROM {SCHEMA_TARGET}.fact_finance_transactions f
        JOIN {SCHEMA_SOURCE}.dim_merchants m
          ON f.merchant_key = m.merchant_key
        GROUP BY COALESCE(m.merchant_state, 'Unknown')
        ORDER BY net_revenue DESC;
    """)

    # Create revenue by MCC view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_finance_revenue_by_mcc AS
        SELECT
            mc.mcc_code,
            COALESCE(mc.description, 'Unknown')      AS description,
            ROUND(SUM(f.gross_revenue)::NUMERIC, 2)  AS gross_revenue,
            ROUND(SUM(f.refund_amount)::NUMERIC, 2)  AS refund_amount,
            ROUND(SUM(f.net_amount)::NUMERIC, 2)     AS net_revenue,
            SUM(f.transaction_count)                 AS transaction_count,
            ROUND((AVG(CASE WHEN f.gross_revenue > 0 THEN f.gross_revenue END))::NUMERIC, 2) AS avg_transaction
        FROM {SCHEMA_TARGET}.fact_finance_transactions f
        JOIN {SCHEMA_SOURCE}.dim_mcc mc
          ON f.mcc_key = mc.mcc_key
        GROUP BY mc.mcc_code, COALESCE(mc.description, 'Unknown')
        ORDER BY net_revenue DESC;
    """)

    # Create revenue by channel view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_finance_by_channel AS
        SELECT
            ch.channel_group,
            ch.source_use_chip,
            ROUND(SUM(f.gross_revenue)::NUMERIC, 2)  AS gross_revenue,
            ROUND(SUM(f.refund_amount)::NUMERIC, 2)  AS refund_amount,
            ROUND(SUM(f.net_amount)::NUMERIC, 2)     AS net_revenue,
            SUM(f.transaction_count)                 AS transaction_count,
            SUM(f.error_flag)                        AS error_count
        FROM {SCHEMA_TARGET}.fact_finance_transactions f
        JOIN {SCHEMA_TARGET}.dim_channel ch
          ON f.channel_key = ch.channel_key
        GROUP BY ch.channel_group, ch.source_use_chip
        ORDER BY net_revenue DESC;
    """)

    cur.close()
    conn.close()
    print("Finance mart built successfully.")


if __name__ == "__main__":
    finance_mart()
