import psycopg2

DB_NAME = "project"
USER = "postgres"
PASSWORD = "Password"
HOST = "127.0.0.1"
PORT = "5432"

SCHEMA_SOURCE = "curated"
SCHEMA_TARGET = "customer_mart"


def customer_mart():
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
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_customer_suspicion_profile CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_suspicious_transactions CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_customer_active_cards CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_customer_channel_behavior CASCADE;")
    cur.execute(f"DROP VIEW IF EXISTS {SCHEMA_TARGET}.vw_customer_ltv CASCADE;")

    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.fact_customer_transactions CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_channel CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_card CASCADE;")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.dim_customer CASCADE;")

    # Create customer dimension
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.dim_customer AS
        SELECT DISTINCT
            customer_key,
            customer_id
        FROM {SCHEMA_SOURCE}.dim_customers;
    """)
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.dim_customer
        ADD PRIMARY KEY (customer_key);
    """)

    # Create card dimension
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.dim_card AS
        SELECT DISTINCT
            card_key,
            credit_limit
        FROM {SCHEMA_SOURCE}.dim_cards;
    """)
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.dim_card
        ADD PRIMARY KEY (card_key);
    """)

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

    # Create core customer fact table
    cur.execute(f"""
        CREATE TABLE {SCHEMA_TARGET}.fact_customer_transactions AS
        SELECT
            ft.transaction_id,
            ft.customer_key,
            ft.date_key,
            ft.card_key,
            ch.channel_key,
            ft.amount,
            1                                                   AS transaction_count,
            CASE WHEN ft.amount > 0 THEN 1 ELSE 0 END           AS positive_transaction_flag,
            CASE WHEN ft.errors IS NOT NULL THEN 1 ELSE 0 END   AS error_transaction_flag,
            ft.errors
        FROM {SCHEMA_SOURCE}.fact_transactions ft
        JOIN {SCHEMA_TARGET}.dim_channel ch
          ON COALESCE(ft.use_chip, '__UNKNOWN__') = ch.source_use_chip;
    """)

    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_customer_transactions
        ADD PRIMARY KEY (transaction_id);
    """)

    # Add foreign keys
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_customer_transactions
        ADD CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_key) REFERENCES {SCHEMA_TARGET}.dim_customer(customer_key);
    """)
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_customer_transactions
        ADD CONSTRAINT fk_fact_card
        FOREIGN KEY (card_key) REFERENCES {SCHEMA_TARGET}.dim_card(card_key);
    """)
    cur.execute(f"""
        ALTER TABLE {SCHEMA_TARGET}.fact_customer_transactions
        ADD CONSTRAINT fk_fact_channel
        FOREIGN KEY (channel_key) REFERENCES {SCHEMA_TARGET}.dim_channel(channel_key);
    """)

    # Create indexes
    cur.execute(f"CREATE INDEX idx_fct_customer   ON {SCHEMA_TARGET}.fact_customer_transactions(customer_key);")
    cur.execute(f"CREATE INDEX idx_fct_date       ON {SCHEMA_TARGET}.fact_customer_transactions(date_key);")
    cur.execute(f"CREATE INDEX idx_fct_card       ON {SCHEMA_TARGET}.fact_customer_transactions(card_key);")
    cur.execute(f"CREATE INDEX idx_fct_channel    ON {SCHEMA_TARGET}.fact_customer_transactions(channel_key);")
    cur.execute(f"CREATE INDEX idx_fct_transaction ON {SCHEMA_TARGET}.fact_customer_transactions(transaction_id);")

    # Create customer lifetime value view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_customer_ltv AS
        SELECT
            dc.customer_key,
            dc.customer_id,
            SUM(CASE WHEN fct.amount > 0 THEN fct.amount ELSE 0 END)             AS total_spend,
            COUNT(*) FILTER (WHERE fct.amount > 0)                               AS positive_transaction_count,
            ROUND((AVG(fct.amount) FILTER (WHERE fct.amount > 0))::NUMERIC, 2)   AS avg_transaction_amount,
            MIN(dd.date) FILTER (WHERE fct.amount > 0)                           AS first_transaction_date,
            MAX(dd.date) FILTER (WHERE fct.amount > 0)                           AS last_transaction_date,
            MAX(dd.date) FILTER (WHERE fct.amount > 0)
              - MIN(dd.date) FILTER (WHERE fct.amount > 0)                       AS lifespan_days
        FROM {SCHEMA_TARGET}.fact_customer_transactions fct
        JOIN {SCHEMA_TARGET}.dim_customer dc
          ON fct.customer_key = dc.customer_key
        JOIN {SCHEMA_SOURCE}.dim_date dd
          ON fct.date_key = dd.date_key
        GROUP BY dc.customer_key, dc.customer_id;
    """)

    # Create customer channel behavior view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_customer_channel_behavior AS
        SELECT
            dc.customer_key,
            dc.customer_id,
            SUM(CASE WHEN ch.channel_group = 'Online'   AND fct.amount > 0 THEN fct.amount ELSE 0 END) AS online_spend,
            COUNT(*) FILTER (WHERE ch.channel_group = 'Online'   AND fct.amount > 0)                   AS online_transaction_count,
            SUM(CASE WHEN ch.channel_group = 'In-Store' AND fct.amount > 0 THEN fct.amount ELSE 0 END) AS instore_spend,
            COUNT(*) FILTER (WHERE ch.channel_group = 'In-Store' AND fct.amount > 0)                   AS instore_transaction_count,
            CASE
                WHEN COUNT(*) FILTER (WHERE ch.channel_group = 'Online'   AND fct.amount > 0)
                   > COUNT(*) FILTER (WHERE ch.channel_group = 'In-Store' AND fct.amount > 0)
                THEN 'Online'
                WHEN COUNT(*) FILTER (WHERE ch.channel_group = 'Online'   AND fct.amount > 0)
                   < COUNT(*) FILTER (WHERE ch.channel_group = 'In-Store' AND fct.amount > 0)
                THEN 'In-Store'
                ELSE 'Balanced'
            END AS preferred_channel
        FROM {SCHEMA_TARGET}.fact_customer_transactions fct
        JOIN {SCHEMA_TARGET}.dim_customer dc
          ON fct.customer_key = dc.customer_key
        JOIN {SCHEMA_TARGET}.dim_channel ch
          ON fct.channel_key = ch.channel_key
        GROUP BY dc.customer_key, dc.customer_id;
    """)

    # Create active cards view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_customer_active_cards AS
        WITH active_cards AS (
            SELECT DISTINCT
                customer_key,
                card_key
            FROM {SCHEMA_TARGET}.fact_customer_transactions
            WHERE amount > 0
        )
        SELECT
            dc.customer_key,
            dc.customer_id,
            COUNT(ac.card_key)                         AS active_card_count,
            SUM(dca.credit_limit)                      AS total_credit_limit,
            ROUND(AVG(dca.credit_limit)::NUMERIC, 2)   AS avg_credit_limit
        FROM active_cards ac
        JOIN {SCHEMA_TARGET}.dim_customer dc
          ON ac.customer_key = dc.customer_key
        JOIN {SCHEMA_TARGET}.dim_card dca
          ON ac.card_key = dca.card_key
        GROUP BY dc.customer_key, dc.customer_id;
    """)

    # Create suspicious transaction view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_suspicious_transactions AS
        WITH customer_stats AS (
            SELECT
                customer_key,
                AVG(amount)         AS avg_spend,
                STDDEV_SAMP(amount) AS stddev_spend,
                COUNT(*)            AS txn_count
            FROM {SCHEMA_TARGET}.fact_customer_transactions
            WHERE amount > 0
            GROUP BY customer_key
        )
        SELECT
            fct.transaction_id,
            dc.customer_key,
            dc.customer_id,
            dd.date         AS transaction_date,
            ch.channel_group,
            fct.amount,
            cs.avg_spend    AS customer_avg_spend,
            cs.stddev_spend AS customer_stddev,
            fct.errors,
            CASE
                WHEN fct.errors IS NOT NULL
                THEN 'Transaction error recorded'
                WHEN cs.txn_count >= 2
                 AND cs.stddev_spend IS NOT NULL
                 AND fct.amount > cs.avg_spend + (3 * cs.stddev_spend)
                THEN 'Amount > 3 std dev above customer average'
                ELSE 'Not flagged'
            END AS suspicion_reason
        FROM {SCHEMA_TARGET}.fact_customer_transactions fct
        JOIN customer_stats cs
          ON fct.customer_key = cs.customer_key
        JOIN {SCHEMA_TARGET}.dim_customer dc
          ON fct.customer_key = dc.customer_key
        JOIN {SCHEMA_SOURCE}.dim_date dd
          ON fct.date_key = dd.date_key
        JOIN {SCHEMA_TARGET}.dim_channel ch
          ON fct.channel_key = ch.channel_key
        WHERE
            fct.errors IS NOT NULL
            OR (
                cs.txn_count >= 2
                AND cs.stddev_spend IS NOT NULL
                AND fct.amount > cs.avg_spend + (3 * cs.stddev_spend)
            );
    """)

    # Create customer suspicion summary view
    cur.execute(f"""
        CREATE VIEW {SCHEMA_TARGET}.vw_customer_suspicion_profile AS
        SELECT
            customer_key,
            customer_id,
            COUNT(*) AS suspicious_transaction_count,
            SUM(CASE WHEN suspicion_reason = 'Transaction error recorded' THEN 1 ELSE 0 END) AS error_flag_count,
            SUM(CASE WHEN suspicion_reason = 'Amount > 3 std dev above customer average' THEN 1 ELSE 0 END) AS amount_outlier_count,
            MAX(transaction_date) AS last_suspicious_transaction_date
        FROM {SCHEMA_TARGET}.vw_suspicious_transactions
        GROUP BY customer_key, customer_id;
    """)

    cur.close()
    conn.close()
    print("Customer mart built successfully.")


if __name__ == "__main__":
    customer_mart()
