import io
import psycopg2
import pandas as pd

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "transformation"
SCHEMA_TARGET = "curated"


def fact_transactions():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    # Fetch query results into pandas without using pd.read_sql
    def fetch(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=cols)

    transactions = fetch(conn, f'SELECT * FROM "{SCHEMA_SOURCE}"."transactions_data"')
    dim_customers = fetch(conn, f'SELECT customer_key, customer_id FROM "{SCHEMA_TARGET}"."dim_customers"')
    dim_cards = fetch(conn, f'SELECT card_key, card_id FROM "{SCHEMA_TARGET}"."dim_cards"')
    dim_merchants = fetch(conn, f'SELECT merchant_key, merchant_id FROM "{SCHEMA_TARGET}"."dim_merchants"')
    dim_mcc = fetch(conn, f'SELECT mcc_key, mcc_code FROM "{SCHEMA_TARGET}"."dim_mcc"')
    dim_date = fetch(conn, f'SELECT date_key, date FROM "{SCHEMA_TARGET}"."dim_date"')

    conn.close()

    # Clean transaction columns
    transactions["id"] = pd.to_numeric(transactions["id"], errors="coerce").astype("Int64")
    transactions["client_id"] = pd.to_numeric(transactions["client_id"], errors="coerce").astype("Int64")
    transactions["card_id"] = pd.to_numeric(transactions["card_id"], errors="coerce").astype("Int64")
    transactions["merchant_id"] = pd.to_numeric(transactions["merchant_id"], errors="coerce").astype("Int64")
    transactions["mcc"] = pd.to_numeric(transactions["mcc"], errors="coerce").astype("Int64")
    transactions["amount"] = pd.to_numeric(transactions["amount"], errors="coerce")
    transactions["date"] = pd.to_datetime(transactions["date"], errors="coerce")

    text_cols = ["use_chip", "errors"]
    for col in text_cols:
        transactions[col] = transactions[col].astype("string").str.strip()

    # Clean dimension join columns
    dim_customers["customer_id"] = pd.to_numeric(dim_customers["customer_id"], errors="coerce").astype("Int64")
    dim_cards["card_id"] = pd.to_numeric(dim_cards["card_id"], errors="coerce").astype("Int64")
    dim_merchants["merchant_id"] = pd.to_numeric(dim_merchants["merchant_id"], errors="coerce").astype("Int64")
    dim_mcc["mcc_code"] = pd.to_numeric(dim_mcc["mcc_code"], errors="coerce").astype("Int64")

    # Build reporting flags
    transactions["is_refund"] = transactions["is_refund"].map({True: "Yes", False: "No"}).fillna("No")
    transactions["has_error"] = transactions["errors"].notna().map({True: "Yes", False: "No"})
    transactions["is_online"] = transactions["is_online"].map({True: "Yes", False: "No"}).fillna("No")

    # Match transaction timestamps to date dimension
    transactions["date_only"] = transactions["date"].dt.date
    dim_date["date"] = pd.to_datetime(dim_date["date"], errors="coerce").dt.date

    # Join transaction data to all dimensions
    fact_df = transactions.merge(
        dim_customers,
        how="left",
        left_on="client_id",
        right_on="customer_id"
    )

    fact_df = fact_df.merge(
        dim_cards,
        how="left",
        on="card_id"
    )

    fact_df = fact_df.merge(
        dim_merchants,
        how="left",
        on="merchant_id"
    )

    fact_df = fact_df.merge(
        dim_mcc,
        how="left",
        left_on="mcc",
        right_on="mcc_code"
    )

    fact_df = fact_df.merge(
        dim_date,
        how="left",
        left_on="date_only",
        right_on="date"
    )

    fact_transactions = pd.DataFrame({
        "transaction_id": fact_df["id"],
        "date_key": fact_df["date_key"],
        "customer_key": fact_df["customer_key"],
        "card_key": fact_df["card_key"],
        "merchant_key": fact_df["merchant_key"],
        "mcc_key": fact_df["mcc_key"],
        "amount": fact_df["amount"],
        "use_chip": fact_df["use_chip"],
        "errors": fact_df["errors"],
        "is_refund": fact_df["is_refund"],
        "has_error": fact_df["has_error"],
        "is_online": fact_df["is_online"]
    })

    # Keep only valid unique transactions
    fact_transactions = fact_transactions.dropna(subset=["transaction_id"]).copy()
    fact_transactions = fact_transactions.drop_duplicates(subset=["transaction_id"], keep="first")
    fact_transactions = fact_transactions.sort_values("transaction_id").reset_index(drop=True)

    print(f"fact_transactions rows: {len(fact_transactions)}")
    print("Missing customer_key:", fact_transactions["customer_key"].isna().sum())
    print("Missing card_key:", fact_transactions["card_key"].isna().sum())
    print("Missing merchant_key:", fact_transactions["merchant_key"].isna().sum())
    print("Missing mcc_key:", fact_transactions["mcc_key"].isna().sum())
    print("Missing date_key:", fact_transactions["date_key"].isna().sum())

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."fact_transactions" CASCADE;')

    # Create the fact table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."fact_transactions" (
            transaction_id BIGINT PRIMARY KEY,
            date_key       INT,
            customer_key   INT,
            card_key       INT,
            merchant_key   INT,
            mcc_key        INT,
            amount         NUMERIC(12,2),
            use_chip       VARCHAR(50),
            errors         VARCHAR(255),
            is_refund      VARCHAR(5),
            has_error      VARCHAR(5),
            is_online      VARCHAR(5)
        );
    """)
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    fact_transactions.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY "{SCHEMA_TARGET}"."fact_transactions"
        (transaction_id, date_key, customer_key, card_key, merchant_key, mcc_key,
         amount, use_chip, errors, is_refund, has_error, is_online)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """,
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.fact_transactions loaded")


if __name__ == "__main__":
    fact_transactions()
