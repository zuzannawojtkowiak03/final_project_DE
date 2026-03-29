import io
import csv
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_NAME = "project"
PASSWORD = "KochamEmila2506!"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "ingestion"
SCHEMA_TARGET = "transformation"
TABLE = "transactions_data"
CHUNK_SIZE = 300_000

INSERT_COLS = [
    "id", "date", "client_id", "card_id", "amount", "refund_amount",
    "use_chip", "merchant_id", "merchant_city", "merchant_state", "zip",
    "mcc", "errors", "is_refund", "is_online",
    "transaction_year", "transaction_month", "transaction_quarter",
    "transaction_hour", "day_of_week",
]

# Valid state abbreviations used for standardization
VALID_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR","GU","VI","AS","MP",
}

# State name mapping for inconsistent values
STATE_NAME_MAP = {
    "california": "CA", "new york": "NY", "texas": "TX",
    "florida": "FL", "illinois": "IL", "pennsylvania": "PA",
    "ohio": "OH"
}


def get_connection():
    return psycopg2.connect(host=HOST, port=PORT, database=DB_NAME, user=USER, password=PASSWORD)


# Create the target table for transformed transactions
def setup_target_table(conn):
    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};
        DROP TABLE IF EXISTS {SCHEMA_TARGET}.{TABLE};

        CREATE TABLE {SCHEMA_TARGET}.{TABLE} (
            id BIGINT PRIMARY KEY,
            date TIMESTAMP,
            client_id INTEGER,
            card_id INTEGER,
            amount DECIMAL(10,2),
            refund_amount DECIMAL(10,2),
            use_chip VARCHAR(50),
            merchant_id INTEGER,
            merchant_city VARCHAR(255),
            merchant_state VARCHAR(10),
            zip VARCHAR(20),
            mcc INTEGER,
            errors VARCHAR(255),
            is_refund BOOLEAN,
            is_online BOOLEAN,
            transaction_year SMALLINT,
            transaction_month SMALLINT,
            transaction_quarter SMALLINT,
            transaction_hour SMALLINT,
            day_of_week VARCHAR(10)
        );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("Target table ready.")


# Standardize merchant state values
def clean_state(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    upper = s.upper()
    if upper in VALID_US_STATES:
        return upper
    return STATE_NAME_MAP.get(s.lower(), None)


def clean_transactions(df):
    original_count = len(df)

    # Remove duplicate transaction IDs
    df = df.drop_duplicates(subset=["id"], keep="first")

    # Clean transaction amount
    df["amount"] = (
        df["amount"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # Separate refund amounts from regular amounts
    df["refund_amount"] = df["amount"].where(df["amount"] < 0).abs()
    df["is_refund"] = df["refund_amount"].notna()
    df["amount"] = df["amount"].abs()

    # Keep only valid positive amounts
    df = df[df["amount"].notna() & (df["amount"] >= 0.01)].copy()

    # Convert date and derive time-based features
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["transaction_year"] = df["date"].dt.year
    df["transaction_month"] = df["date"].dt.month
    df["transaction_quarter"] = df["date"].dt.quarter
    df["transaction_hour"] = df["date"].dt.hour
    df["day_of_week"] = df["date"].dt.day_name()

    # Standardize use_chip values
    df["use_chip"] = (
        df["use_chip"]
        .astype("string")
        .str.replace(" Transaction", "", regex=False)
        .str.strip()
    )

    # Clean merchant city values
    df["merchant_city"] = df["merchant_city"].replace(
        ["", "NaN", "nan", "None"], np.nan
    )
    df["merchant_city"] = df["merchant_city"].where(pd.notna(df["merchant_city"]), None)
    df["merchant_city"] = df["merchant_city"].apply(
        lambda x: x.strip() if isinstance(x, str) else x
    )

    # Detect online transactions
    online_mask = (
        df["use_chip"].astype(str).str.lower() == "online"
    ) | (
        df["merchant_city"].astype(str).str.lower() == "online"
    )

    df["is_online"] = online_mask

    # Standardize fields for online transactions
    df.loc[online_mask, "merchant_city"] = "Online"
    df.loc[online_mask, "merchant_state"] = None
    df.loc[online_mask, "zip"] = None

    # Clean merchant state values
    df["merchant_state"] = df["merchant_state"].apply(clean_state)

    # Clean ZIP codes
    df["zip"] = df["zip"].replace(["", "NaN", "nan"], np.nan)
    df["zip"] = df["zip"].where(pd.notna(df["zip"]), None)
    df["zip"] = df["zip"].apply(
        lambda x: str(x).replace(".0", "").strip() if x is not None else None
    )

    # Keep errors as NULL when missing
    df["errors"] = df["errors"].where(
        df["errors"].notna() & (df["errors"].astype(str).str.lower() != "nan"),
        None
    )

    print(f"Cleaned: {original_count} → {len(df)}")
    return df


# Insert one cleaned chunk into PostgreSQL
def insert_chunk(conn, df):
    df = df.where(pd.notnull(df), None)

    def safe(v):
        if v is None or v is pd.NA:
            return None
        if hasattr(v, "item"):
            v = v.item()
        try:
            if pd.isna(v):
                return None
        except:
            pass
        return v

    rows = [tuple(safe(v) for v in row) for row in df[INSERT_COLS].itertuples(index=False)]

    sql = f"""
        INSERT INTO {SCHEMA_TARGET}.{TABLE}
        ({", ".join(INSERT_COLS)})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=10000)

    conn.commit()
    print(f"Inserted {len(rows)} rows")


def transform_transactions():
    source_conn = get_connection()
    target_conn = get_connection()

    try:
        setup_target_table(target_conn)

        with source_conn.cursor() as info:
            info.execute(f"SELECT * FROM {SCHEMA_SOURCE}.{TABLE} LIMIT 0")
            cols = [d[0] for d in info.description]

        # Stream source data in chunks to avoid loading everything into memory
        with source_conn.cursor(name="stream") as cur:
            cur.itersize = CHUNK_SIZE
            cur.execute(f"SELECT * FROM {SCHEMA_SOURCE}.{TABLE}")

            while True:
                rows = cur.fetchmany(CHUNK_SIZE)
                if not rows:
                    break

                df = pd.DataFrame(rows, columns=cols)
                cleaned = clean_transactions(df)
                insert_chunk(target_conn, cleaned)

    finally:
        source_conn.close()
        target_conn.close()
        print("Done.")


if __name__ == "__main__":
    transform_transactions()
