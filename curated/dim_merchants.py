import io
import csv
import psycopg2
import pandas as pd

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "transformation"
SCHEMA_TARGET = "curated"


def dim_merchants():
    conn = psycopg2.connect(host=HOST, port=PORT, database=DB_NAME, user=USER, password=PASSWORD)
    df = pd.read_sql(
        f"""
        SELECT merchant_id, merchant_city, merchant_state, zip
        FROM "{SCHEMA_SOURCE}"."transactions_data"
        WHERE merchant_id IS NOT NULL;
        """,
        conn
    )
    conn.close()

    # Clean merchant columns
    df["merchant_id"] = pd.to_numeric(df["merchant_id"], errors="coerce").astype("Int64")
    for col in ["merchant_city", "merchant_state", "zip"]:
        df[col] = df[col].astype("string").str.strip()

    df = df.dropna(subset=["merchant_id"]).copy()

    # Keep the most frequent location for each merchant
    merchant_counts = (
        df.groupby(["merchant_id", "merchant_city", "merchant_state", "zip"], dropna=False)
          .size()
          .reset_index(name="cnt")
    )
    merchant_counts = (
        merchant_counts
        .sort_values(
            by=["merchant_id", "cnt", "merchant_city", "merchant_state", "zip"],
            ascending=[True, False, True, True, True]
        )
        .drop_duplicates(subset=["merchant_id"], keep="first")
        .reset_index(drop=True)
    )

    dim = pd.DataFrame({
        "merchant_id": merchant_counts["merchant_id"],
        "merchant_city": merchant_counts["merchant_city"],
        "merchant_state": merchant_counts["merchant_state"],
        "zip": merchant_counts["zip"],
    })

    dim = dim.sort_values("merchant_id").reset_index(drop=True)
    dim.insert(0, "merchant_key", dim.index + 1)

    conn = psycopg2.connect(host=HOST, port=PORT, database=DB_NAME, user=USER, password=PASSWORD)
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."dim_merchants" CASCADE;')

    # Create the merchant dimension table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."dim_merchants" (
            merchant_key    INT PRIMARY KEY,
            merchant_id     INT UNIQUE,
            merchant_city   VARCHAR(100),
            merchant_state  VARCHAR(20),
            zip             VARCHAR(20)
        );
    """)
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    dim.to_csv(buffer, index=False, header=False, na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    cur.copy_expert(
        f"""COPY "{SCHEMA_TARGET}"."dim_merchants"
        (merchant_key, merchant_id, merchant_city, merchant_state, zip)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')""",
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.dim_merchants loaded")


if __name__ == "__main__":
    dim_merchants()
