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


def dim_date():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    # Read distinct calendar dates from transactions
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT DATE(date) AS date
        FROM "{SCHEMA_SOURCE}"."transactions_data"
        WHERE date IS NOT NULL
    """)
    df = pd.DataFrame(cur.fetchall(), columns=["date"])
    cur.close()
    conn.close()

    # Convert to datetime and remove invalid rows
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    dim_date = pd.DataFrame({
        "date": df["date"]
    })

    # Derive calendar attributes
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["month_name"] = dim_date["date"].dt.strftime("%B")
    dim_date["day_name"] = dim_date["date"].dt.strftime("%A")
    dim_date["is_weekend"] = dim_date["day_name"].isin(["Saturday", "Sunday"])

    dim_date = dim_date.sort_values("date").reset_index(drop=True)
    dim_date.insert(0, "date_key", dim_date.index + 1)

    print(f"dim_date rows: {len(dim_date)}")

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."dim_date" CASCADE;')

    # Create the date dimension table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."dim_date" (
            date_key    INT PRIMARY KEY,
            date        DATE UNIQUE,
            year        INT,
            month       INT,
            day         INT,
            month_name  VARCHAR(20),
            day_name    VARCHAR(20),
            is_weekend  BOOLEAN
        );
    """)
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    dim_date.to_csv(buffer, index=False, header=False, na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY "{SCHEMA_TARGET}"."dim_date"
        (date_key, date, year, month, day, month_name, day_name, is_weekend)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """,
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.dim_date loaded")


if __name__ == "__main__":
    dim_date()
