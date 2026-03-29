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


def dim_mcc():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    df = pd.read_sql(f'SELECT * FROM "{SCHEMA_SOURCE}"."mcc_data";', conn)
    conn.close()

    # Clean MCC columns
    df["code"] = pd.to_numeric(df["code"], errors="coerce").astype("Int64")

    text_cols = ["description", "notes", "updated_by"]
    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()

    dim_mcc = pd.DataFrame({
        "mcc_code": df["code"],
        "description": df["description"],
        "notes": df["notes"],
        "updated_by": df["updated_by"]
    })

    # Keep only valid unique MCC codes
    dim_mcc = dim_mcc.dropna(subset=["mcc_code"]).copy()
    dim_mcc = dim_mcc.drop_duplicates(subset=["mcc_code"], keep="first")
    dim_mcc = dim_mcc.sort_values("mcc_code").reset_index(drop=True)
    dim_mcc.insert(0, "mcc_key", dim_mcc.index + 1)

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."dim_mcc" CASCADE;')

    # Create the MCC dimension table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."dim_mcc" (
            mcc_key INT PRIMARY KEY,
            mcc_code INT UNIQUE,
            description VARCHAR(255),
            notes VARCHAR(100),
            updated_by VARCHAR(100)
        );
    """)
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    dim_mcc.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY "{SCHEMA_TARGET}"."dim_mcc"
        (mcc_key, mcc_code, description, notes, updated_by)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """,
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.dim_mcc loaded")


if __name__ == "__main__":
    dim_mcc()
