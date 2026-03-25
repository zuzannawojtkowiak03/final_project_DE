import pyodbc
import pandas as pd
from sqlalchemy import create_engine

DRIVER_PATH   = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME       = 'project'
PASSWORD      = '563634851'
PORT          = '1234'
SCHEMA_SOURCE = 'ingestion'
SCHEMA_TARGET = 'transformation'

conn_str = (
    f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};"
    f"UID=postgres;PWD={PASSWORD};PORT={PORT};"
)

def clean_code(value) -> str | None:
    """
    Normalises MCC code values:
      "3000"   → 3000   (strip surrounding double-quotes)
      MCC3066  → 3066   (strip leading MCC prefix, case-insensitive)
      NOTE     → None   (non-numeric marker rows → dropped)
      COMMENT  → None   (same)
    Returns the cleaned numeric string, or None to signal row deletion.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    s = s.strip('"')                        # rule 1: remove surrounding quotes
    s = s.strip("'")                        # also handle single-quotes just in case
    if s.upper().startswith("MCC"):         # rule 2: remove MCC prefix
        s = s[3:]
    s = s.strip()
    if not s.isnumeric():                   # rule 3: non-numeric → flag for deletion
        return None
    return s                                # return as string; cast to int below


def clean_description(value) -> str | None:
    """
    Standardises description to Title Case.
      STEEL PRODUCTS → Steel Products
      steel drums    → Steel Drums
    """
    if pd.isna(value):
        return None
    return str(value).strip().title()


# MAIN TRANSFORM

def transform_mcc():
    # ── Read source data via SQLAlchemy (avoids pandas UserWarning) ──────
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:{PORT}/{DB_NAME}"
    )
    with engine.connect() as sa_conn:
        df = pd.read_sql(f"SELECT * FROM {SCHEMA_SOURCE}.mcc_data", sa_conn)

    print(f"  Source rows: {len(df)}")

    # ── 1 & 2. Clean code column (strip quotes, strip MCC prefix) ────────
    df["code"] = df["code"].apply(clean_code)

    # ── 3. Drop NOTE / COMMENT rows (code is None after clean_code) ──────
    non_data_mask = df["code"].isna()
    if non_data_mask.any():
        print(f"  Dropped {non_data_mask.sum()} non-data row(s) "
              f"(NOTE / COMMENT / blank code).")
    df = df[~non_data_mask].copy()

    # Safe to cast to int now that all codes are numeric strings
    df["code"] = df["code"].astype(int)

    # ── 4. Title Case description ────────────────────────────────────────
    df["description"] = df["description"].apply(clean_description)

    # ── 5. Deduplicate on code (keep first occurrence) ───────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    dupes = before - len(df)
    if dupes:
        print(f"  Dropped {dupes} duplicate code row(s).")

    print(f"  Clean rows to load: {len(df)}")

    # Write to target schema
    raw_conn = pyodbc.connect(conn_str, autocommit=False)
    try:
        cursor = raw_conn.cursor()

        # Step A: drop and commit immediately so the table is truly gone
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.mcc_data;")
        raw_conn.commit()

        # Step B: create fresh table
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_TARGET}.mcc_data (
                code        INT,
                description VARCHAR(255),
                notes       VARCHAR(100),
                updated_by  VARCHAR(100)
            );
        """)

        # Step C: bulk insert
        insert_sql = (
            f"INSERT INTO {SCHEMA_TARGET}.mcc_data VALUES "
            f"({','.join(['?'] * len(df.columns))})"
        )
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, [tuple(x) for x in df.values])
        raw_conn.commit()
        print(f"  MCC data transformed and loaded: {len(df)} rows.")

    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


if __name__ == "__main__":
    transform_mcc()
