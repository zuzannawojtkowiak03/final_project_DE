#Source rows: 13,305,915

import io
import csv
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# CONFIGURATION

DB_NAME       = 'project'
PASSWORD      = '563634851'
PORT          = '1234'
HOST          = 'localhost'
USER          = 'postgres'
SCHEMA_SOURCE = 'ingestion'
SCHEMA_TARGET = 'transformation'
CHUNK_SIZE    = 300_000   # rows per chunk — tune to available RAM

# REFERENCE DATA

VALID_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
    'DC','PR','GU','VI','AS','MP',
}

STATE_NAME_MAP = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
    'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
    'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
    'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
    'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR',
    'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
    'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
    'district of columbia': 'DC', 'washington dc': 'DC', 'washington d.c.': 'DC',
    'puerto rico': 'PR', 'guam': 'GU', 'virgin islands': 'VI',
    'american samoa': 'AS',
}

# VECTORISED CLEANING

def clean_amount(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.strip()
              .str.replace(r'[$,"]', '', regex=True)
              .pipe(pd.to_numeric, errors='coerce')
    )

def clean_errors(series: pd.Series) -> pd.Series:
    stripped = series.astype(str).str.strip()
    mask = series.notna() & (stripped != '') & (stripped.str.lower() != 'nan')
    return stripped.where(mask, other=None)

def clean_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors='coerce')
    return parsed.dt.strftime('%Y-%m-%d').where(parsed.notna(), other=None)

def clean_merchant_state(series: pd.Series) -> pd.Series:
    upper  = series.astype(str).str.strip().str.upper()
    lower  = series.astype(str).str.strip().str.lower()
    result = upper.where(upper.isin(VALID_US_STATES), other=None)
    unmapped = result.isna()
    result = result.where(~unmapped, other=lower.map(STATE_NAME_MAP))
    return result.where(result.notna() & series.notna(), other=None)

# Column order must exactly match CREATE TABLE below
FINAL_COLS = [
    'id', 'date', 'client_id', 'card_id', 'amount', 'is_refund',
    'use_chip', 'merchant_id', 'merchant_city', 'merchant_state',
    'zip', 'mcc', 'errors',
]

def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['amount']           = clean_amount(df['amount'])
    df['is_refund']        = df['amount'] < 0
    df['amount']           = df['amount'].abs()
    df['errors']           = clean_errors(df['errors'])
    df['date']             = clean_date(df['date'])
    df['merchant_state']   = clean_merchant_state(df['merchant_state'])
    df = df.drop_duplicates(subset=['id'], keep='first')
    existing = [c for c in FINAL_COLS if c in df.columns]
    return df[existing]

# COPY-BASED BULK INSERT
# Fast path: serialise chunk to in-memory CSV buffer,
# push to Postgres via COPY — no row-by-row overhead.

def _copy_chunk(cursor, df: pd.DataFrame, table: str):
    """Write one DataFrame chunk to Postgres using COPY FROM STDIN."""
    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        na_rep='\\N',           # Postgres NULL sentinel in COPY format
        quoting=csv.QUOTE_MINIMAL,
    )
    buf.seek(0)
    cols = ', '.join(df.columns.tolist())
    cursor.copy_expert(
        f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
        buf,
    )

# MAIN

def transform_transactions():
    sa_engine = create_engine(
        f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
    )
    pg_conn = psycopg2.connect(
        dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    target_table = f"{SCHEMA_TARGET}.transactions_data"

    try:
        # ── Count rows for progress display
        with sa_engine.connect() as c:
            total = c.execute(
                text(f"SELECT COUNT(*) FROM {SCHEMA_SOURCE}.transactions_data")
            ).scalar()
        print(f"  Source rows: {total:,}")

        # ── DDL: drop old table, commit, create new
        cur = pg_conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {target_table};")
        pg_conn.commit()
        cur.execute(f"""
            CREATE TABLE {target_table} (
                id               BIGINT PRIMARY KEY,
                date             DATE,
                client_id        INT,
                card_id          INT,
                amount           FLOAT,
                is_refund        BOOLEAN,
                use_chip         VARCHAR(50),
                merchant_id      INT,
                merchant_city    VARCHAR(100),
                merchant_state   CHAR(2),
                zip              VARCHAR(20),
                mcc              INT,
                errors           VARCHAR(100)
            );
        """)
        pg_conn.commit()

        # ── Stream → clean → COPY, chunk by chunk
        loaded = 0
        with sa_engine.connect() as sa_conn:
            for chunk in pd.read_sql(
                f"SELECT * FROM {SCHEMA_SOURCE}.transactions_data",
                sa_conn,
                chunksize=CHUNK_SIZE,
            ):
                chunk = _clean_chunk(chunk)
                _copy_chunk(cur, chunk, target_table)
                pg_conn.commit()

                loaded += len(chunk)
                pct = loaded / total * 100 if total else 0
                print(f"  Loaded {loaded:>10,} / {total:,}  ({pct:.1f}%)", end='\r')

        print(f"\n  Done — {loaded:,} rows loaded.")

    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


if __name__ == "__main__":
    transform_transactions()
