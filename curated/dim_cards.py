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


# Mask card numbers and keep only the last 4 digits visible
def mask_card_number(value):
    if pd.isna(value):
        return None

    s = str(value).replace(" ", "").strip()

    if s == "" or len(s) < 4:
        return None

    return "**** **** **** " + s[-4:]


def dim_cards():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    df = pd.read_sql(f'SELECT * FROM "{SCHEMA_SOURCE}"."cards_data";', conn)
    conn.close()

    # Clean numeric columns
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["client_id"] = pd.to_numeric(df["client_id"], errors="coerce").astype("Int64")
    df["num_cards_issued"] = pd.to_numeric(df["num_cards_issued"], errors="coerce").astype("Int64")
    df["credit_limit"] = pd.to_numeric(df["credit_limit"], errors="coerce")
    df["year_pin_last_changed"] = pd.to_numeric(df["year_pin_last_changed"], errors="coerce").astype("Int64")

    # Clean text columns
    text_cols = [
        "card_brand", "card_type", "card_number", "expires", "has_chip",
        "acct_open_date", "card_on_dark_web", "issuer_bank_name",
        "issuer_bank_state", "issuer_bank_type", "issuer_risk_rating"
    ]

    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()

    df["masked_card_number"] = df["card_number"].apply(mask_card_number)

    dim_cards = pd.DataFrame({
        "card_id": df["id"],
        "client_id": df["client_id"],
        "card_brand": df["card_brand"],
        "card_type": df["card_type"],
        "masked_card_number": df["masked_card_number"],
        "expires": df["expires"],
        "has_chip": df["has_chip"],
        "num_cards_issued": df["num_cards_issued"],
        "credit_limit": df["credit_limit"],
        "acct_open_date": df["acct_open_date"],
        "year_pin_last_changed": df["year_pin_last_changed"],
        "card_on_dark_web": df["card_on_dark_web"],
        "issuer_bank_name": df["issuer_bank_name"],
        "issuer_bank_state": df["issuer_bank_state"],
        "issuer_bank_type": df["issuer_bank_type"],
        "issuer_risk_rating": df["issuer_risk_rating"]
    })

    # Keep only valid unique cards
    dim_cards = dim_cards.dropna(subset=["card_id"]).copy()
    dim_cards = dim_cards.drop_duplicates(subset=["card_id"], keep="first")
    dim_cards = dim_cards.sort_values("card_id").reset_index(drop=True)
    dim_cards.insert(0, "card_key", dim_cards.index + 1)

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."dim_cards" CASCADE;')

    # Create the card dimension table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."dim_cards" (
            card_key INT PRIMARY KEY,
            card_id INT UNIQUE,
            client_id INT,
            card_brand VARCHAR(50),
            card_type VARCHAR(50),
            masked_card_number VARCHAR(25),
            expires VARCHAR(20),
            has_chip VARCHAR(10),
            num_cards_issued INT,
            credit_limit FLOAT,
            acct_open_date VARCHAR(20),
            year_pin_last_changed INT,
            card_on_dark_web VARCHAR(10),
            issuer_bank_name VARCHAR(100),
            issuer_bank_state VARCHAR(20),
            issuer_bank_type VARCHAR(50),
            issuer_risk_rating VARCHAR(20)
        );
    """)
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    dim_cards.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY "{SCHEMA_TARGET}"."dim_cards"
        (card_key, card_id, client_id, card_brand, card_type, masked_card_number, expires,
         has_chip, num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed,
         card_on_dark_web, issuer_bank_name, issuer_bank_state, issuer_bank_type, issuer_risk_rating)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """,
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.dim_cards loaded")


if __name__ == "__main__":
    dim_cards()
