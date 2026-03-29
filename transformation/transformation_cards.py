import pandas as pd
import numpy as np
import re
import psycopg2

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "ingestion"
SCHEMA_TARGET = "transformation"


# Clean and standardize monetary values
def parse_monetary(value):
    if pd.isna(value) or str(value).strip() == "":
        return None

    s = str(value).lower().strip()

    # Handle known textual numbers first
    text_numbers = {
        "ten thousand": 10000,
        "five thousand": 5000,
        "twenty thousand": 20000
    }

    if s in text_numbers:
        return text_numbers[s]

    # Remove invalid placeholders
    text_errors = {"error_value", "not available", "nan", "unknown", "n/a"}
    if s in text_errors:
        return None

    s = s.replace("(", "-").replace(")", "")
    s = s.replace("$", "").replace(",", "").strip()

    multiplier = 1
    if s.endswith("k"):
        multiplier = 1000
        s = s[:-1].strip()

    try:
        val = float(s) * multiplier
    except ValueError:
        return None

    val = abs(val)

    # Filter unrealistic values
    if val > 1_000_000:
        return None

    return val


# Convert different date formats into YYYY-MM
def _parse_month_year_str(s: str):
    m = re.match(r"^([A-Za-z]{3})-(\d{2,4})$", s)
    if m:
        month_str, year_str = m.group(1), m.group(2)
        year = int(year_str)
        if year < 100:
            year += 2000

        parsed = pd.to_datetime(f"01-{month_str}-{year}", format="%d-%b-%Y", errors="coerce")
        if pd.notna(parsed):
            return parsed.strftime("%Y-%m")

    parsed = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m")

    return None


def normalize_expires(value):
    if pd.isna(value) or str(value).strip().lower() in ("", "nan", "not available", "n/a"):
        return None
    return _parse_month_year_str(str(value).strip())


def normalize_acct_open_date(value):
    if pd.isna(value) or str(value).strip().lower() in ("", "nan", "not available", "n/a"):
        return None
    return _parse_month_year_str(str(value).strip())


# Keep only valid 3-digit CVV values
def clean_cvv(value):
    try:
        v = int(float(value))
    except (ValueError, TypeError):
        return None
    if 100 <= v <= 999:
        return v
    return None


# Standardize Yes/No values
def clean_yes_no(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
    )

    mapping = {
        "yes": "Yes",
        "y": "Yes",
        "true": "Yes",
        "1": "Yes",
        "no": "No",
        "n": "No",
        "false": "No",
        "0": "No"
    }

    return cleaned.map(mapping)


BRAND_MAPPING = {
    "v": "Visa",
    "vis": "Visa",
    "visa": "Visa",
    "vissa": "Visa",
    "v!sa": "Visa",
    "vvisa": "Visa",
    "visa-card": "Visa",
    "mastercard": "Mastercard",
    "master card": "Mastercard",
    "master  card": "Mastercard",
    "amex": "American Express",
    "ame x": "American Express",
    "ame  x": "American Express",
    "discover": "Discover",
    "dis cover": "Discover",
    "dis  cover": "Discover",
}

TYPE_MAPPING = {
    "credit": "Credit",
    "cred": "Credit",
    "credt": "Credit",
    "cedit": "Credit",
    "crdeit": "Credit",
    "cr": "Credit",
    "cc": "Credit",
    "credit card": "Credit",
    "card - credit": "Credit",
    "cre dit": "Credit",
    "debit": "Debit",
    "debiit": "Debit",
    "db": "Debit",
    "d": "Debit",
    "debti": "Debit",
    "deibt": "Debit",
    "de bit": "Debit",
    "debit card": "Debit",
    "bank debit": "Debit",
    "deb": "Debit",
    "debit (prepaid)": "Prepaid",
    "debit (prepaid) card": "Prepaid",
    "debit (pre payed)": "Prepaid",
    "prepaid": "Prepaid",
    "prepaid debit": "Prepaid",
    "ppd": "Prepaid",
    "dp": "Prepaid",
    "dpp": "Prepaid",
    "db-pp": "Prepaid",
    "debit prepaid": "Prepaid",
    "debit(prepaid)": "Prepaid",
    "debit (prepiad)": "Prepaid",
    "debti (prepaid)": "Prepaid",
    "debit (prepaid) ": "Prepaid",
    "debit (pre paid)": "Prepaid",
    "debit prepaid card": "Prepaid",
}

BANK_NAME_MAPPING = {
    "wells fargo": "Wells Fargo",
    "citi": "Citi",
    "chase bank": "Chase Bank",
    "chase bk": "Chase Bank",
    "bank of america": "Bank of America",
    "bk of america": "Bank of America",
    "capital one": "Capital One",
    "jpmorgan chase": "JPMorgan Chase",
    "jp morgan chase": "JPMorgan Chase",
    "u.s. bank": "U.S. Bank",
    "u.s. bk": "U.S. Bank",
    "pnc bank": "PNC Bank",
    "pnc bk": "PNC Bank",
    "truist": "Truist",
    "ally bank": "Ally Bank",
    "ally bk": "Ally Bank",
    "discover bank": "Discover Bank",
    "discover bk": "Discover Bank",
}

STATE_MAP = {
    "california": "CA",
    "illinois": "IL",
    "new york": "NY",
    "michigan": "MI",
    "minnesota": "MN",
    "north carolina": "NC",
    "pennsylvania": "PA",
    "virginia": "VA",
}

BANK_TYPE_MAPPING = {
    "national": "National",
    "national bank": "National",
    "online": "Online",
    "online bank": "Online",
    "online only": "Online",
    "regional": "Regional",
    "regional bank": "Regional",
}

RISK_RATING_MAPPING = {
    "low": "Low",
    "low risk": "Low",
    "med": "Medium",
    "medium": "Medium",
    "high": "High",
    "high risk": "High",
}


# Standardize brand names
def clean_brand(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    return cleaned.map(BRAND_MAPPING).fillna("Other")


# Standardize card types
def clean_card_type(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
    )
    return cleaned.map(TYPE_MAPPING).fillna("Other")


# Standardize bank names
def clean_bank_name(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    original = series.astype(str).str.strip().str.title()
    return cleaned.map(BANK_NAME_MAPPING).fillna(original)


# Convert state names to abbreviations
def clean_bank_state(series: pd.Series) -> pd.Series:
    def convert(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        lower = s.lower()
        if lower in STATE_MAP:
            return STATE_MAP[lower]
        abbr = s.upper()[:2]
        return abbr if abbr.isalpha() else None

    return series.apply(convert)


# Standardize bank types
def clean_bank_type(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    return cleaned.map(BANK_TYPE_MAPPING).fillna("Other")


# Standardize risk ratings
def clean_risk_rating(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.lower()
    return cleaned.map(RISK_RATING_MAPPING).fillna("Medium")


# Remove formatting issues from card numbers
def clean_card_number(series: pd.Series) -> pd.Series:
    def convert(val):
        if pd.isna(val):
            return None
        try:
            return str(int(float(val)))
        except (ValueError, OverflowError):
            return str(val).strip()

    return series.apply(convert)


def transform_cards():
    source_conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    try:
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SELECT * FROM {SCHEMA_SOURCE}.cards_data")
        rows = source_cursor.fetchall()
        colnames = [desc[0] for desc in source_cursor.description]
        df = pd.DataFrame(rows, columns=colnames)
    finally:
        source_cursor.close()
        source_conn.close()

    # Remove duplicates based on card ID
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="first").reset_index(drop=True)
    removed = before - len(df)
    if removed:
        print(f"Deduplication: Removed {removed} rows with duplicate IDs, {len(df)} rows remaining.")

    # Apply all cleaning steps
    df["card_brand"] = clean_brand(df["card_brand"])
    df["card_type"] = clean_card_type(df["card_type"])
    df["credit_limit"] = df["credit_limit"].apply(parse_monetary)
    df["acct_open_date"] = df["acct_open_date"].apply(normalize_acct_open_date)
    df["card_number"] = clean_card_number(df["card_number"])
    df["issuer_bank_name"] = clean_bank_name(df["issuer_bank_name"])
    df["issuer_bank_state"] = clean_bank_state(df["issuer_bank_state"])
    df["issuer_bank_type"] = clean_bank_type(df["issuer_bank_type"])
    df["issuer_risk_rating"] = clean_risk_rating(df["issuer_risk_rating"])
    df["expires"] = df["expires"].apply(normalize_expires)
    df["cvv"] = df["cvv"].apply(clean_cvv)
    df["has_chip"] = clean_yes_no(df["has_chip"])
    df["card_on_dark_web"] = clean_yes_no(df["card_on_dark_web"])

    columns_order = [
        "id",
        "client_id",
        "card_brand",
        "card_type",
        "card_number",
        "expires",
        "cvv",
        "has_chip",
        "num_cards_issued",
        "credit_limit",
        "acct_open_date",
        "year_pin_last_changed",
        "card_on_dark_web",
        "issuer_bank_name",
        "issuer_bank_state",
        "issuer_bank_type",
        "issuer_risk_rating"
    ]

    int_cols = {"id", "client_id", "cvv", "num_cards_issued", "year_pin_last_changed"}

    # Ensure correct types before inserting
    def safe_val(col, v):
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass

        if col in int_cols:
            try:
                return int(v)
            except (ValueError, TypeError):
                return None

        if isinstance(v, (np.floating, np.integer)):
            return v.item()

        return v

    df_out = df[columns_order]
    rows_to_insert = [
        tuple(safe_val(col, v) for col, v in zip(columns_order, row))
        for row in df_out.itertuples(index=False, name=None)
    ]

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    try:
        conn.autocommit = False
        cursor = conn.cursor()

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.cards_data;")

        # Create the transformed table
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_TARGET}.cards_data (
                id INT,
                client_id INT,
                card_brand VARCHAR(50),
                card_type VARCHAR(50),
                card_number VARCHAR(20),
                expires VARCHAR(7),
                cvv INT,
                has_chip VARCHAR(10),
                num_cards_issued INT,
                credit_limit FLOAT,
                acct_open_date VARCHAR(7),
                year_pin_last_changed INT,
                card_on_dark_web VARCHAR(10),
                issuer_bank_name VARCHAR(100),
                issuer_bank_state VARCHAR(2),
                issuer_bank_type VARCHAR(50),
                issuer_risk_rating VARCHAR(20)
            );
        """)

        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.cards_data (
                id, client_id, card_brand, card_type, card_number,
                expires, cvv, has_chip, num_cards_issued, credit_limit,
                acct_open_date, year_pin_last_changed, card_on_dark_web,
                issuer_bank_name, issuer_bank_state, issuer_bank_type, issuer_risk_rating
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, rows_to_insert)
        conn.commit()
        print("Cards cleaned and transformed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Transformation error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    transform_cards()
