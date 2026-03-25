import re
import pyodbc
import pandas as pd
from difflib import get_close_matches
from sqlalchemy import create_engine, text


DRIVER_PATH    = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME        = 'project'
PASSWORD       = '563634851'
PORT           = '1234'
SCHEMA_SOURCE  = 'ingestion'
SCHEMA_TARGET  = 'transformation'

conn_str = (
    f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};"
    f"UID=postgres;PWD={PASSWORD};PORT={PORT};"
)


EMPLOYMENT_CANONICAL = [
    "Employed", "Student", "Retired", "Unemployed", "Self-Employed"
]

# Explicit overrides checked BEFORE fuzzy matching.
# Keys are lowercased stripped strings.
EMPLOYMENT_ALIASES = {
    "ret.":          "Retired",
    "self employed": "Self-Employed",   # no-hyphen variant
    "self-employed": "Self-Employed",
    "un-employed":   "Unemployed",      # hyphenated variant
}

EDUCATION_CANONICAL = [
    "Bachelor Degree", "High School", "Masters", "Associate Degree", "Doctorate"
]

EDUCATION_ALIASES = {
    "master degree":     "Masters",
    "master's degree":   "Masters",
    "ms/ma":             "Masters",
    "masters":           "Masters",
    "bachelor's degree": "Bachelor Degree",
    "bachelors":         "Bachelor Degree",
    "ba/bs":             "Bachelor Degree",
    "assoc degree":      "Associate Degree",
    "associate deg.":    "Associate Degree",
    "associate":         "Associate Degree",
    "highschool":        "High School",
    "hs":                "High School",
    "doctorate":         "Doctorate",
}

def _fuzzy_match(value: str, canonicals: list, cutoff: float = 0.65):
    """
    Case-insensitive fuzzy match via difflib.get_close_matches.
    Returns the matched canonical string, or None if no match is good enough.
    """
    lower_canonicals = [c.lower() for c in canonicals]
    matches = get_close_matches(value.lower(), lower_canonicals, n=1, cutoff=cutoff)
    if matches:
        return canonicals[lower_canonicals.index(matches[0])]
    return None


def parse_money(value) -> float:
    """
    Converts all observed currency/number formats to float:
      $29278        -> 29278.0
      $21,950       -> 21950.0
      "$95,945"     -> 95945.0   (quoted)
      15k / 32k     -> 15000.0 / 32000.0
      20599         -> 20599.0
    """
    if pd.isna(value):
        return float("nan")
    s = str(value).strip().strip('"').strip()   # remove surrounding quotes/spaces
    s = s.replace("$", "").replace(",", "").strip()
    if s.lower().endswith("k"):
        try:
            return float(s[:-1]) * 1000.0
        except ValueError:
            return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def normalize_employment(raw):
    """
    Normalises to one of: Employed | Student | Retired | Unemployed | Self-Employed

    Steps (in order):
      1. Strip whitespace; return None for blank/whitespace-only values
      2. Alias table  - abbreviations and known alternate spellings
      3. Exact case-insensitive match against canonical list
      4. Fuzzy match  - catches typos (Empl0yed, Studnt, Retird, Unemployd...)
      5. Fallback     - title-case the original for manual review
    """
    if pd.isna(raw):
        return None
    cleaned = str(raw).strip()
    if not cleaned:
        return None

    key = cleaned.lower()

    if key in EMPLOYMENT_ALIASES:
        return EMPLOYMENT_ALIASES[key]

    for canon in EMPLOYMENT_CANONICAL:
        if key == canon.lower():
            return canon

    result = _fuzzy_match(cleaned, EMPLOYMENT_CANONICAL, cutoff=0.65)
    if result:
        return result

    return cleaned.title()


def normalize_education(raw):
    """
    Normalises to one of:
      Bachelor Degree | High School | Masters | Associate Degree | Doctorate

    Also collapses internal multiple spaces before matching
    (e.g. "High  School" -> "High School").
    """
    if pd.isna(raw):
        return None
    cleaned = re.sub(r"\s+", " ", str(raw).strip())
    if not cleaned:
        return None

    key = cleaned.lower()

    if key in EDUCATION_ALIASES:
        return EDUCATION_ALIASES[key]

    for canon in EDUCATION_CANONICAL:
        if key == canon.lower():
            return canon

    result = _fuzzy_match(cleaned, EDUCATION_CANONICAL, cutoff=0.65)
    if result:
        return result

    return cleaned.title()


def normalize_address(raw):
    """
    Strips leading/trailing whitespace, collapses internal spaces, title-cases.
    """
    if pd.isna(raw):
        return None
    return re.sub(r"\s+", " ", str(raw).strip()).title()


# MAIN TRANSFORM

def transform_users():
    # SQLAlchemy engine – used for pd.read_sql (avoids UserWarning)
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{PASSWORD}@localhost:{PORT}/{DB_NAME}"
    )

    # ── Read source data 
    with engine.connect() as sa_conn:
        df = pd.read_sql(
            f"SELECT * FROM {SCHEMA_SOURCE}.users_data",
            sa_conn,
        )

    # ── 1. Monetary columns 
    # Handles: $symbol, commas, quoted strings, k-suffix, plain numbers
    for col in ["per_capita_income", "yearly_income", "total_debt"]:
        df[col] = df[col].apply(parse_money)

    # yearly_income: raw values < 100 are artefacts (un-multiplied numbers)
    # set to 0 to flag them rather than fabricating a value
    df.loc[df["yearly_income"] < 100, "yearly_income"] = 0.0

    # ── 2. Gender 
    df["gender"] = df["gender"].str.strip().str.capitalize()

    # ── 3. Employment status 
    # Handles: case variants, typos (Empl0yed), abbreviations (Ret.),
    #          spacing issues, alternate separators (Self Employed)
    df["employment_status"] = df["employment_status"].apply(normalize_employment)

    # ── 4. Education level 
    # Handles: ALL-CAPS, abbreviations (HS, Assoc Degree, MS/MA, BA/BS),
    #          merged words (Highschool), possessives (Bachelor's), extra spaces
    df["education_level"] = df["education_level"].apply(normalize_education)

    # ── 5. Address 
    df["address"] = df["address"].apply(normalize_address)

    # ── 6. Deduplicate on primary key (keep first occurrence) 
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    dupes = before - len(df)
    if dupes:
        print(f"  Dropped {dupes} duplicate id row(s).")

    # ── 7. Write to target schema 
    # Use a raw pyodbc connection so we can control autocommit and DDL order.
    # DROP + COMMIT first, then CREATE + INSERT + COMMIT — this guarantees
    # the old table is fully gone before the new one is created, preventing
    # the duplicate-key error that occurs when pyodbc batches DDL together.
    raw_conn = pyodbc.connect(conn_str, autocommit=False)
    try:
        cursor = raw_conn.cursor()

        # Step A: drop and commit immediately
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.users_data;")
        raw_conn.commit()

        # Step B: create fresh table
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_TARGET}.users_data (
                id                INT,
                current_age       INT,
                retirement_age    INT,
                birth_year        INT,
                birth_month       INT,
                gender            VARCHAR(20),
                address           TEXT,
                latitude          FLOAT,
                longitude         FLOAT,
                per_capita_income FLOAT,
                yearly_income     FLOAT,
                total_debt        FLOAT,
                credit_score      INT,
                num_credit_cards  INT,
                employment_status VARCHAR(100),
                education_level   VARCHAR(100)
            );
        """)

        # Step C: bulk insert
        insert_sql = (
            f"INSERT INTO {SCHEMA_TARGET}.users_data VALUES "
            f"({','.join(['?'] * 16)})"
        )
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, [tuple(x) for x in df.values])
        raw_conn.commit()
        print(f"Users transformed and loaded: {len(df)} rows.")

    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


if __name__ == "__main__":
    transform_users()
