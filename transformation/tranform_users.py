import re
import math
import pandas as pd
import psycopg2
from difflib import get_close_matches

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "ingestion"
SCHEMA_TARGET = "transformation"

EMPLOYMENT_CANONICAL = [
    "Employed", "Student", "Retired", "Unemployed", "Self-Employed"
]

EMPLOYMENT_ALIASES = {
    "employed": "Employed",
    "empl0yed": "Employed",

    "student": "Student",
    "studnt": "Student",

    "ret.": "Retired",
    "retired": "Retired",
    "retird": "Retired",

    "un-employed": "Unemployed",
    "unemployed": "Unemployed",
    "unemployd": "Unemployed",

    "self employed": "Self-Employed",
    "self-employed": "Self-Employed",
    "self employd": "Self-Employed",
    "self-employd": "Self-Employed",
}

EDUCATION_CANONICAL = [
    "Bachelor Degree", "High School", "Masters", "Associate Degree", "Doctorate"
]

EDUCATION_ALIASES = {
    "bachelor degree": "Bachelor Degree",
    "bachelor's degree": "Bachelor Degree",
    "bachelors": "Bachelor Degree",
    "ba/bs": "Bachelor Degree",
    "bachelor  degree": "Bachelor Degree",

    "high school": "High School",
    "highschool": "High School",
    "hs": "High School",
    "high  school": "High School",

    "master degree": "Masters",
    "master's degree": "Masters",
    "masters": "Masters",
    "ms/ma": "Masters",
    "master  degree": "Masters",

    "associate degree": "Associate Degree",
    "assoc degree": "Associate Degree",
    "associate deg.": "Associate Degree",
    "associate": "Associate Degree",
    "associate  degree": "Associate Degree",

    "doctorate": "Doctorate"
}

TEXT_MONEY_MAP = {
    "ten thousand": 10000.0,
}


# Fuzzy match misspelled values to the closest valid category
def _fuzzy_match(value: str, canonicals: list[str], cutoff: float = 0.80):
    lower_canonicals = [c.lower() for c in canonicals]
    matches = get_close_matches(value.lower(), lower_canonicals, n=1, cutoff=cutoff)
    if matches:
        return canonicals[lower_canonicals.index(matches[0])]
    return None


# Clean monetary values and convert them to numeric format
def parse_money(value) -> float:
    if pd.isna(value):
        return float("nan")
    s = str(value).strip().strip('"').strip()
    if not s:
        return float("nan")

    s_lower = s.lower()

    if s_lower in TEXT_MONEY_MAP:
        return TEXT_MONEY_MAP[s_lower]

    text_errors = {"error_value", "not available", "nan", "unknown", "n/a"}
    if s_lower in text_errors:
        return float("nan")

    s = s.replace("$", "").replace(",", "").strip()

    if s.lower().endswith("k"):
        try:
            return abs(float(s[:-1]) * 1000.0)
        except ValueError:
            return float("nan")

    try:
        return abs(float(s))
    except ValueError:
        return float("nan")


# Standardize gender values
def normalize_gender(raw):
    if pd.isna(raw):
        return None

    cleaned = re.sub(r"\s+", " ", str(raw).strip()).lower()
    if not cleaned or cleaned in {"nan", "n/a", "unknown"}:
        return None

    gender_map = {
        "m": "Male",
        "male": "Male",
        "f": "Female",
        "female": "Female"
    }

    return gender_map.get(cleaned, cleaned.capitalize())


# Standardize employment status values
def normalize_employment(raw):
    if pd.isna(raw):
        return None

    cleaned = re.sub(r"\s+", " ", str(raw).strip())
    if not cleaned:
        return None

    key = cleaned.lower()

    if key in EMPLOYMENT_ALIASES:
        return EMPLOYMENT_ALIASES[key]

    for canon in EMPLOYMENT_CANONICAL:
        if key == canon.lower():
            return canon

    result = _fuzzy_match(cleaned, EMPLOYMENT_CANONICAL, cutoff=0.80)
    if result:
        return result

    return cleaned.title()


# Standardize education level values
def normalize_education(raw):
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

    result = _fuzzy_match(cleaned, EDUCATION_CANONICAL, cutoff=0.80)
    if result:
        return result

    return cleaned.title()


# Standardize address formatting
def normalize_address(raw):
    if pd.isna(raw):
        return None
    cleaned = re.sub(r"\s+", " ", str(raw).strip())
    return cleaned.title() if cleaned else None


# Keep only valid birth month values
def clean_birth_month(value):
    if pd.isna(value):
        return None
    try:
        month = int(float(value))
        if 1 <= month <= 12:
            return month
    except (ValueError, TypeError):
        pass
    return None


# Keep only realistic birth years
def clean_birth_year(value):
    if pd.isna(value):
        return None
    try:
        year = int(float(value))
        if 1900 <= year <= 2025:
            return year
    except (ValueError, TypeError):
        pass
    return None


# Keep only realistic age values
def clean_current_age(value):
    if pd.isna(value):
        return None
    try:
        age = int(float(value))
        if 0 <= age <= 120:
            return age
    except (ValueError, TypeError):
        pass
    return None


# Keep only realistic retirement age values
def clean_retirement_age(value):
    if pd.isna(value):
        return None
    try:
        age = int(float(value))
        if 0 <= age <= 120:
            return age
    except (ValueError, TypeError):
        pass
    return None


# Keep only valid latitude values
def clean_latitude(value):
    if pd.isna(value):
        return None
    try:
        lat = float(value)
        if -90 <= lat <= 90:
            return lat
    except (ValueError, TypeError):
        pass
    return None


# Keep only valid longitude values
def clean_longitude(value):
    if pd.isna(value):
        return None
    try:
        lon = float(value)
        if -180 <= lon <= 180:
            return lon
    except (ValueError, TypeError):
        pass
    return None


# Convert values to integers when possible
def clean_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


# Keep only valid credit scores
def clean_credit_score(value):
    if pd.isna(value):
        return None
    try:
        score = int(float(value))
        if 300 <= score <= 850:
            return score
    except (ValueError, TypeError):
        pass
    return None


# Keep only valid numbers of credit cards
def clean_num_credit_cards(value):
    if pd.isna(value):
        return None
    try:
        num = int(float(value))
        if num >= 0:
            return num
    except (ValueError, TypeError):
        pass
    return None


# Convert pandas missing values into SQL NULL
def safe_sql_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def transform_users():
    source_conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    try:
        source_cursor = source_conn.cursor()
        source_cursor.execute(f"SELECT * FROM {SCHEMA_SOURCE}.users_data")
        rows = source_cursor.fetchall()
        colnames = [desc[0] for desc in source_cursor.description]
        df = pd.DataFrame(rows, columns=colnames)
    finally:
        source_cursor.close()
        source_conn.close()

    # Clean monetary columns
    for col in ["per_capita_income", "yearly_income", "total_debt"]:
        df[col] = df[col].apply(parse_money)

    # Replace unrealistic yearly income values
    df.loc[df["yearly_income"] < 100, "yearly_income"] = 0.0

    # Apply text standardization
    df["gender"] = df["gender"].apply(normalize_gender)
    df["employment_status"] = df["employment_status"].apply(normalize_employment)
    df["education_level"] = df["education_level"].apply(normalize_education)
    df["address"] = df["address"].apply(normalize_address)

    # Clean numeric columns
    df["id"] = df["id"].apply(clean_int)
    df["birth_month"] = df["birth_month"].apply(clean_birth_month)
    df["birth_year"] = df["birth_year"].apply(clean_birth_year)

    # Recalculate age from birth year and birth month
    today = pd.Timestamp.today()
    df["current_age"] = df.apply(
        lambda row: (
            int(today.year - row["birth_year"] - (1 if today.month < row["birth_month"] else 0))
            if pd.notna(row["birth_year"]) and pd.notna(row["birth_month"])
            else None
        ),
        axis=1
    )
    df["current_age"] = df["current_age"].apply(clean_current_age)

    df["latitude"] = df["latitude"].apply(clean_latitude)
    df["longitude"] = df["longitude"].apply(clean_longitude)
    df["credit_score"] = df["credit_score"].apply(clean_credit_score)
    df["num_credit_cards"] = df["num_credit_cards"].apply(clean_num_credit_cards)
    df["retirement_age"] = df["retirement_age"].apply(clean_retirement_age)

    # Remove duplicate users based on ID
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first").reset_index(drop=True)
    dupes = before - len(df)
    if dupes:
        print(f"Dropped {dupes} duplicate id row(s).")

    columns_order = [
        "id", "current_age", "retirement_age", "birth_year", "birth_month",
        "gender", "address", "latitude", "longitude",
        "per_capita_income", "yearly_income", "total_debt",
        "credit_score", "num_credit_cards",
        "employment_status", "education_level"
    ]

    df_out = df[columns_order].copy()

    rows_to_insert = [
        tuple(safe_sql_value(v) for v in row)
        for row in df_out.itertuples(index=False, name=None)
    ]

    target_conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    try:
        target_conn.autocommit = False
        cursor = target_conn.cursor()

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TARGET};")
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_TARGET}.users_data;")
        target_conn.commit()

        # Create the transformed users table
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_TARGET}.users_data (
                id INT,
                current_age INT,
                retirement_age INT,
                birth_year INT,
                birth_month INT,
                gender VARCHAR(20),
                address TEXT,
                latitude FLOAT,
                longitude FLOAT,
                per_capita_income BIGINT,
                yearly_income BIGINT,
                total_debt BIGINT,
                credit_score INT,
                num_credit_cards INT,
                employment_status VARCHAR(100),
                education_level VARCHAR(100)
            );
        """)

        insert_sql = f"""
            INSERT INTO {SCHEMA_TARGET}.users_data (
                id, current_age, retirement_age, birth_year, birth_month,
                gender, address, latitude, longitude, per_capita_income,
                yearly_income, total_debt, credit_score, num_credit_cards,
                employment_status, education_level
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, rows_to_insert)
        target_conn.commit()
        print(f"Users transformed and loaded: {len(df_out)} rows.")

    except Exception as e:
        target_conn.rollback()
        print(f"Transformation error: {e}")
        raise

    finally:
        cursor.close()
        target_conn.close()


if __name__ == "__main__":
    transform_users()
