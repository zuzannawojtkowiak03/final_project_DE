import psycopg2
import pandas as pd
import io

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_SOURCE = "ingestion"
SCHEMA_TARGET = "transformation"


# Clean MCC codes and keep only valid numeric values
def clean_code(value):
    if pd.isna(value):
        return None

    s = str(value).strip()

    if s in ["", "\\N"]:
        return None

    if s.upper().startswith("MCC"):
        s = s[3:].strip()

    s = s.strip('"').strip("'")

    if s.upper() in ["NOTE", "COMMENT", "NAN", "\\N"]:
        return None

    try:
        s = str(int(float(s)))
    except ValueError:
        return None

    if not s.isdigit():
        return None

    return int(s)


# Clean and standardize descriptions
def clean_description(value):
    if pd.isna(value):
        return None

    s = str(value).strip()

    if s in ["", "\\N"]:
        return None

    s = " ".join(s.split())
    return s.title()


# Standardize note values
def clean_notes(value):
    if pd.isna(value):
        return None

    s = str(value).strip()

    if s in ["", "\\N"]:
        return None

    notes_map = {
        "legacy": "Legacy Data",
        "check": "Needs Check",
        "old_code": "Old Code"
    }

    return notes_map.get(s.lower(), s)


# Clean names in the updated_by column
def clean_updated_by(value):
    if pd.isna(value):
        return None

    s = str(value).strip()

    if s in ["", "\\N"]:
        return None

    s = s.replace("_", " ").replace("-", " ")

    return s.title()


def transform_mcc():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    query = f'SELECT * FROM "{SCHEMA_SOURCE}"."mcc_data";'
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Source rows: {len(df)}")

    # Apply cleaning steps
    df["code"] = df["code"].apply(clean_code)
    df["code"] = pd.to_numeric(df["code"], errors="coerce").astype("Int64")

    df["description"] = df["description"].apply(clean_description)
    df["notes"] = df["notes"].apply(clean_notes)
    df["updated_by"] = df["updated_by"].apply(clean_updated_by)

    # Keep only rows with valid MCC codes
    df = df.dropna(subset=["code"]).copy()

    # Remove duplicate MCC codes
    df = (
        df.sort_values(by=["code"])
          .drop_duplicates(subset=["code"], keep="first")
          .reset_index(drop=True)
    )

    print(f"Clean rows to load: {len(df)}")

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."mcc_data";')

    # Create the transformed table
    cur.execute(f'''
        CREATE TABLE "{SCHEMA_TARGET}"."mcc_data" (
            code INT,
            description VARCHAR(255),
            notes VARCHAR(100),
            updated_by VARCHAR(100)
        );
    ''')

    conn.commit()

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    # Load cleaned data into PostgreSQL
    cur.copy_expert(
        sql=f'''
            COPY "{SCHEMA_TARGET}"."mcc_data"
            (code, description, notes, updated_by)
            FROM STDIN WITH CSV
        ''',
        file=buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("transformation.mcc_data loaded successfully")


if __name__ == "__main__":
    transform_mcc()
