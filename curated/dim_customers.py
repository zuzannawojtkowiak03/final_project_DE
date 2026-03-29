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


def dim_customers():
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )

    df = pd.read_sql(f'SELECT * FROM "{SCHEMA_SOURCE}"."users_data";', conn)
    conn.close()

    # Clean integer columns
    numeric_int_cols = [
        "id", "birth_year", "birth_month", "current_age", "retirement_age",
        "credit_score", "num_credit_cards"
    ]

    for col in numeric_int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Clean numeric decimal columns
    numeric_decimal_cols = [
        "latitude", "longitude", "per_capita_income",
        "yearly_income", "total_debt"
    ]

    for col in numeric_decimal_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean text columns
    text_cols = [
        "gender", "address", "employment_status", "education_level"
    ]

    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()

    dim_customers = pd.DataFrame({
        "customer_id": df["id"],
        "gender": df["gender"],
        "birth_year": df["birth_year"],
        "birth_month": df["birth_month"],
        "current_age": df["current_age"],
        "retirement_age": df["retirement_age"],
        "address": df["address"],
        "latitude": df["latitude"],
        "longitude": df["longitude"],
        "per_capita_income": df["per_capita_income"],
        "yearly_income": df["yearly_income"],
        "total_debt": df["total_debt"],
        "credit_score": df["credit_score"],
        "num_credit_cards": df["num_credit_cards"],
        "employment_status": df["employment_status"],
        "education_level": df["education_level"]
    })

    # Keep only valid unique customers
    dim_customers = dim_customers.dropna(subset=["customer_id"]).copy()
    dim_customers = dim_customers.drop_duplicates(subset=["customer_id"], keep="first")
    dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
    dim_customers.insert(0, "customer_key", dim_customers.index + 1)

    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        database=DB_NAME,
        user=USER,
        password=PASSWORD
    )
    cur = conn.cursor()

    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_TARGET}";')
    cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA_TARGET}"."dim_customers" CASCADE;')

    # Create the customer dimension table
    cur.execute(f"""
        CREATE TABLE "{SCHEMA_TARGET}"."dim_customers" (
            customer_key INT PRIMARY KEY,
            customer_id INT UNIQUE,
            gender VARCHAR(20),
            birth_year INT,
            birth_month INT,
            current_age INT,
            retirement_age INT,
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
    conn.commit()

    # Use \N so NULL values load correctly in COPY
    buffer = io.StringIO()
    dim_customers.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY "{SCHEMA_TARGET}"."dim_customers"
        (customer_key, customer_id, gender, birth_year, birth_month, current_age,
         retirement_age, address, latitude, longitude, per_capita_income, yearly_income,
         total_debt, credit_score, num_credit_cards, employment_status, education_level)
        FROM STDIN WITH (FORMAT CSV, NULL '\\N')
        """,
        buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    print("curated.dim_customers loaded")


if __name__ == "__main__":
    dim_customers()
