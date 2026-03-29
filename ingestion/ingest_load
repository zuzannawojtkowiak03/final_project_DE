import os
import psycopg2
from psycopg2 import sql

DB_NAME = "project"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

SCHEMA_NAME = "ingestion"

CSV_FILES = [
    "users_data.csv",
    "cards_data.csv",
    "mcc_data.csv",
    "transactions_data.csv",
]


def table_name_from_file(filename: str) -> str:
    return os.path.splitext(filename)[0].lower()


def load_all():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    conn = psycopg2.connect(host=HOST, port=PORT, database=DB_NAME, user=USER, password=PASSWORD)
    cursor = conn.cursor()

    for filename in CSV_FILES:
        table_name = table_name_from_file(filename)
        file_path = os.path.join(BASE_DIR, "..", "Dataset-final-project", filename)

        with open(file_path, "r", encoding="utf-8") as file:
            copy_sql = sql.SQL(
                "COPY {}.{} FROM STDIN WITH CSV HEADER DELIMITER ','"
            ).format(
                sql.Identifier(SCHEMA_NAME),
                sql.Identifier(table_name)
            )
            cursor.copy_expert(copy_sql.as_string(conn), file)

        print(f"{filename} loaded into {SCHEMA_NAME}.{table_name}")

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    load_all()
