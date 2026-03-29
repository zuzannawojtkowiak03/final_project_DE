import psycopg2
from psycopg2 import sql

# Configuration
DB_NAME = "project"
SCHEMA_NAME = "ingestion"
PASSWORD = "Password"
PORT = "5432"
HOST = "127.0.0.1"
USER = "postgres"

def create_database():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database="postgres",
            user=USER,
            password=PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s;",
            (DB_NAME,)
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            print(f"Created database: {DB_NAME}")
        else:
            print(f"Database already exists: {DB_NAME}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Database creation error: {e}")


def create_structure():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DB_NAME,
            user=USER,
            password=PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()

        print(f"Creating schema: {SCHEMA_NAME}")
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))

        # 1. Users Table
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.users_data CASCADE").format(sql.Identifier(SCHEMA_NAME)))
        cursor.execute(sql.SQL("""
            CREATE TABLE {}.users_data (
                id INT,
                current_age INT,
                retirement_age INT,
                birth_year INT,
                birth_month INT,
                gender VARCHAR(20),
                address TEXT,
                latitude FLOAT,
                longitude FLOAT,
                per_capita_income VARCHAR(100),
                yearly_income VARCHAR(100),
                total_debt VARCHAR(100),
                credit_score INT,
                num_credit_cards INT,
                employment_status VARCHAR(100),
                education_level VARCHAR(100)
            )
        """).format(sql.Identifier(SCHEMA_NAME)))

        # 2. Cards Table
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.cards_data CASCADE").format(sql.Identifier(SCHEMA_NAME)))
        cursor.execute(sql.SQL("""
            CREATE TABLE {}.cards_data (
                id INT,
                client_id INT,
                card_brand VARCHAR(50),
                card_type VARCHAR(50),
                card_number VARCHAR(50),
                expires VARCHAR(20),
                cvv INT,
                has_chip VARCHAR(10),
                num_cards_issued INT,
                credit_limit VARCHAR(50),
                acct_open_date VARCHAR(20),
                year_pin_last_changed INT,
                card_on_dark_web VARCHAR(10),
                issuer_bank_name VARCHAR(100),
                issuer_bank_state VARCHAR(20),
                issuer_bank_type VARCHAR(50),
                issuer_risk_rating VARCHAR(20)
            )
        """).format(sql.Identifier(SCHEMA_NAME)))

        # 3. MCC Table
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.mcc_data CASCADE").format(sql.Identifier(SCHEMA_NAME)))
        cursor.execute(sql.SQL("""
            CREATE TABLE {}.mcc_data (
                code VARCHAR(50),
                description TEXT,
                notes TEXT,
                updated_by VARCHAR(50)
            )
        """).format(sql.Identifier(SCHEMA_NAME)))

        # 4. Transactions Table
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.transactions_data CASCADE").format(sql.Identifier(SCHEMA_NAME)))
        cursor.execute(sql.SQL("""
            CREATE TABLE {}.transactions_data (
                id VARCHAR(50),
                date VARCHAR(50),
                client_id INT,
                card_id INT,
                amount VARCHAR(50),
                use_chip VARCHAR(50),
                merchant_id VARCHAR(100),
                merchant_city VARCHAR(100),
                merchant_state VARCHAR(50),
                zip VARCHAR(50),
                mcc VARCHAR(50),
                errors VARCHAR(255)
            )
        """).format(sql.Identifier(SCHEMA_NAME)))

        print("Tables created successfully.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"DDL Error: {e}")


if __name__ == "__main__":
    create_database()
    create_structure()
