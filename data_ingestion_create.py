import pyodbc
import os

# CONFIGURATION
DRIVER_PATH = '/opt/homebrew/lib/psqlodbcw.so'
DB_NAME = 'project'
SCHEMA_NAME = 'ingestion'
PASSWORD = '563634851' #personal
PORT = '1234' #personal

# Connection strings
system_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE=postgres;UID=postgres;PWD={PASSWORD};PORT={PORT};"
dwh_conn_str = f"DRIVER={{{DRIVER_PATH}}};SERVER=localhost;DATABASE={DB_NAME};UID=postgres;PWD={PASSWORD};PORT={PORT};"

def create_structure():
    try:
        # 1. Ensure DB exists
        sys_conn = pyodbc.connect(system_conn_str)
        sys_conn.autocommit = True
        sys_cursor = sys_conn.cursor()
        try:
            sys_cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Created database: {DB_NAME}")
        except pyodbc.Error as e:
            if '42P04' not in str(e): raise e
        sys_cursor.close(); sys_conn.close()

        # 2. Create Schema and Tables
        conn = pyodbc.connect(dwh_conn_str)
        conn.autocommit = True
        cursor = conn.cursor()

        print(f"Creating Schema: {SCHEMA_NAME} ")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        # 1. Users Table
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.users_data CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.users_data (
                id int, current_age int, retirement_age int, birth_year int, birth_month int,
                gender VARCHAR(20), address TEXT, latitude FLOAT, longitude FLOAT,
                per_capita_income VARCHAR(100), yearly_income VARCHAR(100), total_debt VARCHAR(100),
                credit_score int, num_credit_cards int, employment_status VARCHAR(100), education_level VARCHAR(100)
            );
        """)

        # 2. Cards Table
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.cards_data CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.cards_data (
                id int, client_id int, card_brand VARCHAR(50), card_type VARCHAR(50),
                card_number VARCHAR(50), expires VARCHAR(20), cvv int, has_chip VARCHAR(10),
                num_cards_issued int, credit_limit VARCHAR(50), acct_open_date VARCHAR(20),
                year_pin_last_changed int, card_on_dark_web VARCHAR(10), issuer_bank_name VARCHAR(100),
                issuer_bank_state VARCHAR(20), issuer_bank_type VARCHAR(50), issuer_risk_rating VARCHAR(20)
            );
        """)

        # 3. MCC Table
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.mcc_data CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.mcc_data (
                code VARCHAR(50), description TEXT, notes TEXT, updated_by VARCHAR(50)
            );
        """)

        # 4. Transactions Table
        cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.transactions_data CASCADE;")
        cursor.execute(f"""
            CREATE TABLE {SCHEMA_NAME}.transactions_data (
                id VARCHAR(50) , date VARCHAR(50), client_id int, card_id int, 
                amount VARCHAR(50), 
                use_chip VARCHAR(50), merchant_id VARCHAR(100), merchant_city VARCHAR(100), 
                merchant_state VARCHAR(50), zip VARCHAR(50), mcc VARCHAR(50), 
                errors VARCHAR(255)
            );
        """)

        print("Tables created successfully.")
        conn.close()

    except Exception as e:
        print(f"DDL Error: {e}")

if __name__ == "__main__":
    create_structure()
