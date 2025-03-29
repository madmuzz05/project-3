import psycopg2
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/dags/scripts"))
from config_etl import etl_config
from config_etl import warehouse_conn_string
from config_etl import oltp_conn_string

# PostgreSQL credentials (modify based on your setup)
DB_HOST = warehouse_conn_string['host']
DB_PORT = warehouse_conn_string['port']
DB_USER = warehouse_conn_string['user']
DB_PASSWORD = warehouse_conn_string['password']
DB_NAME = warehouse_conn_string['database']  # Database name for OLTP

def execute_query(connection, query):
    """Executes a SQL query."""
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()
def run_ddl_dwh():
    # Step 1: Connect to PostgreSQL (default database)
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname="postgres"  # Connect to default DB to drop/create `project3_dwh`
        )
        conn.autocommit = True
        print("Connected to PostgreSQL successfully!")

        # Step 2: Drop and Recreate Database
        execute_query(conn, f"DROP DATABASE IF EXISTS {DB_NAME}")
        execute_query(conn, f"CREATE DATABASE {DB_NAME}")
        print(f"Database `{DB_NAME}` has been recreated.")

        conn.close()

        # Step 3: Connect to the new database
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        print(f"Connected to `{DB_NAME}` successfully!")

        # Step 4: Create Tables
        create_tables_query = """
            CREATE TABLE IF NOT EXISTS dim_user (
                user_id INT NOT NULL PRIMARY KEY,
                user_first_name VARCHAR(255) NOT NULL,
                user_last_name VARCHAR(255) NOT NULL,
                user_gender VARCHAR(50) NOT NULL,
                user_address VARCHAR(255),
                user_birthday DATE NOT NULL,
                user_join DATE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_payment (
                payment_id INT NOT NULL PRIMARY KEY,
                payment_name VARCHAR(255) NOT NULL,
                payment_status BOOLEAN NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_shipper (
                shipper_id INT NOT NULL PRIMARY KEY,
                shipper_name VARCHAR(255) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_rating (
                rating_id INT NOT NULL PRIMARY KEY,
                rating_level INT NOT NULL,
                rating_status VARCHAR(255) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_voucher (
                voucher_id INT NOT NULL PRIMARY KEY,
                voucher_name VARCHAR(255) NOT NULL,
                voucher_price INT,
                voucher_created DATE NOT NULL,
                user_id INT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id INT NOT NULL PRIMARY KEY,
                order_date DATE NOT NULL,
                user_id INT NOT NULL,
                payment_id INT NOT NULL,
                shipper_id INT NOT NULL,
                order_price INT NOT NULL,
                order_discount INT,
                voucher_id INT,
                order_total INT NOT NULL,
                rating_id INT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
                FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id),
                FOREIGN KEY (shipper_id) REFERENCES dim_shipper(shipper_id),
                FOREIGN KEY (voucher_id) REFERENCES dim_voucher(voucher_id),
                FOREIGN KEY (rating_id) REFERENCES dim_rating(rating_id)
            );
        """
        execute_query(conn, create_tables_query)
        print("Tables have been created successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Connection closed.")
