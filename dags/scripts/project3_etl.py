import sys
import os
import warnings
import logging

import pandas as pd

from urllib.parse import quote_plus
from sqlalchemy import create_engine


sys.path.append(os.path.abspath("/opt/airflow/dags/scripts"))
from config_etl import etl_config
from config_etl import warehouse_conn_string
from config_etl import oltp_conn_string

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)

def db_connection(conn_params):
    """Create a connection engine to the database"""
    conn_str = f"postgresql+psycopg2://{conn_params['user']}:{quote_plus(conn_params['password'])}@{conn_params['host']}/{conn_params['database']}"
    print(f"Connecting to database with connection string: {conn_str}")
    engine = create_engine(conn_str)
    return engine.connect()

def validate_config(etl_config):
    """Validate the ETL configuration"""
    required_keys = ['source_table', 'query', 'destination_table', 'column_mapping']
    for table_name, table_config in etl_config.items():
        for key in required_keys:
            if key not in table_config:
                raise ValueError(f"Missing {key} in config for table {table_name}")
    logging.info("Config validation passed")

def extract(table_config):
    """Extract data from the source table"""
    try:
        logging.info(f"Extracting data from {table_config['source_table']}...")
        with db_connection(oltp_conn_string) as conn:
            df = pd.read_sql(table_config["query"], conn)
        return df
    except Exception as err:
        logging.info(f"Error extracting data from {table_config['source_table']}: {err}")
        raise

def transform(df, table_config):
    """Transform the extracted data"""
    try:
        logging.info(f"Transforming data for {table_config['destination_table']}...")
        df.rename(columns=table_config["column_mapping"], inplace=True)
        return df
    except Exception as err:
        logging.info(f"Error transforming data for {table_config['destination_table']}: {err}")
        raise

def load(df, table_config):
    """Load the transformed data into the destination table, replacing the data without dropping the table"""
    try:
        logging.info(f"Replacing data in {table_config['destination_table']}...")

        # Connect to the warehouse database
        with db_connection(warehouse_conn_string) as conn:
            # Step 1: Truncate the table (remove all existing data)
            conn.execute(f"TRUNCATE TABLE {table_config['destination_table']} RESTART IDENTITY CASCADE;")
            
            # Step 2: Insert the new data into the table using append (this won't drop the table)
            df.to_sql(
                table_config["destination_table"], 
                conn, 
                if_exists="append",  # This will replace new data
                index=False
            )
        logging.info(f"Data successfully loaded into {table_config['destination_table']}")
    except Exception as err:
        logging.info(f"Error replacing data in {table_config['destination_table']}: {err}")
        raise

def run_etl():
    """Run the full ETL process."""
    try:
        logging.info("Starting ETL Process...")
        validate_config(etl_config)  # Validate config
        for table_name, table_config in etl_config.items():
            df = extract(table_config)
            df = transform(df, table_config)
            load(df, table_config)
        logging.info("ETL Process Completed Successfully!")
    except Exception as err:
        logging.info(f"ETL process failed: {err}")