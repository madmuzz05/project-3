import sys
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

sys.path.append(os.path.abspath("/opt/airflow/dags/scripts"))  # Adjusted path to match Airflow's structure
from project3_etl import run_etl  # Ensure project3_etl.py exists in the specified directory
from ddl_dwh import run_ddl_dwh  # Ensure ddl_dwh.py exists in the specified directory
from ddl_oltp import run_ddl_oltp  # Ensure ddl_oltp.py exists in the specified directory

with DAG(
    dag_id='etl_dag',
    start_date=datetime(2024, 5, 1),
    schedule_interval='0 0,12 * * *',  # Runs twice a day: at midnight and noon
    catchup=False
) as dag:
    

    start_task = EmptyOperator(
        task_id='start'
    )

    run_ddl_oltp = PythonOperator(
        task_id='run_ddl_oltp',
        python_callable=run_ddl_oltp,
    )

    run_ddl_dwh = PythonOperator(
        task_id='run_ddl_dwh',
        python_callable=run_ddl_dwh,
    )

    run_etl = PythonOperator(
        task_id='run_etl_function',
        python_callable=run_etl,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> run_ddl_oltp >> run_ddl_dwh >> run_etl >> end_task