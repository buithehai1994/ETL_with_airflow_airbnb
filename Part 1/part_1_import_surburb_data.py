import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import airflow
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

# Retrieve the PostgreSQL password from the Variable
postgres_password = Variable.get("password")

dag_default_args = {
    'owner': 'TheHaiBui',
    'start_date': days_ago(1),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='loading_nsw_lga_suburb',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=15
)

# Define GCS bucket/schema
gcs_bucket = 'australia-southeast1-bde-bbc8dd7d-bucket'
data_folder = 'data/'
schema_name = 'raw'

def download_gcs_file(file_path, local_file_path):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(file_path)
    blob.download_to_filename(local_file_path)

def import_load_data_func(file_path, table_name, **kwargs):
    local_file_path = f'/tmp/{table_name}.csv'  # Temporary local file path
    download_gcs_file(file_path, local_file_path)

    if os.path.exists(local_file_path):
        # Read the local CSV file into a DataFrame
        df = pd.read_csv(local_file_path, usecols=["LGA_NAME", "SUBURB_NAME"])

        if not df.empty:
            # Define the INSERT SQL statement to insert data into PostgreSQL
            insert_sql = f"INSERT INTO {schema_name}.{table_name} (LGA_NAME, SUBURB_NAME) VALUES %s"

            # Prepare data for insertion
            values = df.values.tolist()

            # Define postgresql hook
            ps_pg_hook = PostgresHook(postgres_conn_id='postgres', password=postgres_password)
            # Get the PostgreSQL connection
            conn_ps = ps_pg_hook.get_conn()

            # Execute the SQL statement to insert data into PostgreSQL
            result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
            conn_ps.commit()
        else:
            logging.info(f"No data in {table_name} CSV")

        # Clean up: Remove the temporary local file
        os.remove(local_file_path)
    else:
        logging.info(f"Failed to download the file from GCS")

nsw_la_data_surburb = f'{data_folder}NSW_LGA/NSW_LGA_SUBURB.csv'

# Define tasks for importing and loading data

nsw_la_data_surburb_task = PythonOperator(
    task_id="import_load_nsw_lga_surburb",
    python_callable=import_load_data_func,
    op_kwargs={
        'file_path': nsw_la_data_surburb,
        'table_name': 'nsw_lga_suburb',
    },
    provide_context=True,
    dag=dag
)
