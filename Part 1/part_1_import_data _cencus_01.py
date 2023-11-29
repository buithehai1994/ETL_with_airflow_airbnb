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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False
}

dag = DAG(
    dag_id='load_data_cencus_01',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1
)

# Define GCS bucket and data folder
gcs_bucket = 'australia-southeast1-bde-bbc8dd7d-bucket'
data_folder = 'data/'
schema_name = 'raw'

# Function to transform a date string
def transform_date(date_str):
    if isinstance(date_str, str):
        try:
            return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return None
    else:
        return None

# Function to download a GCS file
def download_gcs_file(file_path, local_file_path):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(file_path)
    blob.download_to_filename(local_file_path)

# Function to import and load data from a file
def import_load_data_func(file_path, table_name, **kwargs):
    local_file_path = f'/tmp/{table_name}.csv'
    download_gcs_file(file_path, local_file_path)

    if os.path.exists(local_file_path):
        df = pd.read_csv(local_file_path)

        if not df.empty:
            insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES %s"
            values = df.to_dict('split')['data']

            ps_pg_hook = PostgresHook(postgres_conn_id="postgres", password=f"{postgres_password}")
            conn_ps = ps_pg_hook.get_conn()

            result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
            conn_ps.commit()
        else:
            logging.info(f"No data in {table_name} CSV")

        os.remove(local_file_path)
    else:
        logging.info(f"Failed to download the file from GCS")


# Define data paths
census_data_01 = f'{data_folder}Census LGA/2016Census_G01_NSW_LGA.csv'


# Define tasks for importing and loading data
census_data_01_task = PythonOperator(
    task_id="import_load_census_data_01",
    python_callable=import_load_data_func,
    op_kwargs={
        'file_path': census_data_01,
        'table_name': 'census_g01',
    },
    provide_context=True,
    dag=dag
)



# Define the task dependencies
census_data_01_task
