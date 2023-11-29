# Create dag files
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
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='load_raw_data ',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=15
)

# Define GCS bucket and data folder
gcs_bucket = 'australia-southeast1-bde-bbc8dd7d-bucket'
data_folder = 'data/'
schema_name = 'data_schema'

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

# Function to load listings data files one by one
def import_load_listings_data_files(data_folder, table_name, **kwargs):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=data_folder)

    for blob in blobs:
        if blob.name.endswith('.csv'):
            local_file_path = f'/tmp/{table_name}.csv'
            blob.download_to_filename(local_file_path)

            logging.info(f"Processing file: {blob.name}")
            
            # Extract the file name from the blob
            file_name = os.path.basename(blob.name)
            
            ps_pg_hook = PostgresHook(postgres_conn_id="postgres", password=f"{postgres_password}")
            conn_ps = ps_pg_hook.get_conn()
 

            df = pd.read_csv(local_file_path)

            if not df.empty:
                # Update the 'file_name' column in the DataFrame
                df['file_name'] = file_name
                
                values = [tuple(row) if row else None for row in df.to_dict('split')['data']]
                insert_sql = f"INSERT INTO {schema_name}.{table_name} VALUES %s"
                
                result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
                conn_ps.commit()

            os.remove(local_file_path)


# Define data paths
census_data_01 = f'{data_folder}Census LGA/2016Census_G01_NSW_LGA.csv'
census_data_02 = f'{data_folder}Census LGA/2016Census_G02_NSW_LGA.csv'
listings_data = f'{data_folder}listings/'
nsw_l_code = f'{data_folder}NSW_LGA/NSW_LGA_CODE.csv'
nsw_la_data_surburb = f'{data_folder}NSW_LGA/NSW_LGA_SUBURB.csv'


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

census_data_02_task = PythonOperator(
    task_id="import_load_census_data_02",
    python_callable=import_load_data_func,
    op_kwargs={
        'file_path': census_data_02,
        'table_name': 'census_g02',
    },
    provide_context=True,
    dag=dag
)

listings_data_files = PythonOperator(
    task_id="import_load_listings_data_files",
    python_callable=import_load_listings_data_files,
    op_kwargs={
        'data_folder': listings_data,
        'table_name': 'listings',
    },
    provide_context=True,
    dag=dag
)

nsw_l_code_task = PythonOperator(
    task_id="import_load_nsw_l_code",
    python_callable=import_load_data_func,
    op_kwargs={
        'file_path': nsw_l_code,
        'table_name': 'nsw_lga_code',
    },
    provide_context=True,
    dag=dag
)

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


# Define the task dependencies
[census_data_01_task, census_data_02_task, listings_data_files, nsw_l_code_task, nsw_la_data_surburb_task]

