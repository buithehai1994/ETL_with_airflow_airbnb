import os
import logging
import pandas as pd
from datetime import datetime, timedelta
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
    dag_id='load_data_listing',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=15
)

# Define GCS bucket and data folder
gcs_bucket = 'australia-southeast1-bde-bbc8dd7d-bucket'
data_folder = 'data/'
schema_name = 'raw'

# Function to download a GCS file
def download_gcs_file(file_path, local_file_path):
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(file_path)
    blob.download_to_filename(local_file_path)

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
listings_data = f'{data_folder}listings/'

# Define tasks for importing and loading data
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

# Define the task dependencies
listings_data_files
