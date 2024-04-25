import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator  
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

load_dotenv()

def extract_data(data_interval_end):
    city_name = 'SaoPaulo'
    url = os.path.join(
            'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f"{city_name}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={os.getenv('api_key')}&contentType=csv"
        )

    data = pd.read_csv(url)
    return data

def load_data(data, data_interval_end):
    file_path = f"{os.getenv('path')}/data/week={data_interval_end}/"
    data.to_csv(file_path + 'raw_data.csv')

with DAG(
    'weather_forecast_pipeline',
    description='A simple weather forecasting ELT pipeline using Apache Airflow, BigQuery and Python for the city of São Paulo',
    start_date=datetime(2024, 3, 24),
    schedule='@weekly',
    tags=['data engineering']
) as dag:
    
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket', 
        bucket_name=os.getenv('GCS_BUCKET_NAME'),
        storage_class='MULTI_REGIONAL',
        location='US',
        gcp_conn_id='gcp'
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=os.getenv('DATASET_NAME'),
        gcp_conn_id='gcp'
    )

    create_local_folder = BashOperator(
        task_id='creating_folder',
        bash_command='mkdir -p "$AIRFLOW_HOME/data/week={{data_interval_end.strftime("%Y-%m-%d")}}"',
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'},
        provide_context=True
    )

    load_local = PythonOperator(
        task_id='load_local',
        python_callable=load_data,
        op_args=[extract.output],
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'},
        provide_context=True
    )

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src=os.getenv('path') + '/data/week={{data_interval_end.strftime("%Y-%m-%d")}}/raw_data.csv',
        dst='raw/week={{data_interval_end.strftime("%Y-%m-%d")}}/',
        bucket=os.getenv('GCS_BUCKET_NAME'),
        gcp_conn_id='gcp',
        mime_type='text/csv'
    )
    
    gcs_to_bigquery = aql.load_file(
        task_id='gcs_to_bigquery',
        input_file=File(
            os.getenv('bucket_path') + '/raw/week={{data_interval_end.strftime("%Y-%m-%d")}}/raw_data.csv',
            conn_id='gcp',
            filetype=FileType.CSV
        ),
        output_table=Table(
            name='raw_weather_data',
            conn_id='gcp',
            metadata=Metadata(schema=os.getenv('DATASET_NAME'))
        ),
        if_exists='append',
        use_native_support=False
    )

    [create_bucket, create_dataset] >> create_local_folder >> extract >> load_local >> upload_csv_to_gcs >> gcs_to_bigquery