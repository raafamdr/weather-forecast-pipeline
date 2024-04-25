import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

load_dotenv()

def extract_data(data_interval_end):
    city_name = 'SaoPaulo'
    url = os.path.join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f"{city_name}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={os.getenv('api_key')}&contentType=csv")

    data = pd.read_csv(url)
    return data

def load_data(data, data_interval_end):
    file_path = f"{os.getenv('path')}/data/week={data_interval_end}/"
    data.to_csv(file_path + 'raw_data.csv')

def transform_data(data_interval_end):
    file_path = f"{os.getenv('path')}/data/week={data_interval_end}/"
    data = pd.read_csv(file_path + 'raw_data.csv', index_col=0)
    engine = create_engine(f"postgresql://{os.getenv('db_user')}:{os.getenv('db_password')}@{os.getenv('db_host')}:{os.getenv('db_port')}/{os.getenv('db_name')}")
    data.to_sql('weather_forecast', con=engine.connect(), if_exists='append', index=False)

with DAG(
    'weather_forecast_pipeline',
    description='A simple weather forecasting ELT pipeline using Apache Airflow, PostgreSQL and Python for the city of São Paulo',
    start_date=datetime(2024, 3, 24),
    schedule='@weekly',
    tags=['data engineering']
) as dag:

    creating_folder = BashOperator(
        task_id='creating_folder',
        bash_command='mkdir -p "$AIRFLOW_HOME/data/week={{data_interval_end.strftime("%Y-%m-%d")}}"',
    )

    extracting_data = PythonOperator(
        task_id='extracting_data',
        python_callable=extract_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'},
        provide_context=True
    )

    loading_data = PythonOperator(
        task_id='loading_data',
        python_callable=load_data,
        op_args=[extracting_data.output],
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'},
        provide_context=True
    )

    transforming_data = PythonOperator(
        task_id='transforming_data',
        python_callable=transform_data,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    creating_folder >> extracting_data >> loading_data >> transforming_data