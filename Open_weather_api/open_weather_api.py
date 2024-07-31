from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import json
import pandas as pd
import requests

# S3 connection
S3_CONN_ID = "S3_connection"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,13),
    'retries': 2,
    'retry_delay': timedelta(minutes =5)
}

# Setting up API endpoints and parameters
api_endpoints = "https://api.openweathermap.org/data/2.5/forecast"
api_params = {
        "q":"Assam,India",
        "appid" : Variable.get("api_key")
        }

# To extract data from API
def extract_weather_data(**kwargs):
    print('Extraction started')
    ti = kwargs['ti']
    response = requests.get(api_endpoints,api_params)
    data = response.json()
    print(data)
    df = pd.json_normalize(data['list'])
    print(df)
    ti.xcom_push(key = 'final_data', value = df.to_csv(index=False))


dag = DAG('open_weather_api', default_args=default_args,
          catchup=False, schedule_interval='@daily')


# Python Operator
extract_api_task = PythonOperator(
    task_id='extract_api_task',
    python_callable=extract_weather_data,
    provide_context = True,
    dag=dag
)


# Storing file in S3
upload_to_s3 = S3CreateObjectOperator(
    task_id='upload_to_s3',
    aws_conn_id=S3_CONN_ID,
    s3_bucket='openweather-data-gds',
    s3_key = 'date = {{ ds }}/openweather_data.csv',
    data= "{{ ti.xcom_pull(key = 'final_data')}}",
    dag =dag
)



# Task Dependencies
extract_api_task >> upload_to_s3


