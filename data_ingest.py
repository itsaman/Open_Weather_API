import requests
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import pandas as pd
import requests 
import json

#S3 connection created in Airflow UI
S3_conn_id = "S3_connection"

def ingest_csv(**kwargs):
    ti = kwargs['ti']
    url = ('https://newsapi.org/v2/everything?''q=Apple&''from=2024-04-17&''sortBy=popularity&''apiKey=')
    response = requests.get(url)
    data = response.json()
    articles_element = {}
    article_list = []

    for row in data['articles']:
        publication_name = row['source']['name']
        author_name = row['author']
        title = row['title']
        url = row['url']
        publishedAt = row['publishedAt']
        description = row['description']

        articles_element = {
            'publication_name': publication_name,
            'author_name': author_name,
            'title': title,
            'url': url,
            'publishedAt': publishedAt,
            'description': description
        }
        article_list.append(articles_element)

    article_df = pd.DataFrame(article_list)
    print(article_df)
    ti.xcom_push(key='article_data', value=article_df.to_csv(index=False))
    
#Dag 
dag = DAG('News_data_ingestion', start_date=datetime(2024,4,16), schedule='@daily',catchup=False)


#Task to call the python function
get_news_data = PythonOperator(
    task_id = 'get_data',
    python_callable=ingest_csv,
    provide_context=True,
    dag=dag
)

#Move csv file to s3 bucket
move_to_bucket = S3CreateObjectOperator(
    task_id = 'move_to_bucket',
    aws_conn_id=S3_conn_id,
    s3_bucket='awsbootcamp-24',
    s3_key='date = {{ ds }}/articles.csv',
    data="{{ ti.xcom_pull(key = 'article_data')}}",
    dag=dag
)

get_news_data >> move_to_bucket
