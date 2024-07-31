from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

#Connection details for snowflake, provided in UI
snowflake_conn_id = "snowflake_conn"

dag= DAG('snowflake_connection_airflow', start_date=datetime(2024,4,13), schedule='@daily',catchup=False)

snowflake_task = SnowflakeOperator(
    task_id="snowflake_task",
    sql="SELECT * FROM Passengers",
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
 )

#Dependencies
snowflake_task


