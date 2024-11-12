from airflow import DAG 
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp
import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_import_func',
    schedule = '0 22 * * 3#2',
    start_date = pendulum.datetime(2024,11,12,tz='Asia/Seoul'),
    catchup = False,
    tags = 'practice'
) as dag:
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = get_sftp
    )

    python_t1