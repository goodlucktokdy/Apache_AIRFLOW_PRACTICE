from airflow import DAG
from airflow.task import task
import pendulum
import datetime

with DAG(
    dag_id = 'dags_python_task_template',
    schedule = '45 5 * * 4#2',
    start_date = pendulum.datetime(2024,11,13,tz='Asia/Seoul'),
    tags = ['practice','python','template'],
    catchup = True
) as dag:
    @task(task_id = 'python_t1')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(**kwargs)
    
    python_t1 = show_templates()