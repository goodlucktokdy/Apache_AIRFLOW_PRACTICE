from airflow import DAG
from airflow.decorators import task 
import pendulum
import datetime  

with DAG(
    dag_id = 'dags_python_decorator',
    schedule = '30 22 * * 2',
    start_date = pendulum.datetime(2024,11,12,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup = False,
) as dag:
    
    @task(task_id = 'python_task_1')
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')