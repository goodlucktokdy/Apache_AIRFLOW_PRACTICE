from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task 
import pendulum
import datetime 

with DAG(
    dag_id = 'dags_python_template',
    schedule = '15 5 * * 4',
    start_date = pendulum.datetime(2024,11,12,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup = True
) as dag:
    def print_regist(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)
        print(kwargs)

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = print_regist,
        op_kwargs = {'start_date':'{{data_interval_start|ds}}','end_date':'{{data_interval_end|ds}}'}
    )

    @task(task_id = 'python_t2')
    def print_regist2(**kwargs):
        print(f'ds: {kwargs['ds']}')
        print(f'ts: {kwargs['ts']}')
        print(f'task_instance: {kwargs['ti']}')
        print(f'start_date: {kwargs['data_interval_start']}')
        print(f'end_date: {kwargs['data_interval_end']}')
    
    python_t1 >> python_t2 = print_regist2()