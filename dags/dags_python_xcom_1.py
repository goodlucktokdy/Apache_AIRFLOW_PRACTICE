from airflow import DAG
from airflow.decorators import task
import pendulum
import datetime

with DAG(
    dag_id = 'dags_python_xcom_1',
    schedule = '15 12 * * *',
    start_date = pendulum.datetime(2024,11,15,tz='Asia/Seoul'),
    tags = ['practice','xcom'],
    catchup = False
) as dag:

    @task(task_id = 'python_xcom_push_1')
    def python_xcom_push_1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key = 'result_1', value = 'value_1')
        ti.xcom_push(key = 'result_2', value = [1,2,3,4,5])

    @task(task_id = 'python_xcom_push_2')
    def python_xcom_push_2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key = 'result_1', value = 'value_2')
        ti.xcom_push(key = 'result_2', value = [1,2,3])

    @task(task_id = 'python_xcom_pull_1')
    def python_xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        result1 = ti.xcom_pull(key = 'result_1')
        result2 = ti.xcom_pull(key = 'result_2', task_ids = 'python_xcom_push_1')

        print(f'result1: {result1}')
        print(f'result2: {result2}')

    python_xcom_push_1() >> python_xcom_push_2() >> python_xcom_pull_1()