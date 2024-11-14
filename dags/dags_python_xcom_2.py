from airflow import DAG
from airflow.decorators import task
import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_xcom_2',
    schedule = '13 15 * * *',
    start_date = pendulum.datetime(2024,11,13,tz='Asia/Seoul'),
    tags = ['practice','xcom'],
    catchup = False
) as dag:
    
    @task(task_id = 'xcom_push_1')
    def xcom_push_1(**kwargs):
        return 'SUCCESS'
    
    @task(task_id = 'xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value = ti.xcom_pull(task_ids = 'xcom_push_1')

        print(f'ti 인스턴스 및 xcom_pull메서드로 받은 값: {value}')

    @task(task_id = 'xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print(f'함수 리턴값으로 받은 값: {status}')

    xcom_push_1_by_return = xcom_push_1()
    xcom_pull_2(xcom_push_1_by_return)
    xcom_push_1_by_return >> xcom_pull_1()
