from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator
import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_email_xcom',
    schedule = '15 15 * * 1-5',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice','python','email','xcom'],
    catchup = False
) as dag:


    @task(task_id = 'python_email')
    def python_email():
        import random 
        res = random.choice(['SUCCESS','FAIL'])

        return res
    
    email_pull = EmailOperator(
        task_id = 'email_pull',
        to = 'kd01051@naver.com',
        subject = '{{data_interval_end.in_timezone("Asia/Seoul")|ds}} 수행 결과',
        html_content='{{data_interval_end.in_timezone("Asia/Seoul")|ds}} 수행 결과를 알려드립니다. <br> \
                    {{ti.xcom_pull(task_ids = "python_email")}}'
    )

    python_email() >> email_pull