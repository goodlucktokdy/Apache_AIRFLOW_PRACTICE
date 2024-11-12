from airflow import DAG 
import pendulum
import datetime  
from airflow.operators.email import EmailOperator  

with DAG(
    dag_id = 'dags_email_operator',
    schedule = '0 8 1 * *',
    start_date = pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup = False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'kd01051@naver.com',
        subject = 'airflow_email_오퍼레이터_실습',
        html_content='Airflow email operator 실습입니다.'
    )