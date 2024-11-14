from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    dag_id = 'dags_bash_with_xcom',
    schedule = '15 15 * * *',
    start_date = pendulum.datetime(2024,11,10,tz='Asia/Seoul'),
    tags = ['practice','xcom'],
    catchup = False
) as dag:
    
    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command =  'echo start &&'
                        'echo xcom_pushed'
                        '{{ti.xcom_push(key = "bash_pushed", value = "first_bash_message")}} &&'
                        'echo SUCCESS_COMPLETE'
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {'pushed_value':'{{ti.xcom_pull(key="bash_pushed")}}'
            ,'return_value':'{{ti.xcom_pull(task_ids="bash_push")}}'},
        bash_command = 'echo "PUSHED_VALUE: $pushed_value" && echo "RETURN_VALUE: $return_value"'
    )

    bash_push >> bash_pull