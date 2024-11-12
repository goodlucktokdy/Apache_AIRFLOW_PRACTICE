from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
import datetime  

with DAG(
    dag_id = 'dags_bash_operator_template',
    schedule = '30 5 * * *',
    start_date = pendulum.datetime(2024,11,13,tz='Asia/Seoul'),
    tags = ['practice','template'],
    catchup = False 
) as dag:
    
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = 'echo "data_interval_end: {{data_interval_end | ts}}" && echo "data_interval_start: {{data_interval_start | ts}}"'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            'START_DATE':'{{data_interval_start | ds}}',
            'END_DATE':'{{data_interval_end | ds}}'
        },
        bash_command = 'echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2