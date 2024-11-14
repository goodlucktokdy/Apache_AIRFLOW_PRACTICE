from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import datetime
import pendulum

with DAG(
    dag_id = 'dags_bash_with_variables',
    schedule = '15 9 * * 1-5',
    start_date = pendulum.datetime(2024,11,10,tz='Asia/Seoul'),
    tags = ['practice','bash','variables'],
    catchup = False
) as dag:
    
    variable = Variable.get('sample_key')

    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = f'echo "전역변수 Variable.get: {variable}"'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        bash_command = 'echo 전역변수: {{var.value.sample_key)}}'
    )

    bash_t1 >> bash_t2