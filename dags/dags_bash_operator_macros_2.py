from airflow import DAG
from airflow.operators.bash import BashOperator
from dateutil import relativedelta
import pendulum
import datetime 

with DAG(
    dag_id = 'dags_bash_operator_macros_2',
    schedule = '10 0 * * 6#2',
    start_date = pendulum.datetime(2024,11,1,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup=False
) as dag:
    # start_date: 현재배치 2주전 월요일, end_date: 현재배치 2주전 토요일
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        env = {'start_date':'{{(data_interval_end.in_timezone('Asia/Seoul') + macros.relativedelta.relativedelta(days=-19)) | ds}}',
               'end_date':'{{data_interval_end.in_timezone('Asia/Seoul') + macros.relativedelta.relativedelta(days=-14) | ds}}'}
        bash_command = 'echo "start_date: $start_date && echo "end_date" $end_date"'
    )

    bash_t1