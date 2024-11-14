from airflow import DAG
from airflow.operators.bash import BashOperator
from dateutil import relativedelta 
import datetime
import pendulum

with DAG(
    dag_id = 'dags_bash_operator_macros',
    schedule = '10 10 L * *',
    start_date = pendulum.datetime(2024,11,1,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup = False
) as dag:
    # --start_date: 전월말일, --end_date: 현재 배치 1일전
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        env = {'start_date':'{{data_interval_start.in_timezone('Asia/Seoul')| ds}}',
               'end_date':'{{(data_interval_end.in_timezone('Asia/Seoul') - macros.relativedelta.relativedelta(days=1)) | ds}}'},
        bash_command = 'echo "start_date: $start_date" && echo "end_date: $end_date"'
    )

    bash_t1