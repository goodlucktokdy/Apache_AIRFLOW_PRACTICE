from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.decorators import task  
import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_with_trigger_rule_eg1',
    schedule = '15 15 * * 1-5',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice','trigger_rule'],
    catchup = False
) as dag:
    
       
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo "task_a 정상수행!!"'
    )

    @task.python(task_id = 'task_b')
    def task_b():
        raise AirflowException("task_b 오류!!")
    
    @task.python(task_id = 'task_c')
    def task_c():
        return 'task_c 정상수행!!'
    
    @task.python(task_id = 'task_d', trigger_rule = 'all_done')
    def task_d():
        return 'task_d 정상수행!!'
    task_b = task_b();task_c = task_c();task_d = task_d()

    [task_a, task_b, task_c] >> task_d