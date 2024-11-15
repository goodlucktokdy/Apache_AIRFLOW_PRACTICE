from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task 
import datetime 
import pendulum

with DAG(
    dag_id = 'dags_python_with_trigger_rule_eg2',
    schedule = '15 15 * * 1-5',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    catchup = False, 
    tags = ['trigger_rule','practice']
) as dag:
    
    @task.branch(task_id = 'python_random_choice')
    def random_choice():
        import random  

        tmp = random.randint(0,2)
        tmp_list = ['A','B','C']
        res = tmp_list[tmp]

        if res == 'A':
            return 'task_a'
        elif res in ['B','C']:
            return ['task_b','task_c']
        
    @task.python(task_id = 'task_a')
    def task_a():
        print(f'task_a 정상수행!!')

    @task.python(task_id = 'task_b')
    def task_b():
        print(f'task_b 정상수행!!')

    @task.python(task_id = 'task_c')
    def task_c():
        print(f'task_c 정상수행!!')

    @task.python(task_id = 'task_d', trigger_rule = 'none_skipped')
    def task_d():
        print(f'task_d 정상수행!!')