from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    dag_id = 'dags_branch_python_operator',
    schedule = '15 9 * * *',
    start_date = pendulum.datetime(2024,11,10,tz='Asia/Seoul'),
    tags = ['practice','python','branch'],
    catchup = False
) as dag:
    def select_random():
        import random  

        seq = random.choice(['A','B','C'])
        if seq == 'A':
            return 'task_a'
        elif seq in ['B','C']:
            return ['task_b','task_c']
        
    python_branch_task = BranchPythonOperator(
        task_id = 'python_branch_task',
        python_callable = select_random
    )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable = common_func,
        op_kwargs = {'selected':'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable = common_func,
        op_kwargs = {'selected':'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable = common_func,
        op_kwargs = {'selected':'C'}
    )

    python_branch_task >> [task_a,task_b,task_c]