from airflow import DAG
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator  
import datetime
import pendulum

with DAG(
    dag_id = 'dags_base_branch_operator',
    schedule = '15 09 * * 1-5',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice','branch','python'],
    catchup = False
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random

            tmp = random.choice(['A','B','C'])
            if tmp == 'A':
                return 'task_a'
            else: 
                return ['task_b','task_c']
    custom_branch_operator = CustomBranchOperator(task_id = 'custom_branch_operator')

    def common_func(**kwargs):
        res = kwargs['selected']
        print(res)

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable = common_func,
        op_kwargs = {'selected':'task_a_printed'}
    )

    task_b = PythonOperator(
    task_id = 'task_b',
    python_callable = common_func,
    op_kwargs = {'selected':'task_b_printed'}
    )

    task_c = PythonOperator(
    task_id = 'task_c',
    python_callable = common_func,
    op_kwargs = {'selected':'task_c_printed'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]