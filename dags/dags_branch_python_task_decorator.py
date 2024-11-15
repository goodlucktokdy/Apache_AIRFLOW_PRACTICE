from airflow import DAG
from airflow.decorators import task
import datetime
import pendulum

with DAG(
    dag_id = 'dags_branch_python_task_decorator',
    schedule = '15 15 * * *',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice','python','decorator'],
    catchup = False
) as dag:
    @task.branch(task_id = 'python_branch_decorator')
    def select_random():
        import random  
        selected = random.choice(['A','B','C'])

        if selected == 'A':
            return 'task_a'
        elif selected in ['B','C']:
            return ['task_b','task_c']

    def common_func(**kwargs):
        res = kwargs['selected']
        print(res)    
    
    @task.python(task_id = 'task_a')
    def task_a():
        tmp = {'selected':'selected_A'}
        return common_func(tmp)
    
    @task.python(task_id = 'task_b')
    def task_b():
        tmp = {'selected':'selected_B'}
        return common_func(tmp)
    
    @task.python(task_id = 'task_c')
    def task_c():
        tmp = {'selected':'selected_C'}
        return tmp
    
    select_random() >> [task_a(),task_b(),task_c()]