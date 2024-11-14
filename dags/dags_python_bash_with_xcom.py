from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_bash_with_xcom',
    schedule = '30 6 * * *',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice','python','bash','xcom'],
    catchup = False
) as dag:
    
    @task(task_id = 'python_push')
    def python_push():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {'status':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
               'data':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
               'options_cnt':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'},
        bash_command='echo "status: $status" && echo "data: $data" && echo "options_cnt: $options_cnt"'
    )
    
    python_push() >> bash_pull

    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command = 'echo start_xcom_push &&'
                        'echo "{{ti.xcom_push(key = "bash_pushed", value = 200)}}" &&'
                        'echo complete_push'
    )

    @task(task_id = 'python_pull')
    def python_pull(**kwargs):
        ti = kwargs['ti']
        pushed_value = ti.xcom_pull(key = 'bash_pushed')
        return_value = ti.xcom_pull(task_ids = 'bash_push')

        print(f'pushed_value: {pushed_value}')
        print(f'return_value: {return_value}')


    bash_push >> python_pull()