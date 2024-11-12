from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2
import pendulum
import datetime 

with DAG(
    dag_id = 'dags_python_args_kwargs',
    schedule = '45 22 * * *',
    start_date = pendulum.datetime(2024,11,12,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup = False
) as dag:
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = regist2,
        op_args = ['Kim.','Male','Goyang','I'],
        op_kwargs = {'email' : 'kd','phone' : '010'}
    )

    python_t1