from airflow import DAG
from airflow.operators.python import PythonOperator 
import datetime 
import pendulum
import random 

with DAG(
    dag_id = 'dags_python_operator',
    schedule = '30 6 * * 3#2',
    start_date = pendulum.datetime(2024,11,12,tz='Asia/Seoul'),
    catchup = False 
) as dag:
    def select_fruit_py():
        fruit = ['apple','banana','tomato','avocado']
        rand_fruit = random.randint(0,3)
        print(fruit[rand_fruit])

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = select_fruit_py,
    )

    python_t1