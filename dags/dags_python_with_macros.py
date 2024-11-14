from airflow import DAG
from airflow.decorators import task
import pendulum
import datetime

with DAG(
    dag_id = 'dags_python_with_macros',
    schedule = '10 0 * * *',
    start_date = pendulum.datetime(2024,11,11,tz='Asia/Seoul'),
    tags = ['practice'],
    catchup = False
) as dag:
    
    @task(task_id = 'python_datetime_calc',
          templates_dict={'start_date':'{{(data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1,days=1))|ds}}',
                          'end_date':'{{(data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1))|ds}}'
                          }
    )
    def python_datetime_calc(**kwargs):

        templates_dict = kwargs.get('template_dict') or ''
        start_date = templates_dict.get('start_date') or 'start_date 없음'
        end_date = templates_dict.get('end_date') or 'end_date 없음'

        print(f'start_date: {start_date}')
        print(f'end_date: {end_date}')

    @task(task_id = 'python_datetime_calc2')
    def python_datetime_calc2(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs.get('data_interval_end') or ''
        start_date = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1,days=1)
        end_date = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)

        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        print(f'start_date: {start_date}')
        print(f'end_date: {end_date}')

    python_datetime_calc() >> python_datetime_calc2()