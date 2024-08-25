from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators import DummyOperator
from airflow.utils.dates import days_ago, datetime, timedelta
from scripts.mono_transactions import get_data_from_api
from dotenv import load_dotenv
import os

dotenv_path = '/Users/admin/Desktop/Mono/.env'
load_dotenv(dotenv_path)


xtoken = os.getenv('XToken')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='Mono_transactions',
    default_args=default_args,
    description='Простий DAG з Dummy та Python операторами',
    schedule_interval='@daily',
    schedule_interval='0 0 * * *',
    start_date=datetime(2019, 6, 10),
    catchup=True,
) as dag:

    start = DummyOperator(
        task_id='start'
    )


    end = DummyOperator(
        task_id='end'
    )

    task1 = PythonOperator(
        task_id='get_json_transactions',
        python_callable=get_data_from_api,
        op_kwargs={
            'token': xtoken,
            'account': 0,
            'from_date': '{{ ds }}',
            'to_date': '{{ ds }}' 
        }
    )

    task2 = PythonOperator(
        task_id='my_second_task',
        python_callable=my_second_function
    )

    task3 = PythonOperator(
        task_id='my_third_task',
        python_callable=my_third_function
    )




    start >> [task1, task2] >> task3 >> end

