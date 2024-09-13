from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago, datetime
from scripts.mono_transactions import get_data_from_api, insert_in_postgres, connection_to_db
from datetime import timedelta  
import pendulum 

dotenv_path = '/Users/admin/Desktop/Mono/dags/.env'

xtoken = Variable.get("XToken")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),  
}

def convert_to_unix(ds, **kwargs):
    from_date = pendulum.parse(ds).start_of('day').int_timestamp
    to_date = pendulum.parse(ds).end_of('day').int_timestamp
    print(ds)
    return from_date, to_date

with DAG(
    dag_id='Mono_transactions',
    default_args=default_args,
    description='Отримання даних по операціям Монобанку',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 8, 29),
    catchup=True,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    convert_dates = PythonOperator(
        task_id='convert_dates',
        python_callable=convert_to_unix,
        provide_context=True
    )

    get_data = PythonOperator(
        task_id='get_json_transactions',
        python_callable=get_data_from_api,
        op_kwargs={
            'xtoken': xtoken,
            'account': 0,
            'from_date': '{{ task_instance.xcom_pull(task_ids="convert_dates")[0] }}',
            'to_date': '{{ task_instance.xcom_pull(task_ids="convert_dates")[1] }}'
        }
    )

    connections = PythonOperator(
        task_id='check_connection',
        python_callable=connection_to_db,
        provide_context=True
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_in_postgres,
        provide_context=True
    )

    start >> convert_dates >> get_data >> connections >> insert_data >> end
