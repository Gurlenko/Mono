from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
from scripts.mono_transactions import run_mono_transactions


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_mono_transactions',
    default_args=default_args,
    description='DAG для запуску скрипту mono_transactions.py',
    schedule_interval=None,  # Запускати вручну
    start_date=days_ago(1),
    catchup=False,
)

run_script = PythonOperator(
    task_id='run_mono_transactions_script',
    python_callable=run_mono_transactions,
    dag=dag,
)

run_script
