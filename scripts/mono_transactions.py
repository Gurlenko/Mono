import requests
import psycopg2
from typing import List, Dict
from airflow.exceptions import AirflowException

def get_data_from_api(account: int, from_date: int, to_date: int, xtoken: str, **kwargs) -> dict:
    """
    Отримує дані з API Monobank і зберігає їх в XCom.
    """
    url = f'https://api.monobank.ua/personal/statement/{account}/{from_date}/{to_date}'
    print(xtoken)
    print(url)
    headers = {
        'Content-Type': 'application/json',
        'X-Token': xtoken 
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(response.status_code)
    data = response.json()
    kwargs['ti'].xcom_push(key='api_data', value=data)
    return response.json()
    
def connection_to_db(**kwargs):
    """
    Підключається до бази даних PostgreSQL і перевіряє з'єднання.
    """
    try:
        conn = psycopg2.connect(
            host="postgres_db",
            database="monobank",
            user="admin",
            password="admin",
            port=5432
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            conn.close()
        return True
    except Exception as e:
        print(f"Помилка при підключенні до бази даних: {e}")
        return False

def insert_in_postgres(**kwargs):
    """
    Вставляє дані в таблицю PostgreSQL.
    """
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='get_json_transactions', key='api_data')
    if not isinstance(records, list):
        raise AirflowException(f"Expected list of records, but got {type(records)}")

    try:
        conn = psycopg2.connect(
            host="postgres_db",
            database="monobank",
            user="admin",
            password="admin",
            port=5432
        )
        insert_query = """
        INSERT INTO transactions (
            id, time, description, mcc, originalMcc, hold, amount, operationAmount,
            currencyCode, commissionRate, cashbackAmount, balance, comment, receiptId,
            invoiceId, counterEdrpou, counterIban, counterName
        ) VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING; 
        """

        with conn.cursor() as cursor:
            for record in records:
                cursor.execute(insert_query, (
                    record.get('id'),
                    record.get('time'),
                    record.get('description'),
                    record.get('mcc'),
                    record.get('originalMcc'),
                    record.get('hold'),
                    record.get('amount'),
                    record.get('operationAmount'),
                    record.get('currencyCode'),
                    record.get('commissionRate'),
                    record.get('cashbackAmount'),
                    record.get('balance'),
                    record.get('comment'),
                    record.get('receiptId'),
                    record.get('invoiceId'),
                    record.get('counterEdrpou'),
                    record.get('counterIban'),
                    record.get('counterName')
                ))
            conn.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise AirflowException(f"Failed to insert data: {e}")
    finally:
        if conn:
            conn.close()
