
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

conn_id = 'ETLBanking-conn'

def test_connection():
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    print(f"Connected to: {conn.host}, Database: {conn.schema}")


default_args = {
    "owner": "NAllenov",
    "start_date": datetime(2025, 1, 8),
    "retries": 2
}


with DAG(
    dag_id = "test_postgres_connection",
    default_args = default_args,
    description = "Проверка соединения с базой данных",
    catchup = False,
    schedule_interval='@once'
    ) as dag:
    
    test_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_connection
    )
