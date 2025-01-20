from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Определение DAG
with DAG(
    dag_id="simple_test_dag",          # Уникальный ID DAG
    default_args={"owner": "NAllenov"},
    start_date=datetime(2023, 1, 1),  # Дата начала выполнения
    schedule_interval=None,           # Ручной запуск
    catchup=False,                    # Не догонять прошлые даты
    description="A simple test DAG",  # Описание
) as dag:

    # Задачи
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")

    # Определение порядка выполнения
    start >> stop
