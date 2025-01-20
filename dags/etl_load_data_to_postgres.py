from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import re
import chardet


# Конфигурация
# DATA_PATH = Variable.get("DATA_FILES_PATH")
DATA_PATH = "/airflow_files/"
FILES_TO_PROCESS = [
    "ft_balance_f.csv", 
    "ft_posting_f.csv", 
    "md_account_d.csv", 
    "md_currency_d.csv", 
    "md_exchange_rate_d.csv", 
    "md_ledger_account_s.csv"
]
DS_SCHEMA = "DS"
LOG_SCHEMA = "LOGS"
LOG_TABLE = "etl_logs"
my_db_conn = 'ETLBanking-conn'


# Настройки по умолчанию для DAG
default_args = {
    'owner': 'NAllenov',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


def log_etl_process(process_name, status, start_time, end_time=None, postgres_conn_id=my_db_conn):
    """
    Логирует процесс ETL в таблицу логов.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Создание схемы и таблицы логов при необходимости
        cursor.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};
        CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
            process_name TEXT,
            status TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP
        );
        """)
        conn.commit()

        # Вставка записи лога
        cursor.execute(f"""
        INSERT INTO {LOG_SCHEMA}.{LOG_TABLE} (process_name, status, start_time, end_time)
        VALUES (%s, %s, %s, %s)
        """, (process_name, status, start_time, end_time))
        conn.commit()

    except Exception as e:
        raise RuntimeError(f"Ошибка логирования процесса {process_name}: {e}")

    finally:
        cursor.close()
        conn.close()


def prepare_dataset(file_path, sep=";"):
    """
    Подготавливает датасет для загрузки в БД. 
    """
    try:

        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']

        # Читаем заголовки и определяем столбцы с "code" в названии
        header = pd.read_csv(file_path, nrows=0, sep=sep, encoding=encoding)
        columns_with_code = [col for col in header.columns if "code" in col.lower()]
        dtype_mapping = {col: str for col in columns_with_code}

        # Читаем данные с заданными типами
        data = pd.read_csv(file_path, sep=sep, encoding=encoding, dtype=dtype_mapping)

        # Однострочный вариант проверки заголовков:
        # data = pd.read_csv(file_path, sep=sep, encoding=encoding, dtype={col: str for col in pd.read_csv(file_path, nrows=0).columns if "code" in col})

        data.columns = [col.lower().strip().replace(';', '') for col in data.columns]  # Приведение имен столбцов к нижнему регистру
        data.drop_duplicates(inplace=True)

        # Функция для очистки строк
        def clean_string(s):
            if pd.isnull(s) or s == '':
                return 'N/A'  # Замена пустых строк на 'N/A'
            cleaned = re.sub(r'[^\x20-\x7Eа-яА-ЯёЁ]+', 'N/A', str(s))  # Замена нечитаемых знаков на 'N/A'
            return cleaned.strip()

        for col in data.columns:
            if data[col].dtype == np.int64:
                data[col] = data[col].astype(int)
            elif data[col].dtype == np.float64:
                data[col] = data[col].astype(float)
            elif data[col].dtype == object:
                data[col] = data[col].apply(clean_string)  # Применяем очистку к строковым данным

        for col in data.columns:
            if "date" in col:
                data[col] = pd.to_datetime(data[col], errors='coerce')

        for col in data.columns:
            if data[col].isnull().any():
                if data[col].dtype in [int, float]:
                    data[col].fillna(0, inplace=True)
                elif "date" in col:
                    data[col].fillna(pd.Timestamp("1900-01-01").date(), inplace=True)
                else:
                    data[col].fillna("N/A", inplace=True)
        return data

    except Exception as e:
        raise RuntimeError(f"Ошибка при подготовке датасета из файла {file_path}: {e}")


def load_dataset_to_db(data, table_name, postgres_conn_id, schema=DS_SCHEMA, clear_before_load=True):
    """
    Загружает подготовленный DataFrame в базу данных PostgreSQL.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        if clear_before_load:
            cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE;")
            conn.commit()

        # Преобразование типов данных
        data = data.astype(object).where(pd.notnull(data), None)  # Заменяем NaN на None
        values = [
            tuple(map(lambda x: x.item() if isinstance(x, (np.integer, np.floating)) else x, row))
            for row in data.to_records(index=False)
        ]

        columns = ', '.join([f'{col}' for col in data.columns])
        placeholders = ', '.join(['%s'] * len(data.columns))
        insert_query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"

        cursor.executemany(insert_query, values)
        conn.commit()

    except Exception as e:
        raise RuntimeError(f"Ошибка загрузки данных в таблицу {schema}.{table_name}: {e}")

    finally:
        cursor.close()
        conn.close()


def process_and_load(file_name):
    """
    Выполняет процесс ETL для одного файла с логированием.
    """
    start_time = datetime.now()
    try:
        file_path = os.path.join(DATA_PATH, file_name)
        data = prepare_dataset(file_path)
        table_name = file_name.split(".")[0].lower()
        load_dataset_to_db(data, table_name, my_db_conn, clear_before_load=True)
        log_etl_process(file_name, "SUCCESS", start_time, datetime.now())

    except Exception as e:
        log_etl_process(file_name, "FAILED", start_time, datetime.now())
        raise RuntimeError(f"Ошибка в процессе {file_name}: {e}")

with DAG(
    dag_id="etl_csv_to_db_with_logs",
    default_args=default_args,
    description="ETL процесс: создание схем, загрузка CSV в PostgreSQL с логированием",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ETL", "PostgreSQL"],
    template_searchpath=[DATA_PATH]
) as dag:

    processing_tasks = []
    for file_name in FILES_TO_PROCESS:
        task = PythonOperator(
            task_id=f"process_and_load_{file_name.split('.')[0]}",
            python_callable=process_and_load,
            op_args=[file_name],
        )
        processing_tasks.append(task)
