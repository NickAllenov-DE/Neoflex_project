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
    
    # SQL запросы для создания схем и таблиц
    create_schema_and_tables_sql = """
    -- Создание схемы LOGS
    CREATE SCHEMA IF NOT EXISTS LOGS;

    -- Таблица LOGS.ETL_LOGS
    CREATE TABLE IF NOT EXISTS LOGS.ETL_LOGS (
        log_id SERIAL PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        records_processed INTEGER,
        status VARCHAR(20) NOT NULL,
        additional_info TEXT
    );

    -- Создание схемы DS
    CREATE SCHEMA IF NOT EXISTS DS;

    -- Таблица DS.FT_BALANCE_F
    CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
        on_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        currency_rk NUMERIC,
        balance_out REAL,
        PRIMARY KEY (on_date, account_rk)
    );

    -- Таблица DS.FT_POSTING_F
    CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
        oper_date DATE NOT NULL,
        credit_account_rk NUMERIC NOT NULL,
        debet_account_rk NUMERIC NOT NULL,
        credit_amount REAL,
        debet_amount REAL
    );

    -- Таблица DS.MD_ACCOUNT_D
    CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        account_number VARCHAR(20) NOT NULL,
        char_type CHAR(1) NOT NULL,
        currency_rk NUMERIC NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        PRIMARY KEY (data_actual_date, account_rk)
    );

    -- Таблица DS.MD_CURRENCY_D
    CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
        currency_rk NUMERIC NOT NULL,
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        currency_code VARCHAR(3),
        code_iso_char VARCHAR(3),
        PRIMARY KEY (currency_rk, data_actual_date)
    );

    -- Таблица DS.MD_EXCHANGE_RATE_D
    CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        currency_rk NUMERIC NOT NULL,
        reduced_cource REAL,
        code_iso_num VARCHAR(3),
        PRIMARY KEY (data_actual_date, currency_rk)
    );

    -- Таблица DS.MD_LEDGER_ACCOUNT_S
    CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
        chapter CHAR(1),
        chapter_name VARCHAR(16),
        section_number INTEGER,
        section_name VARCHAR(22),
        subsection_name VARCHAR(21),
        ledger1_account INTEGER,
        ledger1_account_name VARCHAR(47),
        ledger_account INTEGER NOT NULL,
        ledger_account_name VARCHAR(153),
        characteristic CHAR(1),
        is_resident INTEGER,
        is_reserve INTEGER,
        is_reserved INTEGER,
        is_loan INTEGER,
        is_reserved_assets INTEGER,
        is_overdue INTEGER,
        is_interest INTEGER,
        pair_account VARCHAR(5),
        start_date DATE NOT NULL,
        end_date DATE,
        is_rub_only INTEGER,
        min_term VARCHAR(1),
        min_term_measure VARCHAR(1),
        max_term VARCHAR(1),
        max_term_measure VARCHAR(1),
        ledger_acc_full_name_translit VARCHAR(1),
        is_revaluation VARCHAR(1),
        is_correct VARCHAR(1)
    );
    """

    # Создание задач для создания схем и таблиц
    create_schema_and_tables_task = SQLExecuteQueryOperator(
        task_id="create_schemas_and_tables",
        sql=create_schema_and_tables_sql,
        conn_id=my_db_conn,
    )

    processing_tasks = []
    for file_name in FILES_TO_PROCESS:
        task = PythonOperator(
            task_id=f"process_and_load_{file_name.split('.')[0]}",
            python_callable=process_and_load,
            op_args=[file_name],
        )
        processing_tasks.append(task)

    create_schema_and_tables_task >> processing_tasks