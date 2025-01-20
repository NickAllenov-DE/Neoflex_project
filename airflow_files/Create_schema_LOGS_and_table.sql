-- Создание схемы LOGS
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Таблица LOGS.ETL_LOGS
CREATE TABLE LOGS.ETL_LOGS (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INTEGER,
    status VARCHAR(20) NOT NULL,
    additional_info TEXT
);
