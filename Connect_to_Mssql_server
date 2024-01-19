


#Add into docker-compose.yaml
# _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- apache-airflow-providers-docker apache-airflow-providers-microsoft-mssql}

from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(dag_id = 'connect_to_MSSQL', start_date=datetime(2023,1,1)
         ,schedule_interval='@daily', catchup=False) as dag:
    create_table= MsSqlOperator(
        task_id = 'create_new_table',
        mssql_conn_id = 'mssql_connection',
        sql = '''
        -- Create the car_2 table if it does not exist
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'car_2')
    BEGIN
    CREATE TABLE dbo.car_2 (
        brand VARCHAR(255),
        model VARCHAR(255),
        year INT
    );
END;

'''
    )

    insert_data = MsSqlOperator(
        task_id = 'insert_data',
        mssql_conn_id = 'mssql_connection',
        sql = '''
    -- Inserting data into the car_2 table
    INSERT INTO dbo.car_2 (brand, model, year) VALUES ('Toyota', 'Camry', 2020);
    INSERT INTO dbo.car_2 (brand, model, year) VALUES ('Honda', 'Accord', 2019);
    INSERT INTO dbo.car_2 (brand, model, year) VALUES ('Ford', 'Mustang', 2022);
    INSERT INTO dbo.car_2 (brand, model, year) VALUES ('Chevrolet', 'Cruze', 2018);
    INSERT INTO dbo.car_2 (brand, model, year) VALUES ('Nissan', 'Altima', 2021);

        '''
    )

create_table >> insert_data
