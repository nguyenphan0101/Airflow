from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id = 'export_csv_to_postgres', start_date=datetime(2023,1,1),
         schedule_interval="@daily", catchup=False) as dag:
    create_new_table = PostgresOperator(
        task_id='create_new_table',
        postgres_conn_id='postgres_connect',
        sql= '''
            CREATE TABLE IF NOT EXISTS public.transac (
                    index int,
                    InvoiceNo varchar(255),
                    StockCode varchar(255),
                    Description varchar(255),
                    Quantity varchar(255),
                    InvoiceDate varchar(255),
                    UnitPrice float,
                    CustomerID varchar(255),
                    Country varchar(255)
                );
            '''
    )

    insert_data = PostgresOperator(
        task_id="csv_update",
        postgres_conn_id='postgres_connect',
        sql='''
        COPY public.transac FROM 'C:/Program Files/PostgreSQL/16/data/online_retail.csv' DELIMITER ',' CSV HEADER;
    '''
    )
create_new_table >> insert_data