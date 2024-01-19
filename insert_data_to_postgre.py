from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(dag_id = 'import_data', start_date=datetime(2023,1,1), schedule_interval='@once',
         catchup=False) as dag:
    import_task = PostgresOperator(
        task_id='import_data',
        postgres_conn_id='postgres_connect',
        sql='''
            -- Inserting data into the cars_2 table
        INSERT INTO public.cars_2 (brand, model, year) VALUES ('Toyota', 'Camry', 2020);
        INSERT INTO public.cars_2 (brand, model, year) VALUES ('Honda', 'Accord', 2019);
        INSERT INTO public.cars_2 (brand, model, year) VALUES ('Ford', 'Mustang', 2022);
        INSERT INTO public.cars_2 (brand, model, year) VALUES ('Chevrolet', 'Cruze', 2018);
        INSERT INTO public.cars_2 (brand, model, year) VALUES ('Nissan', 'Altima', 2021);
        '''
    )

import_task
