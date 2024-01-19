from airflow import DAG
from datetime import datetime
import io
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

default_args={"owner":"nguyenphan","email_on_failure":"True","email_on_retry":"True","email":["nguyenphan01012002@gmail.com"]}
with DAG(dag_id="Email_From_AWS_SES",start_date=datetime(2024,1,1),schedule_interval="@once",default_args=default_args) as dag:

    email_function=EmailOperator(
        task_id="email_function_2",
        to="nguyenphan01012002@gmail.com",
        subject="Alert Mail",
        html_content="""Error.Check the DAGs now!"""
    )

email_function