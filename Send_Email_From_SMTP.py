from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

default_args={"owner":"nguyenphan","email_on_failure":"True","email_on_retry":"True","email":["nguyenphan.work@outlook.com"]}
with DAG(dag_id="Email_From_SMTP_to_Outlook",start_date=datetime(2024,1,1),schedule_interval="@once",default_args=default_args) as dag:

    email_function=EmailOperator(
        task_id="email_function_1",
        to="anhht@ierp.vn",
        subject="Airflow Mail",
        html_content="""Something went wrong!
        """
    )

email_function
