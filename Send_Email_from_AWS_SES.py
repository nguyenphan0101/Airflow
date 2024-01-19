#Add to docker-compose.yaml
#Send email from AWS SES
    #AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.providers.amazon.aws.utils.emailer.send_email
    #AIRFLOW__EMAIL__EMAIL_CONN_ID: aws_default
    #AIRFLOW__EMAIL__MAIL_FROM: From email nguyenphan01012002@gmail.com 
    #AIRFLOW__EMAIL__FROM_EMAIL: nguyenphan01012002@gmail.com
    #AIRFLOW__EMAIL__SMTP_HOST: email-smtp.ap-southeast-1.amazonaws.com

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
        html_content="Error. Check the DAGs now!"
    )

email_function


