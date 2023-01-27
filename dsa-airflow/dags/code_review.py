from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import random
import pandas as pd 
import numpy as np

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]


default_args = {
    'start_date': days_ago(2), 
    'schedule_interval': timedelta(days=1), 
    'retries': 1, 
    'retry_delay': timedelta(seconds=10), 
}

with DAG(
  'apple',
  description='Prints name into file, reads name with hello, and selects three random apples',
  default_args=default_args
) as dag:

  echo_to_file = BashOperator(
    task_id='echo_to_file'
    bash_command='echo Drew > /opt/airflow/dags/code_review.txt'
  )

  print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello
  )


    




