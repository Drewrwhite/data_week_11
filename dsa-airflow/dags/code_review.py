from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import random
import pandas as pd 
import numpy as np
import os

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
  path = os.path.abspath(__file__)
  dir_name = os.path.dirname(path)
  with open(f"{dir_name}/code_review.txt", "r") as f:
    print(f"Hello, {f.read()}!")

def random_apple():
  choice = random.choice(APPLES)
  print(f"You must like {choice} apples!")


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
    task_id='echo_to_file',
    bash_command='echo "Drew" > /opt/airflow/dags/code_review.txt'
  )

  print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello
  )

  picking_task = BashOperator(
    task_id='picking_task',
    bash_command='echo "Now picking three apples"'
  )

  apple_task = []
  for i in range(3):
    task = PythonOperator(
      task_id=f"apple_{i}",
      python_callable=random_apple
    )
    apple_task.append(task)

  end_task = EmptyOperator(
    task_id='end'
  )




