from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os

dag = DAG(
    dag_id='542_s3_load_example',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}


def upload_from_s3(file_names):
    full_lesson_path = os.path.dirname(os.path.abspath(__file__))
    for file in file_names:
        url = f"https://storage.yandexcloud.net/s3-sprint3-static/lessons/{file}"
        c = pd.read_csv(url)
        c.to_csv(f'{full_lesson_path}/{file}')


t_upload_from_s3 = PythonOperator(task_id='upload_from_s3',
                                  python_callable=upload_from_s3,
                                  op_kwargs={
                                      'file_names': [
                                          'customer_research.csv', 'user_activity_log.csv', 'user_order_log.csv'
                                      ]
                                  },
                                  dag=dag)

t_upload_from_s3
