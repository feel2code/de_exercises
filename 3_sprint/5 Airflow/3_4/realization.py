import datetime
import json

import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='533_api_generate_report',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}

nickname = "nickname"
cohort = "cohort"
api_token = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}


def create_files_request(**headers):
    api_conn = BaseHook.get_connection('create_files_api')
    api_endpoint = api_conn.host

    method_url = '/generate_report'
    r = requests.post(
        'https://' + api_endpoint + method_url,
        headers={"X-API-KEY": api_token, "X-Nickname": nickname, "X-Cohort": cohort}
    )
    response_dict = json.loads(r.content)

    print(headers)

    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']


task = PythonOperator(
        task_id='request_report',
        python_callable=create_files_request,
        op_kwargs=headers,
        dag=dag)

task
