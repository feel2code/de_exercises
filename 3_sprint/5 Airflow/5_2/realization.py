from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

import datetime
import requests
import pandas as pd
import os
import psycopg2, psycopg2.extras

dag = DAG(
    dag_id='552_postgresql_export_function',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}


def load_file_to_pg(file: str, table: str, conn_args: str):

    df = pd.read_csv(f"/lessons/dags/{file}", index_col=0)

    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stage.{table} ({cols}) VALUES %s"
    pg_conn = psycopg2.connect(conn_args)  # insert hardcoded connection string here
    cur = pg_conn.cursor()

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()

    cur.close()
    pg_conn.close()


def load_files(file_names: str):
    pg = BaseHook.get_connection('pg_connection')
    conn_args = f"host='{pg.host}' port='{pg.port}' dbname='{pg.schema}' user='{pg.login}' password='{pg.password}'"
    print(conn_args)
    # comment everything above and leave conn_args as hardcoded values if you have any issues in submitting code, L30
    for file in file_names:
        load_file_to_pg(file, file[:-4], conn_args)


load_stage = PythonOperator(
    task_id='upload_from_s3',
    python_callable=load_files,
    op_kwargs={
      'file_names': [
          'customer_research.csv', 'user_activity_log.csv', 'user_order_log.csv'
      ]
    },
    dag=dag
)

load_stage
