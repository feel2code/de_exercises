import datetime
import time
import psycopg2

import requests
import json
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

api_conn = BaseHook.get_connection('create_files_api')
api_endpoint = api_conn.host
api_token = "5f55e6c0-e9e5-4a9c-b313-63c01fc31460"

# set user constants for api
nickname = 'nickname'
cohort = 'cohort'

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}


def create_files_request(ti, api_endpoint):
    method_url = '/generate_report'
    r = requests.post('https://' + api_endpoint + method_url, headers={
        "X-API-KEY": api_token,
        "X-Nickname": nickname,
        "X-Cohort": cohort
    })
    response_dict = json.loads(r.content)
    print(response_dict)
    ti.xcom_push(key='task_id', value=response_dict['task_id'])
    print(f"task_id is {response_dict['task_id']}")
    return response_dict['task_id']


def check_report(ti, api_endpoint):
    task_ids = ti.xcom_pull(key='task_id', task_ids=['create_files_request'])
    print(task_ids)
    task_id = task_ids[0]

    method_url = '/get_report'
    payload = {'task_id': task_id}

    for i in range(4):
        time.sleep(70)
        r = requests.get('https://' + api_endpoint + method_url, params=payload, headers={
            "X-API-KEY": api_token,
            "X-Nickname": nickname,
            "X-Cohort": cohort
        })
        response_dict = json.loads(r.content)
        print(i, response_dict['status'])
        if response_dict['status'] == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break

    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id is {report_id}")
    return report_id


def upload_from_s3_to_pg(ti, nickname, cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    storage_url = 'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}'

    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort)
    personal_storage_url = personal_storage_url.replace("{NICKNAME}", nickname)
    personal_storage_url = personal_storage_url.replace("{REPORT_ID}", report_id)

    # insert to database
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' "
        f"password='{psql_conn.password}'"
    )
    cur = conn.cursor()

    # get custom_research
    df_customer_research = pd.read_csv(
        personal_storage_url.replace("{FILE_NAME}", "customer_research.csv")
    )
    df_customer_research.reset_index(drop=True, inplace=True)
    insert_cr = "insert into stage.customer_research (date_id,category_id,geo_id,sales_qty,sales_amt) VALUES {cr_val};"
    i = 0
    step = int(df_customer_research.shape[0] / 100)
    while i <= df_customer_research.shape[0]:
        print('df_customer_research', i, end='\r')

        cr_val = str([tuple(x) for x in df_customer_research.loc[i:i + step].to_numpy()])[1:-1]
        print(cr_val)
        cur.execute(insert_cr.replace('{cr_val}', cr_val))
        conn.commit()

        i += step + 1

    # get order log
    df_order_log = pd.read_csv(
        personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv")
    )
    df_order_log.reset_index(drop=True, inplace=True)
    insert_uol = (
        "insert into stage.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, "
        "item_id, item_name, quantity, payment_amount) VALUES {uol_val};"
    )
    i = 0
    step = int(df_order_log.shape[0] / 100)
    while i <= df_order_log.shape[0]:
        print('df_order_log', i, end='\r')

        uol_val = str([
            tuple(x) for x in df_order_log.drop(
                columns=['id'], axis=1).drop(columns=['uniq_id'], axis=1).loc[i:i + step].to_numpy()
        ])[1:-1]
        cur.execute(insert_uol.replace('{uol_val}', uol_val))
        conn.commit()

        i += step + 1

    # get activity log
    df_activity_log = pd.read_csv(
        personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv")
    )
    print(df_activity_log)
    df_activity_log.reset_index(drop=True, inplace=True)
    insert_ual = "insert into stage.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {ual_val};"
    i = 0
    step = int(df_activity_log.shape[0] / 100)
    while i <= df_activity_log.shape[0]:
        print('df_activity_log', i, end='\r')

        if df_activity_log.drop(columns=['id'], axis=1).drop(columns=['uniq_id'], axis=1).loc[i:i + step].shape[0] > 0:
            ual_val = str([
                tuple(x) for x in df_activity_log.drop(
                    columns=['id'], axis=1).drop(columns=['uniq_id'], axis=1).loc[i:i + step].to_numpy()
            ])[1:-1]
            cur.execute(insert_ual.replace('{ual_val}', ual_val))
            conn.commit()

        i += step + 1

    cur.close()
    conn.close()

    return 200


def update_mart_d_tables(ti):
    # connection to database
    conn = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    cur.execute("""
        delete from mart.f_activity;
        delete from mart.f_daily_sales;
        -- d_calendar
        delete from mart.d_calendar where 1=1;
        alter table mart.d_calendar alter column month_name type varchar(10);

        CREATE SEQUENCE example_table_seq
            INCREMENT BY 1
            START WITH 1;

        insert into mart.d_calendar
        (date_id, fact_date, day_num, month_num, month_name, year_num)
        with all_dates as (
            select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time from stage.user_activity_log
            union
            select distinct to_date(date_time::TEXT,'YYYY-MM-DD') from stage.user_order_log
            union
            select distinct to_date(date_id::TEXT,'YYYY-MM-DD') from stage.customer_research
            order by date_time
            )
        select nextval('example_table_seq') as date_id,
        date_time as fact_date,
        extract(day from date_time) as day_num,
        extract(month from date_time) as month_num,
        to_char(date_time, 'Month') as month_name,
        extract('isoyear' from date_time) as year_num
        from all_dates;

        drop sequence example_table_seq;

        -- d_customer
        delete from mart.d_customer  where 1=1;
        insert into mart.d_customer (customer_id, first_name, last_name, city_id)
        select distinct on (customer_id) customer_id, first_name, last_name, city_id
        from stage.user_order_log uol;

        -- d_item
        delete from mart.d_item where 1=1;
        insert into mart.d_item (item_id, item_name)
        select distinct item_id, item_name from stage.user_order_log;""")
    conn.commit()
    cur.close()
    conn.close()
    return 200


def update_mart_f_tables(ti):
    # connection to database
    conn = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    cur.execute("""--f_activity
        delete from mart.f_activity;
        alter table mart.f_activity drop column click_number;
        INSERT INTO mart.f_activity (activity_id, date_id)
        select action_id, date_id
        from stage.user_activity_log ual
        join mart.d_calendar dc on dc.fact_date=ual.date_time
        group by date_id, action_id
        order by date_id;
        alter table mart.f_activity add click_number serial;

        --f_daily_sales
        delete from mart.f_daily_sales;

        INSERT INTO mart.f_daily_sales  (date_id, item_id, customer_id , price , quantity , payment_amount)
        select dc.date_id, uol.item_id, uol.customer_id, avg(payment_amount/quantity) as price,
        sum(quantity) as quantity, sum(payment_amount) as payment_amount
        from stage.user_order_log uol
        join mart.d_calendar dc on dc.fact_date=uol.date_time
        group by dc.date_id, uol.item_id, uol.customer_id;""")
    conn.commit()
    cur.close()
    conn.close()
    return 200


dag = DAG(
    dag_id='591_full_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_file_request = PythonOperator(
    task_id='create_files_request',
    python_callable=create_files_request,
    op_kwargs={
        'api_endpoint': api_endpoint
    },
    dag=dag
)

t_check_report = PythonOperator(
    task_id='check_report',
    python_callable=check_report,
    op_kwargs={
        'api_endpoint': api_endpoint
    },
    dag=dag
)

t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg',
    python_callable=upload_from_s3_to_pg,
    op_kwargs={
        'nickname': nickname,
        'cohort': cohort
    },
    dag=dag
)

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',
    python_callable=update_mart_d_tables,
    dag=dag
)

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',
    python_callable=update_mart_f_tables,
    dag=dag
)

t_file_request >> t_check_report >> t_upload_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables
