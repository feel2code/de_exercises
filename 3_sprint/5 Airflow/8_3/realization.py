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
    dag_id='583_postgresql_mart_update',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)
business_dt = {'dt': '2022-05-06'}


def update_mart_d_tables(ti):
    # connection to database
    conn = psycopg2.connect(f"host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    cur.execute("""
        delete from mart.f_activity;
        delete from mart.f_daily_sales;
        -- d_calendar
        delete from mart.d_calendar where 1=1;

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


t_update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        dag=dag)


t_update_mart_f_tables = PythonOperator(task_id='update_mart_f_tables',
                                        python_callable=update_mart_f_tables,
                                        dag=dag)


t_update_mart_d_tables >> t_update_mart_f_tables
