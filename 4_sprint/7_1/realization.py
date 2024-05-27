import pandas as pd
import psycopg2
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
)


def check_failure_file_customer_research(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_customer_research",
        sql="""
            INSERT INTO dq_checks_results
            values ('customer_research', 'file_sensor', current_date, 1)
          """
    )


def check_success_file_customer_research(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_customer_research",
        sql="""
            INSERT INTO dq_checks_results
            values ('customer_research', 'file_sensor', current_date, 0)
          """
    )


def check_success_file_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'file_sensor', current_date, 0)
          """
    )


def check_failure_file_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'file_sensor', current_date, 1)
          """
    )


def check_success_file_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'file_sensor', current_date, 0)
          """
    )


def check_failure_file_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'file_sensor', current_date, 1)
          """
    )


def check_success_insert_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date, 0 )
          """
    )


def check_failure_insert_user_order_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull', current_date, 1 )
          """
    )


def check_success_insert_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date, 0 )
          """
    )


def check_failure_insert_user_activity_log(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'user_activity_log_isNull', current_date, 1 )
          """
    )


def check_success_insert_user_order_log2(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'check_row_count_user_order_log', current_date, 0 )
          """
    )


def check_failure_insert_user_order_log2(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'check_row_count_user_order_log', current_date, 1 )
          """
    )


def check_success_insert_user_activity_log2(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'check_row_count_user_activity_log', current_date, 0 )
          """
    )


def check_failure_insert_user_activity_log2(context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_activity_log', 'check_row_count_user_activity_log', current_date, 1 )
          """
    )


def load_file_to_pg(filename, pg_table, conn_args):
    """csv-files to pandas dataframe"""
    f = pd.read_csv(filename)

    # load data to postgres
    cols = list(f.columns)
    insert_stmt = f"INSERT INTO {pg_table} ({cols}) VALUES %s"

    pg_conn = psycopg2.connect(**conn_args)
    cur = pg_conn.cursor()
    psycopg2.extras.execute_values(cur, insert_stmt, f.values)
    pg_conn.commit()
    cur.close()
    pg_conn.close()


default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}

with DAG(dag_id="Sprin4_Task71", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    begin = DummyOperator(task_id="begin")
    dt = str(datetime.now().date())
    files = ["customer_research", "user_order_log", "user_activity_log"]
    with TaskGroup(group_id='group1') as fg1:
        f1 = FileSensor(task_id=f"waiting_for_file_{files[0]}",
                        fs_conn_id='fs_local',
                        # filepath=f"{dt}_{files[0]}.csv",
                        filepath=str(datetime.now().date()) + "customer_research.csv",  # just for tests!!!
                        poke_interval=5)
        f2 = FileSensor(task_id=f"waiting_for_file_{files[1]}",
                        fs_conn_id='fs_local',
                        # filepath=f"{dt}_{files[1]}.csv",
                        filepath=str(datetime.now().date()) + "user_order_log.csv",  # just for tests!!!
                        poke_interval=5)
        f3 = FileSensor(task_id=f"waiting_for_file_{files[2]}",
                        fs_conn_id='fs_local',
                        # filepath=f"{dt}_{files[2]}.csv",
                        filepath=str(datetime.now().date()) + "user_activity_log.csv",  # just for tests!!!
                        poke_interval=5)

    with TaskGroup(group_id='group2') as fg2:
        load_customer_research = PythonOperator(
            task_id=f"load_{files[0]}",
            python_callable=load_file_to_pg,
            op_kwargs={'filename': f"{dt}_{files[0]}.csv", 'pg_table': f"stage.{files[0]}"}
        )
        load_user_order_log = PythonOperator(
            task_id=f"load_{files[1]}",
            python_callable=load_file_to_pg,
            op_kwargs={'filename': f"{dt}_{files[1]}.csv", 'pg_table': f"stage.{files[1]}"}
        )
        load_user_activity_log = PythonOperator(
            task_id=f"load_{files[2]}",
            python_callable=load_file_to_pg,
            op_kwargs={'filename': f"{dt}_{files[2]}.csv", 'pg_table': f"stage.{files[2]}"}
        )
        # load_price_log = PythonOperator(...) WTF this table doing here?
        load_price_log = PythonOperator(
            task_id="load_price_log",
            python_callable=load_file_to_pg,
            op_kwargs={'filename': f"{dt}_price_log.csv", 'pg_table': "stage.price_log"}
        )
        sql_check = SQLCheckOperator(
            task_id="user_order_log_isNull",
            sql="user_order_log_isNull_check.sql",
            on_success_callback=check_success_insert_user_order_log,
            on_failure_callback=check_failure_insert_user_order_log
        )
        sql_check2 = SQLCheckOperator(
            task_id="user_activity_log_isNull",
            sql="user_activity_log_isNull_check.sql",
            on_success_callback=check_success_insert_user_activity_log,
            on_failure_callback=check_failure_insert_user_activity_log
        )
        sql_check3 = SQLValueCheckOperator(
            task_id="check_row_count_user_order_log",
            sql="Select count(distinct(customer_id)) from user_order_log",
            pass_value=3,
            tolerance=0.01,
            on_success_callback=check_success_insert_user_order_log2,
            on_failure_callback=check_failure_insert_user_order_log2
        )
        sql_check4 = SQLValueCheckOperator(
            task_id="check_row_count_user_activity_log",
            sql="Select count(distinct(customer_id)) from user_activity_log",
            pass_value=3,
            tolerance=0.01,
            on_success_callback=check_success_insert_user_activity_log2,
            on_failure_callback=check_failure_insert_user_activity_log2
        )
        load_user_order_log >> [sql_check, sql_check3]
        load_user_activity_log >> [sql_check2, sql_check4]
    end = DummyOperator(task_id="end")

    begin >> fg1 >> fg2 >> end
