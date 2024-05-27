from airflow.decorators.task_group import TaskGroup
from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

with DAG(
        dag_id="Sprin4_Task1",
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False
) as dag:
    dt = str(datetime.now().date())
    files = ["customer_research", "user_order_log", "user_activity_log"]

    with TaskGroup(group_id='group1') as fg1:
        f1 = FileSensor(task_id=f"waiting_for_file_{files[0]}", fs_conn_id='fs_local', filepath=f"{dt}_{files[0]}.csv",
                        poke_interval=60)
        f2 = FileSensor(task_id=f"waiting_for_file_{files[1]}", fs_conn_id='fs_local', filepath=f"{dt}_{files[1]}.csv",
                        poke_interval=60)
        f3 = FileSensor(task_id=f"waiting_for_file_{files[2]}", fs_conn_id='fs_local', filepath=f"{dt}_{files[2]}.csv",
                        poke_interval=60)

    fg1

