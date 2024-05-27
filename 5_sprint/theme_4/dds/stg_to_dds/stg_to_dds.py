import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.stg_to_dds.users_loader import UsersLoader
from examples.dds.stg_to_dds.restaurants_loader import RestaurantsLoader
from examples.dds.stg_to_dds.timestamps_loader import TimestampsLoader
from examples.dds.stg_to_dds.products_loader import ProductsLoader
from examples.dds.stg_to_dds.orders_loader import OrdersLoader
from examples.dds.stg_to_dds.sales_loader import SalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'dds', 'example'],
    is_paused_upon_creation=True
)
def sprint5_stg_to_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        rest_loader = UsersLoader(dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = RestaurantsLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="timestamps_load")
    def load_timestamps():
        rest_loader = TimestampsLoader(dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id="products_load")
    def load_products():
        rest_loader = ProductsLoader(dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="orders_load")
    def load_orders():
        rest_loader = OrdersLoader(dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="sales_load")
    def load_sales():
        rest_loader = SalesLoader(dwh_pg_connect, log)
        rest_loader.load_sales()

    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    sales_dict = load_sales()

    [users_dict, restaurants_dict, timestamps_dict] >> products_dict >> orders_dict >> sales_dict


dds_stg_to_dds_dag = sprint5_stg_to_dds_dag()
