import psycopg2
import pandas as pd
import numpy as np

import time
import requests
import os
import sys

login = 'jovyan'
password = 'jovyan'
port = 5432
host = 'localhost'
db = 'de'

conn = psycopg2.connect(
    f"host='{host}' port='{port}' dbname='{db}' user='{login}' password='{password}'"
)
cur = conn.cursor()

insert_map = {
    'customer_research.csv':
        "INSERT INTO stage.customer_research (date_id, category_id, geo_id, sales_qty, sales_amt) VALUES {s_val};",
    'user_activity_log.csv':
        "INSERT INTO stage.user_activity_log (date_time, action_id, customer_id, quantity) VALUES {s_val};",
    'user_order_log.csv': (
        "INSERT INTO stage.user_order_log (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, "
        "item_name, quantity, payment_amount) VALUES {s_val};"
    )
}


def inc_upload(cur, conn, df, insert_s):
    step = int(df.shape[0] / 100)
    i = 0
    while i <= df.shape[0]:
        print(i, end='\r')
        s_val = str([tuple(x) for x in df.loc[i:i + step].to_numpy()])[1:-1]
        cur.execute(insert_s.replace('{s_val}', s_val))
        conn.commit()
        i += step+1


full_lesson_path = os.path.dirname(os.path.abspath(__file__))
files = ['customer_research.csv', 'user_order_log.csv', 'user_activity_log.csv']
for file in files:
    df = pd.read_csv(f'{full_lesson_path}/stage/{file}')
    inc_upload(cur, conn, df, insert_map[file])


cur.close()
conn.close()
