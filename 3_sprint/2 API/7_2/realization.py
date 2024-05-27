import psycopg2
import pandas as pd
import numpy as np

login = 'jovyan'
password = 'jovyan'
port = 5432
host = 'localhost'
db = 'de'

conn = psycopg2.connect(
    f"host='{host}' port='{port}' dbname='{db}' user='{login}' password='{password}'"
)
cur = conn.cursor()
cur.close()
conn.close()
