import time
import requests
import os
import sys
import pandas as pd

cohort = 0
nickname = ''


def generate_report_response(nickname, cohort):
    r = requests.post(
        "https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report",
        headers={
            "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
            "X-Nickname": nickname,
            "X-Cohort": str(cohort)
        }
    ).json()
    print(r)
    return r["task_id"]


def get_report_response(nickname, cohort, task_id):
    return requests.get(
        f"https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id={task_id}",
        headers={
            "X-API-KEY": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
            "X-Nickname": nickname,
            "X-Cohort": str(cohort)
        }
    ).json()


def get_report(nickname, cohort):
    task_id = generate_report_response(nickname, cohort)
    time.sleep(120)
    report_status = ''
    while report_status != 'SUCCESS':
        try:
            report = get_report_response(nickname, cohort, task_id)
            report_status = report['status']
        except KeyError:
            continue

    print(report['data']['report_id'])
    return report['data']['report_id']


full_lesson_path = os.path.dirname(os.path.abspath(__file__))
report_id = get_report(nickname, cohort)
files = ['customer_research.csv', 'user_order_log.csv', 'user_activity_log.csv']
for file in files:
    url = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/{report_id}/{file}"
    c = pd.read_csv(url)
    c.to_csv(f'{full_lesson_path}/stage/{file}')
