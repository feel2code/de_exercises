import json
from typing import Dict, Optional
from confluent_kafka import Consumer

host = 'yandexcloud.net'
port = '9091'
user = 'producer_consumer'
password = ''
topic = 'order-service_orders'
group = 'main-consumer-group' 
cert_path ='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt' 


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': self.error_callback,
            'debug': 'all',
            'client.id': 'someclientkey'}

        self.consumer = Consumer(params)
        self.consumer.subscribe([topic])

    def error_callback(err):
        print('Something went wrong: {}'.format(err))

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.consumer.poll(timeout=timeout)
        return json.loads(msg.value().decode())

