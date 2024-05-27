import json
from typing import Dict
import redis

host = 'yandexcloud.net'
port = '6380'
password = ''
cert_path = '/home/user/.redis/YandexInternalRootCA.crt'

class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path)
              
    def set(self, k:str, v: Dict):
        self._client.set(k, str(v))

    def get(self, k:str) -> Dict:
        result = self._client.get(k).decode("utf-8")
        result = result.replace("\'", "\"")
        return json.loads(result)
