# ETL 
# 클릭하우스 DB에서 데이터를 추출
# 웹 패킷을 전송하기 위해 전처리 (피클 or CSV 파일로 변환 or 메모리에서 바로 전송)

import pandas as pd
import requests
import pickle
import clickhouse_connect
import os

class ClickHouseETL:
    def __init__(self, host: str, attack: str, limit: int = 10000):
        self.host = host
        self.attack = attack
        self.client = clickhouse_connect.get_client(host=host, database="DTIAI")
        self.query = f'SELECT * FROM http_packet_temp WHERE {attack} LIMIT {limit}'
        self.df = None

    def extract(self):
        print("[E] Extracting data from ClickHouse...")
        self.df = self.client.query_df(self.query)
        print(f"[E] Loaded {len(self.df)} rows.")

    def transform_to_pickle(self, path: str):
        print(f"[T] Saving to pickle: {path}")
        with open(path, 'wb') as f:
            pickle.dump(self.df, f)
            
    def transform_to_parquet(self, path: str):
        print(f"[T] Saving to parquet: {path}")
        self.df.to_parquet("data.parquet")

    def load_and_send_requests(self):
        print("[L] Sending HTTP requests...")
        for idx, row in self.df.iterrows():
            try:
                url = row['url']
                method = row.get('method', 'GET').upper()
                headers = eval(row.get('headers', '{}'))  # 혹은 json.loads()
                body = row.get('body', None)

                if method == 'POST':
                    print(f"[{idx}] Sending POST request to {url}")
                    # res = requests.post(url, headers=headers, data=body)
                else:
                    print(f"[{idx}] Sending GET request to {url}")
                    # res = requests.get(url, headers=headers)

                print(f"[{idx}] [{method}] {url} → {res.status_code}")
            except Exception as e:
                print(f"[{idx}] Error: {e}")
