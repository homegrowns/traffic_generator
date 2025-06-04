# ETL 
# 클릭하우스 DB에서 데이터를 추출
# 웹 패킷을 전송하기 위해 전처리 (피클 or CSV 파일로 변환 or 메모리에서 바로 전송)

import pandas as pd
import requests
import pickle
import json
import clickhouse_connect
import os
from requests.exceptions import ConnectTimeout
from clickhouse_connect.driver.exceptions import (
    ClickHouseError,
    OperationalError,
    ProgrammingError
)

class ClickHouseETL:
    def __init__(self, host: str, tg_ip: str, attack: str, limit: int):
        self.host = host
        self.df = None   
        self.total = 0
        self.count = 0
        self.error_count = 0
        self.tg_ip = tg_ip
        self.attack = attack
        self.limit = limit


    def extract(self):
        print("[i] loading data from ClickHouse...")
        
        try:
            client = clickhouse_connect.get_client(host=self.host, database="DTIAI")
            query = f"SELECT * FROM http_packet_temp WHERE label = '{self.attack}' AND dest_port IN (8181) ORDER BY start_date DESC LIMIT {self.limit}"
            print(f"[i] Query: {query}")
            self.df = client.query_df(query)

            print(f"[E] Loaded {len(self.df)}개 rows from ClickHouse.")
            self.total = len(self.df)
            self.__transform_to_parquet() # Save to parquet file

        except (OperationalError, ProgrammingError, ValueError, Exception) as e:
            db_error_log_path = "./errors/DB_connect_failed_log.txt" 
            with open(db_error_log_path, "a", encoding="utf-8") as f:
                failed = f"[!] DB 연결 실패 또는 데이터 없음: {e}"
                print(failed)        # 콘솔 출력
                f.write(failed + "\n")  # 파일에 한 줄로 저장
                pass
            try:
                # 예: 'backup_data.parquet' 파일에서 대체 데이터 로드
                self.df = pd.read_parquet(f"./data/{self.attack}.parquet")
                self.total = len(self.df)
                print(f"[E] Parquet 파일에서 대체 데이터 {self.total}개 로드 완료.")
            except Exception as pe:
                db_error_log_path = "./errors/data_load_failed_log.txt"  # 또는 Path 객체도 가능
                with open(db_error_log_path, "a", encoding="utf-8") as f:
                    failed = f"[!] Parquet 로드 실패: {pe}"
                    print(failed)        # 콘솔 출력
                    f.write(failed + "\n")  # 파일에 한 줄로 저장
                    
                self.df = pd.DataFrame()  # 완전 실패 시 빈 DataFrame 처리
        
    def __transform_to_parquet(self):
        path = f"./data/{self.attack}.parquet"
        print(f"[T] Saving to parquet: {path}")
        df = self.df.astype({col: str for col in self.df.select_dtypes(include='object').columns})
        df.to_parquet(path)


    def __parse_raw_http_header(self, raw_header: str) -> dict:
        headers = {}
        lines = raw_header.strip().split("\\r\\n")  # 문자열 안에 \r\n이 있음
        
        # 첫 줄은 요청 라인 (예: "POST /path HTTP/1.1")이므로 제외하고 헤더 라인만 처리
        for line in lines[1:]:  
            # 헤더 라인은 일반적으로 "Key: Value" 형식이므로 ': ' 포함 여부 확인
            if ": " in line:
                # 첫 번째 ': ' 기준으로 key와 value 분리 (':'가 value에 포함된 경우도 있으므로 maxsplit=1)
                key, value = line.split(": ", 1)
                key = key.strip()
                value = value.strip()
                
                if key.lower() == "referer":
                    print(f"header referer분석: {value}")
                    continue  # referer는 저장하지 않고 건너뜀

            headers[key] = value
       
        # 최종적으로 파싱된 헤더 딕셔너리 반환
        return headers

                


    def send_requests(self, progress_callback=None, stop: bool = False):
        print("[S] Sending HTTP requests..................")
        for idx, row in self.df.iterrows():
            if stop or self.total == self.count:
                print("Stopping the request sending process.")
                break
            if self.count == self.error_count and self.error_count > 5:
                failed = f"[!]: 요청에 문제가 있습니다 에러 로그를 확인하세요."
                progress_callback(self.count, failed, self.error_count)
                print(failed)
                break
            try:
                uuid = row['id']
                pre_url = row['url']
                method = row.get('method', 'GET').upper()
                headers_str = row.get('request_header', '{}')
                headers = self.__parse_raw_http_header(headers_str)
                print(f"[{idx}] Headers: {headers}")
                dest_port = row['dest_port']
                
                # 포트 번호가 없을 경우 처리
                # 포트 번호를 8181으로 설정
                port = "8181" if dest_port is None or dest_port != "8181" else dest_port


                # 받는 서버에서 바는 포트 번호가 열려 있지안으면  아래같은 에러 생기므로 8123같은 포트에 해당하는 앱 배포해 놔야한다.
                # Failed to establish a new connection: [WinError 10061] 대상 컴퓨터에서 연결을 거부했으므로 연결하지 못했습니다'
                url = f"http://{self.tg_ip}:{port}{pre_url}"
                print(f'idx = {idx}, URL = {url}')
                body = row.get('request_body', None)
                # print(f'바디 = {body}')


                if method == 'POST':
                    print(f"[{idx}] Sending POST request to {url}")
                    res = requests.post(url, headers=headers, data=body)
                    success = f"[{idx}] [{method}] {url} → {res.status_code}"
                    print(success)
                    self.count += 1
                    if progress_callback:
                        progress_callback(self.count, success, self.error_count)

                else:
                    print(f"[{idx}] Sending GET request to {url}")
                    res = requests.get(url, headers=headers)
                    success = f"[{idx}] [{method}] {url} → {res.status_code}"
                    print(success)
                    self.count += 1
                    if progress_callback:
                        progress_callback(self.count, success, self.error_count)
                    
            except requests.exceptions.RequestException as e:
                log_path = "./errors/Request_failed_log.txt"  # 또는 Path 객체도 가능
                failed = f"[!]기타 요청 예외: [{idx}] transaction_id : {uuid}\n AND Error: \n{e}"
                with open(log_path, "a", encoding="utf-8") as f:
                    print(failed)        # 콘솔 출력
                    f.write(failed + "\n")  # 파일에 한 줄로 저장
                    
                self.error_count += 1
                self.count += 1
                if progress_callback:
                   progress_callback(self.count, failed, self.error_count)
                   
            except requests.exceptions.Timeout as e:
                failed = f"[!]요청 타임아웃: failed [{idx}] transaction_id : {uuid}\n AND Error: \n{e}"
                log_path = "./errors/failed_log.txt"  # 또는 Path 객체도 가능

                with open(log_path, "a", encoding="utf-8") as f:
                    print(failed)        # 콘솔 출력
                    f.write(failed + "\n")  # 파일에 한 줄로 저장
                
                self.error_count += 1
                self.count += 1
                if progress_callback:
                   progress_callback(self.count, failed, self.error_count)
                   
            except ConnectTimeout as e:
                failed = f"[!]연결 타임아웃 발생: {e}"
                log_path = "./errors/timeout_log.txt"  # 또는 Path 객체도 가능
                with open(log_path, "a", encoding="utf-8") as f:
                    failed = f"failed [{idx}] transaction_id : {uuid} \n AND timeout Error: \n{e}"
                    print(failed)        # 콘솔 출력
                    f.write(failed + "\n")  # 파일에 한 줄로 저장
                break
                
                

        
