import requests
import json
import pandas as pd
import datetime
import time
from dotenv import load_dotenv
import os
import concurrent.futures


class Crawler:
    def __init__(self, cookies, app_id, operation):
        self.result_file_format = 'ctrpcorp/weblog_{date}.csv'
        self.url = 'http://es.ops.ctripcorp.com/clickhouse/?tableName=log.weblog_all'
        self.query_template = """SELECT `cs_uri_stem`,`appid`,`cs_referer`,`cs_host`,`s_rootmessageid`,`_extend_uuid`,`timestamp` 
FROM log.weblog_all WHERE (timestamp >= toDateTime({start}) AND timestamp <= toDateTime({end})) AND (`appid` = '{app_id}') 
AND (cs_uri_stem like '%{operation}%') AND ((1=1)) ORDER BY timestamp desc LIMIT 100 FORMAT JSON    
"""
        self.cookies = cookies
        self.app_id = app_id
        self.operation = operation
        self.max_retries = 3
        self.step_in_second = 1 * 60
        
    def crawl(self, start_time, end_time):
        total_step = int((end_time - start_time).total_seconds() / self.step_in_second)
        results=[]
        for i in range(total_step):
            current_start_time = start_time + datetime.timedelta(seconds = i * self.step_in_second)
            current_end_time = start_time + datetime.timedelta(seconds = (i+1) * self.step_in_second)
            current_start_time_text = datetime.datetime.strftime(current_start_time, "%Y-%m-%d %H:%M:00")
            current_end_time_text = datetime.datetime.strftime(current_end_time, "%Y-%m-%d %H:%M:00")
            print(f"{current_start_time_text}~{current_end_time_text}")
            current_start_timestamp = int((current_start_time - datetime.datetime(1970, 1, 1)).total_seconds())
            current_end_timestamp = int((current_end_time - datetime.datetime(1970, 1, 1)).total_seconds())
            query = self.query_template.format(start=current_start_timestamp, end=current_end_timestamp, app_id=self.app_id, operation=self.operation)

            for i in range(self.max_retries):
                try:
                    response = requests.post(self.url, data=query, cookies=self.cookies)
                    break
                except Exception as e:
                    if i == self.max_retries - 1:
                        print(e)
                        break
                    else:
                        time.sleep(2)
                        continue
            
            if response.status_code == 200:
                response_data = json.loads(response.text)
                for item in response_data['data']:  
                    results.append({
                        'cs_uri_stem': item['cs_uri_stem'], 
                        'cs_referer': item['cs_referer'], 
                        's_rootmessageid': item['s_rootmessageid'],
                        'date_time': item['timestamp'],
                        'timestamp': datetime.datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S").timestamp()})
            else:
                print(response)
            time.sleep(0.005)

        return results;

def worker(cookies, app_id, operation, start_time, end_time):
    crawler = Crawler(cookies, app_id, operation)
    return crawler.crawl(start_time, end_time)

def main():
    load_dotenv()
    cookies = {
        "sessionid": os.getenv("ES_OPS_SESSIONID"),
        "PRO_cas_principal": os.getenv("ES_OPS_CAS_PRINCIPAL"),
    }
    app_id = '100017817'
    operation = 'endHangEvent'
    worker_count = 50
    start_time = datetime.datetime(2023, 5, 23)
    end_time = start_time + datetime.timedelta(days=1)
    delta_seconds = int((end_time - start_time).total_seconds() / worker_count)
 
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        for i in range(worker_count):
            current_start_time = start_time + datetime.timedelta(seconds = i * delta_seconds)
            current_end_time = start_time + datetime.timedelta(seconds = (i + 1) * delta_seconds)
            futures.append(executor.submit(worker, cookies, app_id, operation, current_start_time, current_end_time))

    results = []
    for future in concurrent.futures.as_completed(futures):
        results.extend(future.result())

    # sort results by timestamp 
    results.sort(key=lambda x: x['timestamp'])
    df = pd.DataFrame(results)
    df.to_csv('ctrpcorp/weblog.csv', mode='a', header=False, index=False)

if __name__ == '__main__':
    main()