import requests
import json
import pandas as pd
import datetime
import time
from dotenv import load_dotenv
import os
import threading


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
        self.step_in_second = 5 * 60 # 五分钟
        
    def crawl(self, results_lock, output_results, start_time, end_time):
        # result_file = self.result_file_format.format(date=datetime.datetime.strftime(start_time, "%Y%m%d%H%M"))
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

        with results_lock:
            output_results.extend(results)

class CrawlerThread(threading.Thread):
    def __init__(self, results_lock, results, cookies, app_id, operation, start_time, end_time):
        threading.Thread.__init__(self)
        self.crawler = Crawler(cookies, app_id, operation)
        self.results_lock = results_lock
        self.results = results
        self.start_time = start_time
        self.end_time = end_time

    def run(self):
        self.crawler.crawl(self.results_lock, self.results, self.start_time, self.end_time)

def main():
    load_dotenv()
    cookies = {
        "sessionid": os.getenv("ES_OPS_SESSIONID"),
        "PRO_cas_principal": os.getenv("ES_OPS_CAS_PRINCIPAL"),
    }

    results_lock = threading.Lock() # 全局锁
    results = []
    threads = []
    thread_count = 24
    start_time = datetime.datetime(2023, 5, 23)
    end_time = start_time + datetime.timedelta(days=1)
    delta_seconds = int((end_time - start_time).total_seconds() / thread_count)

    for i in range(thread_count):
        current_start_time = start_time + datetime.timedelta(seconds = i * delta_seconds)
        current_end_time = start_time + datetime.timedelta(seconds = (i + 1) * delta_seconds)
        thread = CrawlerThread(results_lock, results, cookies, '100017817', 'endHangEvent', current_start_time, current_end_time)
        thread.start()
        threads.append(thread)

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    # sort results by timestamp 
    results.sort(key=lambda x: x['timestamp'])
    df = pd.DataFrame(results)
    df.to_csv('ctrpcorp/weblog.csv', mode='a', header=False, index=False)

if __name__ == '__main__':
    main()