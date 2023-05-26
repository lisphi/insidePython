import requests
import json
import datetime as dt
import time

def create_count_crawler(cookies, app_id, operation):
    count_query_template = f"""SELECT count(1) as value, (intDiv(toUInt32(timestamp)*1000,10000)) as t 
FROM log.weblog_all 
WHERE {__query_condition}
GROUP BY t ORDER BY t FORMAT JSON
"""
    return __Crawler(cookies, count_query_template, app_id, operation, __count_result_convert, 10 * 60)


def create_detail_crawler(cookies, app_id, operation):
    detail_query_template = f"""SELECT `cs_uri_stem`,`appid`,`cs_referer`,`cs_host`,`s_rootmessageid`,`_extend_uuid`,`timestamp` 
FROM log.weblog_all 
WHERE {__query_condition}
ORDER BY timestamp desc LIMIT 100 FORMAT JSON    
"""
    return __Crawler(cookies, detail_query_template, app_id, operation, __detail_result_convert, 1 * 60)


class __Crawler:
    def __init__(self, cookies, query_template, app_id, operation, result_converter, step_in_second):
        self.url = 'http://es.ops.ctripcorp.com/clickhouse/?tableName=log.weblog_all'
        self.query_template = query_template
        self.cookies = cookies
        self.app_id = app_id
        self.operation = operation
        self.result_converter = result_converter
        self.max_retries = 3
        self.step_in_second = step_in_second
        
    def crawl(self, start_time, end_time):
        total_step = int((end_time - start_time).total_seconds() / self.step_in_second)
        results=[]
        for i in range(total_step):
            current_start_time = start_time + dt.timedelta(seconds = i * self.step_in_second)
            current_end_time = min(start_time + dt.timedelta(seconds = (i + 1) * self.step_in_second), end_time)
            now_text = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
            current_start_time_text = dt.datetime.strftime(current_start_time, '%Y-%m-%d %H:%M:%S')
            current_end_time_text = dt.datetime.strftime(current_end_time, '%Y-%m-%d %H:%M:%S')
            print(f"{now_text} -> {current_start_time_text}~{current_end_time_text}")
            current_start_timestamp = int((current_start_time - dt.datetime(1970, 1, 1)).total_seconds())
            current_end_timestamp = int((current_end_time - dt.datetime(1970, 1, 1)).total_seconds())
            query = self.query_template.format(start=current_start_timestamp, end=current_end_timestamp, app_id=self.app_id, operation=self.operation)

            for i in range(self.max_retries):
                try:
                    response = requests.post(self.url, data=query, cookies=self.cookies)
                    break
                except Exception as e:
                    if i == self.max_retries - 1:
                        print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {e}")
                        break
                    else:
                        time.sleep(2)
                        continue
            
            if response.status_code == 200:
                response_data = json.loads(response.text)
                for item in response_data['data']:  
                    results.append(self.result_converter(item))
            else:
                print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {response}")
            time.sleep(0.005)

        return results;


__query_condition = """(timestamp >= toDateTime({start}) AND timestamp <= toDateTime({end})) AND (`appid` = '{app_id}') AND (cs_uri_stem like '%{operation}%')"""


def __detail_result_convert(item):
    return {
        'cs_uri_stem': item['cs_uri_stem'], 
        'cs_referer': item['cs_referer'], 
        's_rootmessageid': item['s_rootmessageid'],
        'date_time': item['timestamp'],
        'timestamp': dt.datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S").timestamp(),
    }


def __count_result_convert(item):
    return {
        'timestamp': item['t'], 
        'value': item['value'],
    }


