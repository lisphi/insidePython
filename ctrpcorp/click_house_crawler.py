import requests
import json
import datetime as dt
import time

__query_condition = """(timestamp >= toDateTime({start}) AND timestamp <= toDateTime({end})) AND (group = 'soa2.{service_code}') AND (requestPath like '%{operation}%')"""

# SELECT count(1) as __count, count(1) as value, (intDiv(toUInt32(timestamp)*1000,5000)) as t 
# FROM log.fx_apirouter_access_log_all 
# WHERE (timestamp >= toDateTime(1688324100) AND timestamp <= toDateTime(1688324700)) AND (requestPath like '%queryCreateSubEvent%') AND (((group='soa2.16566'))) 
# GROUP BY t ORDER BY t FORMAT JSON

def create_count_crawler(cookies, table_name, service_code, operation):
    count_query_template = f"""SELECT count(1) as value, (intDiv(toUInt32(timestamp)*1000,1000)) as t 
FROM {table_name} 
WHERE {__query_condition}
GROUP BY t ORDER BY t FORMAT JSON
"""
    return __Crawler(cookies, count_query_template, table_name, service_code, operation, __count_result_convert, 5 * 60)


# SELECT `domain`,`referer`,`requestPath`,`responseStatus`,`rootMessageId`,`timestamp`
# FROM log.fx_apirouter_access_log_all 
# WHERE (timestamp >= toDateTime(1688324100) AND timestamp <= toDateTime(1688324700)) AND (requestPath like '%queryCreateSubEvent%') AND (((group='soa2.16566'))) 
# ORDER BY timestamp asc LIMIT 500 FORMAT JSON

def create_detail_crawler(cookies, table_name, service_code, operation):
    detail_query_template = f"""SELECT `domain`,`referer`,`requestPath`,`responseStatus`,`rootMessageId`,`userAgent`,`timestamp` 
FROM {table_name} 
WHERE {__query_condition}
ORDER BY timestamp desc LIMIT 500 FORMAT JSON    
"""
    return __Crawler(cookies, detail_query_template, table_name, service_code, operation, __detail_result_convert, 5 * 60)


class __Crawler:
    def __init__(self, cookies, query_template, table_name, service_code, operation, result_converter, step_in_second):
        self.url = '''http://es.ops.ctripcorp.com/clickhouse/?tableName={table_name}'''
        self.query_template = query_template
        self.cookies = cookies
        self.service_code = service_code
        self.operation = operation
        self.result_converter = result_converter
        self.max_retries = 3
        self.step_in_second = step_in_second
        self.headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36' }
        self.ch_command = 'count' if 'count(' in query_template else 'detail'
        
    def crawl(self, start_time, end_time):
        log_title = f'{self.operation}-{self.ch_command}'
        total_step = int((end_time - start_time).total_seconds() / self.step_in_second)
        results=[]
        for i in range(total_step):
            current_start_time = start_time + dt.timedelta(seconds = i * self.step_in_second)
            current_end_time = min(start_time + dt.timedelta(seconds = (i + 1) * self.step_in_second), end_time)
            current_start_time_text = dt.datetime.strftime(current_start_time, '%Y-%m-%d %H:%M:%S')
            current_end_time_text = dt.datetime.strftime(current_end_time, '%H:%M:%S')
            current_time_range_text = f"{current_start_time_text}~{current_end_time_text}"
            if i == 0:
                print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}")
            current_start_timestamp = int((current_start_time - dt.datetime(1970, 1, 1)).total_seconds())
            current_end_timestamp = int((current_end_time - dt.datetime(1970, 1, 1)).total_seconds())
            query = self.query_template.format(start=current_start_timestamp, end=current_end_timestamp, service_code=self.service_code, operation=self.operation)

            for i in range(self.max_retries):
                try:
                    if i > 0:
                        print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: retying {i} time(s) ...")
                    response = requests.post(self.url, data=query, headers=self.headers, cookies=self.cookies)
                    if response.status_code == 200:
                        break
                    time.sleep(1)    
                except Exception as e:
                    if i == self.max_retries - 1:
                        print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: {e}")
                        break
                    else:
                        time.sleep(1)
                        continue
            
            if response.status_code == 200:
                response_data = json.loads(response.text)
                for item in response_data['data']:  
                    results.append(self.result_converter(item))
            else:
                print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: {response}")
            time.sleep(0.005)

        return results


    def crawl_minutes(self, minute_timestamps):
        log_title = f'{self.operation}-{self.ch_command}'
        results=[]
        for i in range(len(minute_timestamps)):
            current_start_timestamp = minute_timestamps[i]
            current_end_timestamp = current_start_timestamp + 60
            current_start_time = dt.datetime.fromtimestamp(current_start_timestamp)
            current_end_time = dt.datetime.fromtimestamp(current_end_timestamp)
            current_start_time_text = dt.datetime.strftime(current_start_time, '%Y-%m-%d %H:%M:%S')
            current_end_time_text = dt.datetime.strftime(current_end_time, '%H:%M:%S')
            current_time_range_text = f"{current_start_time_text}~{current_end_time_text}"
            print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}")

            query = self.query_template.format(start=current_start_timestamp, end=current_end_timestamp, service_code=self.service_code, operation=self.operation)
            for i in range(self.max_retries):
                try:
                    if i > 0:
                        print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: retying {i} time(s) ...")
                    response = requests.post(self.url, data=query, cookies=self.cookies)
                    if response.status_code == 200:
                        break
                    time.sleep(1)    
                except Exception as e:
                    if i == self.max_retries - 1:
                        print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: {e}")
                        break
                    else:
                        time.sleep(1)
                        continue
            
            if response.status_code == 200:
                response_data = json.loads(response.text)
                for data_item in response_data['data']:  
                    results.append(self.result_converter(data_item))
            else:
                print(f"{dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')} -> {log_title} {current_time_range_text}: {response}")
            time.sleep(0.005)

        return results

def __detail_result_convert(data_item):
    local_time =  dt.datetime.strptime(data_item['timestamp'], "%Y-%m-%d %H:%M:%S") + dt.timedelta(hours=8)
    return {
        'domain': data_item['domain'],
        'requestPath': data_item['requestPath'], 
        'referer': data_item['referer'], 
        'rootMessageId': data_item['rootMessageId'],
        'userAgent': data_item['userAgent'],
        'date_time': dt.datetime.strftime(local_time, "%Y-%m-%d %H:%M:%S"),
        'timestamp': int(local_time.timestamp()),
    }


def __count_result_convert(data_item):
    return {
        'timestamp': int(data_item['t']), 
        'value': int(data_item['value']),
        'date_time': dt.datetime.strftime(dt.datetime.fromtimestamp(int(data_item['t'])), "%Y-%m-%d %H:%M:%S"),
    }


