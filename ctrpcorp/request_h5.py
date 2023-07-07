import pandas as pd
import datetime as dt
from dotenv import load_dotenv
import os
import concurrent.futures
import h5_crawler as hc
 

def count_worker(cookies, service_code, operation, start_time, end_time):
    crawler = hc.create_count_crawler(cookies, service_code, operation)
    return crawler.crawl(start_time, end_time)


def request_count(cookies, service_code, operation, start_time, end_time, total_seconds, seconds_per_task):
    count_futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for i in range(0, total_seconds, seconds_per_task):
            current_start_time = start_time + dt.timedelta(seconds = i)
            current_end_time = current_start_time + dt.timedelta(seconds = seconds_per_task)
            if (current_end_time > end_time):
                current_end_time = end_time
            count_futures.append(executor.submit(count_worker, cookies, service_code, operation, current_start_time, current_end_time))

    count_results = []
    for future in concurrent.futures.as_completed(count_futures):
        count_results.extend(future.result())
    count_results.sort(key=lambda x: x['timestamp'])
    df = pd.DataFrame(count_results)
    count_excel = f"ctrpcorp/output/{operation}_{dt.datetime.strftime(dt.datetime.now(), '%m%d%H%M%S')}_count.csv"
    df.to_csv(count_excel, mode='a', header=True, index=False)
    return count_results  


def detail_worker(cookies, service_code, operation, minute_timestamps):
    crawler = hc.create_detail_crawler(cookies, service_code, operation)
    return crawler.crawl_minutes(minute_timestamps)


def request_detail(cookies, service_code, operation, minute_start_list):
    detail_futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for i in range(0, len(minute_start_list), 30):
            detail_futures.append(executor.submit(detail_worker, cookies, service_code, operation, minute_start_list[i:i+30]))

    detail_results = []
    for future in concurrent.futures.as_completed(detail_futures):
        detail_results.extend(future.result())
    detail_results.sort(key=lambda x: x['timestamp'])
    df = pd.DataFrame(detail_results)
    detail_excel = f"ctrpcorp/output/{operation}_{dt.datetime.strftime(dt.datetime.now(), '%m%d%H%M%S')}_detail.csv"
    df.to_csv(detail_excel, mode='a', header=True, index=False)

def load_minute_start_list(excel_file):
    df = pd.read_csv(excel_file)
    return df['timestamp'].astype(int).tolist()


def main():
    load_dotenv()
    cookies = {
        "sessionid": os.getenv("ES_COOKIE_SESSION_ID"),
    }
    service_code = '16566'
    operation = 'queryCreateSubEvent'
    start_time = dt.datetime(2023, 6, 15, 0, 0, 0)
    end_time = start_time + dt.timedelta(days=22)
    total_seconds = int((end_time - start_time).total_seconds())
    seconds_per_task = 60 * 60 # 六十分钟一个任务
    count_results = request_count(cookies, service_code, operation, start_time, end_time, total_seconds, seconds_per_task)
 
    # 从count结果中提取出所有的分钟开始时间，去重，排序
    minute_start_list = list(set([(count_result['timestamp'] // 60) * 60 for count_result in count_results]))
    minute_start_list.sort()

    # minute_start_list = load_minute_start_list('ctrpcorp/output/queryCreateSubEvent_0707170013_count.csv')
    # minute_start_list.sort()

    request_detail(cookies, service_code, operation, minute_start_list) 


if __name__ == '__main__':
    main()
