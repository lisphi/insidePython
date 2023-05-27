import pandas as pd
import datetime as dt
from dotenv import load_dotenv
import os
import concurrent.futures
import weblog_crawler as wc

 
def count_worker(cookies, app_id, operation, start_time, end_time):
    crawler = wc.create_count_crawler(cookies, app_id, operation)
    return crawler.crawl(start_time, end_time)


def main():
    load_dotenv()
    cookies = {
        "sessionid": os.getenv("ES_OPS_SESSIONID"),
    }
    app_id = '100017817'
    operation = 'endHangEvent'
    start_time = dt.datetime(2023, 5, 21)
    end_time = start_time + dt.timedelta(days=7)
    total_seconds = int((end_time - start_time).total_seconds())
    step_seconds = 1800

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        for i in range(0, total_seconds, step_seconds):
            current_start_time = start_time + dt.timedelta(seconds = i)
            current_end_time = current_start_time + dt.timedelta(seconds = step_seconds)
            if (current_end_time > end_time):
                current_end_time = end_time
            futures.append(executor.submit(count_worker, cookies, app_id, operation, current_start_time, current_end_time))

    results = []
    for future in concurrent.futures.as_completed(futures):
        results.extend(future.result())
    results.sort(key=lambda x: x['timestamp'])
    df = pd.DataFrame(results)
    file_name = f"ctrpcorp/{app_id}_{operation}_{dt.datetime.strftime(start_time, '%Y%m%d%H%M')}_{int(dt.datetime.now().timestamp())}.csv"
    df.to_csv(file_name, mode='a', header=False, index=False)

if __name__ == '__main__':
    main()