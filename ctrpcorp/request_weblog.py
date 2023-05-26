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
    start_time = dt.datetime(2023, 5, 20)
    end_time = start_time + dt.timedelta(days=7)
    total_seconds = (end_time - start_time).total_seconds()
    days = (end_time - start_time).days
    block_count_per_day = 24

    futures_group = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=36) as executor:
        for d in range(days):
            futures = []
            for h in range(block_count_per_day):
                current_start_time = start_time + dt.timedelta(days = d, hours = h)  
                current_end_time = start_time + dt.timedelta(days = d, hours = h + 1)
                futures.append(executor.submit(count_worker, cookies, app_id, operation, current_start_time, current_end_time))
            futures_group.append(futures)
        
        futures = []
        second_step = (total_seconds - days * 24 * 3600) // block_count_per_day
        for i in range(block_count_per_day):
            current_start_time = start_time + dt.timedelta(days = days, seconds = i * second_step)
            current_end_time = start_time + dt.timedelta(days = days, seconds = (i + 1) * second_step)
            futures.append(executor.submit(count_worker, cookies, app_id, operation, current_start_time, current_end_time))
        futures_group.append(futures)

    for futures in futures_group:
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.extend(future.result())
        results.sort(key=lambda x: x['timestamp'])
        df = pd.DataFrame(results)
        file_name = f"ctrpcorp/{operation}_{dt.datetime.strftime(start_time, '%Y%m%d%H%M')}.csv"
        df.to_csv(file_name, mode='a', header=False, index=False)


if __name__ == '__main__':
    main()