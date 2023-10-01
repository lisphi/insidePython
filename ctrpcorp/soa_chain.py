import requests
import pandas as pd
import urllib
import time
import json
from dotenv import load_dotenv
import os
import datetime as dt
import re
import concurrent.futures


def get_operations_per_hour(app_id, date):
    reqdata = {
        "version": 1,
        "time-series-pattern": {
            "namespace": "ns-null",
            "metrics-name": "soa.service.request.count",
            "tag-search-part": {
                "appid": [
                    str(app_id)
                ]
            }
        },
        "aggregator": {
            "accept-linear-interpolation": True,
            "function": "sum"
        },
        "downsampler": {
            "interval": "1h",
            "function": "sum"
        },
        "max-datapoint-count": 100,
        "start-time": dt.datetime.strftime(date, '%Y-%m-%d %H:%M:%S'),
        "end-time": dt.datetime.strftime(date + dt.timedelta(hours=1), '%Y-%m-%d %H:%M:%S'),  
        "rate": False,
        "group-by": [
            "operation"
        ],
        "maxGroupCount": 100
    }
    userMsg = {
        "userName": "spli"
    }
    callback = 'jQuery1910414389029554032_1696167237353'
    ts = str(round(time.time() * 1000))

    params = {
        'reqdata': json.dumps(reqdata),
        'userMsg': json.dumps(userMsg),
        'callback': callback,
        '_': ts
    }
 
    url = f'http://engine.dashboard.fx.ctripcorp.com/jsonp/getgroupeddatapoints?{urllib.parse.urlencode(params)}'
    cookies = {
        'PRO_cas_principal': os.getenv('DASHBOARD_COOKIE__PRO_cas_principal')
    }

    print(f'getgroupeddatapoints({dt.datetime.strftime(date, "%Y-%m-%d %H:%M:%S")})')

    response = requests.get(url, cookies=cookies)
    if response.status_code == 200:
        matched = re.search(r'\((.*)\)', response.text, re.S)
        json_str = matched.group(1)
        try:
            data = json.loads(json_str)
            return [item['time-series-group']['operation'] for item in data['time-series-group-list']]
        except json.decoder.JSONDecodeError:
            print("Invalid JSON string:", json_str)
        
    return []

def get_operations_of_dates(app_id, date, days):
    get_operations_per_hour_futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for h in range(days * 24):
            get_operations_per_hour_futures.append(executor.submit(get_operations_per_hour, app_id, date + dt.timedelta(hours=h)))
            # operations = get_operations_per_hour(app_id, date + dt.timedelta(hours=h))

    operations = []
    for future in concurrent.futures.as_completed(get_operations_per_hour_futures):
        operations.extend(future.result())

    operation_set = set()
    for operation in operations:
        operation_set.add(operation)
    return sorted(list(operation_set))


def main():
    operations = get_operations_of_dates(100002548, dt.datetime(2023, 9, 1), 30)
    print(operations)

if __name__ == "__main__":
    main()