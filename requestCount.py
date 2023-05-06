import requests
import json
import csv
import datetime
from dotenv import load_dotenv
import os


load_dotenv()

# 定义请求的开始和结束时间
start_time = datetime.datetime(2023, 5, 1)
end_time = datetime.datetime(2023, 5, 7)
# party_type = "DMC"
party_type = "TDS"
# 定义请求的参数模板
query_template = """ SELECT count(1) as __count, count(1) as value, (intDiv(toUInt32(timestamp)*1000,30000)) as t 
    FROM log.weblog_all 
    WHERE (timestamp >= toDateTime({start}) AND timestamp <= toDateTime({end})) AND (`appid` = '100018832') 
        AND (cs_uri_stem like '%queryEventsByOrderIdWhenCreate%') AND (cs_referer like '%vbooking.ctrip.com%') 
        /* AND (cs_referer like '%{party_type}%') */ AND ((1=1)) 
    GROUP BY t ORDER BY t FORMAT JSON
"""
# 定义CSV文件头部
csv_header = ["Hour", "Count"]

# 定义CSV文件名
csv_filename = "minute_counts.csv"

# 定义请求URL
url = "http://es.ops.ctripcorp.com/clickhouse/?tableName=log.weblog_all"

# 定义Cookie
cookies = {
    "sessionid": os.getenv("ES_OPS_SESSIONID"),
    "PRO_cas_principal": os.getenv("ES_OPS_CAS_PRINCIPAL"),
}

# 发送POST请求并处理响应数据
minute_block_capicity = 10
minute_block_counts = {}
total_minute_block = int((end_time - start_time).total_seconds() / minute_block_capicity / 60)
for i in range(total_minute_block):
    start_minute = datetime.datetime.strftime(start_time + datetime.timedelta(minutes=i*minute_block_capicity), "%Y-%m-%d %H:%M:00")
    end_minute = datetime.datetime.strftime(start_time + datetime.timedelta(minutes=(i+1)*minute_block_capicity), "%Y-%m-%d %H:%M:00")
    print(f"{start_minute}~{end_minute}")
    start_unixtime = int((datetime.datetime.strptime(start_minute, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds())
    end_unixtime = int((datetime.datetime.strptime(end_minute, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds())
    query = query_template.format(start=start_unixtime, end=end_unixtime, party_type=party_type)
    response = requests.post(url, data=query, cookies=cookies)
    
    if response.status_code == 200:
        response_data = json.loads(response.text)
        if response_data['rows'] > 0:
            print(f"{response.text}\n")

        minute_block_count = sum(int(row['value']) for row in response_data['data'])
        minute_str = start_minute[:16]
        if minute_str in minute_block_counts:
            minute_block_counts[minute_str] += int(minute_block_count)
        else:
            minute_block_counts[minute_str] = int(minute_block_count)

# 写入CSV文件
with open(csv_filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(csv_header)
    for hour, count in minute_block_counts.items():
        writer.writerow([hour, count])
