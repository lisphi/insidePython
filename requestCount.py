import requests
import json
import csv
import datetime

# 定义请求的开始和结束时间
start_time = datetime.datetime(2023, 4, 1)
end_time = datetime.datetime(2023, 4, 16)
# party_type = "DMC"
party_type = "TDS"
# 定义请求的参数模板
query_template = """ SELECT count(1) as __count, count(1) as value, (intDiv(toUInt32(timestamp)*1000,30000)) as t 
    FROM log.weblog_all 
    WHERE (timestamp >= toDateTime({start}) AND timestamp <= toDateTime({end})) AND (`appid` = '100018832') 
        AND (cs_uri_stem like '%queryEventsByOrderIdWhenCreate%') AND (cs_referer like '%bst.ctrip.com%') 
        AND (cs_referer like '%{party_type}%') AND ((1=1)) 
    GROUP BY t ORDER BY t FORMAT JSON
"""
# 定义CSV文件头部
csv_header = ["Hour", "Count"]

# 定义CSV文件名
csv_filename = "hourly_count.csv"

# 定义请求URL
url = "http://es.ops.corp.com/clickhouse/?tableName=log.weblog_all"

# 定义Cookie
cookies = {
    "sessionid": "xxx",
    "PRO_cas_principal": "PRO-xxx"
}

# 发送POST请求并处理响应数据
hourly_counts = {}
total_hours = int((end_time - start_time).total_seconds() / 3600)
for i in range(total_hours):
    start_hour = datetime.datetime.strftime(start_time + datetime.timedelta(hours=i), "%Y-%m-%d %H:00:00")
    end_hour = datetime.datetime.strftime(start_time + datetime.timedelta(hours=i+1), "%Y-%m-%d %H:00:00")
    print(f"{start_hour}~{end_hour}")
    start_unixtime = int((datetime.datetime.strptime(start_hour, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds())
    end_unixtime = int((datetime.datetime.strptime(end_hour, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds())
    query = query_template.format(start=start_unixtime, end=end_unixtime, party_type=party_type)
    response = requests.post(url, data=query, cookies=cookies)
    print(f"{response.text}\n")
    
    if response.status_code == 200:
        response_data = json.loads(response.text)
        hour_count = sum(int(row['value']) for row in response_data['data'])
        hour_str = start_hour[:13]
        if hour_str in hourly_counts:
            hourly_counts[hour_str] += int(hour_count)
        else:
            hourly_counts[hour_str] = int(hour_count)

# 将统计结果写入CSV文件
with open(csv_filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(csv_header)
    for hour, count in hourly_counts.items():
        writer.writerow([hour, count])
