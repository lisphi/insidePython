import json
import datetime as dt
import pandas as pd


def convert_dispatch_log(log):
    return {
        'timestamp': log['timestamp'],
        'hostName': log['hostName'],
        'threadId': log['threadId'] }


def convert_distribute_order_event_log(log):
    msg = json.loads(log['message'])
    return { 
        'timestamp': log['timestamp'],
        'hostName': log['hostName'],
        'threadId': log['threadId'],
        'event_ids': msg['orderEventIDList'],
        'role_type': msg['roleType'] }


def find_distribute_order_event_log(timestamp, host_name, thread_id):
    for distribute_log in distribute_order_event_messages:
        delte_timestamp = distribute_log['timestamp'] - timestamp
        if (0 < delte_timestamp and delte_timestamp < 5000) and (distribute_log['hostName'] == host_name) and (distribute_log['threadId'] == thread_id):
            return distribute_log
    return None


with open('input/dispatchTourFRTEvent.json') as f:
    dispatch_tour_frt_event_messages = [ convert_dispatch_log(log) for log in json.load(f)]
    dispatch_tour_frt_event_messages.sort(key=lambda x: x['timestamp'])


with open('input/dispatchTourGroupEvent.json') as f:
    dispatch_tour_group_event_messages = [ convert_dispatch_log(log) for log in json.load(f)]
    dispatch_tour_group_event_messages.sort(key=lambda x: x['timestamp'])


with open('input/distributeOrderEvent.json') as f:
    distribute_order_event_messages = [ convert_distribute_order_event_log(log) for log in json.load(f)]
    distribute_order_event_messages.sort(key=lambda x: x['timestamp'])


merged_dispatch_tour_frt_event_messages = []
for dispatch_log in dispatch_tour_frt_event_messages:
    host_name = dispatch_log['hostName']
    thread_id = dispatch_log['threadId']
    timestamp = dispatch_log['timestamp']
    datetime = dt.datetime.strftime(dt.datetime.fromtimestamp(timestamp/1000), '%Y-%m-%d %H:%M:%S')
    distribute_timestamp = None
    distribute_event_ids = None
    distribute_role_type = None
    distribute_log = find_distribute_order_event_log(timestamp, host_name, thread_id)
    if distribute_log is not None:
        distribute_timestamp = distribute_log['timestamp']
        distribute_event_ids = distribute_log['event_ids']
        distribute_role_type = distribute_log['role_type']
    merged_dispatch_tour_frt_event_messages.append({
        'datetime': datetime,
        'timestamp': timestamp,
        'hostName': host_name,
        'threadId': thread_id,
        'distribute_timestamp': distribute_timestamp,
        'distribute_event_ids': distribute_event_ids,
        'distribute_role_type': distribute_role_type})
    

merged_dispatch_tour_group_event_messages = []
for dispatch_log in dispatch_tour_group_event_messages:
    host_name = dispatch_log['hostName']
    thread_id = dispatch_log['threadId']
    timestamp = dispatch_log['timestamp']
    datetime = dt.datetime.strftime(dt.datetime.fromtimestamp(timestamp/1000), '%Y-%m-%d %H:%M:%S')
    distribute_timestamp = None
    distribute_event_ids = None
    distribute_log = find_distribute_order_event_log(timestamp, host_name, thread_id)
    if distribute_log is not None:
        distribute_timestamp = distribute_log['timestamp']
        distribute_event_ids = distribute_log['event_ids']
        distribute_role_type = distribute_log['role_type']
    merged_dispatch_tour_group_event_messages.append({
        'datetime': datetime,
        'timestamp': timestamp,
        'hostName': host_name,
        'threadId': thread_id,
        'distribute_timestamp': distribute_timestamp,
        'distribute_event_ids': distribute_event_ids,
        'distribute_role_type': distribute_role_type})


frt_event_messages = []
for message in merged_dispatch_tour_frt_event_messages:
    if message.get('distribute_timestamp') is not None:
        frt_event_messages.append({
            'datetime': message['datetime'],
            'duration': message['distribute_timestamp'] - message['timestamp'],
            'event_ids': message['distribute_event_ids'],
            'role_type': message['distribute_role_type']})

frt_event_results = []
for i in range(len(frt_event_messages)):
    if i > 0:
        prev = frt_event_messages[i-1]
        curr = frt_event_messages[i]
        frt_event_results.append({
            'datetime': curr['datetime'],
            'duration': curr['duration'],
            'event_ids': curr['event_ids'],
            'distributing_datetime': prev['datetime'],
            'distributing_event_ids': prev['event_ids'],
            'distributed_event_ids': sorted([event_id for event_id in prev['event_ids'] if event_id not in curr['event_ids']]),
            'new_event_ids': sorted([event_id for event_id in curr['event_ids'] if event_id not in prev['event_ids']]),
        })

for i in range(len(frt_event_results)):
    print(f"{frt_event_results[i]['datetime']} - {len(frt_event_results[i]['event_ids'])}")


results = [result for result in frt_event_results if len(result['distributed_event_ids']) > 100]
# for result in results:
#     print(result['datetime'], f"new={len(result['new_event_ids'])}" )
#     print(result['distributing_datetime'], f"{len(result['distributed_event_ids'])}/{len(result['distributing_event_ids'])}" , result['distributed_event_ids'][0:5], result['distributed_event_ids'][-5:])

# print(results[1]['distributing_event_ids'])
# print(results[1]['distributed_event_ids'])