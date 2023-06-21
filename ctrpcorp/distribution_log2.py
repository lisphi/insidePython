import json
import datetime as dt

def convert_log(log):
    msg = json.loads(log['message'])
    return { 
        'timeStr': log['timeStr'],
        'event_ids': msg['orderEventIDList'],
        'event_size': len(msg['orderEventIDList']) }


with open('input/clog2.json') as f:
    messages = [ convert_log(log) for log in json.load(f)]
    for message in messages:
        print(f"{message['timeStr']} - {message['event_size']}")