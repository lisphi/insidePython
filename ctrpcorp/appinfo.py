import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os

# 定义app_id列表
app_ids = [
    100016728,
    100015749,
    100025952,
    100032985,
    100005311,
    100014955,
    100009398,
    100021699,
    100008601,
    100011125,
    100002134,
    100005056,
    100003258,
    100017765,
    100007129,
    100016428,
    100019571,
    100008237,
    100017870,
    100004984,
    100014970,
    100010976,
    100005249,
    100020201,
    100017034,
    100027743,
    100021740,
    100005835,
    100018746,
    100009134,
    100018557,
    100033078,
    100027563,
    100017817,
    100027625,
    100003671,
    100002548,
    100009600,
    100028992,
    100009085,
    100026451,
    100008207,
    100005929,
    100016024,
    100009705,
    100007382,
    100007343,
    100025630,
    100022635,
    100013526,
    100038793,
    100029804,
    100011437,
    100026734,
    100017914,
    100012025,
    100009191,
    100012569,
    100020001,
    100029776,
    100007650,
    100021096,
    100006021,
    100004679,
    100005232,
    100032933,
    100032082,
    100011255,
    100031958,
    100028750,
    100007714,
    100023901,
    100031004,
    100032434,
    100014315,
    100018832,
    100010318,
    100012713,
    100032373,
    100025527,
    100005431,
    100031612,
    100019138,
    100037798,
    100020650,
    100016554,
    100008755,
    100022200,
    100010226,
    100010963,
    100013930,
    100038909,
    100005233,
    100032456,
    100015128,
    100037796,
    100015445,
    100010262,
    100028549,
    100032332,
    100017529,
    100029051,
    100020959,
    100005869,
    100031303,
    100009022,
    100032700,
    100028419,
    100031075,
    100005564,
    100012825,
    100026821,
    100034682,
    100017842,
    100013961,
    100019725,
    100029870,
    100019880,
    100035154,
    100007965,
    100029702,
    100029747,
    100025342,
    100009648,
    100020250,
    100019269,
    100030410,
    100002591,
    100019927,
    100032259,
    100009597,
    100043920,
    100028993,
    100022756,
    100005680,
    100006749
    ]

def get_app_info(app_id):
    url = 'http://webinfo7.ops.ctripcorp.com/api/basicInfo'
    payload = {'id': str(app_id), 'type': 'application'}
    cookies = {
        'PRO_cas_principal': os.getenv("WEBINFO_COOKIE__PRO_cas_principal")
    }
    
    response = requests.post(url, cookies = cookies, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('result'):
            app_info = data['data'][0]
            return {
                'app_id': app_id,
                'app_name': app_info.get('app_name'),
                'app_chinese_name': app_info.get('app_chinese_name'),
                'app_owner': app_info.get('app_owner'),
            }
        print(data['message'], url)
    return None


def get_sin_aws_status(service_id, service_code):
    if service_id is None or service_code is None:
        return ''

    url = 'http://gov.soa.fx.ctripcorp.com/forward/sin-aws/api/service/get-service-instances'
    payload = { 'serviceId': service_id, 'serviceCode':service_code }
    cookies = {}
    headers = { 
        'X-Soa-Portal-Token': os.getenv('SOA_GOV_HEADER__X_Soa_Portal_Token')
    }

    response = requests.post(url, json=payload, cookies=cookies, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('status') is not None:
            print('POST', url, data.get('status'), data.get('message'))
            return ''
        status_set = set()
        for instance in data.get('logicalInstances'):
            status_set.add(instance['zoneId'])
        for instance in data.get('instances'):
            status_set.add(instance['zoneId'])
        return ','.join(status_set)
    return ''

def load_services():
    with open('input/get-services.json', 'r', encoding='utf-8') as get_services_file:
        res = json.load(get_services_file)
        services = {}
        for service in res['services']:
            for appId in service['appIds']:
                services[appId] = service
        return services

def main():
    load_dotenv()
    services = load_services()
    results = []
    for app_id in app_ids:
        print(app_id)
        app_info = get_app_info(app_id)
        service = services.get(str(app_id))
        sin_aws = ''
        service_key = ''
        if service is not None:
            service_key = service['serviceKey']
            service_code = service['serviceCode']
            sin_aws = get_sin_aws_status(service_key, service_code)
        results.append({
            "app_id": app_info.get("app_id"),
            "app_name": app_info.get("app_name"),
            "app_chinese_name": app_info.get("app_chinese_name"),
            "app_owner": app_info.get("app_owner"),
            "service_key": service_key,
            "sin_aws": sin_aws
        })
        
    df = pd.DataFrame(results)

    # 导出到Excel文件
    df.to_excel("ctrpcorp/output/app_info.xlsx", index=False)

if __name__ == "__main__":
    main()