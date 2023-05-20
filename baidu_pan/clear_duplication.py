import pandas as pd
import requests
import json
from urllib.parse import quote
import time
import random


delete_url_format = 'https://pan.baidu.com/api/filemanager?async=2&onnest=fail&opera=delete&bdstoken={bdstoken}&newVerify=1&clienttype=0&app_id=250528&web=1&dp-logid={dp_logid}'

bdstoken = 'a0cb7f1c82aee2c2d6c2d5d4b0b6267b'
dp_logid = '50286600892695880134'
delete_url = delete_url_format.format(bdstoken=bdstoken, dp_logid=dp_logid)
cookies = { 
    'BDUSS': 'o1LWNCQVBtN2FCQWNmY1FwYjVEeWxWSkhwd0x6dUtzNEtqUjNYUWFsNlFhbDVoSVFBQUFBJCQAAAAAAAAAAAEAAAAQfMcAbGlzcGhpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJDdNmGQ3TZhNk' ,
    'STOKEN': '9a4bdd9b58b5933ec82e1b22769aa62cedbbb8f28e0733108642b2024789e6f2' }


df = pd.read_csv('baidu_pan/baidu_pan.csv')
df.columns = ['path', 'md5', 'fs_id']

dict = {}
duplication_rows = []
for index, row in df.iterrows():
    md5 = row['md5']
    if md5 in dict:
        row['duplication_path'] = dict[md5]
        duplication_rows.append(row)
    else:
        dict[md5] = row['path']

filelist_to_delete = []
for row in duplication_rows:
    print(f"{row['path']} --> {row['duplication_path']}")
    filelist_to_delete.append(quote(row['path']))
    if len(filelist_to_delete) >= 10:
        response = requests.post(delete_url, cookies=cookies, data={'filelist': json.dumps(filelist_to_delete)})
        print(response.text)
        filelist_to_delete = []
        time.sleep(random.uniform(0.5, 2.0))

if len(filelist_to_delete) > 0:
    response = requests.post(delete_url, cookies=cookies, data={'filelist': json.dumps(filelist_to_delete)})
    print(response.text)