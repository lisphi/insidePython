import requests
import pandas as pd
from urllib.parse import quote
import json
import time
import random


class Crawler:
    def __init__(self, cookies, root_path, page_size=100):
        self.result_file = 'baidu_pan.csv'
        self.url_format = 'https://pan.baidu.com/api/list?clienttype=0&app_id=250528&web=1&dp-logid=50286600892695880039&order=time&desc=1&dir={path}&num={page_size}&page={page_index}'
        self.cookies = cookies
        self.root_path = root_path
        self.page_size = page_size
        self.results = []

    def crawl(self):
        self.crawl_path(self.root_path, 1)

    def crawl_path(self, path, page_index):
        time.sleep(random.uniform(0.5, 2.0))
        url = self.url_format.format(path=quote(path), page_size=self.page_size, page_index=page_index)
        response = requests.get(url, cookies=self.cookies)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            if json_data['errno'] == 0:
                list = json_data['list']
                dir_pathes = []
                current_results = []
                for item in list:
                    if (item['isdir'] == 1):
                        dir_pathes.append(item['path'])
                    else:
                        print(item['path'])
                        current_results.append({ 'path': item['path'], 'md5': item['md5'], 'fs_id': item['fs_id']})
                
                df = pd.DataFrame(current_results)
                df.to_csv(self.result_file, mode='a', header=False, index=False)
                
                self.results.append(current_results)

                if len(list) == self.page_size:
                    self.crawl_path(path, page_index + 1)

                for dir_path in dir_pathes:
                    self.crawl_path(dir_path, 1)

cookies = { 
    'BDUSS': 'o1LWNCQVBtN2FCQWNmY1FwYjVEeWxWSkhwd0x6dUtzNEtqUjNYUWFsNlFhbDVoSVFBQUFBJCQAAAAAAAAAAAEAAAAQfMcAbGlzcGhpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJDdNmGQ3TZhNk' ,
    'STOKEN': '9a4bdd9b58b5933ec82e1b22769aa62cedbbb8f28e0733108642b2024789e6f2' }
root_path = '/来自：Redmi K30 Pro'
crawler = Crawler(cookies, root_path)
crawler.crawl()


 
