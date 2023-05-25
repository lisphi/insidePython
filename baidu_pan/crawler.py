import requests
import pandas as pd
from urllib.parse import quote
import json
import time
import random


class Crawler:
    def __init__(self, cookies, root_path, page_size=100):
        self.result_file = 'baidu_pan/baidu_pan.csv'
        self.url_format = 'https://pan.baidu.com/api/list?clienttype=0&app_id=250528&web=1&dp-logid=60000600932653500048&order=time&desc=1&dir={path}&num={page_size}&page={page_index}'
        self.cookies = cookies
        self.root_path = root_path
        self.page_size = page_size
        self.results = []
        self.max_retries = 3

    def crawl(self):
        self.crawl_path(self.root_path, 1)

    def crawl_path(self, path, page_index):
        time.sleep(random.uniform(0.5, 2.0))
        url = self.url_format.format(path=quote(path), page_size=self.page_size, page_index=page_index)

        for i in range(self.max_retries):
            try:
                response = requests.get(url, cookies=self.cookies)
                break
            except Exception as e:
                if i == self.max_retries - 1:
                    print(url)
                    raise e
                else:
                    time.sleep(2)
                    continue

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
    'BDUSS': 'JoeHl1bnpWSDJtSE1IVHdhdXNwT3lrSHVkSy1KdmtBblFqcng4RGI3MDlSWDFpRVFBQUFBJCQAAAAAAAAAAAEAAAAQfMcAbGlzcGhpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD24VWI9uFVibm' ,
    'STOKEN': 'a9367563b4ab183989cb873cf94e06a592b1dc4dd95d8c1aec674612dc42744c' }
root_path = '/00极客时间'
crawler = Crawler(cookies, root_path)
crawler.crawl()
