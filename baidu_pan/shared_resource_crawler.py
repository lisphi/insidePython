import requests
import pandas as pd
from urllib.parse import quote
import json
import time
import random


class Crawler:
    def __init__(self, cookies, root_dir_id, page_size=100):
        self.result_file = 'baidu_pan/baidu_pan_shared.csv'
        self.url_format = 'https://pan.baidu.com/mbox/msg/shareinfo?from_uk=503999894&msg_id=7800613875593693886&type=2&num={page_size}&page={page_index}&fs_id={dir_id}&gid=340690313591134186&limit={page_size}&desc=1&clienttype=0&app_id=250528&web=1&dp-logid=80746100407486540069'
        self.cookies = cookies
        self.root_dir_id = root_dir_id
        self.page_size = page_size
        self.results = []

    def crawl(self):
        self.crawl_path(self.root_dir_id, 1)

    def crawl_path(self, dir_id, page_index):
        time.sleep(random.uniform(0.5, 2.0))
        url = self.url_format.format(dir_id=dir_id, page_size=self.page_size, page_index=page_index)
        response = requests.get(url, cookies=self.cookies)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            if json_data['errno'] == 0:
                records = json_data['records']
                dir_ids = []
                current_results = []
                for record in records:
                    if (record['isdir'] == 1):
                        dir_ids.append(record['fs_id'])
                    else:
                        print(record['path'])
                        current_results.append({ 'path': record['path'], 'fs_id': record['fs_id']})
                
                df = pd.DataFrame(current_results)
                df.to_csv(self.result_file, mode='a', header=False, index=False)
                
                self.results.append(current_results)

                if len(records) == self.page_size:
                    self.crawl_path(dir_id, page_index + 1)

                for dir_id in dir_ids:
                    self.crawl_path(dir_id, 1)

cookies = { 
    'BDUSS': 'JoeHl1bnpWSDJtSE1IVHdhdXNwT3lrSHVkSy1KdmtBblFqcng4RGI3MDlSWDFpRVFBQUFBJCQAAAAAAAAAAAEAAAAQfMcAbGlzcGhpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD24VWI9uFVibm' ,
    'STOKEN': 'a9367563b4ab183989cb873cf94e06a592b1dc4dd95d8c1aec674612dc42744c' }
root_dir_id = 855595139621673 # VIP会员资源1
crawler = Crawler(cookies, root_dir_id)
crawler.crawl()