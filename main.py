import urllib3
import random
import math
import os
import time
import pandas as pd
from bs4 import BeautifulSoup
import validators


class Crawler:
    method = 'GET'
    baseUrl = 'http://www.yonghui.com.cn/mapi/proajax'
    imageBaseUrl = 'http://www.yonghui.com.cn'
    imageBaseFolder = 'images'
    openedFolder = '已开业门店'
    buildingFolder = '筹建中门店'

    region = {
        '华东地区': 357882,
        '华中地区': 488127,
        '华南地区': 562876,
        '西南地区': 825132,
        '西北地区': 563631,
        '华北地区': 146826,
        '东北地区': 651318
    }

    regionProvinces = {
        '华东地区': {
            '福建': 778382,
            '安徽': 314345,
            '江苏': 868464,
            '浙江': 243845,
            '上海': 272752,
            '江西': 747631,
            '山东': 713862
        },
        '华中地区': {
            '河南': 825686,
            '湖南': 465672,
            '湖北': 715616
        },
        '华南地区': {
            '广东': 743852,
            '广西': 745781
        },
        '西南地区': {
            '重庆': 557868,
            '贵州': 263628,
            '四川': 731426,
            '云南': 468364
        },
        '西北地区': {
            '陕西': 865883,
            '宁夏': 722355,
        },
        '华北地区': {
            '北京': 613834,
            '天津': 587185,
            '河北': 583442,
            '山西': 213714,
        },
        '东北地区': {
            '黑龙江': 466858,
            '吉林': 476277,
            '辽宁': 135835
        }
    }

    openedColumns = [
        '地区',
        '省市',
        '店名',
        '门店地址',
        '联系电话',
        '开业日期',
        '公交路线',
    ]

    buildingColumns = [
        '地区',
        '省市',
        '店名',
        '门店地址',
    ]

    def __init__(self):
        self.http = urllib3.PoolManager()

    def request(self, act, id, page, keyword):
        randomNum = random.random() * 1000

        fields = {
            'act': act,
            'ctlgid': id,
            'keyword': keyword,
            'p': page,
            'rnd': randomNum
        }

        rawResult = self.http.request_encode_url(Crawler.method, Crawler.baseUrl, fields)
        result = rawResult.data.replace(b'<br>', b'').replace(b'\r', b'').decode('utf-8')
        arr = result.split('|')
        storeNum = int(arr[0])
        pageNum = math.ceil(storeNum / 5)
        rawData = arr[1]
        return rawData, pageNum

    def extractInfo(self, region, province, rawData, imageBaseUrl, columns):
        soup = BeautifulSoup(rawData, features='html.parser')
        storeXml = soup.find('li')
        stores = []
        while storeXml != None:
            imageSrc = storeXml.find('img').get('src')
            if not validators.url(imageSrc):
                imageSrc = imageBaseUrl + imageSrc

            title = storeXml.find('h1').text.strip()
            if len(columns) == 7:
                folder = Crawler.openedFolder
            elif len(columns) == 4:
                folder = Crawler.buildingFolder
            else:
                folder = ''
            imagePath = os.path.join(Crawler.imageBaseFolder, folder, title + '.' + imageSrc.rsplit('.', 1)[1])
            img = self.http.request('GET', imageSrc, preload_content=False)
            with open(imagePath, 'wb') as out_file:
                out_file.write(img.data)

            for child in storeXml.find('span').findAll():
                child.extract()
            info = storeXml.text.strip()
            store = {
                columns[0]: region,
                columns[1]: province,
                columns[2]: title,
            }
            for i in range(3, len(columns)):
                column = columns[i]
                key = column + '：'
                startIdx = info.find(key) + len(key)
                endIdx = info.find(columns[i + 1] if i < len(columns) - 1 else '<')
                value = info[startIdx: endIdx]
                store[column] = value

            stores.append(store)

            storeXml = storeXml.findNext('li')
        return stores

    def extractStores(self, act):
        keyword = ''
        stores = []
        if act == 2:
            columns = Crawler.openedColumns
        elif act == 3:
            columns = Crawler.buildingColumns
        else:
            columns = []

        for region in Crawler.regionProvinces:
            for province in Crawler.regionProvinces[region]:
                id = Crawler.regionProvinces[region][province]
                page = 0
                rawData, pageNum = self.request(act, id, page, keyword)

                stores.extend(self.extractInfo(region, province, rawData, Crawler.imageBaseUrl, columns))

                for p in range(1, pageNum):
                    rawData, _ = self.request(act, id, p, keyword)
                    stores.extend(self.extractInfo(region, province, rawData, Crawler.imageBaseUrl, columns))
        return stores

    def main(self):
        if not os.path.exists(Crawler.imageBaseFolder):
            os.mkdir(Crawler.imageBaseFolder)
        if not os.path.exists(os.path.join(Crawler.imageBaseFolder, Crawler.openedFolder)):
            os.mkdir(os.path.join(Crawler.imageBaseFolder, Crawler.openedFolder))
        if not os.path.exists(os.path.join(Crawler.imageBaseFolder, Crawler.buildingFolder)):
            os.mkdir(os.path.join(Crawler.imageBaseFolder, Crawler.buildingFolder))

        openedStores = self.extractStores(2)
        buildingStores = self.extractStores(3)

        df = pd.DataFrame(openedStores, columns=Crawler.openedColumns)
        df.to_excel('已开业门店.xlsx')
        df = pd.DataFrame(buildingStores, columns=Crawler.buildingColumns)
        df.to_excel('筹建中门店.xlsx')
        print('Extracted information from {} stores'.format(len(openedStores) + len(buildingStores)))


if __name__ == '__main__':
    crawler = Crawler()
    crawler.main()
    print('Time consumption: {} seconds'.format(time.process_time()))
