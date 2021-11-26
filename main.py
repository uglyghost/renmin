import requests						# 发起网络请求
from bs4 import BeautifulSoup		# 解析HTML文本
import pandas as pd					# 处理数据
import os
import time			# 处理时间戳
import json			# 用来解析json文本
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
mongodb = client.renmin

'''
用于发起网络请求
url : Request Url
kw  : Keyword
page: Page number
'''

def fetchUrl(url, kw, page):

    # 请求头 (人民网)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "Host": "search.people.cn",
        "Connection": "keep-alive",
        "Content-Length": "139",
        "sec-ch-ua": '"Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "Origin": "https://search.people.cn",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://search.people.cn/s?keyword=%E4%B8%AD%E5%8D%B0%E8%BE%B9%E7%95%8C&st=0&_=1637634865850",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": "__jsluid_h=165c465e454409f92e64a8f2cd1da20c; __jsluid_s=ac7811bc85181ae2f6bd3c72e63de4cb; sso_c=0; sfr=1"
    }

    # 请求参数 (人民网)
    payloads = {
        "endTime": 0,
        "hasContent": True,
        "hasTitle": True,
        "isFuzzy": True,
        "key": kw,
        "limit": 10,
        "page": page,
        "sortType": 2,
        "startTime": 0,
        "type": 0,
    }

    headers = {
        "Host": "newssearch.chinadaily.com.cn",
        "Proxy-Connection": "keep-alive",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "http://newssearch.chinadaily.com.cn/search",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9"
    }

    payloads = {
        "fullMust": kw,
        "sort": "dp",
        "duplication": "off",
        "page": page,
        "type": "",
        "channel": "",
        "source": ""
    }
    # 发起 get 请求
    r = requests.get(url, headers=headers, data=json.dumps(payloads))
    return r.json()


def parseJson(jsonObj):
    # 解析数据
    records = jsonObj["content"]

    #for item in records:

    #    pid = item["id"]
    #    originalName = item["originalName"]
    #    belongsName = item["belongsName"]
    #    content = BeautifulSoup(item["content"], "html.parser").text
    #    displayTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item["displayTime"] / 1000))
    #    subtitle = item["subtitle"]
    #    title = BeautifulSoup(item["title"], "html.parser").text
    #    url = item["url"]

    #    yield [[pid, title, subtitle, displayTime, originalName, belongsName, content, url]]

    return records

'''
用于将数据保存成 csv 格式的文件（以追加的模式）
path   : 保存的路径，若文件夹不存在，则自动创建
filename: 保存的文件名
data   : 保存的数据内容
'''
def saveFile(path, filename, data):
    # 如果路径不存在，就创建路径
    if not os.path.exists(path):
        os.makedirs(path)
    # 保存数据
    dataframe = pd.DataFrame(data)
    dataframe.to_csv(path + filename + ".csv", encoding='utf_8_sig', mode='a', index=False, sep=',', header=False )


if __name__ == "__main__":
    # 起始页，终止页，关键词设置
    kw = "China-India+boundary"
    # china-india border
    # Sino-Indian border
    # China-India+boundary

    # 爬取数据
    page = 1
    print("开始爬取数据: " + kw + "\n")
    while 1:
        url = "http://newssearch.chinadaily.com.cn/rest/search?fullMust=" + kw + "&sort=dp&duplication=off&page=" + str(page) + "&type=&channel=&source="
        html = fetchUrl(url, kw, page)
        page = page + 1
        for data in parseJson(html):
            query = {"id": data['id']}

            # 重复检查，看是否存在数据
            count = mongodb['rm_list'].count_documents(query)
            if count == 0:
                result = mongodb['rm_list'].insert_one(data)

        print("已经爬取到第: " + str(page) + "页\n")

        time.sleep(1)
